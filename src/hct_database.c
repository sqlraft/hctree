/*
** 2020 October 13
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/

#include "hctInt.h"
#include "vdbeInt.h"
#include <string.h>
#include <assert.h>

typedef struct HctBuffer HctBuffer;
typedef struct HctDatabase HctDatabase;
typedef struct HctDbIndexEntry HctDbIndexEntry;
typedef struct HctDbIndexLeaf HctDbIndexLeaf;
typedef struct HctDbIndexNode HctDbIndexNode;
typedef struct HctDbIndexNodeEntry HctDbIndexNodeEntry;
typedef struct HctDbIndexNodeHdr HctDbIndexNodeHdr;
typedef struct HctDbIntkeyEntry HctDbIntkeyEntry;
typedef struct HctDbIntkeyLeaf HctDbIntkeyLeaf;
typedef struct HctDbIntkeyNodeEntry HctDbIntkeyNodeEntry;
typedef struct HctDbIntkeyNode HctDbIntkeyNode;
typedef struct HctDbLeaf HctDbLeaf;
typedef struct HctDbLeafHdr HctDbLeafHdr;
typedef struct HctDbWriter HctDbWriter;
typedef struct HctDbPageHdr HctDbPageHdr;

typedef struct HctCsrIntkeyOp HctCsrIntkeyOp;
typedef struct HctCsrIndexOp HctCsrIndexOp;

/* 
** Growable buffer type used for various things.
*/
struct HctBuffer {
  u8 *aBuf;
  int nBuf;
  int nAlloc;
};

struct HctCsrIntkeyOp {
  HctCsrIntkeyOp *pNextOp;
  i64 iFirst;
  i64 iLast;
  int nPg;
  u32 aPg[1];
};

struct HctCsrIndexOp {
  HctCsrIndexOp *pNextOp;
  u8 *pFirst;
  int nFirst;
  u8 *pLast;
  int nLast;
  int nPg;
  u32 aPg[1];
};

struct CsrIntkey {
  HctCsrIntkeyOp *pOpList;
  HctCsrIntkeyOp *pCurrentOp;
};
struct CsrIndex {
  HctCsrIndexOp *pOpList;
  HctCsrIndexOp *pCurrentOp;
};

/*
** iRoot:
**   Logical root page of tree structure that this cursor is open on.
**
** pKeyInfo:
**   NULL for cursors open on intkey trees, otherwise points to the 
**   KeyInfo used to compare keys in the open index tree. For cursors
**   opened by the user, this is set when the cursor is opened within
**   sqlite3HctDbCsrOpen() and never modified.
**
** pRec:
**   UnpackedRecord structure suitable for use with pKeyInfo. This is
**   allocated the first time it is required and then retained for
**   the lifetime of the HctDbCsr structure.
**
** eDir:
**   One of BTREE_DIR_NONE, BTREE_DIR_FORWARD or BTREE_DIR_REVERSE.
**
** pIntkeyOps:
*/ 
struct HctDbCsr {
  HctDatabase *pDb;               /* Database that owns this cursor */
  u32 iRoot;                      /* Root page cursor is opened on */
  KeyInfo *pKeyInfo;
  UnpackedRecord *pRec;
  int eDir;                       /* Direction cursor will step after Seek() */

  u8 *aRecord;                    /* Record in allocated memory */
  int nRecord;                    /* Size of aRecord[] in bytes */
  HctBuffer rec;

  struct CsrIntkey intkey;
  struct CsrIndex index;
  HctDbCsr *pNextScanner;

  int iCell;                      /* Current cell within page */
  HctFilePage pg;                 /* Current leaf page */

  int iOldCell;                   /* Cell within old page */
  HctFilePage oldpg;              /* Old page, if required */
};

/*
**
** iHeight:
**   The height of the list that this writer is writing to. 0 for leaves,
**   1 for the parents of leaves, etc.
**
** aWritePg/nWritePg:
**
** nWriteKey:
**   Number of hctDbInsert() calls since last flush - i.e. how many have to
**   be retried if we hit a CAS failure and have to redo this write operation.
**
** iWriteFpKey/aWriteFpKey:
**   These two variables store the fence-post key for the peer page of
**   the rightmost page in the aWritePg[] array - aWritePg[nWritePg-1].
**   For intkey tables, iWriteFpKey is the 64-bit integer key value. For
**   index tables, aWriteFpKey points to a buffer containing the FP key,
**   and iWriteFpKey its size in bytes. The buffer is allocated with
**   sqlite3_malloc().
**
**   If there is no peer page and writing to an intkey list, iWriteFpKey 
**   is set to LARGEST_INT64. If writing to an index list, aWriteFpKey is
**   set to NULL and iWriteFpKey to 0.
**
** aDiscard:
**   Each time a logical page is removed from the list by the current
**   write operation, an entry is added to the aDiscard[] array representing
**   the deleted page. At the conclusion of the write operation the 
**   corresponding entries will be removed from the parent list. The 
**   logical page number for entry x is in aDiscard[x].iPg and the FP key 
**   is the first key in the page at aDiscard[x].aOld.  
*/
#define HCTDB_MAX_DIRTY 8
struct HctDbWriter {
  int iHeight;                    /* Height to write at (0==leaves) */
  HctFilePage aWritePg[HCTDB_MAX_DIRTY+2];
  int nWritePg;                   /* Number of valid entries in aWritePg[] */
  int nWriteKey;                  /* Number of new keys in aWritePg[] array */
  i64 iWriteFpKey;
  u8 *aWriteFpKey;
  HctDbCsr writecsr;

  int iOldPgno;
  HctFilePage aDiscard[HCTDB_MAX_DIRTY+2];
  int nDiscard;

  int nEvictLocked;
  u32 iEvictLockedPgno;
};

/*
** pScannerList:
**   Linked list of cursors used by the current transaction. If this turns
**   out to be a write transaction, this list is used to detect read/write
**   conflicts.
*/
struct HctDatabase {
  HctFile *pFile;
  HctConfig *pConfig;
  i64 nCasFail;                   /* Number cas-collisions so far */
  int pgsz;                       /* Page size in bytes */
  u8 *aTmp;                       /* Temp buffer pgsz bytes in size */

  HctDbCsr *pScannerList;

  HctTMap *pTmap;                 /* Transaction map (non-NULL if trans open) */
  u64 iSnapshotId;                /* Snapshot id for reading */
  u64 iTid;                       /* Transaction id for writing */
  HctDbWriter pa;

  int bRollback;                  /* True when in rollback mode */
  int bValidate;                  /* True when in validate mode */
};

/* 
** 8-byte database page header. Described in fileformat.wiki.
*/
struct HctDbPageHdr {
  u8 hdrFlags;
  u8 nHeight;                     /* 0 for leaves, 1 for parents etc. */
  u16 nEntry;
  u32 iPeerPg;
};

/*
** Page types. These are the values that may appear in the page-type
** field of a page header.
*/
#define HCT_PAGETYPE_INTKEY      0x01
#define HCT_PAGETYPE_INTKEY_EDKS 0x02
#define HCT_PAGETYPE_INDEX       0x03
#define HCT_PAGETYPE_INDEX_EDKS  0x04
#define HCT_PAGETYPE_OVERFLOW    0x05
#define HCT_PAGETYPE_EDKS_FAN    0x06

#define HCT_PAGETYPE_MASK     0x07

/*
** Page types may be ORed with the following:
*/
#define HCT_PAGETYPE_LEFTMOST 0x80

#define hctPagetype(p)   (((HctDbPageHdr*)(p))->hdrFlags&HCT_PAGETYPE_MASK)
#define hctIsLeftmost(p) (((HctDbPageHdr*)(p))->hdrFlags&HCT_PAGETYPE_LEFTMOST)
#define hctPageheight(p)   (((HctDbPageHdr*)(p))->nHeight)

/*
** 24-byte leaf page header. Used by both index and intkey leaf pages.
** Described in fileformat.wiki.
*/
struct HctDbLeafHdr {
  u16 nFreeGap;                   /* Size of free-space region, in bytes */
  u16 nFreeBytes;                 /* Total free bytes on page */
  u16 nDelete;                    /* Number of delete keys on page */
  u16 nDeleteBytes;               /* Bytes in record area used by del keys */
  u64 iEdksVal;                   /* EDKS TID value, if any. 0 otherwise */
  u32 iEdksPg;                    /* Physical root of first EDKS tree, if any */
  u32 unused;
};

struct HctDbLeaf {
  HctDbPageHdr pg;
  HctDbLeafHdr hdr;
};


struct HctDbIntkeyEntry {
  u32 nSize;                      /* 8: Total size of data (local+overflow) */
  u16 iOff;                       /* 12: Offset of record within this page */
  u8 flags;                       /* 14: Flags (see below) */
  u8 unused;                      /* 15: */
  i64 iKey;                       /* 0: Integer key value */
};

struct HctDbIndexEntry {
  u32 nSize;                      /* 0: Total size of data (local+overflow) */
  u16 iOff;                       /* 4: Offset of record within this page */
  u8 flags;                       /* 6: Flags (see below) */
  u8 unused;                      /* 7: */
};

struct HctDbIndexNodeEntry {
  u32 nSize;
  u16 iOff;
  u8 flags;
  u8 unused;
  u32 iChildPg;
};


struct HctDbIntkeyNodeEntry {
  i64 iKey;                       /* Value of FP key on page iChild */
  u32 iChildPg;                   /* Child page */
  u32 unused;
};

struct HctDbIntkeyNode {
  HctDbPageHdr pg;
  HctDbIntkeyNodeEntry aEntry[0];
};



struct HctDbIntkeyLeaf {
  HctDbPageHdr pg;
  HctDbLeafHdr hdr;
  HctDbIntkeyEntry aEntry[0];
};

struct HctDbIndexLeaf {
  HctDbPageHdr pg;
  HctDbLeafHdr hdr;
  HctDbIndexEntry aEntry[0];
};

struct HctDbIndexNodeHdr {
  u16 nFreeGap;                   /* Size of free-space region, in bytes */
  u16 nFreeBytes;                 /* Total free bytes on page */
};

struct HctDbIndexNode {
  HctDbPageHdr pg;
  HctDbIndexNodeHdr hdr;
  HctDbIndexNodeEntry aEntry[0];
};


struct HctDbIntkeyEdksEntry {
  i64 iRowid;
  u64 iTid;
  u32 iOldPgno;
  u32 iChildPgno;
};

/*
** Flags for HctDbIntkeyEntry.flags
*/
#define HCTDB_HAS_TID   0x01
#define HCTDB_HAS_OLD   0x02
#define HCTDB_HAS_OVFL  0x04
#define HCTDB_IS_DELETE 0x08

static u64 hctDbTMapLookup(HctDatabase *pDb, u64 iTid, u64 *peState){
  u64 iVal = 0;
  HctTMap *pTmap = pDb->pTmap;
  if( iTid<pTmap->iFirstTid ){
    *peState = HCT_TMAP_COMMITTED;
  }else{
    int iMap = (iTid - pTmap->iFirstTid) / HCT_TMAP_PAGESIZE;

    if( iMap>=pTmap->nMap ){
      HctTMapClient *pTMapClient = sqlite3HctFileTMapClient(pDb->pFile);
      sqlite3HctTMapUpdate(pTMapClient, &pDb->pTmap);
      pTmap = pDb->pTmap;
      assert( iMap<pTmap->nMap );
    }
    if( iMap<pTmap->nMap ){
      int iOff = (iTid - pTmap->iFirstTid) % HCT_TMAP_PAGESIZE;
      iVal = AtomicLoad(&pTmap->aaMap[iMap][iOff]);
    }

    *peState = (iVal & HCT_TMAP_STATE_MASK);
  }
  return (iVal & HCT_TMAP_CID_MASK);
}

HctDatabase *sqlite3HctDbOpen(
  int *pRc,
  const char *zFile, 
  HctConfig *pConfig
){
  int rc = *pRc;
  HctDatabase *pNew = 0;

  pNew = (HctDatabase*)sqlite3HctMalloc(&rc, sizeof(*pNew));
  if( pNew ){
    pNew->pFile = sqlite3HctFileOpen(&rc, zFile, pConfig);
  }

  if( rc==SQLITE_OK ){
    pNew->pgsz = sqlite3HctFilePgsz(pNew->pFile);
    pNew->aTmp = (u8*)sqlite3HctMalloc(&rc, pNew->pgsz);
    pNew->pConfig = pConfig;
  }

  if( rc!=SQLITE_OK ){
    sqlite3HctDbClose(pNew);
    pNew = 0;
  }else{
    pNew->pgsz = sqlite3HctFilePgsz(pNew->pFile);
  }

  *pRc = rc;
  return pNew;
}

void sqlite3HctDbClose(HctDatabase *p){
  if( p ){
    assert( p->pa.aWriteFpKey==0 );
    sqlite3_free(p->aTmp);
    sqlite3HctFileClose(p->pFile);
    p->pFile = 0;
    sqlite3_free(p);
  }
}

HctFile *sqlite3HctDbFile(HctDatabase *pDb){
  return pDb->pFile;
}

int sqlite3HctDbRootNew(HctDatabase *p, u32 *piRoot){
  return sqlite3HctFileRootNew(p->pFile, piRoot);
}

int sqlite3HctDbRootFree(HctDatabase *p, u32 iRoot){
  return sqlite3HctFileRootFree(p->pFile, iRoot);
}

void sqlite3HctDbRootPageInit(
  int bIndex,                     /* True for an index, false for intkey */
  u8 *aPage,                      /* Buffer to initialize */
  int szPage                      /* Size of aPage[] in bytes */
){
  HctDbLeaf *pLeaf = (HctDbLeaf*)aPage;
  memset(aPage, 0, szPage);
  if( bIndex ){
    pLeaf->pg.hdrFlags = HCT_PAGETYPE_INDEX | HCT_PAGETYPE_LEFTMOST;
  }else{
    pLeaf->pg.hdrFlags = HCT_PAGETYPE_INTKEY | HCT_PAGETYPE_LEFTMOST;
  }
  pLeaf->hdr.nFreeBytes = szPage - sizeof(HctDbLeaf);
  pLeaf->hdr.nFreeGap = pLeaf->hdr.nFreeBytes;
}

static void hctDbRootPageInit(
  int bIndex,                     /* True for an index, false for intkey */
  int nHeight,                    /* Initial height */
  u32 iChildPg,                   /* Child page number */
  u8 *aPage,                      /* Buffer to initialize */
  int szPage                      /* Size of aPage[] in bytes */
){
  HctDbPageHdr *pPg = (HctDbPageHdr*)aPage;
  memset(aPage, 0, szPage);
  if( bIndex ){
    pPg->hdrFlags = HCT_PAGETYPE_INDEX | HCT_PAGETYPE_LEFTMOST;
  }else{
    pPg->hdrFlags = HCT_PAGETYPE_INTKEY | HCT_PAGETYPE_LEFTMOST;
  }
  if( nHeight>0 ){
    pPg->nHeight = nHeight;
    pPg->nEntry = 1;
    if( bIndex ){
      HctDbIndexNode *pNode = (HctDbIndexNode*)pPg;
      pNode->aEntry[0].iChildPg = iChildPg;
      pNode->hdr.nFreeBytes = 
        szPage - sizeof(HctDbIndexNode) - sizeof(HctDbIndexNodeEntry);
      pNode->hdr.nFreeGap = pNode->hdr.nFreeBytes;
    }else{
      HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)pPg;
      pNode->aEntry[0].iKey = SMALLEST_INT64;
      pNode->aEntry[0].iChildPg = iChildPg;
    }
  }else{
    HctDbLeaf *pLeaf = (HctDbLeaf*)pPg;
    pLeaf->hdr.nFreeBytes = szPage - sizeof(HctDbLeaf);
    pLeaf->hdr.nFreeGap = pLeaf->hdr.nFreeBytes;
  }
}

static void hctDbSnapshotOpen(HctDatabase *pDb){
  assert( (pDb->iSnapshotId==0)==(pDb->pTmap==0) );
  if( pDb->iSnapshotId==0 ){
    HctTMapClient *pTMapClient = sqlite3HctFileTMapClient(pDb->pFile);
    int rc = sqlite3HctTMapBegin(pTMapClient, &pDb->pTmap);
    assert( rc==SQLITE_OK );  /* todo */
    pDb->iSnapshotId = sqlite3HctFileGetSnapshotid(pDb->pFile);
    assert( pDb->iSnapshotId>0 );
  }
}

static u64 hctGetU64(const u8 *a){
  u64 ret;
  memcpy(&ret, a, sizeof(u64));
  return ret;
}
static u32 hctGetU32(const u8 *a){
  u32 ret;
  memcpy(&ret, a, sizeof(u32));
  return ret;
}

/*
** Return true if TID iTid maps to a commit-id visible to the current
** client. Or false otherwise.
*/
static int hctDbTidIsVisible(HctDatabase *pDb, u64 iTid){

  while( 1 ){
    u64 eState = 0;
    u64 iCid = hctDbTMapLookup(pDb, iTid, &eState);
    if( eState==HCT_TMAP_WRITING || eState==HCT_TMAP_ROLLBACK ) return 0;
    if( eState==HCT_TMAP_COMMITTED ){
      return (iCid <= pDb->iSnapshotId);
    }
  }

  return 0;
}

/*
** This is called when writing keys to the database as part of committing
** a transaction. One of the writes will clobber a key associated with
** transaction-id iTid. This function returns true if this represents
** a write/write conflict and the transaction should be rolled back, or
** false if the write should proceed.
*/
static int hctDbTidIsConflict(HctDatabase *pDb, u64 iTid){
  u64 eState = 0;
  u64 iCid = hctDbTMapLookup(pDb, iTid, &eState);
  if( eState==HCT_TMAP_WRITING || eState==HCT_TMAP_VALIDATING ) return 1;

  /* It's tempting to return 0 here - how can a key that has been rolled
  ** back be a conflict? The problem is that the previous version of the
  ** key - the one before this rolled back version - may be a write/write
  ** conflict. Ideally, this code would check that and return accordingly. */
  if( eState==HCT_TMAP_ROLLBACK ) return 1;

  assert( eState==HCT_TMAP_COMMITTED );
  return (iCid > pDb->iSnapshotId);
}


static int hctDbOffset(int iOff, int flags){
  return iOff 
    + ((flags & HCTDB_HAS_TID) ? 8 : 0)
    + ((flags & HCTDB_HAS_OLD) ? 4 : 0)
    + ((flags & HCTDB_HAS_OVFL) ? 4 : 0);
}

/*
** Load the meta-data record from the database and store it in buffer aBuf
** (size nBuf bytes). The meta-data record is stored with rowid=0 int the
** intkey table with root-page=2.
*/
int sqlite3HctDbGetMeta(HctDatabase *pDb, u8 *aBuf, int nBuf){
  HctFilePage pg;
  int rc;

  memset(aBuf, 0, nBuf);
  hctDbSnapshotOpen(pDb);
  rc = sqlite3HctFilePageGet(pDb->pFile, 2, &pg);
  while( rc==SQLITE_OK ){
    HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)pg.aOld;
    int iOff;
    u8 flags;

    if( pLeaf->pg.nEntry==0 ){
      break;
    }

    assert( pLeaf->pg.nEntry==1 );
    assert( pLeaf->aEntry[0].iKey==0 );
    assert( pLeaf->aEntry[0].nSize==nBuf );
    iOff = pLeaf->aEntry[0].iOff;
    flags = pLeaf->aEntry[0].flags;

    assert( flags==HCTDB_HAS_TID || flags==(HCTDB_HAS_OLD|HCTDB_HAS_TID) );
    if( (flags & HCTDB_HAS_OLD)
     && 0==hctDbTidIsVisible(pDb, hctGetU64(&pg.aOld[iOff])) 
    ){
      u32 iOld = hctGetU32(&pg.aOld[iOff+8]);
      if( iOld==0 ) break;
      sqlite3HctFilePageRelease(&pg);
      rc = sqlite3HctFilePageGetPhysical(pDb->pFile, iOld, &pg);
    }else{
      iOff = hctDbOffset(iOff, pLeaf->aEntry[0].flags );
      memcpy(aBuf, &pg.aOld[iOff], nBuf);
      sqlite3HctFilePageRelease(&pg);
      break;
    }
  }

  return rc;
}

int sqlite3HctDbRootInit(HctDatabase *p, int bIndex, u32 iRoot){
  HctFilePage pg;
  int rc;

  rc = sqlite3HctFilePageNew(p->pFile, iRoot, &pg);
  if( rc==SQLITE_OK ){
    sqlite3HctDbRootPageInit(bIndex, pg.aNew, p->pgsz);
    rc = sqlite3HctFilePageRelease(&pg);
  }
  return rc;
}

static i64 hctDbIntkeyFPKey(const void *aPg){
  if( ((HctDbPageHdr*)aPg)->nHeight==0 ){
    return ((HctDbIntkeyLeaf*)aPg)->aEntry[0].iKey;
  }
  return ((HctDbIntkeyNode*)aPg)->aEntry[0].iKey;
}

/*
** Buffer aPg contains an intkey leaf page.
**
** This function searches the leaf page for key iKey. If found, it returns
** the index of the matching key within the page and sets output variable
** (*pbExact) to 1. If there is no match for key iKey, this function returns
** the index of the smallest key on the page that is larger than iKey, or 
** (nEntry) if all keys on the page are smaller than iKey. (*pbExact) is 
** set to 0 before returning in this case.
*/
static int hctDbIntkeyLeafSearch(
  u8 *aPg, 
  i64 iKey,
  int *pbExact
){
  HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)aPg;
  int i1 = 0;
  int i2 = pLeaf->pg.nEntry;

  assert( pLeaf->pg.nHeight==0 );
  while( i2>i1 ){
    int iTest = (i1+i2)/2;
    i64 iPgKey = pLeaf->aEntry[iTest].iKey;
    if( iPgKey==iKey ){
      *pbExact = 1;
      return iTest;
    }else if( iPgKey<iKey ){
      i1 = iTest+1;
    }else{
      i2 = iTest;
    }
  }
  assert( i1==i2 );

  assert( i2>=0 );
  assert( i2==pLeaf->pg.nEntry || iKey<pLeaf->aEntry[i2].iKey );
  assert( i2==0 || iKey>pLeaf->aEntry[i2-1].iKey );

  *pbExact = 0;
  return i2;
}

static int hctDbIntkeyLocalsize(int pgsz, int nSize){
  int nMax = pgsz-sizeof(HctDbIntkeyLeaf)-sizeof(HctDbIntkeyEntry)-12;
  int nLocal;
  if( nSize<nMax ){
    nLocal = nSize;
  }else{
    int nMin = pgsz / 8;
    nLocal = nSize % (pgsz-sizeof(HctDbPageHdr));
    if( nLocal<nMin || nLocal> (nMax-sizeof(u32)) ){
      nLocal = nMin;
    }
  }

  return nLocal;
}

static int hctDbIndexLocalsize(int pgsz, int nSize){
  int nLocal;
  int nMax = pgsz/4;
  if( nSize<nMax ){
    nLocal = nSize;
  }else{
    int nMin = 64;
    nLocal = nSize % (pgsz-sizeof(HctDbPageHdr));
    if( nLocal<nMin || nLocal>nMax ){
      nLocal = nMin;
    }
  }
  return nLocal;
}

static int hctDbLocalsize(const u8 *aPg, int pgsz, int nSize){
  if( hctPagetype(aPg)==HCT_PAGETYPE_INTKEY ){
    return hctDbIntkeyLocalsize(pgsz, nSize);
  }
  return hctDbIndexLocalsize(pgsz, nSize);
}

static int hctDbIntkeyEntrySize(HctDbIntkeyEntry *pEntry, int pgsz){
  int sz = hctDbIntkeyLocalsize(pgsz, pEntry->nSize)
    + ((pEntry->flags & HCTDB_HAS_TID) ? 8 : 0)
    + ((pEntry->flags & HCTDB_HAS_OLD) ? 4 : 0)
    + ((pEntry->flags & HCTDB_HAS_OVFL) ? 4 : 0);
  return sz;
}

static int hctDbIndexEntrySize(HctDbIndexEntry *pEntry, int pgsz){
  int sz = hctDbIndexLocalsize(pgsz, pEntry->nSize) + 
    + ((pEntry->flags & HCTDB_HAS_TID) ? 8 : 0)
    + ((pEntry->flags & HCTDB_HAS_OLD) ? 4 : 0)
    + ((pEntry->flags & HCTDB_HAS_OVFL) ? 4 : 0);
  return sz;
}

static int hctDbIndexNodeEntrySize(HctDbIndexNodeEntry *pEntry, int pgsz){
  return hctDbIndexLocalsize(pgsz, pEntry->nSize)
    + ((pEntry->flags & HCTDB_HAS_OVFL) ? 4 : 0);
}

/*
** The pointer passed as the first argument is a pointer to a buffer
** containing a page that uses variable sized records. That is, an
** intkey leaf page, or an index leaf or node page. This function
** returns the number of bytes of record-area space consumed by
** entry iEntry on the page.
*/
static int hctDbPageRecordSize(void *aPg, int pgsz, int iEntry){
  int eType = hctPagetype(aPg);
  if( eType==HCT_PAGETYPE_INTKEY ){
    assert( hctPageheight(aPg)==0 );
    return hctDbIntkeyEntrySize(&((HctDbIntkeyLeaf*)aPg)->aEntry[iEntry], pgsz);
  }else if( hctPageheight(aPg)==0 ){
    return hctDbIndexEntrySize(&((HctDbIndexLeaf*)aPg)->aEntry[iEntry], pgsz);
  }
  return hctDbIndexNodeEntrySize(&((HctDbIndexNode*)aPg)->aEntry[iEntry], pgsz);
}
static int hctDbPageEntrySize(void *aPg){
  int eType = hctPagetype(aPg);
  if( eType==HCT_PAGETYPE_INTKEY ){
    assert( hctPageheight(aPg)==0 );
    return sizeof(HctDbIntkeyEntry);
  }else if( hctPageheight(aPg)==0 ){
    return sizeof(HctDbIndexEntry);
  }
  return sizeof(HctDbIndexNodeEntry);
}

/*
** The buffer passed as the first argument contains a page that is 
** guaranteed to be either an intkey leaf, or an index leaf or node.
** This function returns the offset of entry iEntry on the page and
** populates output variable *pFlags with the entry flags.
*/
static int hctDbEntryInfo(const void *aPg, int iEntry, int *pnSz, int *pFlags){
  int iOff;
  if( hctPagetype(aPg)==HCT_PAGETYPE_INTKEY ){
    HctDbIntkeyEntry *pEntry = &((HctDbIntkeyLeaf*)aPg)->aEntry[iEntry];
    iOff = pEntry->iOff;
    *pFlags = pEntry->flags;
    if( pnSz ) *pnSz = pEntry->nSize;
  }else if( hctPageheight(aPg)==0 ){
    HctDbIndexEntry *pEntry = &((HctDbIndexLeaf*)aPg)->aEntry[iEntry];
    iOff = pEntry->iOff;
    *pFlags = pEntry->flags;
    if( pnSz ) *pnSz = pEntry->nSize;
  }else{
    HctDbIndexNodeEntry *pEntry = &((HctDbIndexNode*)aPg)->aEntry[iEntry];
    iOff = pEntry->iOff;
    *pFlags = pEntry->flags;
    if( pnSz ) *pnSz = pEntry->nSize;
  }
  return iOff;
}

static int hctBufferGrow(HctBuffer *pBuf, int nSize){
  int rc = SQLITE_OK;
  if( nSize>pBuf->nAlloc ){
    u8 *aNew = sqlite3_realloc(pBuf->aBuf, nSize);
    if( aNew==0 ){
      rc = SQLITE_NOMEM_BKPT;
    }else{
      pBuf->aBuf = aNew;
      pBuf->nAlloc = nSize;
    }
  }
  return rc;
}
static void hctBufferFree(HctBuffer *pBuf){
  sqlite3_free(pBuf->aBuf);
  memset(pBuf, 0, sizeof(HctBuffer));
}

/*
** Argument aPg[] is a buffer containing either an index tree page, or an 
** intkey leaf page. This function locates the record associated with
** cell iCell on the page, and populates output variables *pnData and
** *paData with the size and a pointer to a buffer containing the record,
** respectively.
**
** If the record in cell iCell does not overflow the page, (*paData) is
** set to point into the body of the page itself. If the record does
** overflow the page, then buffer pBuf is used to store the record and
** (*paData) is set to point to the buffer's allocation. In this case
** it is the responsibility of the caller to eventually release the buffer.
**
** SQLITE_OK is returned if successful, or an SQLite error code otherwise.
*/
static int hctDbLoadRecord(
  HctDatabase *pDb,
  HctBuffer *pBuf,
  const u8 *aPg,
  int iCell,
  int *pnData,
  const u8 **paData
){
  int rc = SQLITE_OK;
  int iOff;
  int nSize;
  int flags;

  iOff = hctDbEntryInfo(aPg, iCell, &nSize, &flags);
  *pnData = nSize;
  if( paData ){
    if( flags & HCTDB_HAS_OVFL ){
      rc = hctBufferGrow(pBuf, nSize);
      *paData = pBuf->aBuf;
      if( rc==SQLITE_OK ){
        u32 pgOvfl;
        int nLocal = hctDbLocalsize(aPg, pDb->pgsz, nSize);

        iOff = hctDbOffset(iOff, flags);
        memcpy(pBuf->aBuf, &aPg[iOff], nLocal);
        pgOvfl = hctGetU32(&aPg[iOff-sizeof(u32)]);
        iOff = nLocal;

        while( rc==SQLITE_OK && iOff<nSize ){
          HctFilePage ovfl;
          rc = sqlite3HctFilePageGetPhysical(pDb->pFile, pgOvfl, &ovfl);
          if( rc==SQLITE_OK ){
            int nCopy = MIN(pDb->pgsz-8, nSize-iOff);
            memcpy(&pBuf->aBuf[iOff],&ovfl.aOld[sizeof(HctDbPageHdr)],nCopy);
            iOff += nCopy;
            pgOvfl = ((HctDbPageHdr*)ovfl.aOld)->iPeerPg;
            sqlite3HctFilePageRelease(&ovfl);
          }
        }
      }
    }else{
      iOff = hctDbOffset(iOff, flags);
      *paData = &aPg[iOff];
    }
  }

  return rc;
}

static int hctDbIndexSearch(
  HctDatabase *pDb,
  u8 *aPg, 
  UnpackedRecord *pRec,
  int *piPos,
  int *pbExact
){
  int rc = SQLITE_OK;
  HctBuffer buf;
  int i1 = 0;
  int i2 = ((HctDbPageHdr*)aPg)->nEntry;

  memset(&buf, 0, sizeof(buf));

  while( i2>i1 ){
    int iTest = (i1+i2)/2;
    int res;
    int nRec = 0;
    const u8 *aRec = 0;

    rc = hctDbLoadRecord(pDb, &buf, aPg, iTest, &nRec, &aRec);
    if( rc!=SQLITE_OK ) break;
    if( nRec==0 ){
      res = -1;
    }else{
      res = sqlite3VdbeRecordCompare(nRec, aRec, pRec);
    }

    if( res==0 ){
      *pbExact = 1;
      *piPos = iTest;
      hctBufferFree(&buf);
      return SQLITE_OK;
    }else if( res<0 ){
      i1 = iTest+1;
    }else{
      i2 = iTest;
    }
  }

  assert( i1==i2 && i2>=0 );
  hctBufferFree(&buf);
  *pbExact = 0;
  *piPos = i2;
  return rc;
}


/*
** The first argument is a pointer to an intkey internal node page.
**
** This function searches the node page for key iKey. If found, it returns
** the index of the matching key within the page and sets output variable
** (*pbExact) to 1. If there is no match for key iKey, this function returns
** the index of the smallest key on the page that is larger than iKey, or 
** (nEntry) if all keys on the page are smaller than iKey. (*pbExact) is 
** set to 0 before returning in this case.
*/
static int hctDbIntkeyNodeSearch(
  void *aPg,
  i64 iKey,
  int *pbExact
){
  HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)aPg;
  int i1 = 0;
  int i2 = pNode->pg.nEntry;

  assert( hctPagetype(pNode)==HCT_PAGETYPE_INTKEY && pNode->pg.nHeight>0 );
  while( i2>i1 ){
    int iTest = (i1+i2)/2;
    i64 iPgKey = pNode->aEntry[iTest].iKey;
    if( iPgKey==iKey ){
      *pbExact = 1;
      return iTest;
    }else if( iPgKey<iKey ){
      i1 = iTest+1;
    }else{
      i2 = iTest;
    }
  }
  assert( i1==i2 );

  assert( i2>=0 );
  assert( i2==pNode->pg.nEntry || iKey<pNode->aEntry[i2].iKey );
  assert( i2==0 || iKey>pNode->aEntry[i2-1].iKey );

  *pbExact = 0;
  return i2;
}

/*
** Set (*bGe) to true if (pRec >= (FP-key for aPg)).
*/
static int hctDbCompareFPKey(
  HctDatabase *pDb, 
  UnpackedRecord *pRec, 
  const u8 *aPg,
  int *pbGe
){
  const u8 *aFP = 0;
  int nFP = 0;
  int res;
  int rc;
  HctBuffer buf = {0,0,0};

  rc = hctDbLoadRecord(pDb, &buf, aPg, 0, &nFP, &aFP);
  if( rc==SQLITE_OK ){
    res = sqlite3VdbeRecordCompare(nFP, aFP, pRec);
    hctBufferFree(&buf);
    *pbGe = (res<=0);
  }
  return rc;
}

int hctDbCsrSeek(
  HctDbCsr *pCsr,                 /* Cursor to seek */
  int iHeight,                    /* Height to seek at (0==leaf, 1==parent) */
  UnpackedRecord *pRec,           /* Key for index/without rowid tables */
  i64 iKey,                       /* Key for intkey tables */
  int *pbExact
){
  HctFile *pFile = pCsr->pDb->pFile;
  u32 iPg = pCsr->iRoot;
  int rc = SQLITE_OK;

  while( rc==SQLITE_OK ){
    if( iPg ) rc = sqlite3HctFilePageGet(pFile, iPg, &pCsr->pg);
    if( rc==SQLITE_OK ){
      HctDbPageHdr *pHdr = (HctDbPageHdr*)pCsr->pg.aOld;
      int i2 = pHdr->nEntry-1;
      int bExact;
      if( pHdr->nHeight==0 ){
        if( pRec ){
          rc = hctDbIndexSearch(pCsr->pDb, pCsr->pg.aOld, pRec, &i2, &bExact);
        }else{
          i2 = hctDbIntkeyLeafSearch(pCsr->pg.aOld, iKey, &bExact);
        }
        if( bExact==0 ) i2--;
      }else if( pRec ){
        HctDbIndexNode *pNode = (HctDbIndexNode*)pCsr->pg.aOld;
        rc = hctDbIndexSearch(pCsr->pDb, pCsr->pg.aOld, pRec, &i2, &bExact);
        i2 -= !bExact;
        iPg = pNode->aEntry[i2].iChildPg;
        assert( iPg );
      }else{
        HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)pCsr->pg.aOld;
        i2 = hctDbIntkeyNodeSearch(pNode, iKey, &bExact);
        assert( i2==pHdr->nEntry || iKey<=pNode->aEntry[i2].iKey );
        assert( i2==pHdr->nEntry || bExact==(iKey==pNode->aEntry[i2].iKey) );
        assert( i2<pHdr->nEntry || bExact==0 );
        i2 -= !bExact;
        iPg = pNode->aEntry[i2].iChildPg;
        assert( iPg );
      }

      /* Test if it is necessary to skip to the peer node. */
      if( i2>=0 && i2==pHdr->nEntry-1 && pHdr->iPeerPg!=0 ){
        HctFilePage peer;
        rc = sqlite3HctFilePageGet(pFile, pHdr->iPeerPg, &peer);
        if( rc==SQLITE_OK ){
          int bGotoPeer;
          if( pRec ){
            rc = hctDbCompareFPKey(pCsr->pDb, pRec, peer.aOld, &bGotoPeer);
          }else{
            i64 iFP = hctDbIntkeyFPKey(peer.aOld);
            bGotoPeer = (iFP<=iKey);
          }
          if( bGotoPeer ){
            SWAP(HctFilePage, pCsr->pg, peer);
            sqlite3HctFilePageRelease(&peer);
            assert( pCsr->pg.aOld );
            iPg = 0;
            continue;
          }
          sqlite3HctFilePageRelease(&peer);
        }
      }

      if( pHdr->nHeight==iHeight ){
        pCsr->iCell = i2;
        if( pbExact ) *pbExact = bExact;
        break;
      }

      sqlite3HctFilePageRelease(&pCsr->pg);
      assert( rc!=SQLITE_OK || iPg!=0 );
    }
  }

  return rc;
}

void sqlite3HctDbCsrDir(HctDbCsr *pCsr, int eDir){
  pCsr->eDir = eDir;
}

static int hctDbFindKeyInPage(HctDbIntkeyLeaf *pPg, i64 iKey){
  int i1 = 0;
  int i2 = pPg->pg.nEntry;

  while( i1<=i2 ){
    int iTest = (i1+i2)/2;
    i64 iPgKey = pPg->aEntry[iTest].iKey;
    if( iPgKey==iKey ){
      return iTest;
    }else if( iPgKey<iKey ){
      i1 = iTest+1;
    }else{
      i2 = iTest-1;
    }
  }

  return -1;
}

static int hctDbCellOffset(const u8 *aPage, int iCell, u8 *pFlags){
  HctDbPageHdr *pHdr = (HctDbPageHdr*)aPage;
  int iRet;
  if( hctPagetype(pHdr)==HCT_PAGETYPE_INTKEY ){
    HctDbIntkeyEntry *pEntry = &((HctDbIntkeyLeaf*)pHdr)->aEntry[iCell];
    *pFlags = pEntry->flags;
    iRet = pEntry->iOff;
  }else{
    HctDbIndexEntry *pEntry = &((HctDbIndexLeaf*)pHdr)->aEntry[iCell];
    *pFlags = pEntry->flags;
    iRet = pEntry->iOff;
  }
  return iRet;
}

/*
** If the cursor is open on an index tree, ensure that the UnpackedRecord
** structure is allocated. Return SQLITE_NOMEM if an OOM is encountered
** while attempting to allocate said structure, or SQLITE_OK otherwise.
*/
static int hctDbAllocateUnpacked(HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  if( pCsr->pKeyInfo && pCsr->pRec==0 ){
    pCsr->pRec = sqlite3VdbeAllocUnpackedRecord(pCsr->pKeyInfo);
    if( pCsr->pRec==0 ){
      rc = SQLITE_NOMEM_BKPT;
    }
  }
  return rc;
}

/*
** Find the cell in page HctDbCsr.oldpg with a key identical to the current
** cell in HctDbCsr.pg. Set HctDbCsr.iOldCell to the cell index.
**
** Return SQLITE_OK if successful, or SQLITE_CORRUPT if the required key
** cannot be found in HctDbCsr.oldpg.
*/
static int hctDbFindKeyInOldPage(HctDbCsr *pCsr){
  int iCell;
  if( pCsr->pKeyInfo ){
    HctDbIndexLeaf *pLeaf = (HctDbIndexLeaf*)pCsr->pg.aOld;
    HctDbIndexEntry *p = &pLeaf->aEntry[pCsr->iCell];

    int rc;
    int bExact;
    u8 *aCell = &pCsr->pg.aOld[hctDbOffset(p->iOff, p->flags)];

    rc = hctDbAllocateUnpacked(pCsr);
    if( rc==SQLITE_OK ){
      sqlite3VdbeRecordUnpack(pCsr->pKeyInfo, p->nSize, aCell, pCsr->pRec);
      rc = hctDbIndexSearch(pCsr->pDb, pCsr->oldpg.aOld, pCsr->pRec, &iCell, &bExact);
    }
    if( rc ) return rc;
    if( bExact==0 ) iCell = -1;
  }else{
    i64 iKey = ((HctDbIntkeyLeaf*)pCsr->pg.aOld)->aEntry[pCsr->iCell].iKey;
    iCell = hctDbFindKeyInPage((HctDbIntkeyLeaf*)pCsr->oldpg.aOld, iKey);
  }

  pCsr->iOldCell = iCell;
  return (iCell<0 ? SQLITE_CORRUPT_BKPT : SQLITE_OK);
}

/*
** This function attempts to find a version of the current key for
** cursor pCsr that is visible within the current snapshot. It returns
** true if successful, or if the cursor points to EOF. If there is
** no version of the current key visible to the current transaction,
** false is returned. If the visible version of the key is stored
** on a page other than the current page being traversed by the
** cursor, HctDbCsr.iOldCell and HctDbCsr.oldpg are populated
** accordingly before returning.
**
** This function is a no-op if *pRc is set to something other than
** SQLITE_OK when it is called. Otherwise, if an error occurs, *pRc
** is set to contain an SQLite error code and true is returned.
*/
static int hctDbCsrFindVersion(int *pRc, HctDbCsr *pCsr){
  int rc = *pRc;
  HctFile *pFile = pCsr->pDb->pFile;
  u32 iOld = 0;

  sqlite3HctFilePageRelease(&pCsr->oldpg);
  if( rc!=SQLITE_OK ) return 1;
  if( pCsr->iCell<0 ) return 1;    /* EOF */

  {
    u8 *aPage = pCsr->pg.aOld;
    u8 flags = 0;
    int iOff = hctDbCellOffset(aPage, pCsr->iCell, &flags);

    /* Check if the current entry is visible to the client. It is visible
    ** if either (a) there is no TID value, or (b) the TID value corresponds
    ** to a visible CID. If the current entry is visible, return 0 if it is
    ** a delete-key or 1 if it is a real entry.  */
    if( (flags & HCTDB_HAS_TID)==0                              /* (a) */
     || hctDbTidIsVisible(pCsr->pDb, hctGetU64(&aPage[iOff]))   /* (b) */
    ){
      return ((flags & HCTDB_IS_DELETE) ? 0 : 1);
    }

    /* Current entry is not visible to the client. Check if it has an 
    ** old-page pointer. If so, code below will search that page for a visible
    ** version of the key. Otherwise, return 0 to indicate that no visible
    ** entry with the current key could be found.  */
    if( flags & HCTDB_HAS_OLD ){
      iOld = hctGetU32(&aPage[iOff+8]);
    }else{
      return 0;
    }
  }

  while( rc==SQLITE_OK ){
    sqlite3HctFilePageRelease(&pCsr->oldpg);
    rc = sqlite3HctFilePageGetPhysical(pFile, iOld, &pCsr->oldpg);
    if( rc==SQLITE_OK ){
      rc = hctDbFindKeyInOldPage(pCsr);
    }
    if( rc==SQLITE_OK ){
      u8 *aPage = pCsr->oldpg.aOld;
      u8 flags = 0;
      int iOff = hctDbCellOffset(aPage, pCsr->iOldCell, &flags);

      if( (flags & HCTDB_HAS_TID)==0 
       || hctDbTidIsVisible(pCsr->pDb, hctGetU64(&aPage[iOff]))
      ){
        return ((flags & HCTDB_IS_DELETE) ? 0 : 1);
      }

      if( (flags & HCTDB_HAS_OLD)==0 ) return 0;
      iOld = hctGetU32(&aPage[iOff+8]);
    }
  }

  *pRc = rc;
  return 1;
}

static void hctDbCsrReset(HctDbCsr *pCsr){
  sqlite3HctFilePageRelease(&pCsr->pg);
  sqlite3HctFilePageRelease(&pCsr->oldpg);
  pCsr->iCell = -1;
  /* pCsr->eDir = 0; */
}

static int hctDbCsrScanStart(HctDbCsr *pCsr, UnpackedRecord *pRec, i64 iKey){
  int rc = SQLITE_OK;

  if( pCsr->pDb->iTid==0 ){
    if( pCsr->pKeyInfo==0 ){
      HctCsrIntkeyOp *pOp = 0;
      pOp = sqlite3MallocZero(sizeof(HctCsrIntkeyOp));
      if( pOp==0 ){
        rc = SQLITE_NOMEM_BKPT;
      }else{
        assert( pCsr->intkey.pCurrentOp==0 );
        pOp->iFirst = pOp->iLast = iKey;
        pCsr->intkey.pCurrentOp = pOp;
      }
    }else{
      HctCsrIndexOp *pOp = 0;
      pOp = sqlite3MallocZero(sizeof(HctCsrIndexOp));
      if( pOp==0 ){
        rc = SQLITE_NOMEM_BKPT;
      }else{
        if( pRec ){
          rc = sqlite3HctSerializeRecord(pRec, &pOp->pFirst, &pOp->nFirst);
          pOp->pLast = pOp->pFirst;
          pOp->nLast = pOp->nFirst;
        }
        assert( pCsr->index.pCurrentOp==0 );
        pCsr->index.pCurrentOp = pOp;
      }
    }
  }

  return rc;
}

static int hctDbCsrScanVisit(HctDbCsr *pCsr, u32 iLogical, u32 iPhysical){
  return SQLITE_OK;
}

static int hctDbCsrScanFinish(HctDbCsr *pCsr){
  int rc = SQLITE_OK;

  if( pCsr->pKeyInfo==0 ){
    HctCsrIntkeyOp *pOp = pCsr->intkey.pCurrentOp;
    pCsr->intkey.pCurrentOp = 0;
    if( pOp ){
      HctCsrIntkeyOp *pPrev = pCsr->intkey.pOpList;

      if( pCsr->eDir!=BTREE_DIR_NONE ){
        i64 iVal = 0; 
        if( sqlite3HctDbCsrEof(pCsr) ){
          if( pCsr->eDir==BTREE_DIR_FORWARD ){
            iVal = LARGEST_INT64;
          }else{
            iVal = SMALLEST_INT64;
          }
        }else{
          sqlite3HctDbCsrKey(pCsr, &iVal);
        }

        if( iVal>=pOp->iFirst ){
          pOp->iLast = iVal;
        }else{
          pOp->iLast = pOp->iFirst;
          pOp->iFirst = iVal;
        }
      }

      if( pPrev && pOp->iLast<=pPrev->iLast && pOp->iFirst>=pPrev->iFirst ){
        sqlite3_free(pOp);
      }else{
        pOp->pNextOp = pPrev;
        pCsr->intkey.pOpList = pOp;
      }
    }
  }else{
    HctCsrIndexOp *pOp = pCsr->index.pCurrentOp;
    pCsr->index.pCurrentOp = 0;
    if( pOp ){
      if( pCsr->eDir!=BTREE_DIR_NONE ){
        int nKey = 0;
        u8 *aCopy = 0;
        if( !sqlite3HctDbCsrEof(pCsr) ){
          const u8 *aKey = 0;
          rc = sqlite3HctDbCsrData(pCsr, &nKey, &aKey);
          if( rc==SQLITE_OK ){
            aCopy = sqlite3_malloc(nKey);
            if( aCopy==0 ){
              rc = SQLITE_NOMEM_BKPT;
            }else{
              memcpy(aCopy, aKey, nKey);
            }
          }
        }

        if( pCsr->eDir==BTREE_DIR_FORWARD ){
          pOp->pLast = aCopy;
          pOp->nLast = nKey;
        }else{
          pOp->pFirst = aCopy;
          pOp->nFirst = nKey;
        }
      }

      pOp->pNextOp = pCsr->index.pOpList;
      pCsr->index.pOpList = pOp;
    }
  }

  return rc;
}

static int hctDbCsrFirst(HctDbCsr *pCsr){
  int rc = SQLITE_OK;

  /* Starting at the root of the tree structure, follow the left-most 
  ** pointers to find the left-most node in the list of leaves. */
  u32 iPg = pCsr->iRoot;
  HctFile *pFile = pCsr->pDb->pFile;
  HctFilePage pg;
  while( 1 ){
    HctDbPageHdr *pPg;
    rc = sqlite3HctFilePageGet(pFile, iPg, &pg);
    if( rc!=SQLITE_OK ) break;
    pPg = (HctDbPageHdr*)pg.aOld;
    if( pPg->nHeight==0 ){
      break;
    }else if( hctPagetype(pPg)==HCT_PAGETYPE_INTKEY ){
      iPg = ((HctDbIntkeyNode*)pPg)->aEntry[0].iChildPg;
    }else{
      iPg = ((HctDbIndexNode*)pPg)->aEntry[0].iChildPg;
    }
    sqlite3HctFilePageRelease(&pg);
  }
  memcpy(&pCsr->pg, &pg, sizeof(pg));
  if( ((HctDbPageHdr*)pCsr->pg.aOld)->nEntry>0 ){
    pCsr->iCell = 0;
  }else{
    pCsr->iCell = -1;
  }
  return rc;
}

static int hctDbCsrFirstValid(HctDbCsr *pCsr){
  int rc = SQLITE_OK;

  rc = hctDbCsrFirst(pCsr);

  /* Skip forward to the first visible entry, if any. */
  if( rc==SQLITE_OK ){
    pCsr->iCell = -1;
    rc = sqlite3HctDbCsrNext(pCsr);
  }

  return rc;
}


/*
** An integer is written into *pRes which is the result of
** comparing the key with the entry to which the cursor is 
** pointing.  The meaning of the integer written into
** *pRes is as follows:
**
**     *pRes<0      The cursor is left pointing at an entry that
**                  is smaller than iKey/pRec or if the table is empty
**                  and the cursor is therefore left point to nothing.
**
**     *pRes==0     The cursor is left pointing at an entry that
**                  exactly matches iKey/pRec.
**
**     *pRes>0      The cursor is left pointing at an entry that
**                  is larger than iKey/pRec.
*/
int sqlite3HctDbCsrSeek(
  HctDbCsr *pCsr,                 /* Cursor to seek */
  UnpackedRecord *pRec,           /* Key for index tables */
  i64 iKey,                       /* Key for intkey tables */
  int *pRes                       /* Result of seek (see above) */
){
  int rc = SQLITE_OK;
  int bExact;

  rc = hctDbCsrScanFinish(pCsr);
  if( rc==SQLITE_OK ){
    hctDbCsrReset(pCsr);
    rc = hctDbCsrScanStart(pCsr, pRec, iKey);
  }

  if( rc==SQLITE_OK ){
    rc = hctDbCsrSeek(pCsr, 0, pRec, iKey, &bExact);
  }

  /* The main cursor now points to the largest entry less than or equal 
  ** to the supplied key (pRec or iKey). If the supplied key is smaller 
  ** than all entries in the table, then pCsr->iCell is set to -1.  */
  if( rc==SQLITE_OK ){

    if( pCsr->iCell<0 ){
      /* If the cursor is BTREE_DIR_REVERSE or NONE, then leave it as it is
      ** at EOF. Otherwise, if the cursor is BTREE_DIR_FORWARD, attempt
      ** to move it to the first valid entry. */
      if( pCsr->eDir==BTREE_DIR_FORWARD ){
        rc = hctDbCsrFirstValid(pCsr);
        *pRes = sqlite3HctDbCsrEof(pCsr) ? -1 : +1;
      }else{
        *pRes = -1;
      }
    }else{
      if( rc==SQLITE_OK && 0==hctDbCsrFindVersion(&rc, pCsr) ){
        switch( pCsr->eDir ){
          case BTREE_DIR_FORWARD:
            *pRes = 1;
            rc = sqlite3HctDbCsrNext(pCsr);
            break;
          case BTREE_DIR_REVERSE:
            rc = sqlite3HctDbCsrPrev(pCsr);
            break;
          default: assert( pCsr->eDir==BTREE_DIR_NONE );
            hctDbCsrReset(pCsr);
            *pRes = -1;
            break;
        }
      }else{
        *pRes = (bExact ? 0 : -1); 
      }
    }
  }

  return rc;
}

int sqlite3HctDbIsIndex(HctDatabase *pDb, u32 iRoot, int *pbIndex){
  HctFilePage pg;
  int rc = sqlite3HctFilePageGet(pDb->pFile, iRoot, &pg);
  if( rc==SQLITE_OK ){
    *pbIndex = (hctPagetype(pg.aOld)==HCT_PAGETYPE_INDEX);
    sqlite3HctFilePageRelease(&pg);
  }
  return rc;
}

char *sqlite3HctDbLogFile(HctDatabase *pDb){
  return sqlite3HctFileLogFile(pDb->pFile);
}

static void hctDbCsrInit(HctDatabase *pDb, u32 iRoot, HctDbCsr *pCsr){
  memset(pCsr, 0, sizeof(HctDbCsr));
  pCsr->pDb = pDb;
  pCsr->iRoot = iRoot;
}

static int hctDbCellPut(
  u8 *aBuf, 
  u64 iTid, 
  u32 iOld, 
  u32 pgOvfl,
  const u8 *aData, 
  int nData
){
  int iOff = 0;
  if( iTid ){
    memcpy(&aBuf[iOff], &iTid, sizeof(u64));
    iOff += sizeof(u64);
  }
  if( iOld ){
    memcpy(&aBuf[iOff], &iOld, sizeof(u32));
    iOff += sizeof(u32);
  }
  if( pgOvfl ){
    memcpy(&aBuf[iOff], &pgOvfl, sizeof(u32));
    iOff += sizeof(u32);
  }
  memcpy(&aBuf[iOff], aData, nData);
  return iOff+nData;
}

/*
** Return the size of the local part of a nData byte record stored on
** an intkey leaf page.
*/
#if 0
static int hctDbLocalSize(HctDatabase *pDb, int nData){
  int nOther = sizeof(HctDbIntkeyLeaf) + sizeof(HctDbIntkeyEntry) + 12;
  if( nData<=(pDb->pgsz-nOther) ){
    return nData;
  }
  assert( !"todo" );
  return 0;
}
#endif

#if 0
static i64 hctDbIntkeyGetKey(u8 *aPg, int ii){
  HctDbIntkeyLeaf *p = (HctDbIntkeyLeaf*)aPg;
  return p->aEntry[ii].iKey;
}
#endif



/*
** Return the maximum number of entries that fit on an intkey internal
** node if the database page size is as specified by the only parameter.
*/
static int hctDbMaxCellsPerIntkeyNode(int pgsz){
  return (pgsz - sizeof(HctDbIntkeyNode)) / sizeof(HctDbIntkeyNodeEntry);
}
static int hctDbMinCellsPerIntkeyNode(int pgsz){
  return (pgsz - sizeof(HctDbIntkeyNode)) / (3*sizeof(HctDbIntkeyNodeEntry));
}

static void hctDbIrrevocablyEvictPage(HctDatabase *pDb, HctDbWriter *p);

/*
** Cleanup the writer object passed as the first argument.
*/
static void hctDbWriterCleanup(HctDatabase *pDb, HctDbWriter *p, int bRevert){
  int ii;

  sqlite3HctFileDebugPrint(pDb->pFile, 
      "writer cleanup height=%d bRevert=%d\n", p->iHeight, bRevert
  );

  for(ii=0; ii<p->nWritePg; ii++){
    HctFilePage *pPg = &p->aWritePg[ii];
    if( bRevert ){
      if( pPg->aNew ){
        sqlite3HctFilePageUnwrite(pPg);
      }else if( ii>0 ){
        sqlite3HctFileClearInUse(pPg, 1);
      }
    }
    sqlite3HctFilePageRelease(pPg);
  }

  for(ii=0; ii<p->nDiscard; ii++){
    if( bRevert && pDb->pConfig->nTryBeforeUnevict>1 ){
      sqlite3HctFilePageUnevict(&p->aDiscard[ii]);
    }
    sqlite3HctFilePageRelease(&p->aDiscard[ii]);
  }

  hctDbCsrReset(&p->writecsr);
  p->nWritePg = 0;
  p->nDiscard = 0;
  sqlite3_free(p->aWriteFpKey);
  p->aWriteFpKey = 0;

  if( p->iEvictLockedPgno ){
    p->nEvictLocked++;
    if( p->nEvictLocked>=pDb->pConfig->nTryBeforeUnevict ){
      p->nEvictLocked = -1;
      hctDbIrrevocablyEvictPage(pDb, p);
      p->nEvictLocked = 0;
    }
  }else{
    p->nEvictLocked = 0;
  }
  p->iEvictLockedPgno = 0;

  /* Free/zero various buffers and caches */
  if( p->writecsr.pKeyInfo ){
    sqlite3DbFree(p->writecsr.pKeyInfo->db, p->writecsr.pRec);
    p->writecsr.pRec = 0;
  }
  hctBufferFree(&p->writecsr.rec);
}

static int hctDbInsert(
  HctDatabase *pDb,
  HctDbWriter *p,
  u32 iRoot,
  UnpackedRecord *pRec,           /* The key value for index tables */
  i64 iKey,                       /* For intkey tables, the key value */
  u32 iChildPg,                   /* For internal node ops, the child pgno */
  int bDel,                       /* True for a delete operation */
  int nData, const u8 *aData      /* Record/key to insert */
);

typedef struct HctDbWriterOrigin HctDbWriterOrigin;
struct HctDbWriterOrigin {
  u8 bDiscard;                    /* 1 for aDiscard[], 0 for aWritePg[] */
  i16 iPg;                        /* Index of page in array*/
};

static int hctdbWriterSortFPKeys(
  HctDatabase *pDb, 
  int eType,
  HctDbWriter *p, 
  HctDbWriterOrigin *aOrigin      /* Populate this array */
){
  int iDiscard = 0;
  int iWP = 1;
  int iOut = 0;
  int rc = SQLITE_OK;

  assert( eType==HCT_PAGETYPE_INDEX || eType==HCT_PAGETYPE_INTKEY );

  while( iDiscard<p->nDiscard || iWP<p->nWritePg ){
    if( iDiscard>=p->nDiscard ){
      aOrigin[iOut].bDiscard = 0;
      aOrigin[iOut].iPg = iWP++;
      iOut++;
    }
    else if( iWP>=p->nWritePg ){
      aOrigin[iOut].bDiscard = 1;
      aOrigin[iOut].iPg = iDiscard++;
      iOut++;
    }else{
      int bDiscard = 0;
      const u8 *aD = p->aDiscard[iDiscard].aOld;
      const u8 *aW = p->aWritePg[iWP].aOld;

      if( eType==HCT_PAGETYPE_INTKEY ){
        i64 i1 = hctDbIntkeyFPKey(aD);
        i64 i2 = hctDbIntkeyFPKey(aW);
        bDiscard = (i1<=i2);
      }else{
        int nFP = 0;
        const u8 *aFP = 0;
        UnpackedRecord *pRec = p->writecsr.pRec;
        rc = hctDbLoadRecord(pDb, &p->writecsr.rec, aW, 0, &nFP, &aFP);
        if( rc!=SQLITE_OK ) break;
        sqlite3VdbeRecordUnpack(p->writecsr.pKeyInfo, nFP, aFP, pRec);
        rc = hctDbCompareFPKey(pDb, pRec, aD, &bDiscard);
        if( rc!=SQLITE_OK ) break;
      }

      aOrigin[iOut].bDiscard = bDiscard;
      if( bDiscard ){
        aOrigin[iOut].iPg = iDiscard++;
      }else{
        aOrigin[iOut].iPg = iWP++;
      }
      iOut++;
    }
  }

  return rc;
}

#if 0
/*
**
*/
static int hctDbTruncateRecord(
  HctBuffer *pBuf,                /* Buffer to use for storage space */
  KeyInfo *pKeyInfo,              /* Description of index */
  int *pnFP,                      /* IN/OUT: Size of record */
  const u8 **aFP                  /* IN/OUT: Pointer to record */
){
}
#endif

/*
** This is a wrapper around:
**
**   sqlite3HctFilePageEvict(pPg, 0);
**
** If the call fails with SQLITE_LOCKED because page pPg has been evicted,
** HctDbWriter.iEvictLockedPgno is set to the logical page number of pPg.
*/
static int hctDbFilePageEvict(HctDbWriter *p, HctFilePage *pPg){
  int rc = sqlite3HctFilePageEvict(pPg, 0);
  if( rc==SQLITE_LOCKED && sqlite3HctFilePageIsEvicted(pPg->pFile, pPg->iPg) ){
    p->iEvictLockedPgno = pPg->iPg;
  }
  return rc;
}

static int hctDbFilePageCommit(HctDbWriter *p, HctFilePage *pPg){
  int rc = sqlite3HctFilePageCommit(pPg);
  if( rc==SQLITE_LOCKED && sqlite3HctFilePageIsEvicted(pPg->pFile, pPg->iPg) ){
    p->iEvictLockedPgno = pPg->iPg;
  }
  return rc;
}

static void hctDbRecordTrim(UnpackedRecord *pRec){
  if( pRec && pRec->pKeyInfo->nUniqField ){
    int ii;
    u16 nUniqField = pRec->pKeyInfo->nUniqField;
    for(ii=0; ii<nUniqField; ii++){
      if( pRec->aMem[ii].flags & MEM_Null ){
        return;
      }
    }
    pRec->nField = nUniqField;
  }
}

static int hctDbInsertFlushWrite(HctDatabase *pDb, HctDbWriter *p){
  int rc = SQLITE_OK;
  int ii;
  int eType = hctPagetype(p->aWritePg[0].aNew);
  HctFilePage root;
  int bUnevict = 0;

static int nCall = 0;
nCall++;

  memset(&root, 0, sizeof(root));

#ifdef SQLITE_DEBUG
  for(ii=1; ii<p->nWritePg; ii++){
    u32 iPeer = ((HctDbPageHdr*)p->aWritePg[ii-1].aNew)->iPeerPg;
    assert( p->aWritePg[ii].iPg==iPeer );
  }
#endif

  /* Test if this is a split of a root page of the tree. */
  if( p->nWritePg>1 && p->aWritePg[0].iPg==p->writecsr.iRoot ){
    memcpy(&root, &p->aWritePg[0], sizeof(HctFilePage));
    memset(&p->aWritePg[0], 0, sizeof(HctFilePage));
    rc = sqlite3HctFilePageNew(pDb->pFile, 0, &p->aWritePg[0]);
    if( rc==SQLITE_OK ){
      memcpy(p->aWritePg[0].aNew, root.aNew, pDb->pgsz);
      hctDbRootPageInit(eType==HCT_PAGETYPE_INDEX,
          hctPageheight(root.aNew)+1, p->aWritePg[0].iPg, root.aNew, pDb->pgsz
      );
    }
  }

  /* Loop through the set of pages to write out. They must be
  ** written in reverse order - so that page aWritePg[0] is written
  ** last. */
  assert( p->nWritePg>0 );
  for(ii=p->nWritePg-1; rc==SQLITE_OK && ii>=0; ii--){
    rc = hctDbFilePageCommit(p, &p->aWritePg[ii]);
  }

  /* If there is one, write the new root page to disk */
  if( rc==SQLITE_OK && root.iPg ){
    rc =hctDbFilePageCommit(p, &root);
    sqlite3HctFilePageRelease(&root);
  }

  if( rc!=SQLITE_OK ){
    bUnevict = 1;
  }

  if( (p->nWritePg>1 || p->nDiscard>0) && rc==SQLITE_OK ){
    do {
      const u32 iRoot = p->writecsr.iRoot;
      const int nOrig = p->nDiscard + p->nWritePg - 1;
      HctDbWriterOrigin aOrig[(HCTDB_MAX_DIRTY+2)*2];
      HctDbWriter wr;
      HctBuffer buf;

      memset(&wr, 0, sizeof(wr));
      memset(&buf, 0, sizeof(buf));
      wr.iHeight = p->iHeight + 1;
      rc = hctDbAllocateUnpacked(&p->writecsr);
      assert( ArraySize(aOrig)==ArraySize(p->aWritePg)+ArraySize(p->aDiscard) );
      if( rc==SQLITE_OK ){
        rc = hctdbWriterSortFPKeys(pDb, eType, p, aOrig);
      }

      for(ii=0; ii<nOrig && rc==SQLITE_OK; ii++){
        HctFilePage *pPg;
        i64 iKey = 0;
        const u8 *aFP = 0;
        int nFP = 0;
        UnpackedRecord *pRec = 0;
        int bDel = aOrig[ii].bDiscard;

        pPg = &(bDel ? p->aDiscard : p->aWritePg)[aOrig[ii].iPg];
        if( eType==HCT_PAGETYPE_INTKEY ){
          iKey = hctDbIntkeyFPKey(pPg->aOld);
        }else{
          rc = hctDbLoadRecord(pDb, &buf, pPg->aOld, 0, &nFP, &aFP);
          if( rc!=SQLITE_OK ) break;
          pRec = p->writecsr.pRec;
          sqlite3VdbeRecordUnpack(p->writecsr.pKeyInfo, nFP, aFP, pRec);
          hctDbRecordTrim(pRec);
        }

        rc = hctDbInsert(pDb, &wr, iRoot, pRec, iKey, pPg->iPg, bDel, nFP, aFP);
      }
      hctBufferFree(&buf);

      if( rc==SQLITE_OK ){
        rc = hctDbInsertFlushWrite(pDb, &wr);
      }else{
        hctDbWriterCleanup(pDb, &wr, 1);
      }
      if( rc==SQLITE_LOCKED ){
        pDb->nCasFail++;
      }
    }while( rc==SQLITE_LOCKED );
  }

  if( rc==SQLITE_OK ){
    for(ii=0; ii<p->nDiscard; ii++){
      sqlite3HctFileClearInUse(&p->aDiscard[ii], 0);
    }
  }

  /* Clean up the Writer object */
  hctDbWriterCleanup(pDb, p, bUnevict);
  return rc;
}

void sqlite3HctDbRollbackMode(HctDatabase *pDb, int bRollback){
  assert( bRollback==0 || bRollback==1 );
  assert( pDb->bRollback==0 || pDb->bRollback==1 );
  assert( pDb->bRollback==0 || bRollback==0 );
  pDb->pa.nWriteKey = 0;
  pDb->bRollback = bRollback;
}

i64 sqlite3HctDbNCasFail(HctDatabase *pDb){
  return pDb->nCasFail;
}

#if 0
static HctDbIntkeyEntry *hctDbIntkeyEntry(u8 *aPg, int iCell){
  return iCell<0 ? 0 : (&((HctDbIntkeyLeaf*)aPg)->aEntry[iCell]);
}
#endif

/*
** This function is used when reverting changes as part of a rollback. It
** searches physical page iOld for the entry with key iKey (if pRec==0) or
** pRec (if pRec!=0). Unless the database is corrupt, the key is guaranteed
** to be on the page. Once found, the four output variables are populated
** as follows:
**
**   (*piFlags): value of flags field for entry,
**   (*pnSize):  value of nSize header field for entry (i.e. bytes of data,
**               including local and overflow)
**   (*pnData):  Bytes of data to copy from (*paData)
**   (*paData):  Copy data for old record from here.
**
** The output buffer (*pnData)/(*paData) contains all data stored in the
** cell area for the database entry. Including any TID or old-page or
** overflow page number.
**
** The old page reference (physical page iOld) is stored in
** HctDatabase.pa.writecsr.oldpg, so the (*paData) buffer is stable until the
** next call to this function.
*/
static int hctDbFindRollbackData(
  HctDatabase *pDb,
  i64 iKey,
  UnpackedRecord *pRec,
  u32 iOld,
  u8 *piFlags,                    /* OUT: Value of flags for rollback entry */
  int *pnSize,                    /* OUT: Value of nData for rollback entry */
  int *pnData,                    /* OUT: Bytes to copy from (*paData) */
  const u8 **paData               /* OUT: Value of aData for rollback entry */
){
  int rc;

  assert( pDb->bRollback );

  rc = sqlite3HctFilePageGetPhysical(pDb->pFile, iOld, &pDb->pa.writecsr.oldpg);
  if( rc==SQLITE_OK ){
    u8 *aPg = pDb->pa.writecsr.oldpg.aOld;
    int b = 0;
    int iCell = 0;
    if( pRec ){
      rc = hctDbIndexSearch(pDb, aPg, pRec, &iCell, &b);
    }else{
      iCell = hctDbIntkeyLeafSearch(aPg, iKey, &b);
    }

    if( rc==SQLITE_OK ){
      int flags = 0;
      int nSz = 0;
      int iCellOff = hctDbEntryInfo(aPg, iCell, &nSz, &flags);

      *piFlags = flags;
      *pnSize = nSz;
      *paData = &aPg[iCellOff];
      *pnData = hctDbLocalsize(aPg, pDb->pgsz, nSz)
        + ((flags & HCTDB_HAS_TID) ? 8 : 0)
        + ((flags & HCTDB_HAS_OLD) ? 4 : 0)
        + ((flags & HCTDB_HAS_OVFL) ? 4 : 0);
    }
  }

  return rc;
}

int sqlite3HctDbInsertFlush(HctDatabase *pDb, int *pnRetry){
  int rc = SQLITE_OK;
  if( pDb->pa.nWritePg ){
    rc = hctDbInsertFlushWrite(pDb, &pDb->pa);
    if( rc==SQLITE_LOCKED ){
      *pnRetry = pDb->pa.nWriteKey;
      rc = SQLITE_OK;
      pDb->nCasFail++;
    }else{
      *pnRetry = 0;
    }
    pDb->pa.nWriteKey = 0;
  }
  return rc;
}

/*
** If pRec is not NULL, it contains an unpacked index key. Compare this key
** with the write-fp-key in pDb->pa.aWriteFpKey. Return true if pRec is greater
** than or equal to the write-fp-key.
**
** Or, if pRec is NULL, iKey is the key and it is compared to 
** pDb->iWriteFpKey.
*/
static int hctDbTestWriteFpKey(
  HctDbWriter *p, 
  UnpackedRecord *pRec, 
  i64 iKey
){
  if( pRec ){
    int r;
    if( p->aWriteFpKey==0 ){
      r = 1;
    }else{
      r = sqlite3VdbeRecordCompare(p->iWriteFpKey, p->aWriteFpKey, pRec);
    }
    return (r <= 0);
  }
  return iKey>=p->iWriteFpKey;
}

static int hctDbSetWriteFpKey(HctDatabase *pDb, HctDbWriter *p){
  int rc = SQLITE_OK;
  HctDbPageHdr *pHdr = (HctDbPageHdr*)p->aWritePg[0].aNew;

  sqlite3_free(p->aWriteFpKey);
  p->aWriteFpKey = 0;
  p->iWriteFpKey = 0;

  if( pHdr->iPeerPg==0 ){
    if( hctPagetype(pHdr)==HCT_PAGETYPE_INTKEY ){
      p->iWriteFpKey = LARGEST_INT64;
    }
  }else{
    HctFilePage pg;
    memset(&pg, 0, sizeof(HctFilePage));
    rc = sqlite3HctFilePageGet(pDb->pFile, pHdr->iPeerPg, &pg);
    if( rc==SQLITE_OK ){
      if( hctPagetype(pHdr)==HCT_PAGETYPE_INTKEY ){
        p->iWriteFpKey = hctDbIntkeyFPKey(pg.aOld);
      }else{
        const u8 *aData = 0;
        int nData = 0;
        HctBuffer buf;
        memset(&buf, 0, sizeof(buf));
        rc = hctDbLoadRecord(pDb, &buf, pg.aOld, 0, &nData, &aData);
        if( rc==SQLITE_OK && buf.aBuf==0 ){
          rc = hctBufferGrow(&buf, nData);
          if( rc==SQLITE_OK ){
            memcpy(buf.aBuf, aData, nData);
          }
        }
        if( rc==SQLITE_OK ){
          p->aWriteFpKey = buf.aBuf;
          p->iWriteFpKey = nData;
          memset(&buf, 0, sizeof(buf));
        }
        hctBufferFree(&buf);
      }
      sqlite3HctFilePageRelease(&pg);
    }
  }

  return rc;
}

/*
** Buffer aTarget[] contains a page that contains variable length keys
** (i.e. an intkey leaf or an index leaf or node). This function returns
** the offset of the aEntry[] array in aTarget. Before doing so, it sets
** output variable (*pszEntry) to the sizeof(aEntry[0]).
*/
static int hctDbEntryArrayDim(const u8 *aTarget, int *pszEntry){
  int eType = hctPagetype(aTarget);
  int nHeight = hctPageheight(aTarget);
  int nRet;

  assert( eType==HCT_PAGETYPE_INTKEY || eType==HCT_PAGETYPE_INDEX );
  assert( eType==HCT_PAGETYPE_INDEX || nHeight==0 );
  if( eType==HCT_PAGETYPE_INTKEY ){
    *pszEntry = sizeof(HctDbIntkeyEntry);
    nRet = sizeof(HctDbIntkeyLeaf);
  }else if( nHeight==0 ){
    *pszEntry = sizeof(HctDbIndexEntry);
    nRet = sizeof(HctDbIndexLeaf);
  }else{
    *pszEntry = sizeof(HctDbIndexNodeEntry);
    nRet = sizeof(HctDbIndexNode);
  }

  return nRet;
}

static int hctIsVarRecords(const u8 *aTarget){
  int eType = hctPagetype(aTarget);
  int nHeight = hctPageheight(aTarget);
  return (nHeight==0 || eType==HCT_PAGETYPE_INDEX);
}

#ifdef SQLITE_DEBUG

static void print_out_page(const char *zCaption, const u8 *aData, int nData){
  HctDbPageHdr *pPg = (HctDbPageHdr*)aData;

  if( hctPagetype(pPg)==HCT_PAGETYPE_INTKEY && pPg->nHeight==0 ){
    HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)pPg;
    char *zPrint = 0;
    const char *zSep = "";
    int ii;

    for(ii=0; ii<pLeaf->pg.nEntry; ii++){
      HctDbIntkeyEntry *pEntry = &pLeaf->aEntry[ii];
      zPrint = sqlite3_mprintf("%z%s(%d..%d)", zPrint, zSep, 
          pEntry->iOff, pEntry->iOff+ hctDbIntkeyEntrySize(pEntry, nData)
      );
      zSep = ",";
    }

    printf("%s: nFreeGap=%d nFreeBytes=%d\n", zCaption,
      pLeaf->hdr.nFreeGap,
      pLeaf->hdr.nFreeBytes
    );
    printf("%s: %s\n", zCaption, zPrint);
    sqlite3_free(zPrint);
  }

  if( hctPagetype(pPg)==HCT_PAGETYPE_INDEX && pPg->nHeight==0 ){
    HctDbIndexLeaf *pLeaf = (HctDbIndexLeaf*)pPg;
    char *zPrint = 0;
    const char *zSep = "";
    int ii;

    for(ii=0; ii<pLeaf->pg.nEntry; ii++){
      HctDbIndexEntry *pEntry = &pLeaf->aEntry[ii];
      zPrint = sqlite3_mprintf("%z%s(%d..%d)", zPrint, zSep, 
          pEntry->iOff, pEntry->iOff + hctDbIndexEntrySize(pEntry, nData)
      );
      zSep = ",";
    }

    printf("%s: nFreeGap=%d nFreeBytes=%d\n", zCaption,
      pLeaf->hdr.nFreeGap,
      pLeaf->hdr.nFreeBytes
    );
    printf("%s: %s\n", zCaption, zPrint);
    fflush(stdout);
    sqlite3_free(zPrint);
  }


}

#define assert_or_print(E)                         \
  if( !(E) ){                                      \
    print_out_page("page", aData, nData);          \
    assert( E );                                   \
  }

typedef struct VarCellReader VarCellReader;
struct VarCellReader {
  const u8 *aData;
  int nData;
  int szEntry;
  int iEntry0;
};

static void hctVCRInit(VarCellReader *p, const u8 *aData, int nData){
  p->aData = aData;
  p->nData = nData;
  p->iEntry0 = hctDbEntryArrayDim(aData, &p->szEntry);
}

static int hctVCRFindCell(VarCellReader *p, int iCell, int *pnByte){
  HctDbIndexNodeEntry *pEntry;

  pEntry = (HctDbIndexNodeEntry*)&p->aData[p->iEntry0 + iCell*p->szEntry];
  *pnByte = hctDbLocalsize(p->aData, p->nData, pEntry->nSize)
    + ((pEntry->flags & HCTDB_HAS_TID) ? 8 : 0)
    + ((pEntry->flags & HCTDB_HAS_OLD) ? 4 : 0)
    + ((pEntry->flags & HCTDB_HAS_OVFL) ? 4 : 0);

  return pEntry->iOff;
}

static void assert_page_is_ok(const u8 *aData, int nData){

  if( aData && hctIsVarRecords(aData) ){
    HctDbIndexNode *p = (HctDbIndexNode*)aData;
    VarCellReader vcr;
    int iEnd = nData;
    int iStart = 0;
    int nRecTotal = 0;
    int ii = 0;
    int nFreeExpect;

    hctVCRInit(&vcr, aData, nData);
    for(ii=0; ii<p->pg.nEntry; ii++){
      int sz = 0;
      int iOff = hctVCRFindCell(&vcr, ii, &sz);
      if( iOff ){
        assert_or_print( (iOff+sz)<=nData );
        iEnd = MIN(iEnd, iOff);
        nRecTotal += sz;
      }else{
        assert( sz==0 && ii==0 );
      }
    }

    iStart = vcr.iEntry0 + vcr.szEntry * p->pg.nEntry;
    nFreeExpect = nData - (iStart + nRecTotal);

    assert_or_print( p->hdr.nFreeGap==(iEnd - iStart) );
    assert_or_print( p->hdr.nFreeBytes==nFreeExpect);
  }

}
#else
# define assert_page_is_ok(x,y)
#endif

/*
** HOW INSERT/DELETE OPERATIONS WORK:
**
** 1. If the page array is not empty, flush it to disk if required. It 
**    should be flushed to disk if either:
**
**      a) the key being written (specified by iKey/pRec) is greater or
**         equal to the FP key to the right of the page array (stored
**         in HctDbWriter.iWriteFpKey/aWriteFpKey).
**
**      b) there are more than HCTDB_MAX_DIRTY pages in the array.
**
** 2. If the page array is empty, either because it was flushed to disk
**    in (1) or because it was empty when this function was called, seek
**    the write-cursor (HctDbWriter.writecsr) to the key being written.
**    The page the cursor seeks to becomes the first page of the page
**    array.
**
** 3. Locate within the page array the page into which the new key 
**    or delete-key should be inserted. There are three possible outcomes:
**
**      i)   the new key may just be written to the page.
**
**      ii)  the new key fits on the page, but leaves it underfull. In this
**           context, "underfull" means that the total amount of free space
**           on the page is less than or equal to (pgsz*2/3).
**
**      iii) the new key does not fit on the page.
**
** In cases (ii) or (iii), first ensure that that the page has two peers in
** the page array (unless there are fewer than three pages in the list, in
** which case the entire list should be loaded). Then redistribute the keys
** between the minimum number of pages, discarding or adding nodes as
** required.
*/

/*
** Insert nPg new pages at index iPg into the write-array of the HctDbWriter
** passed as the second argument and link them into the list.
*/
static int hctDbExtendWriteArray(
  HctDatabase *pDb,
  HctDbWriter *p,
  int iPg,
  int nPg
){
  int rc = SQLITE_OK;
  int ii;

  assert( iPg>0 );
  assert( (p->nWritePg+nPg)>0 );

  /* Add any new pages required */
  for(ii=iPg; rc==SQLITE_OK && ii<iPg+nPg; ii++){
    assert( p->nWritePg<ArraySize(p->aWritePg) );
    assert( ii>0 );
    if( ii<p->nWritePg ){
      int nByte = sizeof(HctFilePage) * (p->nWritePg-ii);
      memmove(&p->aWritePg[ii+1], &p->aWritePg[ii], nByte);
    }
    p->nWritePg++;
    memset(&p->aWritePg[ii], 0, sizeof(HctFilePage));
    rc = sqlite3HctFilePageNew(pDb->pFile, 0, &p->aWritePg[ii]);
    if( rc==SQLITE_OK ){
      HctDbPageHdr *pNew = (HctDbPageHdr*)p->aWritePg[ii].aNew;
      HctDbPageHdr *pPrev = (HctDbPageHdr*)p->aWritePg[ii-1].aNew;
      memset(pNew, 0, sizeof(HctDbPageHdr));
      pNew->hdrFlags = hctPagetype(pPrev);
      pNew->nHeight = pPrev->nHeight;
      pNew->iPeerPg = pPrev->iPeerPg;
      pPrev->iPeerPg = p->aWritePg[ii].iPg;
    }
  }

  /* Remove pages that are not required */
  for(ii=nPg; ii<0; ii++){
    int iRem = iPg;
    HctDbPageHdr *pPrev = (HctDbPageHdr*)(p->aWritePg[iRem-1].aNew);
    HctDbPageHdr *pRem = (HctDbPageHdr*)(p->aWritePg[iRem].aNew);
    pPrev->iPeerPg = pRem->iPeerPg;
    assert( p->nWritePg>1 );
    p->nWritePg--;
    if( p->aWritePg[iRem].aOld ){
      /* TODO: Is this necessary? */
      p->aDiscard[p->nDiscard++] = p->aWritePg[iRem];
    }else{
      sqlite3HctFilePageUnwrite(&p->aWritePg[iRem]);
    }
    if( iRem!=p->nWritePg ){
      int nByte = sizeof(HctFilePage) * (p->nWritePg-iRem);
      assert( nByte>0 );
      memmove(&p->aWritePg[iRem], &p->aWritePg[iRem+1], nByte);
    }
  }

  return rc;
}

static int hctDbCsrLoadAndDecode(
  HctDbCsr *pCsr, 
  int iCell, 
  UnpackedRecord **ppRec
){
  const u8 *aPg = pCsr->pg.aNew ? pCsr->pg.aNew : pCsr->pg.aOld;
  int nData = 0;
  const u8 *aData = 0;
  int rc;

  rc = hctDbLoadRecord(pCsr->pDb, &pCsr->rec, aPg, iCell, &nData, &aData);
  if( rc==SQLITE_OK ){
    rc = hctDbAllocateUnpacked(pCsr);
  }
  if( rc==SQLITE_OK ){
    *ppRec = pCsr->pRec;
    sqlite3VdbeRecordUnpack(pCsr->pKeyInfo, nData, aData, pCsr->pRec);
  }

  return rc;
}

int sqlite3HctDbCsrLoadAndDecode(HctDbCsr *pCsr, UnpackedRecord **ppRec){
  return hctDbCsrLoadAndDecode(pCsr, pCsr->iCell, ppRec);
}

/*
**
*/
static int hctDbFindLhsPeer(
  HctDatabase *pDb,
  HctDbWriter *p,
  HctFilePage *pPg,
  HctFilePage *pOut
){
  HctDbCsr csr;
  u8 *aLeft = pPg->aNew ? pPg->aNew : pPg->aOld;
  int rc = SQLITE_OK;

  hctDbCsrInit(pDb, p->writecsr.iRoot, &csr);
  if( hctPagetype(aLeft)==HCT_PAGETYPE_INTKEY ){
    i64 iKey = hctDbIntkeyFPKey(aLeft);
    assert( iKey!=SMALLEST_INT64 );
    rc = hctDbCsrSeek(&csr, p->iHeight, 0, iKey-1, 0);
  }else{
    UnpackedRecord *pRec = 0;
    HctBuffer buf;
    int nData = 0;
    const u8 *aData = 0;
    memset(&buf, 0, sizeof(buf));
    rc = hctDbLoadRecord(pDb, &buf, aLeft, 0, &nData, &aData);
    if( rc==SQLITE_OK ){
      rc = hctDbAllocateUnpacked(&p->writecsr);
    }
    if( rc==SQLITE_OK ){
      pRec = p->writecsr.pRec;
      sqlite3VdbeRecordUnpack(p->writecsr.pKeyInfo, nData, aData, pRec);
      hctDbRecordTrim(pRec);
      pRec->default_rc = 1;
      rc = hctDbCsrSeek(&csr, p->iHeight, pRec, 0, 0);
      pRec->default_rc = 0;

      assert( csr.pg.iPg!=pPg->iPg );
    }
    hctBufferFree(&buf);
  }

  if( rc==SQLITE_OK 
   && ((HctDbPageHdr*)csr.pg.aOld)->iPeerPg==pPg->iPg
  ){
    *pOut = csr.pg;
  }else{
    memset(pOut, 0, sizeof(HctFilePage));
    rc = SQLITE_LOCKED_ERR(pPg->iPg);
  }

  return rc;
}

static void hctDbIrrevocablyEvictPage(HctDatabase *pDb, HctDbWriter *p){
  int rc = SQLITE_OK;
  u32 iLocked = p->iEvictLockedPgno;
  int bDone = 0;

  sqlite3HctFileDebugPrint(pDb->pFile,"BEGIN forced eviction of %d\n", iLocked);
  
  do {
    HctFilePage pg1;
    HctFilePage pg0;
    memset(&pg1, 0, sizeof(pg1));
    rc = sqlite3HctFilePageGet(pDb->pFile, iLocked, &pg1);
    while( rc==SQLITE_OK ){
      memset(&pg0, 0, sizeof(pg0));
      rc = hctDbFindLhsPeer(pDb, p, &pg1, &pg0);
      if( rc ) break;
      if( 0==sqlite3HctFilePageIsEvicted(pg0.pFile, pg0.iPg) ) break;
      sqlite3HctFilePageRelease(&pg1);
      pg1 = pg0;
      memset(&pg0, 0, sizeof(pg0));
    }

    if( rc==SQLITE_OK ){
      bDone = (pg1.iPg==iLocked);
      sqlite3HctFileDebugPrint(
          pDb->pFile, "forcing write of %d->%d\n", pg0.iPg, pg1.iPg
      ); 

      rc = sqlite3HctFilePageEvict(&pg1, 1);
      if( rc==SQLITE_OK ){
        rc = sqlite3HctFilePageWrite(&pg0);
      }
      if( rc==SQLITE_OK ){
        memcpy(pg0.aNew, pg0.aOld, pDb->pgsz);
      }
      if( rc==SQLITE_OK ){
        p->aWritePg[0] = pg0;
        p->nWritePg = 1;
        rc = hctDbExtendWriteArray(pDb, p, 1, 1);
      }
      if( rc==SQLITE_OK ){
        memcpy(p->aWritePg[1].aNew, pg1.aOld, pDb->pgsz);
        p->aDiscard[0] = pg1;
        p->nDiscard = 1;
      }

      if( rc==SQLITE_OK ){
        rc = hctDbInsertFlushWrite(pDb, p);
      }else{
        hctDbWriterCleanup(pDb, p, 1);
      }

    }else{
      sqlite3HctFilePageRelease(&pg0);
      sqlite3HctFilePageRelease(&pg1);
    }
  }while( rc==SQLITE_OK && bDone==0 );

  sqlite3HctFileDebugPrint(pDb->pFile,"END forced eviction of %d\n", iLocked);
}

/*
**
*/
static int hctDbLoadPeers(HctDatabase *pDb, HctDbWriter *p, int *piPg){
  int rc = SQLITE_OK;
  int iPg = *piPg;

  if( p->nWritePg==1 ){
    HctFilePage *pLeft = &p->aWritePg[0];

    assert( iPg==0 );
    if( 0==hctIsLeftmost(pLeft->aNew) ){
      HctFilePage *pCopy = 0;

      /* First, evict the page currently in p->aWritePg[0]. If we 
      ** successfully evict the page here, then of course no other thread
      ** can - which guarantees that the seek operation below really does
      ** find the left-hand peer (assuming the db is not corrupt).  */
      rc = hctDbFilePageEvict(p, pLeft);

      /* Assuming the LOGICAL_EVICTED flag was successfully set, seek 
      ** cursor csr to the leaf page immediately to the left of pLeft. */
      if( rc==SQLITE_OK ){
        pCopy = &p->aDiscard[p->nDiscard++];
        *pCopy = *pLeft;
        rc = hctDbFindLhsPeer(pDb, p, pCopy, pLeft);
      }
      if( rc==SQLITE_OK ){
        assert( ((HctDbPageHdr*)pLeft->aOld)->iPeerPg==pCopy->iPg );
        rc = sqlite3HctFilePageWrite(pLeft);
      }
      
      if( rc==SQLITE_OK ){
        memcpy(pLeft->aNew, pLeft->aOld, pDb->pgsz);
        rc = hctDbExtendWriteArray(pDb, p, 1, 1);
      }
      if( rc==SQLITE_OK ){
        memcpy(p->aWritePg[1].aNew, pCopy->aNew, pDb->pgsz);
        sqlite3HctFilePageUnwrite(pCopy);
        *piPg = 1;
      }
    }

    if( rc==SQLITE_OK ){
      HctDbPageHdr *pHdr = (HctDbPageHdr*)p->aWritePg[p->nWritePg-1].aNew;
      if( pHdr->iPeerPg ){
        HctFilePage *pPg = &p->aWritePg[p->nWritePg++];
        HctFilePage *pCopy = &p->aDiscard[p->nDiscard];

        rc = sqlite3HctFilePageGet(pDb->pFile, pHdr->iPeerPg, pCopy);
        if( rc==SQLITE_OK ){
          /* Evict the page immediately */
          rc = hctDbFilePageEvict(p, pCopy);
          if( rc!=SQLITE_OK ){
            sqlite3HctFilePageRelease(pCopy);
          }else{
            p->nDiscard++;
          }
        }

        if( rc==SQLITE_OK ){
          rc = sqlite3HctFilePageNew(pDb->pFile, 0, pPg);
        }
        if( rc==SQLITE_OK ){
          HctDbPageHdr *pPrev = (HctDbPageHdr*)p->aWritePg[p->nWritePg-2].aNew;
          memcpy(pPg->aNew, pCopy->aOld, pDb->pgsz);
          pPrev->iPeerPg = pPg->iPg;
        }
      }
    }
  }

  return rc;
}

/*
** This is used by the rebalance operation implemented by hctDbBalance().
** The first step of that operation is to assemble an array of these
** structures - one for each cell that will be distributed between the
** output pages.
**
** nByte:
**   Total bytes of space required by cell on new page. This includes
**   the header entry and the data stored in the cell area.
**
** iPg:
**   Index of source page within array of input pages. Or -1 to indicate
**   that the HctDbCellSz structure corresponds to a new cell being
**   written (that is not on any input page).
**
** iEntry:
**   Only valid if (iPg>=0). The index of the cell within input page iPg.
**
** bEdks:
**   True if the cell may be moved to an EKDS. In other words, if the
**   cell is a delete-key only and is not the FP key of the leftmost
**   page of the balance.
*/
typedef struct HctDbCellSz HctDbCellSz;
struct HctDbCellSz {
  int nByte;                      /* Size of cell in bytes */
  i16 iPg;                        /* Index of input page */
  u16 iEntry;                     /* Entry of cell on input page */
  u8 bEdks;                       /* True if cell is eligible for EKDS */
};

/* 
** Populate the aSz[] array with the sizes and locations of each cell
**
** (bClobber && nNewCell==0)   ->   full-delete
** (bClobber)                  ->   clobber
** (bClobber==0)               ->   insert of new key
*/
static void 
__attribute__ ((noinline)) 
hctDbBalanceGetCellSz(
  HctDatabase *pDb,
  int iPg,
  int iInsert,
  int bClobber,
  int nNewCell,                     /* Bytes stored on page for new cell */
  u8 **aPgCopy,
  int nPgCopy,
  HctDbCellSz *aSz,
  int *pnSz                         /* OUT: number of entries in aSz[] */
){
  int ii;                           /* Current index in aPgCopy[] */
  int iCell;                        /* Current cell of aPgCopy[ii] */
  int iSz = 0;                      /* Current populated size of aSz[] */
  int szEntry = hctDbPageEntrySize(aPgCopy[0]);
  int iIns = iInsert;
  u64 iSafeTid = sqlite3HctFileSafeTID(pDb->pFile);

  assert( bClobber || nNewCell>0 );

  ii = 0;
  iCell = 0;
  for(iSz=0; ii<nPgCopy; iSz++){
    HctDbPageHdr *pPg = (HctDbPageHdr*)aPgCopy[ii];
    HctDbCellSz *pSz = &aSz[iSz];

    if( ii==iPg && iCell==iIns ){
      assert( nNewCell>0 || bClobber );
      if( nNewCell ){
        pSz->nByte = szEntry + nNewCell;
        pSz->iPg = -1;
      }else{
        iSz--;
      }
      if( bClobber ){
        iCell++;
      }
      iIns = -1;
    }else{
      int bSkip = 0;
      if( iSz>0 ){
        int flags = 0;
        int iOff = hctDbEntryInfo((void*)pPg, iCell, 0, &flags);
        if( flags & HCTDB_IS_DELETE ){
          u64 iTid = hctGetU64(&((u8*)pPg)[iOff]);
          if( iSz>0 && iTid<=iSafeTid ) bSkip = 1;
          assert( flags & HCTDB_HAS_TID );
        }
      }
      if( bSkip==0 ){
        pSz->nByte = szEntry + hctDbPageRecordSize(pPg, pDb->pgsz, iCell);
        pSz->iPg = ii;
        pSz->iEntry = iCell;
      }else{
        iSz--;
      }
      iCell++;
    }

    if( iCell>=pPg->nEntry && (ii!=iPg || iCell!=iIns) ){
      iCell = 0;
      ii++;
    }
  }
  *pnSz = iSz;
}


/*
** Rebalance routine for pages with variably-sized records - intkey leaves,
** index leaves and index nodes.
** 
*/
static int 
__attribute__ ((noinline)) 
hctDbBalance(
  HctDatabase *pDb,
  HctDbWriter *p,
  int bSingle,
  int *piPg,                      /* IN/OUT: Page for new cell */
  int *piInsert,                  /* IN/OUT: Position in page for new cell */
  int bClobber,
  int nNewCell                    /* Size of new cell, if any */
){
  int rc = SQLITE_OK;             /* Return code */
  int iPg = *piPg;
  int iIns = *piInsert;

  int iLeftPg;                    /* Index of leftmost page used in balance */
  int nIn = 1;                    /* Number of input peers for balance */
  int ii;                         /* Iterator used for various things */
  int nOut = 1;                   /* Number of output peers */
  int szEntry = 0;
  int iEntry0 = 0;

  HctDbCellSz *aSz = 0;
  int nSzAlloc = 0;
  int nSz = 0;
  int iSz = 0;

  u8 *aPgCopy[5];
  u8 *pFree = 0;

  int nRem;

  int aPgRem[5];
  int aPgFirst[6];

  /* If the HctDbWriter.aWritePg[] array still contains a single page, load
  ** some peer pages into it. */
  if( !bSingle ){
    rc = hctDbLoadPeers(pDb, p, &iPg);
    if( rc!=SQLITE_OK ){
      return rc;
    }
  }

  /* Determine the subset of HctDbWriter.aWritePg[] pages that will be 
  ** rebalanced. Variable nIn is set to the number of input pages, and
  ** iLeftPg to the index of the leftmost of them.  */
  iLeftPg = iPg;
  if( iPg==0 ){
    nIn = MIN(p->nWritePg, 3);
  }else if( iPg==p->nWritePg-1 ){
    nIn = MIN(p->nWritePg, 3);
    iLeftPg -= (nIn-1);
  }else{
    nIn = 3;
    iLeftPg--;
  }

  /* Figure out an upper limit on how many cells there will be involved
  ** in the rebalance. This is used to size the allocation of aSz[] only,
  ** the actual number of cells is calculated by hctDbBalanceGetCellSz(). */
  nSzAlloc = 1;
  for(ii=iLeftPg; ii<iLeftPg+nIn; ii++){
    HctDbPageHdr *pPg = (HctDbPageHdr*)p->aWritePg[ii].aNew;
    nSzAlloc += pPg->nEntry;
  }

  /* Allocate enough space for the cell-size array and for a copy of 
  ** each input page. */
  pFree = (u8*)sqlite3MallocZero(nIn*pDb->pgsz + nSzAlloc*sizeof(HctDbCellSz));
  if( pFree==0 ) return SQLITE_NOMEM;
  aSz = (HctDbCellSz*)&pFree[pDb->pgsz * nIn];

  /* Make a copy of each input page */
  for(ii=0; ii<nIn; ii++){
    aPgCopy[ii] = &pFree[pDb->pgsz * ii];
    memcpy(aPgCopy[ii], p->aWritePg[iLeftPg+ii].aNew, pDb->pgsz);
  }

  /* Populate the aSz[] array with the sizes and locations of each cell */
  hctDbBalanceGetCellSz(
      pDb, iPg-iLeftPg, iIns, bClobber, nNewCell, aPgCopy, nIn, aSz, &nSz
  );
  for(ii=1; ii<nSz; ii++){
    assert( aSz[ii].nByte>0 );
  }

  /* Figure out how many output pages will be required. This loop calculates
  ** a mapping heavily biased to the left. */
  aPgFirst[0] = 0;
  if( bSingle ){
    nOut = 1;
  }else{
    assert( sizeof(HctDbIntkeyLeaf)==sizeof(HctDbIndexLeaf) );
    nRem = pDb->pgsz - sizeof(HctDbIntkeyLeaf);
    for(iSz=0; iSz<nSz; iSz++){
      if( aSz[iSz].nByte>nRem ){
        aPgRem[nOut-1] = nRem;
        aPgFirst[nOut] = iSz;
        nOut++;
        nRem = pDb->pgsz - sizeof(HctDbIntkeyLeaf);
        assert( nOut<=ArraySize(aPgRem) );
      }
      nRem -= aSz[iSz].nByte;
    }
    aPgRem[nOut-1] = nRem;
  }
  aPgFirst[nOut] = nSz;

  /* Adjust the packing calculated by the previous loop. */
  for(ii=nOut-1; ii>0; ii--){
    /* Try to shift cells from output page (ii-1) to output page (ii). Shift
    ** cells for as long as (a) there is more free space on page (ii) than on
    ** page (ii-1), and (b) there is enough free space on page (ii) to fit
    ** the last cell from page (ii-1).  */
    while( aPgRem[ii]>aPgRem[ii-1] ){       /* condition (a) */
      HctDbCellSz *pLast = &aSz[aPgFirst[ii]-1];
      if( pLast->nByte>aPgRem[ii] ) break;  /* condition (b) */
      aPgRem[ii] -= pLast->nByte;
      aPgRem[ii-1] += pLast->nByte;
      aPgFirst[ii] = (pLast - aSz);
    }
  }

  /* Allocate any required new pages and link them into the list. */
  rc = hctDbExtendWriteArray(pDb, p, iLeftPg+1, nOut-nIn);
assert( rc==SQLITE_OK );

  /* Populate the output pages */
  iEntry0 = hctDbEntryArrayDim(aPgCopy[0], &szEntry);
  for(ii=0; ii<nOut; ii++){
    int iIdx = ii+iLeftPg;
    u8 *aTarget = p->aWritePg[iIdx].aNew;
    HctDbIndexLeaf *pLeaf = (HctDbIndexLeaf*)aTarget;
    int iOff = pDb->pgsz;         /* Start of data area in aTarget[] */
    int iLast = (ii==(nOut-1) ? nSz : aPgFirst[ii+1]);
    int nNewEntry = 0;            /* Number of entries on this output page */
    int i2;

    for(i2=0; i2<(iLast - aPgFirst[ii]); i2++){
      HctDbCellSz *pSz = &aSz[aPgFirst[ii] + i2];
      if( pSz->iPg>=0 ){
        u8 *aETo = &aTarget[iEntry0 + nNewEntry*szEntry];
        const u8 *aData = 0;
        int nCopy = pSz->nByte - szEntry;

        u8 *aOldPg = aPgCopy[pSz->iPg];
        u8 *aEFrom = &aOldPg[iEntry0 + pSz->iEntry*szEntry];
        memcpy(aETo, aEFrom, szEntry);
        aData = &aOldPg[((HctDbIndexEntry*)aEFrom)->iOff];

        iOff -= nCopy;
        ((HctDbIndexEntry*)aETo)->iOff = iOff;
        memcpy(&aTarget[iOff], aData, nCopy);
        nNewEntry++;
      }else{
        *piPg = iIdx;
        *piInsert = i2;
      }
    }

    pLeaf->pg.nEntry = nNewEntry;
    pLeaf->hdr.nFreeBytes = iOff - (iEntry0 + nNewEntry*szEntry);
    pLeaf->hdr.nFreeGap = iOff - (iEntry0 + nNewEntry*szEntry);
  }

  sqlite3_free(pFree);
  return rc;
}

/*
** Return true if page aPg[] can be rebuilt so that the free-gap is 
** at least nReq bytes in size. Or false otherwise.
*/
static int hctDbTestPageCapacity(HctDatabase *pDb, u8 *aPg, int nReq){
  u64 iSafeTid = sqlite3HctFileSafeTID(pDb->pFile);
  HctDbIndexNode *p = (HctDbIndexNode*)aPg;
  int szEntry = 0;
  int nFree = p->hdr.nFreeBytes;
  int ii;

  hctDbEntryArrayDim(aPg, &szEntry);
  for(ii=1; ii<p->pg.nEntry && nFree<nReq; ii++){
    int flags = 0;
    int iOff = hctDbEntryInfo((void*)aPg, ii, 0, &flags);
    if( flags & HCTDB_IS_DELETE ){
      u64 iTid = hctGetU64(&aPg[iOff]);
      if( iTid<=iSafeTid ){
        nFree += szEntry + hctDbPageRecordSize(aPg, pDb->pgsz, ii);
      }
    }
  }

  return nFree>=nReq;
}

static int hctDbBalanceIntkeyNode(
  HctDatabase *pDb,
  HctDbWriter *p,
  int iPg,
  int iInsert,                    /* Index in iPg for new key, if any */
  i64 iKey,                       /* Integer key value */
  u32 iChildPg                    /* The child pgno */
){
  int nMax = hctDbMaxCellsPerIntkeyNode(pDb->pgsz);
  int rc = SQLITE_OK;
  int nIn;                        /* Number of input pages */
  int nOut;                       /* Number of output pages */
  int iLeftPg;                    /* Index of left-most page in balance */
  int ii;                         /* Iterator variable */
  int nTotal = 0;                 /* Total number of keys for balance */
  u8 *aPgCopy[3];
  u8 *pFree = 0;

  assert( p->aWritePg[p->nWritePg-1].aNew );
  rc = hctDbLoadPeers(pDb, p, &iPg);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  iLeftPg = iPg;
  if( iPg==0 ){
    nIn = MIN(p->nWritePg, 3);
  }else if( iPg==p->nWritePg-1 ){
    nIn = MIN(p->nWritePg, 3);
    iLeftPg -= (nIn-1);
  }else{
    nIn = MIN(p->nWritePg, 3);
    iLeftPg--;
    assert( iLeftPg+nIn<=p->nWritePg );
  }

  /* Take a copy of each input page. Make the buffer used to store each
  ** copy larger than required by the size of one entry. Then, there is
  ** a new entry to add in stack variables (iKey/iChildPg), add it to the
  ** copy of its page. This is to make the loop that populates the output
  ** pages below easier to write. A real candidate for optimization, this. */
  pFree = (u8*)sqlite3Malloc(nIn*(pDb->pgsz+sizeof(HctDbIntkeyNodeEntry)));
  if( pFree==0 ) return SQLITE_NOMEM;
  for(ii=0; ii<nIn; ii++){
    aPgCopy[ii] = &pFree[(pDb->pgsz + sizeof(HctDbIntkeyNodeEntry)) * ii];
    memcpy(aPgCopy[ii], p->aWritePg[iLeftPg+ii].aNew, pDb->pgsz);
  }
  if( iInsert>=0 ){
    HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)aPgCopy[iPg-iLeftPg];
    if( iInsert<pNode->pg.nEntry ){
      int nByte = sizeof(HctDbIntkeyNodeEntry) * (pNode->pg.nEntry-iInsert);
      memmove(&pNode->aEntry[iInsert+1], &pNode->aEntry[iInsert], nByte);
    }
    pNode->pg.nEntry++;
    pNode->aEntry[iInsert].iKey = iKey;
    pNode->aEntry[iInsert].iChildPg = iChildPg;
  }

  /* Figure out how many entries there are, in total */
  for(ii=0; ii<nIn; ii++){
    HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)aPgCopy[ii];
    nTotal += pNode->pg.nEntry;
  }

  /* Figure out how many output pages are required */
  nOut = (nTotal + (nMax-1)) / nMax;
  rc = hctDbExtendWriteArray(pDb, p, iLeftPg+1, nOut-nIn);
  assert( rc==SQLITE_OK );  /* todo */

  /* Populate the output pages */
  if( rc==SQLITE_OK ){
    int nRem = nTotal;
    int iIn = 0;
    int iInEntry = 0;

    for(ii=0; ii<nOut; ii++){
      HctDbIntkeyNodeEntry *pEntry;
      int nCell = nRem / (nOut-ii); /* Cells for this output page */
      HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)p->aWritePg[ii+iLeftPg].aNew;
      for(pEntry=pNode->aEntry; pEntry<&pNode->aEntry[nCell]; pEntry++){
        HctDbIntkeyNode *pIn = (HctDbIntkeyNode*)aPgCopy[iIn];
        *pEntry = pIn->aEntry[iInEntry++];
        if( iInEntry>=pIn->pg.nEntry ){
          iInEntry = 0;
          iIn++;
        }
      }
      pNode->pg.nEntry = nCell;
      nRem -= nCell;
    }
  }

  sqlite3_free(pFree);
  return rc;
}

/*
** This function handles the second part of an insert or delete operation
** on an internal intkey node key. The implementation is separate from the
** usual insert/delete routine because internal intkey nodes use fixed size
** records. The other three types of pages found in lists - intkey leaves,
** index leaves and index nodes - all use variable sized entries.
*/
static int hctDbInsertIntkeyNode(
  HctDatabase *pDb,
  HctDbWriter *p,
  int iPg,
  int iInsert,
  i64 iKey,                       /* Integer key value */
  u32 iChildPg,                   /* The child pgno */
  int bClobber,                   /* True to clobber entry iInsert */
  int bDel                        /* True for a delete operation */
){
  int nMax = hctDbMaxCellsPerIntkeyNode(pDb->pgsz);
  int nMin = hctDbMinCellsPerIntkeyNode(pDb->pgsz);
  HctDbIntkeyNode *pNode;
  int rc = SQLITE_OK;

  /* If bDel is set, then bClobber must also be set. */
  assert( bDel==0 || bClobber );

  pNode = (HctDbIntkeyNode*)p->aWritePg[iPg].aNew;
  if( (pNode->pg.nEntry>=nMax && bClobber==0 && bDel==0 ) ){
    /* Need to do a balance operation to make room for the new entry */
    rc = hctDbBalanceIntkeyNode(pDb, p, iPg, iInsert, iKey, iChildPg);
  }else if( bDel ){
    assert( iInsert<pNode->pg.nEntry );
    if( iInsert==0 ){
      rc = hctDbLoadPeers(pDb, p, &iPg);
      pNode = (HctDbIntkeyNode*)p->aWritePg[iPg].aNew;
    }
    if( rc==SQLITE_OK ){
      if( iInsert<(pNode->pg.nEntry-1) ){
        int nByte = sizeof(HctDbIntkeyNodeEntry) * (pNode->pg.nEntry-1-iInsert);
        memmove(&pNode->aEntry[iInsert], &pNode->aEntry[iInsert+1], nByte);
      }
      pNode->pg.nEntry--;
      if( iInsert==0 || pNode->pg.nEntry<nMin ){
        rc = hctDbBalanceIntkeyNode(pDb, p, iPg, -1, 0, 0);
      }
    }
  }else{
    if( bClobber==0 ){
      if( iInsert<pNode->pg.nEntry ){
        int nByte = sizeof(HctDbIntkeyNodeEntry) * (pNode->pg.nEntry-iInsert);
        memmove(&pNode->aEntry[iInsert+1], &pNode->aEntry[iInsert], nByte);
      }
      pNode->pg.nEntry++;
    }
    pNode->aEntry[iInsert].iKey = iKey;
    pNode->aEntry[iInsert].iChildPg = iChildPg;
    pNode->aEntry[iInsert].unused = 0;
  }

  return rc;
}


/*
** The buffer passed as the first
*/
static int hctDbFreegap(void *aPg){
  assert( 
      (hctPagetype(aPg)==HCT_PAGETYPE_INTKEY && hctPageheight(aPg)==0)
   || (hctPagetype(aPg)==HCT_PAGETYPE_INDEX)
  );
  return ((HctDbIndexNode*)aPg)->hdr.nFreeGap;
}

static int hctDbFreebytes(void *aPg){
  assert( 
      (hctPagetype(aPg)==HCT_PAGETYPE_INTKEY && hctPageheight(aPg)==0)
   || (hctPagetype(aPg)==HCT_PAGETYPE_INDEX)
  );
  return ((HctDbIndexNode*)aPg)->hdr.nFreeBytes;
}

static int hctDbInsertOverflow(
  HctDatabase *pDb, 
  u8 *aTarget, 
  int nData, 
  const u8 *aData, 
  int *pnWrite,
  u32 *ppgOvfl
){
  int rc = SQLITE_OK;
  int nLocal = hctDbLocalsize(aTarget, pDb->pgsz, nData);

  if( nLocal==nData ){
    *pnWrite = nData;
    *ppgOvfl = 0;
  }else{
    const int sz = (pDb->pgsz - sizeof(HctDbPageHdr));
    int nRem;
    int nCopy;
    u32 iPg = 0;

    nRem = nData;
    nCopy = (nRem-nLocal) % sz;
    if( nCopy==0 ) nCopy = sz;
    while( rc==SQLITE_OK && nRem>nLocal ){
      HctFilePage pg;
      rc = sqlite3HctFilePageNewPhysical(pDb->pFile, &pg);
      if( rc==SQLITE_OK ){
        HctDbPageHdr *pPg = (HctDbPageHdr*)pg.aNew;
        memset(pPg, 0, sizeof(HctDbPageHdr));
        pPg->iPeerPg = iPg;
        pPg->nEntry = nCopy;
        memcpy(&pPg[1], &aData[nRem-nCopy], nCopy);
        iPg = pg.iNewPg;
        sqlite3HctFilePageRelease(&pg);
      }
      nRem -= nCopy;
      nCopy = sz;
    }

    *ppgOvfl = iPg;
    *pnWrite = nLocal;
  }

  return rc;
}

static void hctDbRemoveCell(HctDatabase *pDb, u8 *aTarget, int iRem){
  HctDbIndexNode *p = (HctDbIndexNode*)aTarget;
  const int eType = hctPagetype(aTarget);
  const int nHeight = hctPageheight(aTarget);
  const int pgsz = pDb->pgsz;

  int szEntry = 0;                /* Size of each entry in aEntry[] array */
  int iArrayOff = 0;              /* Offset of aEntry array in aTarget */
  int iData = 0;                  /* Offset of cell in aTarget[] */
  int nData = 0;                  /* Local size of cell to remove */

  /* Populate stack variables szEntry, iArrayOff, iData and nData. */
  assert( eType==HCT_PAGETYPE_INTKEY || eType==HCT_PAGETYPE_INDEX );
  assert( eType==HCT_PAGETYPE_INDEX || nHeight==0 );
  if( eType==HCT_PAGETYPE_INTKEY ){
    HctDbIntkeyEntry *pEntry = &((HctDbIntkeyLeaf*)aTarget)->aEntry[iRem]; 
    iData = pEntry->iOff;
    nData = hctDbIntkeyEntrySize(pEntry, pgsz);
    szEntry = sizeof(*pEntry);
    iArrayOff = sizeof(HctDbIntkeyLeaf);
  }else if( nHeight==0 ){
    HctDbIndexEntry *pEntry = &((HctDbIndexLeaf*)aTarget)->aEntry[iRem]; 
    iData = pEntry->iOff;
    nData = hctDbIndexEntrySize(pEntry, pgsz);
    szEntry = sizeof(*pEntry);
    iArrayOff = sizeof(HctDbIndexLeaf);
  }else{
    HctDbIndexNodeEntry *pEntry = &((HctDbIndexNode*)aTarget)->aEntry[iRem]; 
    iData = pEntry->iOff;
    nData = hctDbIndexNodeEntrySize(pEntry, pgsz);
    szEntry = sizeof(*pEntry);
    iArrayOff = sizeof(HctDbIndexNode);
  }

  /* Remove the aEntry[] array entry */
  if( iRem<p->pg.nEntry-1 ){
    u8 *aTo = &aTarget[iArrayOff + iRem*szEntry];
    memmove(aTo, &aTo[szEntry], (p->pg.nEntry-iRem-1) * szEntry);
  }
  p->pg.nEntry--;
  p->hdr.nFreeBytes += szEntry;
  p->hdr.nFreeGap += szEntry;

  /* Remove the cell from the data area */
  if( iData==(iArrayOff + szEntry*p->pg.nEntry + p->hdr.nFreeGap) ){
    int ii;
    int iFirst = pDb->pgsz;
    p->hdr.nFreeGap += nData;
    for(ii=0; ii<p->pg.nEntry; ii++){
      int iOff = ((HctDbIndexEntry*)&aTarget[iArrayOff + szEntry*ii])->iOff;
      if( iOff && iOff<iFirst ) iFirst = iOff;
    }
    p->hdr.nFreeGap = iFirst - (iArrayOff + szEntry*p->pg.nEntry);
  }
  p->hdr.nFreeBytes += nData;


}

static void hctDbInsertEntry(
  HctDatabase *pDb,
  u8 *aTarget,
  int iIns,
  const u8 *aEntry,
  int nEntry
){
  HctDbIndexNode *p = (HctDbIndexNode*)aTarget;
  int szEntry = 0;                /* Size of each entry in aEntry[] array */
  int iEntry0 = 0;              /* Offset of aEntry array in aTarget */
  int iOff = 0;                   /* Offset of new cell data in aTarget */
  u8 *aFrom = 0;

  iEntry0 = hctDbEntryArrayDim(aTarget, &szEntry);

  /* This might fail if the db is corrupt */
  assert( p->hdr.nFreeGap>=(nEntry + szEntry) );

  /* Insert the new zeroed entry into the aEntry[] array */
  aFrom = &aTarget[iEntry0 + szEntry*iIns];
  if( iIns<p->pg.nEntry ){
    memmove(&aFrom[szEntry], aFrom, (p->pg.nEntry-iIns) * szEntry);
    memset(aFrom, 0, szEntry);
  }
  p->hdr.nFreeBytes -= szEntry;
  p->hdr.nFreeGap -= szEntry;
  p->pg.nEntry++;

  /* Insert the cell into the data area */ 
  iOff = iEntry0 + p->pg.nEntry*szEntry + p->hdr.nFreeGap - nEntry;
  memcpy(&aTarget[iOff], aEntry, nEntry);
  p->hdr.nFreeBytes -= nEntry;
  p->hdr.nFreeGap -= nEntry;

  /* Set the aEntry[].iOff field */
  ((HctDbIndexEntry*)aFrom)->iOff = iOff;
}

/*
** Parameter aTarget points to a buffer containing an intkey or index
** internal node. Return the child-page number for entry iInsert on
** that page.
*/
u32 hctDbGetChildPage(u8 *aTarget, int iInsert){
  const int eType = hctPagetype(aTarget);
  u32 iChildPg;
  if( eType==HCT_PAGETYPE_INTKEY ){
    iChildPg = ((HctDbIntkeyNode*)aTarget)->aEntry[iInsert].iChildPg;
  }else{
    assert( eType==HCT_PAGETYPE_INDEX );
    iChildPg = ((HctDbIndexNode*)aTarget)->aEntry[iInsert].iChildPg;
  }
  return iChildPg;
}

static int hctDbInsert(
  HctDatabase *pDb,
  HctDbWriter *p,
  u32 iRoot,
  UnpackedRecord *pRec,           /* The key value for index tables */
  i64 iKey,                       /* For intkey tables, the key value */
  u32 iChildPg,                   /* For internal node ops, the child pgno */
  int bDel,                       /* True for a delete operation */
  int nData, const u8 *aData      /* Record/key to insert */
){
  int rc = SQLITE_OK;
  int iInsert;
  int iPg = 0;                    /* Page in HctDbWriter.aWritePg[] to write */
  int bClobber = 0;
  u8 *aTarget;                    /* Page to write new entry to */
  int nWrite = nData;

  int bFullDel = 0;               /* True to do a real delete from leaf */

  u8 entryFlags = 0;              /* Flags for page entry added by this call */
  u8 *aEntry = 0;                 /* Buffer containing formatted entry */
  int nEntry = 0;                 /* Size of aEntry[] */
  int nEntrySize = 0;             /* Value for page header nSize field */
  int bBalance = 0;               /* True if requires a rebalance operation */
  int bSingle = 0;               /* True if requires a rebalance operation */

static int nCall = 0;
nCall++;

  p->nWriteKey++;

  /* Check if any existing dirty pages need to be flushed to disk before 
  ** this key can be inserted. If they do, flush them. */
  assert( p->nWritePg==0 || iRoot==p->writecsr.iRoot );
  if( p->nWritePg ){
    if( p->nWritePg>HCTDB_MAX_DIRTY || hctDbTestWriteFpKey(p, pRec, iKey) ){
      rc = hctDbInsertFlushWrite(pDb, p);
      if( rc ) return rc;
      p->nWriteKey = 1;
    }
  }

  /* If the page array is empty, seek the write cursor to find the leaf
  ** page on which to insert this new entry or delete key. */
  if( p->nWritePg==0 ){
    hctDbCsrInit(pDb, iRoot, &p->writecsr);
    if( pRec ) p->writecsr.pKeyInfo = pRec->pKeyInfo;
    rc = hctDbCsrSeek(&p->writecsr, p->iHeight, pRec, iKey, 0);
    if( rc ) return rc;

    p->aWritePg[0] = p->writecsr.pg;
    memset(&p->writecsr.pg, 0, sizeof(HctFilePage));
    p->nWritePg = 1;
    rc = sqlite3HctFilePageWrite(&p->aWritePg[0]);
    if( rc ) return rc;
    memcpy(p->aWritePg[0].aNew, p->aWritePg[0].aOld, pDb->pgsz);
    rc = hctDbSetWriteFpKey(pDb, p);
    if( rc ) return rc;
    
    /* TODO: Do not like this. HctFilePage.iOldPg should not be accessed
    ** outside of hct_file.c. */
    p->iOldPgno = p->aWritePg[0].iOldPg;
  }
  assert( p->nWritePg>0 && p->aWritePg[0].aNew );

  /* Figure out which page in the HctDbWriter.aWritePg[] array the new entry
  ** belongs on. TODO: This can be optimized by remembering which page the
  ** previous key was stored on.  This block sets stack variables:
  **
  **   iPg:      Index of page in HctDbWriter.aWritePg[] to write to.
  **   iInsert:  The index of the new (or overwritten) entry within the page.
  **   bClobber: True if this write overwrites an existing key.
  **   aTarget:  Pointer to aNew[] buffer of page iPg.
  */
  if( pRec ){
    HctBuffer buf = {0,0,0};
    for(iPg=0; iPg<p->nWritePg-1; iPg++){
      const u8 *aK;
      int nK;
      rc = hctDbLoadRecord(pDb, &buf, p->aWritePg[iPg+1].aNew, 0, &nK, &aK);
      if( rc!=SQLITE_OK ){
        hctBufferFree(&buf);
        return rc;
      }
      if( sqlite3VdbeRecordCompare(nK, aK, pRec)>0 ) break;
    }
    hctBufferFree(&buf);
    rc = hctDbIndexSearch(pDb,
        p->aWritePg[iPg].aNew, pRec, &iInsert, &bClobber
    );
    if( rc!=SQLITE_OK ) return rc;
  }else{
    for(iPg=0; iPg<p->nWritePg-1; iPg++){
      if( hctDbIntkeyFPKey(p->aWritePg[iPg+1].aNew)>iKey ) break;
    }
    if( p->iHeight==0 ){
      iInsert = hctDbIntkeyLeafSearch(p->aWritePg[iPg].aNew, iKey, &bClobber);
    }else{
      iInsert = hctDbIntkeyNodeSearch(p->aWritePg[iPg].aNew, iKey, &bClobber);
    }
  }
  aTarget = p->aWritePg[iPg].aNew;

  /* At this point, once the page that will be modified has been loaded
  ** and marked as writable, if the operation is on an internal list:
  **
  **   1) For an insert, check if the child page has already been marked
  **      as EVICTED by some other client. If so, return early.
  **
  **   2) For a delete, check that there is an entry to delete. And if so,
  **      that the value of its child-page field matches iChildPg. If
  **      not, return early.
  */
  assert( rc==SQLITE_OK );
  if( p->iHeight>0 ){
    if( bDel==0 && sqlite3HctFilePageIsEvicted(pDb->pFile, iChildPg) ){
      return SQLITE_OK;
    }
    if( bDel ){
      if( bClobber==0 ){
        return SQLITE_OK;
      }else{
        u32 iChild = hctDbGetChildPage(aTarget, iInsert);
        if( iChild!=iChildPg ) return SQLITE_OK;
      }
    }
  }

  /* Writes to an intkey internal node are handled separately. They are
  ** different because they used fixed size key/data pairs. All other types
  ** of page use variably sized key/data entries. */
  if( pRec==0 && p->iHeight>0 ){
    return hctDbInsertIntkeyNode(
        pDb, p, iPg, iInsert, iKey, iChildPg, bClobber, bDel
    );
  }

  /* Special handling for two cases when writing to leaf lists only:
  **
  **   1) This is a rollback (HctDatabase.bRollback is set): 
  **
  **     * If this write is not a clobber, or it is a clobber of
  **       a database key with some other TID value, then the rollback
  **       operation is over. Return SQLITE_DONE to the caller.
  **
  **     * Otherwise, if this is a clobber of a key with the same TID
  **       value as the current transaction, load the old data to be
  **       re-inserted.
  **
  **   2) This not a rollback (HctDatabase.bRollback is clear):
  **
  **     * If this is a clobber, check that the key being clobbered
  **       was visible when the current transaction was run. If it
  **       was not, we have a write conflict! Abandon the current
  **       write operation and return SQLITE_BUSY.
  */
  if( p->iHeight==0 ){
    if( bClobber ){
      u64 iClobberTid = 0;
      int flags = 0;
      int iOff = hctDbEntryInfo(aTarget, iInsert, 0, &flags);
      if( flags & HCTDB_HAS_TID ){
        iClobberTid = hctGetU64(&aTarget[iOff]);
      }

      if( pDb->bRollback ){
        if( iClobberTid!=pDb->iTid ){
          rc = SQLITE_DONE;
        }else{
          u32 iOldPgno = 0;
          assert( flags & HCTDB_HAS_TID );
          if( flags & HCTDB_HAS_OLD ){
            iOldPgno = hctGetU32(&aTarget[iOff+8]);
          }

          if( iOldPgno==0 ){
            /* This occurs when rolling back an entry with the HCTDB_HAS_OLD
            ** flag clear. In this case the entry should simply be removed.  */
            bFullDel = bDel = 1;
            aData = 0;
            nWrite = nData = 0;
          }else{
            rc = hctDbFindRollbackData(pDb, iKey, pRec, iOldPgno, 
                &entryFlags, &nEntrySize, &nEntry, (const u8**)&aEntry
            );
            assert( nData==0 || aData!=0 );
            assert( nEntry<pDb->pgsz );
          }
        }
      }else{
        /* Check for a write conflict */
        if( hctDbTidIsConflict(pDb, iClobberTid) ){
          rc = SQLITE_BUSY;
        }
      }
    }else if( pDb->bRollback ){
      rc = SQLITE_DONE;
    }
  }else{
    /* If iHeight>0 */
    bFullDel = bDel;
  }

  if( rc ){
    if( rc==SQLITE_DONE ){
      hctDbInsertFlushWrite(pDb, p);
    }else{
      hctDbWriterCleanup(pDb, p, 1);
    }
    return rc;
  }

  /* If this is a rollback operation, then any required overflow pages have
  ** already been populated (the rolled back entry uses the same overflow
  ** chain as the original did). As have the following variables:
  **
  **   entryFlags
  **   aEntry
  **   nEntry
  **   nEntrySize
  **
  ** This block populates any overflow pages and the above variables for
  ** all other cases. */
  if( (p->iHeight!=0 || pDb->bRollback==0) && bFullDel==0 ){
    u32 pgOvfl = 0;
    i64 iTid = 0;
    u32 iOld = 0;

    if( p->iHeight==0 ){
      iTid = pDb->iTid;

      /* TODO: THIS IS INCORRECT. NEED TO SEARCH THE OLD-PAGE ARRAY TO
      ** FIND THE CORRECT iOld VALUE IN ALL CASES.  */
      if( bClobber ) iOld = p->iOldPgno;
    }

    rc = hctDbInsertOverflow(pDb, aTarget, nData, aData, &nWrite, &pgOvfl);
    assert( rc==SQLITE_OK );  /* todo */

    aEntry = pDb->aTmp;
    nEntry = hctDbCellPut(aEntry, iTid, iOld, pgOvfl, aData, nWrite);
    nEntrySize = nData;
    entryFlags =  (iTid ? HCTDB_HAS_TID : 0);
    entryFlags |= (iOld ? HCTDB_HAS_OLD : 0);
    entryFlags |= (bDel ? HCTDB_IS_DELETE : 0);
    entryFlags |= (pgOvfl ? HCTDB_HAS_OVFL : 0);
  }

  /* There are now two choices - either the aTarget[] page can be updated
  ** directly (if the new entry fits on the page), or the balance-tree()
  ** routine runs to redistribute cells between aTarget[] and its peers,
  ** writing the new entry at the same time. A balance is required if:
  **
  **   1) there is insufficient space in the free-gap for any new
  **      cell and array entry, or
  **
  **   2) this is a full-delete of the fpkey of the page (iInsert==0), or
  **
  **   3) this operation would leave the page underfull, and it is not
  **      the only page in its list.
  */
  {
    int szEntry = hctDbPageEntrySize(aTarget);
    int nFree = hctDbFreebytes(aTarget);
    int nReq = 0;
    int bIsOnly = 0;
    if( bClobber ){
      nFree += hctDbPageRecordSize(aTarget, pDb->pgsz, iInsert);
      nFree += szEntry;
    }
    if( bFullDel==0 ){
      nFree -= nEntry;
      nFree -= szEntry;
      nReq = nEntry + (bClobber ? 0 : szEntry);
    }
    bIsOnly = hctIsLeftmost(aTarget) && ((HctDbPageHdr*)aTarget)->iPeerPg==0;

    if( (bIsOnly==0 && iInsert==0 && bFullDel) || (nFree>(2*pDb->pgsz/3)) ){
      bBalance = 1;
    }else if( hctDbFreegap(aTarget)<nReq ){
      bBalance = 1;
      if( p->nWritePg==1 ) bSingle = hctDbTestPageCapacity(pDb, aTarget, nReq);
    }
  }

  if( bBalance ){
    assert( bFullDel==0 || aEntry==0 );
    assert( bFullDel==0 || nEntry==0 );
    assert_page_is_ok(aTarget, pDb->pgsz);
    rc = hctDbBalance(pDb, p, bSingle, &iPg, &iInsert, bClobber, nEntry);
    assert_page_is_ok(aTarget, pDb->pgsz);
    aTarget = p->aWritePg[iPg].aNew;
  }else{
    if( bClobber ){
      assert_page_is_ok(aTarget, pDb->pgsz);
      hctDbRemoveCell(pDb, aTarget, iInsert);
      assert_page_is_ok(aTarget, pDb->pgsz);
    }
  }

  /* Unless this is a full-delete operation, update rest of the aEntry[]
  ** entry fields for the new cell. */
  if( rc==SQLITE_OK && bFullDel==0 ){
    int eType = hctPagetype(aTarget);
    assert_page_is_ok(aTarget, pDb->pgsz);
    
//char *z = sqlite3_mprintf("before %d (physical=%d)", p->aWritePg[iPg].iPg, p->aWritePg[iPg].iNewPg);
// print_out_page(z, aTarget, pDb->pgsz);
    hctDbInsertEntry(pDb, aTarget, iInsert, aEntry, nEntry);

    assert( (pRec==0)==(eType==HCT_PAGETYPE_INTKEY) );
    if( eType==HCT_PAGETYPE_INTKEY ){
      HctDbIntkeyEntry *pE = &((HctDbIntkeyLeaf*)aTarget)->aEntry[iInsert];
      pE->iKey = iKey;
      pE->nSize = nEntrySize;
      pE->flags = entryFlags;
    }else if( p->iHeight==0 ){
      HctDbIndexEntry *pE = &((HctDbIndexLeaf*)aTarget)->aEntry[iInsert];
      pE->nSize = nEntrySize;
      pE->flags = entryFlags;
    }else{
      HctDbIndexNodeEntry *pE = &((HctDbIndexNode*)aTarget)->aEntry[iInsert];
      pE->nSize = nEntrySize;
      pE->flags = entryFlags;
      pE->iChildPg = iChildPg;
    }
//z = sqlite3_mprintf("after %d (physical=%d)", p->aWritePg[iPg].iPg, p->aWritePg[iPg].iNewPg);
//print_out_page(z, aTarget, pDb->pgsz);
  }

  if( rc!=SQLITE_OK ){
    hctDbWriterCleanup(pDb, p, 1);
  }
  assert_page_is_ok(aTarget, pDb->pgsz);
  return rc;
}

int sqlite3HctDbInsert(
  HctDatabase *pDb,               /* Database to insert into or delete from */
  u32 iRoot,                      /* Root page of table to modify */
  UnpackedRecord *pRec,           /* The key value for index tables */
  i64 iKey,                       /* For intkey tables, the key value */
  int bDel,                       /* True for a delete, false for insert */
  int nData, const u8 *aData,     /* Record/key to insert */
  int *pnRetry                    /* OUT: number of operations to retry */
){
  int rc;
  int nRecField = pRec ? pRec->nField : 0;

  /* If this operation is inserting an index entry, figure out how many of
  ** the record fields to consider when determining if a potential write
  ** collision is found in the data structure.  */
  hctDbRecordTrim(pRec);

  rc = hctDbInsert(pDb, &pDb->pa, iRoot, pRec, iKey, 0, bDel, nData, aData);
  if( rc==SQLITE_LOCKED ){
    rc = SQLITE_OK;
    *pnRetry = pDb->pa.nWriteKey;
    pDb->pa.nWriteKey = 0;
    pDb->nCasFail++;
  }else{
    *pnRetry = 0;
  }

  if( pRec ) pRec->nField = nRecField;
  return rc;
}

/*
** Start the write-phase of a transaction.
*/
int sqlite3HctDbStartWrite(HctDatabase *p, u64 *piTid){
  int rc = SQLITE_OK;
  HctTMapClient *pTMapClient = sqlite3HctFileTMapClient(p->pFile);

  assert( p->iTid==0 );
  assert( p->bRollback==0 );
  memset(&p->pa, 0, sizeof(p->pa));

  p->iTid = sqlite3HctFileAllocateTransid(p->pFile);
  rc = sqlite3HctTMapNewTID(pTMapClient, p->iSnapshotId, p->iTid, &p->pTmap);
  *piTid = p->iTid;
  return rc;
}

static u64 *hctDbFindTMapEntry(HctTMap *pTmap, u64 iTid){
  int iMap, iEntry;
  assert( pTmap->iFirstTid<=iTid );
  assert( pTmap->iFirstTid+(pTmap->nMap*HCT_TMAP_PAGESIZE)>iTid );
  iMap = (iTid - pTmap->iFirstTid) / HCT_TMAP_PAGESIZE;
  iEntry = (iTid - pTmap->iFirstTid) % HCT_TMAP_PAGESIZE;
  return &pTmap->aaMap[iMap][iEntry];
}

/*
** This is called once the current transaction has been completely 
** written to disk and validated. The CID is passed as the second argument.
** Or, if the transaction was abandoned and rolled back, iCid is passed
** zero.
*/
int sqlite3HctDbEndWrite(HctDatabase *p, u64 iCid){
  int rc = SQLITE_OK;
  u64 *pEntry = hctDbFindTMapEntry(p->pTmap, p->iTid);

  assert( p->bRollback==0 );
  assert( p->pa.nWritePg==0 );
  assert( p->pa.aWriteFpKey==0 );

  HctAtomicStore(pEntry, iCid | HCT_TMAP_COMMITTED);

  p->iTid = 0;
  return rc;
}

static void hctDbFreeCsr(HctDbCsr *pCsr){
  while( pCsr->intkey.pOpList ){
    HctCsrIntkeyOp *pOp = pCsr->intkey.pOpList;
    pCsr->intkey.pOpList = pOp->pNextOp;
    sqlite3_free(pOp);
  }
  while( pCsr->index.pOpList ){
    HctCsrIndexOp *pOp = pCsr->index.pOpList;
    pCsr->index.pOpList = pOp->pNextOp;
    if( pOp->pLast!=pOp->pFirst ){
      sqlite3_free(pOp->pLast);
    }
    sqlite3_free(pOp->pFirst);
    sqlite3_free(pOp);
  }
  if( pCsr->pRec ) sqlite3DbFree(pCsr->pKeyInfo->db, pCsr->pRec);
  hctBufferFree(&pCsr->rec);
  sqlite3_free(pCsr);
}

static void hctDbFreeCsrList(HctDbCsr *pList){
  HctDbCsr *pNext = pList;
  while( pNext ){
    HctDbCsr *pDel = pNext;
    pNext = pNext->pNextScanner;
    hctDbFreeCsr(pDel);
  }
}

int sqlite3HctDbEndRead(HctDatabase *pDb){
  HctTMapClient *pTMapClient = sqlite3HctFileTMapClient(pDb->pFile);
  assert( (pDb->iSnapshotId==0)==(pDb->pTmap==0) );
  hctDbFreeCsrList(pDb->pScannerList);
  pDb->pScannerList = 0;
  if( pDb->iSnapshotId ){
    sqlite3HctTMapEnd(pTMapClient, pDb->iSnapshotId);
    pDb->pTmap = 0;
    pDb->iSnapshotId = 0;
  }
  return SQLITE_OK;
}

/*
** If recovery is still required, this function grabs the file-server
** mutex and returns non-zero. Or, if recovery is not required, returns
** zero without grabbing the mutex.
*/
int sqlite3HctDbStartRecovery(HctDatabase *pDb){
  assert( pDb->bRollback==0 );
  if( sqlite3HctFileStartRecovery(pDb->pFile) ){
    pDb->bRollback = 1;
  }
  return pDb->bRollback;
}

void sqlite3HctDbRecoverTid(HctDatabase *pDb, u64 iTid){
  pDb->iTid = iTid;
}

int sqlite3HctDbFinishRecovery(HctDatabase *pDb, int rc){
  pDb->iTid = 0;
  pDb->bRollback = 0;
  return sqlite3HctFileFinishRecovery(pDb->pFile, rc);
}

/*
** Open a cursor.
*/
int sqlite3HctDbCsrOpen(
  HctDatabase *pDb, 
  KeyInfo *pKeyInfo,
  u32 iRoot, 
  HctDbCsr **ppCsr
){
  int rc = SQLITE_OK;
  HctDbCsr *p;

  /* Search for an existing cursor that can be reused. */
  HctDbCsr **pp;
  for(pp=&pDb->pScannerList; *pp; pp=&(*pp)->pNextScanner){
    if( (*pp)->iRoot==iRoot ){
      *ppCsr = *pp;
      *pp = (*pp)->pNextScanner;
      return SQLITE_OK;
    }
  }

  /* If no existing cursor was found, allocate a new one */
  p = (HctDbCsr*)sqlite3MallocZero(sizeof(HctDbCsr));
  if( p==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    p->pDb = pDb;
    p->iRoot = iRoot;
    p->iCell = -1;
    p->pKeyInfo = pKeyInfo;
    hctDbSnapshotOpen(pDb);
  }
  *ppCsr = p;
  return rc;
}

/*
** Close a cursor opened with sqlite3HctDbCsrOpen().
*/
void sqlite3HctDbCsrClose(HctDbCsr *pCsr){
  if( pCsr ){
    HctDatabase *pDb = pCsr->pDb;
    hctDbCsrScanFinish(pCsr);
    hctDbCsrReset(pCsr);
    if( pDb->iTid==0 ){
      pCsr->pNextScanner = pDb->pScannerList;
      pDb->pScannerList = pCsr;
    }else{
      hctDbFreeCsr(pCsr);
    }
  }
}

/*
** The cursor passed as the first argument must be open on an intkey
** table and pointed at a valid entry. This function sets output variable
** (*piKey) to the integer key value associated with that entry before 
** returning.
*/
void sqlite3HctDbCsrKey(HctDbCsr *pCsr, i64 *piKey){
  HctDbIntkeyLeaf *pPg;
  int iCell;
  if( pCsr->oldpg.aOld ){
    pPg = (HctDbIntkeyLeaf*)pCsr->oldpg.aOld;
    iCell = pCsr->iOldCell;
  }else{
    pPg = (HctDbIntkeyLeaf*)pCsr->pg.aOld;
    iCell = pCsr->iCell;
  }
  assert( iCell>=0 && iCell<pPg->pg.nEntry );
  assert( hctPagetype(pPg)==HCT_PAGETYPE_INTKEY );
  assert( hctPageheight(pPg)==0 );
  *piKey = pPg->aEntry[iCell].iKey;
}

/*
** Return true if the cursor is at EOF. Otherwise false.
*/
int sqlite3HctDbCsrEof(HctDbCsr *pCsr){
  return pCsr==0 || pCsr->iCell<0;
}

/*
** Set the cursor to point to the first entry in its table. If it is
** stepped, this cursor will be stepped with sqlite3HctDbCsrNext().
*/
int sqlite3HctDbCsrFirst(HctDbCsr *pCsr){
  int rc = SQLITE_OK;

  rc = hctDbCsrScanFinish(pCsr);
  if( rc==SQLITE_OK ){
    hctDbCsrReset(pCsr);
    pCsr->eDir = BTREE_DIR_FORWARD;
    rc = hctDbCsrScanStart(pCsr, 0, SMALLEST_INT64);
  }
  pCsr->eDir = BTREE_DIR_FORWARD;

  if( rc==SQLITE_OK ){
    rc = hctDbCsrFirstValid(pCsr);
  }

  return rc;
}

/*
** Set the cursor to point to the last entry in its table. If it is
** stepped, this cursor will be stepped with sqlite3HctDbCsrPrev().
*/
int sqlite3HctDbCsrLast(HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  HctFile *pFile = pCsr->pDb->pFile;
  u32 iPg = pCsr->iRoot;
  HctDbPageHdr *pPg = 0;
  HctFilePage pg;

  rc = hctDbCsrScanFinish(pCsr);
  if( rc==SQLITE_OK ){
    hctDbCsrReset(pCsr);
    pCsr->eDir = BTREE_DIR_REVERSE;
    rc = hctDbCsrScanStart(pCsr, 0, LARGEST_INT64);
  }

  /* Find the last page in the leaf page list. */
  while( 1 ){
    rc = sqlite3HctFilePageGet(pFile, iPg, &pg);
    if( rc!=SQLITE_OK ) break;

    pPg = (HctDbPageHdr*)pg.aOld;
    if( pPg->iPeerPg ){
      iPg = pPg->iPeerPg;
    }else if( pPg->nHeight==0 ){
      break;
    }else if( hctPagetype(pPg)==HCT_PAGETYPE_INTKEY ){
      HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)pPg;
      iPg = pNode->aEntry[pPg->nEntry-1].iChildPg;
    }else{
      HctDbIndexNode *pNode = (HctDbIndexNode*)pPg;
      iPg = pNode->aEntry[pPg->nEntry-1].iChildPg;
    }
    sqlite3HctFilePageRelease(&pg);
  }

  /* Set the cursor to point to one position past the last entry on the
  ** page located above. Then call sqlite3HctDbCsrPrev() to step back to
  ** the first entry visible to the current transaction.  */
  if( rc==SQLITE_OK ){
    assert( pPg->nHeight==0 && pPg->iPeerPg==0 );
    memcpy(&pCsr->pg, &pg, sizeof(pg));
    pCsr->iCell = pPg->nEntry;
    rc = sqlite3HctDbCsrPrev(pCsr);
  }
  return rc;
}

static int hctDbCsrNext(HctDbCsr *pCsr){
  HctDbPageHdr *pPg = (HctDbPageHdr*)pCsr->pg.aOld;
  int rc = SQLITE_OK;

  assert( pCsr->iCell>=-1 && pCsr->iCell<pPg->nEntry );
  assert( pPg->nHeight==0 );

  pCsr->iCell++;
  if( pCsr->iCell==pPg->nEntry ){
    u32 iPeerPg = pPg->iPeerPg;
    if( iPeerPg==0 ){
      /* Main cursor is now at EOF */
      pCsr->iCell = -1;
      sqlite3HctFilePageRelease(&pCsr->pg);
    }else{
      /* Jump to peer page */
      rc = sqlite3HctFilePageRelease(&pCsr->pg);
      if( rc==SQLITE_OK ){
        rc = sqlite3HctFilePageGet(pCsr->pDb->pFile, iPeerPg, &pCsr->pg);
        pCsr->iCell = 0;
      }
    }
  }
  return rc;
}

int sqlite3HctDbCsrNext(HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  sqlite3HctFilePageRelease(&pCsr->oldpg);
  do {
    rc = hctDbCsrNext(pCsr);
  }while( hctDbCsrFindVersion(&rc, pCsr)==0 );
  return rc;
}

int sqlite3HctDbCsrPrev(HctDbCsr *pCsr){
  int rc = SQLITE_OK;

  sqlite3HctFilePageRelease(&pCsr->oldpg);

  do {
    /* Advance the cursor */
    pCsr->iCell--;
    if( pCsr->iCell<0 ){

      /* TODO: This can be made much faster by using parent list pointers. */
      if( pCsr->pKeyInfo ){
        UnpackedRecord *pRec = 0;
        rc = hctDbCsrLoadAndDecode(pCsr, 0, &pRec);
        if( rc==SQLITE_OK ){
          int bDummy;
          HctFilePage pg = pCsr->pg;
          memset(&pCsr->pg, 0, sizeof(HctFilePage));
          pRec->default_rc = 1;
          hctDbCsrSeek(pCsr, 0, pRec, 0, &bDummy);
          pRec->default_rc = 0;
          sqlite3HctFilePageRelease(&pg);
        }
      }else if( hctIsLeftmost(pCsr->pg.aOld)==0 ){
        i64 iKey = hctDbIntkeyFPKey(pCsr->pg.aOld);
        sqlite3HctFilePageRelease(&pCsr->pg);
        rc = hctDbCsrSeek(pCsr, 0, 0, iKey-1, 0);
      }
    }
  }while( rc==SQLITE_OK && hctDbCsrFindVersion(&rc, pCsr)==0 );

  return rc;
}

int sqlite3HctDbCsrData(HctDbCsr *pCsr, int *pnData, const u8 **paData){
  u8 *pPg;
  int iCell;
  if( pCsr->oldpg.aOld ){
    pPg = pCsr->oldpg.aOld;
    iCell = pCsr->iOldCell;
  }else{
    pPg = pCsr->pg.aOld;
    iCell = pCsr->iCell;
  }
  assert( hctPageheight(pPg)==0 );
  return hctDbLoadRecord(pCsr->pDb, &pCsr->rec, pPg, iCell, pnData, paData);
}

static int hctDbValidateEntry(HctDatabase *pDb, HctDbCsr *pCsr){
  u8 flags;
  int iOff = hctDbCellOffset(pCsr->pg.aOld, pCsr->iCell, &flags);
  if( flags & HCTDB_HAS_TID ){
    u64 iTid = hctGetU64(&pCsr->pg.aOld[iOff]);
    if( iTid!=pDb->iTid && hctDbTidIsConflict(pCsr->pDb, iTid) ){
      return SQLITE_BUSY;
    }
  }
  return SQLITE_OK;
}

static int hctDbValidateIntkey(HctDatabase *pDb, HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  HctCsrIntkeyOp *pOpList = pCsr->intkey.pOpList;
  HctCsrIntkeyOp *pOp;

  pCsr->intkey.pOpList = 0;
  assert( pCsr->intkey.pCurrentOp==0 );
  for(pOp=pOpList; pOp && rc==SQLITE_OK; pOp=pOp->pNextOp){
    assert( pOp->iFirst<=pOp->iLast );
    if( pOp->iFirst==SMALLEST_INT64 ){
      pCsr->eDir = BTREE_DIR_FORWARD;
      rc = hctDbCsrFirst(pCsr);
    }else{
      int bDummy = 0;
      if( pOp->iFirst==pOp->iLast ){
        pCsr->eDir = BTREE_DIR_NONE;
      }else{
        pCsr->eDir = BTREE_DIR_FORWARD;
      }
      rc = hctDbCsrSeek(pCsr, 0, 0, pOp->iFirst, &bDummy);
    }

    while( rc==SQLITE_OK && !sqlite3HctDbCsrEof(pCsr) ){
      i64 iKey = 0;
      sqlite3HctDbCsrKey(pCsr, &iKey);
      if( iKey>=pOp->iFirst && iKey<=pOp->iLast ){
        rc = hctDbValidateEntry(pDb, pCsr);
      }
      if( rc!=SQLITE_OK || iKey>=pOp->iLast ) break;
      rc = hctDbCsrNext(pCsr);
    }
    hctDbCsrReset(pCsr);
  }
  assert( pCsr->intkey.pOpList==0 && pCsr->intkey.pCurrentOp==0 );
  pCsr->intkey.pOpList = pOpList;

  return rc;
}

static int hctDbValidateIndex(HctDatabase *pDb, HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  HctCsrIndexOp *pOpList = pCsr->index.pOpList;
  HctCsrIndexOp *pOp;

  pCsr->index.pOpList = 0;
  assert( pCsr->index.pCurrentOp==0 );
  rc = hctDbAllocateUnpacked(pCsr);
  for(pOp=pOpList; pOp && rc==SQLITE_OK; pOp=pOp->pNextOp){
    UnpackedRecord *pRec = pCsr->pRec;
    hctDbCsrReset(pCsr);
    pCsr->eDir = (pOp->pFirst==pOp->pLast) ? BTREE_DIR_NONE : BTREE_DIR_FORWARD;
    if( pOp->pFirst==0 ){
      rc = hctDbCsrFirst(pCsr);
    }else{
      int bExact = 0;
      sqlite3VdbeRecordUnpack(pCsr->pKeyInfo, pOp->nFirst, pOp->pFirst, pRec);
      rc = hctDbCsrSeek(pCsr, 0, pRec, 0, &bExact);
      if( rc==SQLITE_OK && bExact==0 ){
        rc = hctDbCsrNext(pCsr);
      }
    }
    if( pOp->pLast && pOp->pLast!=pOp->pFirst ){
      sqlite3VdbeRecordUnpack(pCsr->pKeyInfo, pOp->nLast, pOp->pLast, pRec);
    }else{
      pRec = 0;
    }
    if( rc!=SQLITE_OK ) break;

    if( pOp->pLast==pOp->pFirst ){
      assert( !sqlite3HctDbCsrEof(pCsr) );
      rc = hctDbValidateEntry(pDb, pCsr);
    }else{
      while( !sqlite3HctDbCsrEof(pCsr) ){
        int res = -1;
        if( pRec ){
          const u8 *aKey = 0;
          int nKey = 0;
          rc = sqlite3HctDbCsrData(pCsr, &nKey, &aKey);
          if( rc!=SQLITE_OK ) break;
          res = sqlite3VdbeRecordCompare(nKey, aKey, pRec);
          if( res<0 ) break;
        }
        rc = hctDbValidateEntry(pDb, pCsr);
        if( res==0 || rc!=SQLITE_OK ) break;
        rc = hctDbCsrNext(pCsr);
        if( rc!=SQLITE_OK ) break;
      }
    }
  }

  assert( pCsr->index.pOpList==0 && pCsr->index.pCurrentOp==0 );
  pCsr->index.pOpList = pOpList;
  return rc;
}


int 
__attribute__ ((noinline)) 
sqlite3HctDbValidate(HctDatabase *pDb, u64 *piCid){
  HctDbCsr *pCsr = 0;
  u64 *pEntry = hctDbFindTMapEntry(pDb->pTmap, pDb->iTid);
  u64 iCid = 0;
  int rc = SQLITE_OK;

  assert( *pEntry==0 );
  HctAtomicStore(pEntry, HCT_TMAP_VALIDATING);
  iCid = sqlite3HctFileAllocateCID(pDb->pFile);

  assert( pDb->bValidate==0 );
  pDb->bValidate = 1;
  for(pCsr=pDb->pScannerList; pCsr && rc==SQLITE_OK; pCsr=pCsr->pNextScanner){
    if( pCsr->pKeyInfo==0 ){
      rc = hctDbValidateIntkey(pDb, pCsr);
    }else{
      rc = hctDbValidateIndex(pDb, pCsr);
    }
  }

  pDb->bValidate = 0;
  *piCid = iCid;
  return rc;
}



/*************************************************************************
**************************************************************************
** Below are the virtual table implementations. These are debugging 
** aids only.
*/

typedef struct hctdb_vtab hctdb_vtab;
struct hctdb_vtab {
  sqlite3_vtab base;              /* Base class - must be first */
  sqlite3 *db;
};

/* templatevtab_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct hctdb_cursor hctdb_cursor;
struct hctdb_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  HctDatabase *pDb;          /* Database to report on */
  u64 iMaxPgno;              /* Maximum page number for this scan */

  u64 pgno;                  /* The page-number/rowid value */
  const char *zPgtype;
  u32 iPeerPg;
  u32 nEntry;
  u32 nHeight;
  u64 iEdksVal;
  u32 iEdksPg;
  char *zFpKey;
};

/*
** The hctdbConnect() method is invoked to create a new
** template virtual table.
**
** Think of this routine as the constructor for hctdb_vtab objects.
**
** All this routine needs to do is:
**
**    (1) Allocate the hctdb_vtab object and initialize all fields.
**
**    (2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
**        result set of queries against the virtual table will look like.
*/
static int hctdbConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  hctdb_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db,
      "CREATE TABLE x("
        "pgno INTEGER, pgtype TEXT, nheight INTEGER, "
        "peer INTEGER, nentry INTEGER, edks_pg INTEGER, "
        "edks_tid INTEGER, fpkey TEXT"
      ")"
  );

  if( rc==SQLITE_OK ){
    pNew = sqlite3MallocZero( sizeof(*pNew) );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ) return SQLITE_NOMEM;
    pNew->db = db;
  }
  return rc;
}

/*
** This method is the destructor for hctdb_vtab objects.
*/
static int hctdbDisconnect(sqlite3_vtab *pVtab){
  hctdb_vtab *p = (hctdb_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new hctdb_cursor object.
*/
static int hctdbOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  hctdb_cursor *pCur;
  pCur = sqlite3MallocZero(sizeof(*pCur));
  if( pCur==0 ) return SQLITE_NOMEM;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a hctdb_cursor.
*/
static int hctdbClose(sqlite3_vtab_cursor *cur){
  hctdb_cursor *pCur = (hctdb_cursor*)cur;
  sqlite3_free(pCur->zFpKey);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static char *hex_encode(const u8 *aIn, int nIn){
  char *zRet = sqlite3MallocZero(nIn*2+1);
  if( zRet ){
    static const char aDigit[] = "0123456789ABCDEF";
    int i;
    for(i=0; i<nIn; i++){
      zRet[i*2] = aDigit[ (aIn[i] >> 4) ];
      zRet[i*2+1] = aDigit[ (aIn[i] & 0xF) ];
    }
  }
  return zRet;
}


static char *hctDbRecordToText(sqlite3 *db, const u8 *aRec, int nRec){
  char *zRet = 0;
  const char *zSep = "";
  const u8 *pEndHdr;              /* Points to one byte past record header */
  const u8 *pHdr;                 /* Current point in record header */
  const u8 *pBody;                /* Current point in record data */
  u64 nHdr;                       /* Bytes in record header */

  if( nRec==0 ){
    return sqlite3_mprintf("");
  }

  pHdr = aRec + sqlite3GetVarint(aRec, &nHdr);
  pBody = pEndHdr = &aRec[nHdr];
  while( pHdr<pEndHdr ){
    u64 iSerialType;
    Mem mem;

    memset(&mem, 0, sizeof(mem));
    mem.db = db;
    mem.enc = ENC(db);
    pHdr += sqlite3GetVarint(pHdr, &iSerialType);
    sqlite3VdbeSerialGet(pBody, (u32)iSerialType, &mem);
    pBody += sqlite3VdbeSerialTypeLen((u32)iSerialType);
    switch( sqlite3_value_type(&mem) ){
      case SQLITE_TEXT: {
        int nText = sqlite3_value_bytes(&mem);
        const char *zText = (const char*)sqlite3_value_text(&mem);
        assert( zText[nText]=='\0' );
        zRet = sqlite3_mprintf("%z%s%Q", zRet, zSep, zText);
        break;
      }

      case SQLITE_INTEGER: {
        i64 iVal = sqlite3_value_int64(&mem);
        zRet = sqlite3_mprintf("%z%s%lld", zRet, zSep, iVal);
        break;
      }

      case SQLITE_NULL: {
        zRet = sqlite3_mprintf("%z%sNULL", zRet, zSep);
        break;
      }

      case SQLITE_BLOB: {
        int nBlob = sqlite3_value_bytes(&mem);
        const u8 *aBlob = (const u8*)sqlite3_value_blob(&mem);
        char *zHex = hex_encode(aBlob, nBlob);
        zRet = sqlite3_mprintf("%z%sX'%z'", zRet, zSep, zHex);
        break;
      }


      default: {
        zRet = sqlite3_mprintf("%z%sunsupported(%d)", zRet, zSep, 
            sqlite3_value_type(&mem)
        );
        break;
      }
    }

    zSep = ",";
    if( mem.szMalloc ) sqlite3DbFree(db, mem.zMalloc);
  }

/* printf("%s\n", zRet); */
  return zRet;
}

/*
** Load the values for xColumn() associated with the current value of
** hctdb_cursor.pgno into memory.
*/
static int hctdbLoadPage(
  hctdb_cursor *pCur,
  const u8 *aPg
){
  static const char *azType[] = {
    "!INVALID!",                /* 0x00 */
    "intkey",                   /* 0x01 */
    "intkey_edks",              /* 0x02 */
    "index",                    /* 0x03 */
    "index_edks",               /* 0x04 */
    "overflow",                 /* 0x05 */
    "edks_fan",                 /* 0x06 */
  };
  int rc = SQLITE_OK;
  HctDbPageHdr *pHdr = (HctDbPageHdr*)aPg;
  sqlite3 *db = ((hctdb_vtab*)pCur->base.pVtab)->db;
  int eType;

  sqlite3_free(pCur->zFpKey);
  pCur->zFpKey = 0;

  eType = hctPagetype(pHdr);
  if( eType<ArraySize(azType) ){
    pCur->zPgtype = azType[hctPagetype(pHdr)];
  }else{
    pCur->zPgtype = "!INVALID!";
  }
  pCur->iPeerPg = pHdr->iPeerPg;
  pCur->nEntry = pHdr->nEntry;
  pCur->nHeight = pHdr->nHeight;
  pCur->iEdksPg = 0;
  pCur->iEdksVal = 0;

  if( (eType==HCT_PAGETYPE_INTKEY || eType==HCT_PAGETYPE_INDEX) 
   && pHdr->nHeight==0 
  ){
    HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)aPg;
    pCur->iEdksPg = pLeaf->hdr.iEdksPg;
    pCur->iEdksVal = pLeaf->hdr.iEdksVal;
  }

  if( eType==HCT_PAGETYPE_INTKEY ){
    if( pHdr->nHeight==0 ){
      HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)aPg;
      char *zFpKey = sqlite3_mprintf("%lld", pLeaf->aEntry[0].iKey);
      if( zFpKey==0 ) rc = SQLITE_NOMEM_BKPT;
      pCur->zFpKey = zFpKey;
    }else{
      HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)aPg;
      char *zFpKey = sqlite3_mprintf("%lld", pNode->aEntry[0].iKey);
      if( zFpKey==0 ) rc = SQLITE_NOMEM_BKPT;
      pCur->zFpKey = zFpKey;
    }
  }else if( eType==HCT_PAGETYPE_INDEX ){
    HctBuffer buf = {0,0,0};
    const u8 *aRec = 0;
    int nRec = 0;

    rc = hctDbLoadRecord(pCur->pDb, &buf, aPg, 0, &nRec, &aRec);
    if( rc==SQLITE_OK ){
      char *zFpKey = hctDbRecordToText(db, aRec, nRec);
      if( zFpKey==0 ) rc = SQLITE_NOMEM_BKPT;
      pCur->zFpKey = zFpKey;
    }

    hctBufferFree(&buf);
  }
  return rc;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int hctdbEof(sqlite3_vtab_cursor *cur){
  hctdb_cursor *pCur = (hctdb_cursor*)cur;
  return pCur->pgno>pCur->iMaxPgno;
}

/*
** Advance a hctdb_cursor to its next row of output.
*/
static int hctdbNext(sqlite3_vtab_cursor *cur){
  hctdb_cursor *pCur = (hctdb_cursor*)cur;
  int rc = SQLITE_OK;
  HctFilePage pg;

  memset(&pg, 0, sizeof(pg));
  do {
    sqlite3HctFilePageRelease(&pg);
    pCur->pgno++;
    if( hctdbEof(cur) ) return SQLITE_OK;
    rc = sqlite3HctFilePageGetPhysical(pCur->pDb->pFile, pCur->pgno, &pg);
  }while( rc==SQLITE_OK && pg.aOld==0 );

  if( pg.aOld ){
    rc = hctdbLoadPage(pCur, pg.aOld);
  }
  return rc;
}

/*
** Return values of columns for the row at which the hctdb_cursor
** is currently pointing.
*/
static int hctdbColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  hctdb_cursor *pCur = (hctdb_cursor*)cur;
  switch( i ){
    case 0: /* pgno */
      sqlite3_result_int64(ctx, (i64)pCur->pgno);
      break;
    case 1: /* pgtype */
      sqlite3_result_text(ctx, pCur->zPgtype, -1, SQLITE_TRANSIENT);
      break;
    case 2: /* nHeight */
      sqlite3_result_int64(ctx, (i64)pCur->nHeight);
      break;
    case 3: /* peer */
      sqlite3_result_int64(ctx, (i64)pCur->iPeerPg);
      break;
    case 4: /* nEntry */
      sqlite3_result_int64(ctx, (i64)pCur->nEntry);
      break;
    case 5: /* edks_pg */
      sqlite3_result_int64(ctx, (i64)pCur->iEdksPg);
      break;
    case 6: /* edks_tid */
      sqlite3_result_int64(ctx, (i64)pCur->iEdksVal);
      break;
    case 7: /* fpkey */
      sqlite3_result_text(ctx, pCur->zFpKey, -1, SQLITE_TRANSIENT);
      break;
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int hctdbRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  hctdb_cursor *pCur = (hctdb_cursor*)cur;
  *pRowid = pCur->pgno;
  return SQLITE_OK;
}

/*
** This method is called to "rewind" the hctdb_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to hctdbColumn() or hctdbRowid() or 
** hctdbEof().
*/
static int hctdbFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  hctdb_cursor *pCur = (hctdb_cursor*)pVtabCursor;
  hctdb_vtab *pTab = (hctdb_vtab*)(pCur->base.pVtab);
 
  pCur->pDb = sqlite3HctDbFind(pTab->db, 0);
  if( argc==1 ){
    u32 iVal = (u32)sqlite3_value_int64(argv[0]);
    pCur->iMaxPgno = iVal;
    pCur->pgno = iVal-1;
  }else{
    pCur->pgno = 0;
    pCur->iMaxPgno = sqlite3HctFileMaxpage(pCur->pDb->pFile);
  }
  return hctdbNext(pVtabCursor);
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int hctdbBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;
  pIdxInfo->estimatedCost = (double)10000;
  pIdxInfo->estimatedRows = 10000;

  for(i=0; i<pIdxInfo->nConstraint; i++){
    struct sqlite3_index_constraint *p = &pIdxInfo->aConstraint[i];
    if( p->iColumn!=0 ) continue;
    if( p->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( !p->usable ) continue;
    pIdxInfo->aConstraintUsage[i].argvIndex = 1;
    pIdxInfo->idxNum = 1;
    pIdxInfo->estimatedCost = (double)10;
    pIdxInfo->estimatedRows = 10;
    break;
  }

  return SQLITE_OK;
}

typedef struct hctentry_vtab hctentry_vtab;
struct hctentry_vtab {
  sqlite3_vtab base;              /* Base class - must be first */
  sqlite3 *db;
};

/* templatevtab_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct hctentry_cursor hctentry_cursor;
struct hctentry_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  HctDatabase *pDb;          /* Database to report on */
  int iEntry;
  HctFilePage pg;
  u32 iPg;                   /* Current physical page number */
  u32 iLastPg;               /* Last physical page to report on */
};

/*
** The hctentryConnect() method is invoked to create a new
** template virtual table.
**
** Think of this routine as the constructor for hctentry_vtab objects.
**
** All this routine needs to do is:
**
**    (1) Allocate the hctentry_vtab object and initialize all fields.
**
**    (2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
**        result set of queries against the virtual table will look like.
*/
static int hctentryConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  hctentry_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db,
      "CREATE TABLE x("
        "pgno INTEGER, entry INTEGER, "
        "ikey INTEGER, size INTEGER, offset INTEGER, "
        "child INTEGER, isdel BOOLEAN, "
        "tid INTEGER, oldpg INTEGER, ovfl INTEGER, record TEXT"
      ")"
  );

  if( rc==SQLITE_OK ){
    pNew = sqlite3MallocZero( sizeof(*pNew) );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ) return SQLITE_NOMEM;
    pNew->db = db;
  }
  return rc;
}

/*
** This method is the destructor for hctentry_vtab objects.
*/
static int hctentryDisconnect(sqlite3_vtab *pVtab){
  hctentry_vtab *p = (hctentry_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new hctentry_cursor object.
*/
static int hctentryOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  hctentry_cursor *pCur;
  pCur = sqlite3MallocZero(sizeof(*pCur));
  if( pCur==0 ) return SQLITE_NOMEM;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a hctentry_cursor.
*/
static int hctentryClose(sqlite3_vtab_cursor *cur){
  hctentry_cursor *pCur = (hctentry_cursor*)cur;
  sqlite3HctFilePageRelease(&pCur->pg);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int hctentryEof(sqlite3_vtab_cursor *cur){
  hctentry_cursor *pCur = (hctentry_cursor*)cur;
  return pCur->pg.aOld==0;
}

/*
** Advance a hctentry_cursor to its next row of output.
*/
static int hctentryNext(sqlite3_vtab_cursor *cur){
  int rc = SQLITE_OK;
  hctentry_cursor *pCur = (hctentry_cursor*)cur;

  while( rc==SQLITE_OK ){
    HctDbPageHdr *pPg = (HctDbPageHdr*)pCur->pg.aOld;
    int eType = hctPagetype(pPg);
    if( eType==HCT_PAGETYPE_INTKEY 
     || eType==HCT_PAGETYPE_INDEX 
     || eType==HCT_PAGETYPE_INTKEY_EDKS 
     || eType==HCT_PAGETYPE_EDKS_FAN 
    ){
      pCur->iEntry++;
      if( pCur->iEntry<pPg->nEntry ) break;
    }
    pCur->iEntry = -1;
    pCur->iPg++;
    sqlite3HctFilePageRelease(&pCur->pg);
    if( pCur->iPg>pCur->iLastPg ) break;
    rc = sqlite3HctFilePageGetPhysical(pCur->pDb->pFile, pCur->iPg, &pCur->pg);
  }

  return rc;
}

/*
** Return values of columns for the row at which the hctentry_cursor
** is currently pointing.
*/
static int hctentryColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  hctentry_cursor *pCur = (hctentry_cursor*)cur;
  int eType = hctPagetype(pCur->pg.aOld);
  int nHeight = hctPageheight(pCur->pg.aOld);

  HctDbIntkeyEntry *pIntkey = 0;
  HctDbIntkeyNodeEntry *pIntkeyNode = 0;
  HctDbIndexEntry *pIndex = 0;
  HctDbIndexNodeEntry *pIndexNode = 0;

  switch( eType ){
    case HCT_PAGETYPE_INTKEY:
      if( nHeight==0 ){
        pIntkey = &((HctDbIntkeyLeaf*)pCur->pg.aOld)->aEntry[pCur->iEntry];
      }else{
        pIntkeyNode = &((HctDbIntkeyNode*)pCur->pg.aOld)->aEntry[pCur->iEntry];
      }
      break;

    case HCT_PAGETYPE_INDEX:
      if( nHeight==0 ){
        pIndex = &((HctDbIndexLeaf*)pCur->pg.aOld)->aEntry[pCur->iEntry];
      }else{
        pIndexNode = &((HctDbIndexNode*)pCur->pg.aOld)->aEntry[pCur->iEntry];
      }
      break;
  }

  switch( i ){
    case 0: /* pgno */
      sqlite3_result_int64(ctx, (i64)pCur->iPg);
      break;
    case 1: /* iEntry */
      sqlite3_result_int64(ctx, (i64)pCur->iEntry);
      break;
    case 2: /* ikey */
      if( pIntkey ) sqlite3_result_int64(ctx, pIntkey->iKey);
      if( pIntkeyNode ) sqlite3_result_int64(ctx, pIntkeyNode->iKey);
      break;
    case 3: /* size */
      if( pIntkey ) sqlite3_result_int64(ctx, pIntkey->nSize);
      if( pIndex ) sqlite3_result_int64(ctx, pIndex->nSize);
      if( pIndexNode ) sqlite3_result_int64(ctx, pIndexNode->nSize);
      break;
    case 4: /* offset */
      if( pIntkey ) sqlite3_result_int64(ctx, pIntkey->iOff);
      if( pIndex ) sqlite3_result_int64(ctx, pIndex->iOff);
      if( pIndexNode ) sqlite3_result_int64(ctx, pIndexNode->iOff);
      break;
    case 5: /* child */
      if( pIndexNode ) sqlite3_result_int64(ctx, pIndexNode->iChildPg);
      if( pIntkeyNode ) sqlite3_result_int64(ctx, pIntkeyNode->iChildPg);
      break;
    case 6: /* isdel */
    case 7: /* tid */
    case 8: /* oldpg */
    case 9: /* ovfl */
      if( pIntkeyNode==0 ){
        u8 *aPg = pCur->pg.aOld;
        int iOff;
        int flags;
        iOff = hctDbEntryInfo(aPg, pCur->iEntry, 0, &flags);
        if( i==6 ){
          sqlite3_result_int(ctx, (flags & HCTDB_IS_DELETE) ? 1 : 0);
        }
        if( i==7 && (flags & HCTDB_HAS_TID) ){
          i64 iTid;
          memcpy(&iTid, &aPg[iOff], sizeof(i64));
          sqlite3_result_int64(ctx, iTid);
        }
        if( i==8 && (flags & HCTDB_HAS_OLD) ){
          u32 iOld;
          iOff += (flags & HCTDB_HAS_TID) ? 8 : 0;
          memcpy(&iOld, &aPg[iOff], sizeof(u32));
          sqlite3_result_int64(ctx, (i64)iOld);
        }
        if( i==9 && (flags & HCTDB_HAS_OVFL) ){
          u32 iOvfl;
          iOff += (flags & HCTDB_HAS_TID) ? 8 : 0;
          iOff += (flags & HCTDB_HAS_OLD) ? 4 : 0;
          memcpy(&iOvfl, &aPg[iOff], sizeof(u32));
          sqlite3_result_int64(ctx, (i64)iOvfl);
        }
      }
      break;
    case 10: /* record */
      if( pIntkeyNode==0 ){
        sqlite3 *db = sqlite3_context_db_handle(ctx);
        u8 *aPg = pCur->pg.aOld;
        char *zRec;
        int sz;
        const u8 *aRec = 0;
        HctBuffer buf = {0,0,0};

        hctDbLoadRecord(pCur->pDb, &buf, aPg, pCur->iEntry, &sz, &aRec);

        zRec = hctDbRecordToText(db, aRec, sz);
        if( zRec ){
          sqlite3_result_text(ctx, zRec, -1, SQLITE_TRANSIENT);
          sqlite3_free(zRec);
        }
        hctBufferFree(&buf);
      }
      break;
  }

  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int hctentryRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  hctentry_cursor *pCur = (hctentry_cursor*)cur;
  *pRowid = (((i64)pCur->iPg) << 32) + pCur->iEntry;
  return SQLITE_OK;
}

/*
** This method is called to "rewind" the hctentry_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to hctentryColumn() or hctentryRowid() or 
** hctentryEof().
*/
static int hctentryFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  int rc;
  hctentry_cursor *pCur = (hctentry_cursor*)pVtabCursor;
  hctentry_vtab *pTab = (hctentry_vtab*)(pCur->base.pVtab);
  u32 iLastPg; 
 
  pCur->pDb = sqlite3HctDbFind(pTab->db, 0);
  pCur->iEntry = -1;
  iLastPg = sqlite3HctFileMaxpage(pCur->pDb->pFile);

  if( idxNum==1 ){
    u32 iPg = (u32)sqlite3_value_int64(argv[0]);
    assert( argc==1 );
    if( iPg<1 || iPg>iLastPg ) return SQLITE_OK;
    pCur->iPg = pCur->iLastPg = iPg;
  }else{
    pCur->iPg = 1;
    pCur->iLastPg = iLastPg;
  }

  rc = sqlite3HctFilePageGetPhysical(pCur->pDb->pFile, pCur->iPg, &pCur->pg);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  return hctentryNext(pVtabCursor);
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int hctentryBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;
  int iPgnoEq = -1;

  pIdxInfo->estimatedCost = (double)1000000;
  pIdxInfo->estimatedRows = 1000000;

  /* Search for a pgno=? constraint */
  for(i=0; i<pIdxInfo->nConstraint; i++){
    struct sqlite3_index_constraint *p = &pIdxInfo->aConstraint[i];
    if( p->usable && p->iColumn==0 && p->op==SQLITE_INDEX_CONSTRAINT_EQ ){
      iPgnoEq = i;
    }
  }

  if( iPgnoEq>=0 ){
    pIdxInfo->aConstraintUsage[iPgnoEq].argvIndex = 1;
    pIdxInfo->idxNum = 1;
    pIdxInfo->estimatedCost = (double)1000;
    pIdxInfo->estimatedRows = 1000;
  }

  return SQLITE_OK;
}

typedef struct hctvalid_vtab hctvalid_vtab;
typedef struct hctvalid_cursor hctvalid_cursor;
struct hctvalid_vtab {
  sqlite3_vtab base;              /* Base class - must be first */
  sqlite3 *db;
};
struct hctvalid_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  HctDatabase *pDb;          /* Database to report on */
  int iEntry;                /* Current entry (i.e. rowid) */

  u32 rootpgno;              /* Value of rootpgno column */
  char *zFirst;
  char *zLast;
  char *zPglist;
};
static int hctvalidConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  hctvalid_vtab *pNew = 0;
  int rc = SQLITE_OK;

  *ppVtab = 0;
  rc = sqlite3_declare_vtab(db,
      "CREATE TABLE x(rootpgno, first, last, pglist)"
  );

  if( rc==SQLITE_OK ){
    pNew = sqlite3MallocZero( sizeof(*pNew) );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ) return SQLITE_NOMEM;
    pNew->db = db;
  }
  return rc;
}
static int hctvalidBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  pIdxInfo->estimatedCost = (double)10000;
  pIdxInfo->estimatedRows = 10000;
  return SQLITE_OK;
}
static int hctvalidDisconnect(sqlite3_vtab *pVtab){
  hctvalid_vtab *p = (hctvalid_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}
static int hctvalidOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  hctvalid_cursor *pCur;
  pCur = sqlite3MallocZero(sizeof(*pCur));
  if( pCur==0 ) return SQLITE_NOMEM;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}
static int hctvalidClose(sqlite3_vtab_cursor *cur){
  hctvalid_cursor *pCur = (hctvalid_cursor*)cur;
  sqlite3_free(pCur);
  return SQLITE_OK;
}
static int hctvalidNext(sqlite3_vtab_cursor *cur){
  hctvalid_cursor *pCsr = (hctvalid_cursor*)cur;
  hctvalid_vtab *pTab = (hctvalid_vtab*)(pCsr->base.pVtab);
  int ii;
  HctDbCsr *pDbCsr = 0;
  HctCsrIntkeyOp *pIntkeyOp = 0;
  HctCsrIndexOp *pIndexOp = 0;

  sqlite3_free(pCsr->zFirst);
  sqlite3_free(pCsr->zLast);
  sqlite3_free(pCsr->zPglist);
  pCsr->zFirst = 0;
  pCsr->zLast = 0;
  pCsr->zPglist = 0;
  pCsr->rootpgno = 0;
  pCsr->iEntry++;
  pDbCsr = pCsr->pDb->pScannerList;
  pIntkeyOp = pDbCsr->intkey.pOpList;
  pIndexOp = pDbCsr->index.pOpList;
  for(ii=0; pDbCsr && ii<pCsr->iEntry; ii++){
    if( pIntkeyOp ) pIntkeyOp = pIntkeyOp->pNextOp;
    if( pIndexOp ) pIndexOp = pIndexOp->pNextOp;
    if( pIntkeyOp==0 && pIndexOp==0 ){
      pDbCsr = pDbCsr->pNextScanner;
      if( pDbCsr ){
        pIntkeyOp = pDbCsr->intkey.pOpList;
        pIndexOp = pDbCsr->index.pOpList;
      }
    }
  }

  if( pDbCsr ){
    pCsr->rootpgno = pDbCsr->iRoot;
    if( pIntkeyOp ){
      if( pIntkeyOp->iFirst!=SMALLEST_INT64 ){
        pCsr->zFirst = sqlite3_mprintf("%lld", pIntkeyOp->iFirst);
      }
      if( pIntkeyOp->iFirst!=LARGEST_INT64 ){
        pCsr->zLast = sqlite3_mprintf("%lld", pIntkeyOp->iLast);
      }
    }else{
      if( pIndexOp->pFirst ){
        pCsr->zFirst = hctDbRecordToText(
            pTab->db, pIndexOp->pFirst, pIndexOp->nFirst
        );
      }
      if( pIndexOp->pLast ){
        pCsr->zLast = hctDbRecordToText(
            pTab->db, pIndexOp->pLast, pIndexOp->nLast
        );
      }
    }
  }

  return SQLITE_OK;
}
static int hctvalidFilter(
  sqlite3_vtab_cursor *cur, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  hctvalid_cursor *pCsr = (hctvalid_cursor*)cur;
  hctvalid_vtab *pTab = (hctvalid_vtab*)(pCsr->base.pVtab);
 
  pCsr->pDb = sqlite3HctDbFind(pTab->db, 0);
  pCsr->iEntry = -1;
  return hctvalidNext(cur);
}
static int hctvalidEof(sqlite3_vtab_cursor *cur){
  hctvalid_cursor *pCsr = (hctvalid_cursor*)cur;
  return (pCsr->rootpgno==0);
}
static int hctvalidColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  hctvalid_cursor *pCsr = (hctvalid_cursor*)cur;
  switch( i ){
    case 0:
      sqlite3_result_int64(ctx, (i64)pCsr->rootpgno);
      break;
    case 1:
      sqlite3_result_text(ctx, pCsr->zFirst, -1, SQLITE_TRANSIENT);
      break;
    case 2:
      sqlite3_result_text(ctx, pCsr->zLast, -1, SQLITE_TRANSIENT);
      break;
    case 3:
      sqlite3_result_text(ctx, pCsr->zPglist, -1, SQLITE_TRANSIENT);
      break;
  }
  return SQLITE_OK;
}
static int hctvalidRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  hctvalid_cursor *pCsr = (hctvalid_cursor*)cur;
  *pRowid = pCsr->iEntry;
  return SQLITE_OK;
}



int sqlite3HctVtabInit(sqlite3 *db){
  static sqlite3_module hctdbModule = {
    /* iVersion    */ 0,
    /* xCreate     */ 0,
    /* xConnect    */ hctdbConnect,
    /* xBestIndex  */ hctdbBestIndex,
    /* xDisconnect */ hctdbDisconnect,
    /* xDestroy    */ 0,
    /* xOpen       */ hctdbOpen,
    /* xClose      */ hctdbClose,
    /* xFilter     */ hctdbFilter,
    /* xNext       */ hctdbNext,
    /* xEof        */ hctdbEof,
    /* xColumn     */ hctdbColumn,
    /* xRowid      */ hctdbRowid,
    /* xUpdate     */ 0,
    /* xBegin      */ 0,
    /* xSync       */ 0,
    /* xCommit     */ 0,
    /* xRollback   */ 0,
    /* xFindMethod */ 0,
    /* xRename     */ 0,
    /* xSavepoint  */ 0,
    /* xRelease    */ 0,
    /* xRollbackTo */ 0,
    /* xShadowName */ 0
  };

  static sqlite3_module hctentryModule = {
    /* iVersion    */ 0,
    /* xCreate     */ 0,
    /* xConnect    */ hctentryConnect,
    /* xBestIndex  */ hctentryBestIndex,
    /* xDisconnect */ hctentryDisconnect,
    /* xDestroy    */ 0,
    /* xOpen       */ hctentryOpen,
    /* xClose      */ hctentryClose,
    /* xFilter     */ hctentryFilter,
    /* xNext       */ hctentryNext,
    /* xEof        */ hctentryEof,
    /* xColumn     */ hctentryColumn,
    /* xRowid      */ hctentryRowid,
    /* xUpdate     */ 0,
    /* xBegin      */ 0,
    /* xSync       */ 0,
    /* xCommit     */ 0,
    /* xRollback   */ 0,
    /* xFindMethod */ 0,
    /* xRename     */ 0,
    /* xSavepoint  */ 0,
    /* xRelease    */ 0,
    /* xRollbackTo */ 0,
    /* xShadowName */ 0
  };

  static sqlite3_module hctvalidModule = {
    /* iVersion    */ 0,
    /* xCreate     */ 0,
    /* xConnect    */ hctvalidConnect,
    /* xBestIndex  */ hctvalidBestIndex,
    /* xDisconnect */ hctvalidDisconnect,
    /* xDestroy    */ 0,
    /* xOpen       */ hctvalidOpen,
    /* xClose      */ hctvalidClose,
    /* xFilter     */ hctvalidFilter,
    /* xNext       */ hctvalidNext,
    /* xEof        */ hctvalidEof,
    /* xColumn     */ hctvalidColumn,
    /* xRowid      */ hctvalidRowid,
    /* xUpdate     */ 0,
    /* xBegin      */ 0,
    /* xSync       */ 0,
    /* xCommit     */ 0,
    /* xRollback   */ 0,
    /* xFindMethod */ 0,
    /* xRename     */ 0,
    /* xSavepoint  */ 0,
    /* xRelease    */ 0,
    /* xRollbackTo */ 0,
    /* xShadowName */ 0
  };

  int rc;

  rc = sqlite3_create_module(db, "hctdb", &hctdbModule, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_module(db, "hctentry", &hctentryModule, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_module(db, "hctvalid", &hctvalidModule, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFileVtabInit(db);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctPManVtabInit(db);
  }
  return rc;
}
