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
typedef struct HctDbEdksCsr HctDbEdksCsr;
typedef struct HctDbEdksCsr1 HctDbEdksCsr1;
typedef struct HctDbGlobal HctDbGlobal;
typedef struct HctDbIndexEntry HctDbIndexEntry;
typedef struct HctDbIndexLeaf HctDbIndexLeaf;
typedef struct HctDbIndexNode HctDbIndexNode;
typedef struct HctDbIndexNodeEntry HctDbIndexNodeEntry;
typedef struct HctDbIndexNodeHdr HctDbIndexNodeHdr;
typedef struct HctDbIntkeyEdksEntry HctDbIntkeyEdksEntry;
typedef struct HctDbIntkeyEdksLeaf HctDbIntkeyEdksLeaf;
typedef struct HctDbIntkeyEntry HctDbIntkeyEntry;
typedef struct HctDbIntkeyLeaf HctDbIntkeyLeaf;
typedef struct HctDbIntkeyNodeEntry HctDbIntkeyNodeEntry;
typedef struct HctDbIntkeyNode HctDbIntkeyNode;
typedef struct HctDbLeaf HctDbLeaf;
typedef struct HctDbLeafHdr HctDbLeafHdr;
typedef struct HctDbWriter HctDbWriter;
typedef struct HctDbPageHdr HctDbPageHdr;
typedef struct HctDbTMap HctDbTMap;
typedef struct HctDbEdksFan HctDbEdksFan;
typedef struct HctDbEdksFanEntry HctDbEdksFanEntry;

struct HctBuffer {
  u8 *aBuf;
  int nBuf;
  int nAlloc;
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
** pEdks/eEdks:
**   If it is not NULL, then pEdks is an EDKS cursor. Usually open
**   on the EDKS indicated by leaf page HctDbCsr.pg. In this case, if
**   eEdks is set to HCT_EDKS_NO, then the current entry is cell iCell
**   on page pg. Or, if eEdks is HCT_EDKS_YES, then the EDKS cursor is
**   the current entry.
**
**   For cursors that iterate forwards (eDir==BTREE_DIR_FORWARD), eEdks
**   may also be set to HCT_EDKS_TRAIL. This means that pEdks corresponds
**   to the EDKS indicated by the left-hand peer of page pg. The main
**   cursor has already moved on to the "next" page, but there are still
**   entries corresponding to the left-hand peer of the "next" page in the 
**   EDKS cursor.
**
**   eEdks is never set to HCT_EDKS_TRAIL for BTREE_DIR_NONE or
**   BTREE_DIR_REVERSE cursors.
** 
*/ 
struct HctDbCsr {
  HctDatabase *pDb;               /* Database that owns this cursor */
  HctDbCsr *pCsrNext;             /* Next cursor in list belonging to pDb */
  u32 iRoot;                      /* Root page cursor is opened on */
  KeyInfo *pKeyInfo;
  UnpackedRecord *pRec;
  int eDir;                       /* Direction cursor will step after Seek() */

  u8 *aRecord;                    /* Record in allocated memory */
  int nRecord;                    /* Size of aRecord[] in bytes */
  HctBuffer rec;

  int iCell;                      /* Current cell within page */
  HctFilePage pg;                 /* Current leaf page */
  int eEdks;                      /* An HCT_EDKS_*** constant */
  HctDbEdksCsr *pEdks;            /* EDKS cursor for page pg, if any */

  int iOldCell;                   /* Cell within old page */
  HctFilePage oldpg;              /* Old page, if required */
};

/*
** Candidate values for HctDbCsr.eEdks
*/
#define HCT_EDKS_NO    0
#define HCT_EDKS_YES   1
#define HCT_EDKS_TRAIL 2

/* Maximum height of EDKS tree */
#define HCTDB_EKDS_MAX_HEIGHT 12

/*
** A cursor to be used on a single EDKS structure.
**
** aPg[0] is the EDKS root page. 
*/
struct HctDbEdksCsr1 {
  int iPg;                        /* Current index in aPg[] */
  int aiCell[HCTDB_EKDS_MAX_HEIGHT];        /* Current index in each page */
  HctFilePage aPg[HCTDB_EKDS_MAX_HEIGHT];   /* Pages from root down */
};

/*
** A cursor to be used on the set of EDKS structures all linked to a single
** tree structure leaf page.
**
** bSkip:
**   If true, then the cursor may skip over any entries with TIDs indicating
**   that they are invisible to the current transaction (according to
**   HctDatabase.iSnapshotId).
*/
struct HctDbEdksCsr {
  HctDatabase *pDb;
  int bSkip;                      /* True to skip deleted keys */
  int eDir;                       /* BTREE_DIR_FORWARD/REVERSE/NONE */
  int iCurrent;                   /* Current aCsr[] entry. Or -1 for EOF */
  int nCsr;
  HctDbEdksCsr1 aCsr[0];
};


/*
** aDiscard:
**   An array of pages removed from the list by this write operation. Mark 
**   them as dirty and remove the FP keys from the parent list. The logical 
**   page number is in aDiscard[x].iPg and the FP key is the first key in 
**   the page at aDiscard[x].aOld.  
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
};

/*
**
** nWriteKey:
**   Number of sqlite3HctDbInsert() calls since last flush.
**
** iWriteFpKey/aWriteFpKey:
**   These two variables store the fence-post key for the peer page of
**   the rightmost page in the aWritePg[] array - aWritePg[nWritePg-1].
**   For intkey tables, iWriteFpKey is the 64-bit integer key value. For
**   index tables, aWriteFpKey points to a buffer containing the FP key,
**   and iWriteFpKey its size in bytes.
*/
struct HctDatabase {
  HctFile *pFile;
  HctDbGlobal *pGlobal;           /* Shared by all connections to same db */
  int pgsz;                       /* Page size in bytes */
  HctDbCsr *pCsrList;             /* List of open cursors */

  HctTMap *pTmap;                 /* Transaction map (non-NULL if trans open) */
  u64 iSnapshotId;                /* Snapshot id for reading */
  u64 iTid;                    
  HctDbWriter pa;

  int bRollback;                  /* True when in rollback mode */
};

struct HctDbGlobal {
  sqlite3_mutex *pMutex;          /* MUST BE FIRST */
  HctDbTMap *pTMap;               /* Newest transaction map object */
};


/*
** Transaction map object.
**
** When a transaction begins committing, it is assigned a "transaction id"
** using a monotonic increasing counter. This value is written into each
** new record added to the database as part of the commit. However, this
** is different from the transactions "commit id", which is assigned after
** all new entries have been written to the db and the transaction has
** been validated. Transactions are serialized in commit id order.
** During a read phase, a transaction ignores any entries written to the
** db after a specified commit id. As a result, each transaction needs
** some method of mapping from transaction id to commit id.
*/
#define HCTDB_TMAP_SIZE 1024
struct HctDbTMap {
  int nRef;                       /* Number of pointers to this object */
  u64 iFirstTid;                  /* First transacton id in map */
  int nMap;                       /* Number of mapping pages in aaMap[] */
  u64 **aaMap;                    /* Array of u64[HCTDB_TMAP_SIZE] arrays */
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
** The iEdksVal fields in both HctDbLeafHdr and HctDbEdksFanEntry encodes
** two values:
**
**   * The TID value - if all TIDs equal to or smaller than this value
**     correspond to transactions visible to the reader, the pointer need
**     not (and cannot safely) be followed. This is the least-significant
**     56 bytes of the iEdksVal field.
**
**   * The 8 bit EDKS merge-count field. If this is set to 0xFF, then the 
**     page indicated by iEdksPg is an EDKS fan page. Otherwise, it is set
**     to the number of times the EDKS has been merged since it was created.
*/

/* Macros for reading iEdksVal values */
#define hctEdksValTid(iVal) ((u64)(iVal) & ~(((u64)0xFF) << 56))
#define hctEdksValMergeCount(iVal) ((u64)(iVal) >> 56)
#define hctEdksValIsFan(iVal) (hctEdksValMergeCount(iVal)==0xFF)

#define hctEdksValNew(iTid, nMerge) (                \
  hctEdksValTid(iTid) + ((u64)(nMerge & 0xFF) << 56) \
)

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
  i64 iKey;                       /* 0: Integer key value */
  u32 nSize;                      /* 8: Total size of data (local+overflow) */
  u16 iOff;                       /* 12: Offset of record within this page */
  u8 flags;                       /* 14: Flags (see below) */
  u8 unused;                      /* 15: */
};

struct HctDbIntkeyLeaf {
  HctDbPageHdr pg;
  HctDbLeafHdr hdr;
  HctDbIntkeyEntry aEntry[0];
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

struct HctDbIndexEntry {
  u32 nSize;                      /* 0: Total size of data (local+overflow) */
  u16 iOff;                       /* 4: Offset of record within this page */
  u8 flags;                       /* 6: Flags (see below) */
  u8 unused;                      /* 7: */
};

struct HctDbIndexLeaf {
  HctDbPageHdr pg;
  HctDbLeafHdr hdr;
  HctDbIndexEntry aEntry[0];
};

struct HctDbIndexNodeEntry {
  u32 nSize;
  u32 iChildPg;
  u16 iOff;
  u8 flags;
  u8 unused;
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
** An entry on an EDKS fan page.
*/
struct HctDbEdksFanEntry {
  u64 iEdksVal;
  u32 iRoot;
};

struct HctDbIntkeyEdksLeaf {
  HctDbPageHdr pg;
  HctDbIntkeyEdksEntry aEntry[0];
};

struct HctDbEdksFan {
  HctDbPageHdr pg;
  HctDbEdksFanEntry aEntry[0];
};


/*
** Flags for HctDbIntkeyEntry.flags
*/
#define HCTDB_HAS_TID   0x01
#define HCTDB_HAS_OLD   0x02
#define HCTDB_HAS_OVFL  0x04
#define HCTDB_IS_DELETE 0x08


static void hctDbFreeGlobal(HctFileGlobal *pFileGlobal){
  HctDbGlobal *p = (HctDbGlobal*)pFileGlobal;
  HctDbTMap *pTMap;
  if( (pTMap = p->pTMap) ){
    int ii;
    assert( pTMap->nRef==1 );
    for(ii=0; ii<pTMap->nMap; ii++){
      sqlite3_free(pTMap->aaMap[ii]);
    }
    sqlite3_free(pTMap);
    p->pTMap = 0;
  }
}

static u64 hctDbTMapLookup(HctDatabase *pDb, u64 iTid){
  HctTMap *pTmap = pDb->pTmap;
  int iMap = (iTid - pTmap->iFirstTid) / HCT_TMAP_PAGESIZE;

  assert( iMap<pTmap->nMap );
  if( iMap>=0 && iMap<pTmap->nMap ){
    int iOff = (iTid - pTmap->iFirstTid) % HCTDB_TMAP_SIZE;
    return pTmap->aaMap[iMap][iOff];
  }

  return 0;
}

int sqlite3HctDbOpen(const char *zFile, HctDatabase **ppDb){
  int rc = SQLITE_OK;
  HctDatabase *pNew;

  pNew = (HctDatabase*)sqlite3MallocZero(sizeof(*pNew));
  if( pNew ){
    rc = sqlite3HctFileOpen(zFile, &pNew->pFile);
  }else{
    rc = SQLITE_NOMEM_BKPT;
  }

  if( rc==SQLITE_OK ){
    pNew->pGlobal = (HctDbGlobal*)sqlite3HctFileGlobal(
        pNew->pFile, sizeof(HctDbGlobal), hctDbFreeGlobal
    );
    if( pNew->pGlobal==0 ){
      rc = SQLITE_NOMEM_BKPT;
    }
  }

  if( rc!=SQLITE_OK ){
    sqlite3HctDbClose(pNew);
    pNew = 0;
  }else{
    pNew->pgsz = sqlite3HctFilePgsz(pNew->pFile);
  }

  *ppDb = pNew;
  return rc;
}

void sqlite3HctDbClose(HctDatabase *p){
  if( p ){
    assert( p->pa.aWriteFpKey==0 );
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

void sqlite3HctDbMetaPageInit(u8 *aPage, int szPage){
  HctDbIntkeyLeaf *pPg = (HctDbIntkeyLeaf*)aPage;
  HctDbIntkeyEntry *pEntry = &pPg->aEntry[0];
  const int nRecord = 8 + 4 + SQLITE_N_BTREE_META*4;
  int nFree;

  memset(aPage, 0, szPage);
  pPg->pg.hdrFlags = HCT_PAGETYPE_INTKEY|HCT_PAGETYPE_LEFTMOST;
  pPg->pg.nEntry = 1;
  nFree = szPage - sizeof(HctDbIntkeyLeaf) - sizeof(HctDbIntkeyEntry) - nRecord;
  pPg->hdr.nFreeBytes = pPg->hdr.nFreeGap = nFree;
  pEntry->iKey = 0;
  pEntry->nSize = SQLITE_N_BTREE_META*4;
  pEntry->iOff = szPage - nRecord;
  pEntry->flags = HCTDB_HAS_TID | HCTDB_HAS_OLD;
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
** Load the meta-data record from the database and store it in buffer aBuf
** (size nBuf bytes). The meta-data record is stored with rowid=0 int the
** intkey table with root-page=2.
*/
int sqlite3HctDbGetMeta(HctDatabase *pDb, u8 *aBuf, int nBuf){
  HctFilePage pg;
  int rc;

  hctDbSnapshotOpen(pDb);
  rc = sqlite3HctFilePageGet(pDb->pFile, 2, &pg);
  while( rc==SQLITE_OK ){
    HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)pg.aOld;
    u64 iTid;
    u32 iOld;
    int iOff;

    assert( pLeaf->pg.nEntry==1 );
    assert( pLeaf->aEntry[0].iKey==0 );
    assert( pLeaf->aEntry[0].nSize==nBuf );
    assert( pLeaf->aEntry[0].flags==(HCTDB_HAS_TID|HCTDB_HAS_OLD) );
    iOff = pLeaf->aEntry[0].iOff;

    iTid = hctGetU64(&pg.aOld[iOff]);
    iOld = hctGetU32(&pg.aOld[iOff+8]);
    if( hctDbTMapLookup(pDb, iTid)<=pDb->iSnapshotId ){
      memcpy(aBuf, &pg.aOld[iOff+12], nBuf);
      sqlite3HctFilePageRelease(&pg);
      break;
    }

    sqlite3HctFilePageRelease(&pg);
    rc = sqlite3HctFilePageGetPhysical(pDb->pFile, iOld, &pg);
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

static int hctDbOffset(int iOff, int flags){
  return iOff 
    + ((flags & HCTDB_HAS_TID) ? 8 : 0)
    + ((flags & HCTDB_HAS_OLD) ? 4 : 0)
    + ((flags & HCTDB_HAS_OVFL) ? 4 : 0);
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

/*
** Like hctDbIntkeyLeafSearch(), but for index or without rowid tables.
*/
static int hctDbIndexLeafSearch(
  u8 *aPg, 
  UnpackedRecord *pRec,
  int *piPos,
  int *pbExact
){
  HctDbIndexLeaf *pLeaf = (HctDbIndexLeaf*)aPg;
  int i1 = 0;
  int i2 = pLeaf->pg.nEntry;

  assert( pLeaf->pg.nHeight==0 );
  while( i2>i1 ){
    int iTest = (i1+i2)/2;
    HctDbIndexEntry *pEntry = &pLeaf->aEntry[iTest];
    int res;
    
    if( pEntry->flags & HCTDB_HAS_OVFL ){
      assert( 0 );
    }else{
      int iOff = hctDbOffset(pEntry->iOff, pEntry->flags);
      res = sqlite3VdbeRecordCompare(pEntry->nSize, &aPg[iOff], pRec);
    }

    if( res==0 ){
      *pbExact = 1;
      *piPos = iTest;
      return SQLITE_OK;
    }else if( res<0 ){
      i1 = iTest+1;
    }else{
      i2 = iTest;
    }
  }

  assert( i1==i2 && i2>=0 );
  *pbExact = 0;
  *piPos = i2;
  return SQLITE_OK;
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

static int hctDbEntrySize(int nData, int bClobber, u32 pgOvfl){
  int sz = nData + 8 + (bClobber ? 4 : 0) + (pgOvfl ? 4 : 0);
  return sz;
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
** Like hctDbIntkeyNodeSearch(), but for index or without rowid tables.
*/
static int hctDbIndexNodeSearch(
  void *aPg, 
  UnpackedRecord *pRec,
  int *pbExact
){
  HctDbIndexNode *pNode = (HctDbIndexNode*)aPg;
  int i1 = 0;
  int i2 = pNode->pg.nEntry;

  assert( pNode->pg.nHeight>0 );
  while( i2>i1 ){
    int iTest = (i1+i2)/2;
    HctDbIndexNodeEntry *pEntry = &pNode->aEntry[iTest];
    int iOff = hctDbOffset(pEntry->iOff, pEntry->flags);
    int res;
    
    if( pEntry->nSize==0 ){
      res = -1;
    }else{
      res = sqlite3VdbeRecordCompare(pEntry->nSize, &((u8*)aPg)[iOff], pRec);
    }

    if( res==0 ){
      *pbExact = 1;
      return iTest;
    }else if( res<0 ){
      i1 = iTest+1;
    }else{
      i2 = iTest;
    }
  }

  assert( i1==i2 && i2>=0 );
  *pbExact = 0;
  return i2;
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
assert( rc==SQLITE_OK );
  res = sqlite3VdbeRecordCompare(nFP, aFP, pRec);
  hctBufferFree(&buf);

  *pbGe = (res<=0);
  return SQLITE_OK;
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
** Close and delete an EDKS cursor.
*/
static void hctDbEdksCsrClose(HctDbEdksCsr *pEdks){
  if( pEdks ){
    int i;
    for(i=0; i<pEdks->nCsr; i++){
      int jj = 0;
      while( pEdks->aCsr[i].aPg[jj].aOld ){
        sqlite3HctFilePageRelease(&pEdks->aCsr[i].aPg[jj]);
      }
    }
    sqlite3_free(pEdks);
  }
}

static int hctDbEdksCsrBuild(
  HctDatabase *pDb,               /* Database handle */
  u32 iPg,                        /* EDKS fan or root page */
  HctDbEdksCsr **ppCsr            /* IN/OUT: Cursor to add to */
){
  HctFilePage pg;
  HctDbEdksCsr *pCsr = *ppCsr;
  int rc;

  rc = sqlite3HctFilePageGetPhysical(pDb->pFile, iPg, &pg);
  if( rc==SQLITE_OK ){
    HctDbPageHdr *pHdr = (HctDbPageHdr*)pg.aOld; 
    if( pHdr->hdrFlags==HCT_PAGETYPE_INTKEY_EDKS ){
      if( pCsr==0 || (pCsr->nCsr & (pCsr->nCsr-1))==0 ){
        int nNew = pCsr ? pCsr->nCsr*2 : 1;
        HctDbEdksCsr *pNew = (HctDbEdksCsr*)sqlite3_realloc(pCsr,
          sizeof(HctDbEdksCsr) + sizeof(HctDbEdksCsr1)*nNew
        );
        if( pNew==0 ){
          rc = SQLITE_NOMEM;
        }else{
          if( pCsr==0 ){
            memset(pNew, 0, sizeof(*pNew) + 1*sizeof(HctDbEdksCsr1));
          }else{
            memset(&pNew->aCsr[pNew->nCsr],0,pNew->nCsr*sizeof(HctDbEdksCsr1));
          }
          pCsr = pNew;
          pCsr->pDb = pDb;
        }
      }
      if( rc==SQLITE_OK ){
        pCsr->aCsr[pCsr->nCsr].aPg[0] = pg;
        pCsr->nCsr++;
      }
    }else{
      int ii;
      HctDbEdksFan *pFan = (HctDbEdksFan*)pg.aOld;
      assert( pCsr==0 );
      for(ii=0; ii<pFan->pg.nEntry; ii++){
        HctDbEdksFanEntry *pEntry = &pFan->aEntry[ii];
        rc = hctDbEdksCsrBuild(pDb, pEntry->iRoot, &pCsr);
      }
    }
  }

  if( rc!=SQLITE_OK ){
    hctDbEdksCsrClose(pCsr);
    pCsr = 0;
  }
  *ppCsr = pCsr;
  return rc;
}


static int hctDbEdksCsr1Eof(HctDbEdksCsr1 *p1){
  return p1->iPg<0; /* TODO! */
}

static int hctDbEdksCsrSetCurrent(HctDbEdksCsr *pCsr){
  int ii;
  HctDbIntkeyEdksEntry *pBest = 0;
  int iBest = -1;
  
  for(ii=0; ii<pCsr->nCsr; ii++){
    HctDbEdksCsr1 *p1 = &pCsr->aCsr[ii];
    if( hctDbEdksCsr1Eof(p1)==0 ){
      HctDbIntkeyEdksLeaf *pLeaf = (HctDbIntkeyEdksLeaf*)p1->aPg[p1->iPg].aOld;
      HctDbIntkeyEdksEntry *pNew = &pLeaf->aEntry[p1->aiCell[p1->iPg]];
      assert( p1->aiCell[p1->iPg]<pLeaf->pg.nEntry && p1->aiCell[p1->iPg]>=0 );
      if( pBest==0 
          || (pCsr->eDir!=BTREE_DIR_REVERSE && pNew->iRowid<pBest->iRowid)
          || (pCsr->eDir==BTREE_DIR_REVERSE && pNew->iRowid>pBest->iRowid)
          || (pNew->iRowid==pBest->iRowid && pNew->iTid>pBest->iTid)
        ){
        iBest = ii;
        pBest = pNew;
      }
    }
  }

  pCsr->iCurrent = iBest;
  return SQLITE_OK;
}

/*
** Return true if TID iTid maps to a commit-id visible to the current
** client. Or false otherwise.
*/
static int hctDbTidIsVisible(HctDatabase *pDb, u64 iTid){
  u64 iCid = hctDbTMapLookup(pDb, iTid);
  return (iCid<=pDb->iSnapshotId);
}

/*
** Return the TID of the entry that the
*/
static u64 hctDbEdksCsr1Tid(HctDbEdksCsr1 *p1){
  int iCell = p1->aiCell[p1->iPg];
  HctDbIntkeyEdksLeaf *pPg = (HctDbIntkeyEdksLeaf*)p1->aPg[p1->iPg].aOld;
  assert( iCell>=0 && iCell<pPg->pg.nEntry );
  return pPg->aEntry[iCell].iTid;
}


/*
** Advance the cursor passed as the only argument forwards (iDir==1) or
** backwards (iDir==-1).
*/
static int hctDbEdksCsr1Advance(
  HctDbEdksCsr *pCsr, 
  HctDbEdksCsr1 *p1, 
  int iDir
){
  HctDatabase *pDb = pCsr->pDb;
  HctDbIntkeyEdksLeaf *pLeaf;
  u32 iChild;
  assert( iDir==1 || iDir==-1 );

  do {
    pLeaf = (HctDbIntkeyEdksLeaf*)p1->aPg[p1->iPg].aOld;
    if( iDir==1 ){
      iChild = pLeaf->aEntry[ p1->aiCell[p1->iPg] ].iChildPgno;
      if( iChild==0 ){
        while( p1->iPg>=0 && p1->aiCell[p1->iPg]==(pLeaf->pg.nEntry-1) ){
          sqlite3HctFilePageRelease(&p1->aPg[p1->iPg]);
          p1->iPg--;
          pLeaf = (HctDbIntkeyEdksLeaf*)p1->aPg[p1->iPg].aOld;
        }
        if( p1->iPg>=0 ){
          p1->aiCell[p1->iPg]++;
        }
      }else{
        do{
          p1->iPg++;
          sqlite3HctFilePageGetPhysical(pDb->pFile, iChild, &p1->aPg[p1->iPg]);
          pLeaf = (HctDbIntkeyEdksLeaf*)p1->aPg[p1->iPg].aOld;
          p1->aiCell[p1->iPg] = -1;
          iChild = pLeaf->pg.iPeerPg;
        }while( iChild!=0 );
        p1->aiCell[p1->iPg] = 0;
      }
    }else{
      if( pLeaf->pg.nHeight==0 ){
        p1->aiCell[p1->iPg]--;
        while( p1->iPg>=0 && p1->aiCell[p1->iPg]<0 ){
          sqlite3HctFilePageRelease(&p1->aPg[p1->iPg]);
          p1->iPg--;
        }
      }else{
        p1->aiCell[p1->iPg]--;
        if( p1->aiCell[p1->iPg]>=0 ){
          iChild = pLeaf->aEntry[ p1->aiCell[p1->iPg] ].iChildPgno;
        }else{
          iChild = pLeaf->pg.iPeerPg;
        }
        do{
          p1->iPg++;
          sqlite3HctFilePageGetPhysical(pDb->pFile, iChild, &p1->aPg[p1->iPg]);
          pLeaf = (HctDbIntkeyEdksLeaf*)p1->aPg[p1->iPg].aOld;
          p1->aiCell[p1->iPg] = pLeaf->pg.nEntry-1;
          iChild = pLeaf->aEntry[ pLeaf->pg.nEntry-1 ].iChildPgno;
        }while( iChild );
      }
    }
  }while( p1->iPg>=0 && pCsr->bSkip 
       && hctDbTidIsVisible(pCsr->pDb, hctDbEdksCsr1Tid(p1))
  );

  return SQLITE_OK; /* todo */
}

/*
** Return the rowid value that the single EDKS cursor currently points to.
*/
static i64 hctDbEdksCsr1Intkey(HctDbEdksCsr1 *p1){
  int iCell = p1->aiCell[p1->iPg];
  HctDbIntkeyEdksLeaf *pPg = (HctDbIntkeyEdksLeaf*)p1->aPg[p1->iPg].aOld;
  assert( iCell>=0 && iCell<pPg->pg.nEntry );
  return pPg->aEntry[iCell].iRowid;
}

/*
** Seek cursor p1, which is part of pCsr, to point to the largest value 
** smaller than or equal to iKey.
*/
static int hctDbEdksCsr1Seek(
  HctDbEdksCsr *pCsr, 
  HctDbEdksCsr1 *p1, 
  i64 iKey
){
  int rc = SQLITE_OK;

  assert( p1->aPg[0].aOld && p1->aPg[1].aOld==0 && p1->iPg==0 );

  do{
    HctDbIntkeyEdksLeaf *pPg = (HctDbIntkeyEdksLeaf*)p1->aPg[p1->iPg].aOld;
    int i1 = -1;
    int i2 = pPg->pg.nEntry-1;
    u32 iChild;

    /* Set variable i2 to the index of the cell containing the largest
    ** value smaller than or equal to iKey. Or, if all values on the
    ** page are larger than iKey, set i2 to -1.  */
    while( i1<i2 ){
      int iTest = (i1+i2+1)/2;
      i64 iVal = pPg->aEntry[iTest].iRowid;
      assert( iTest>=0 );
      if( iVal==iKey ){
        p1->aiCell[p1->iPg] = iTest;
        goto csr1_seek_done;
      }else if( iVal<iKey ){
        i1 = iTest;
      }else{ assert( iVal>iKey );
        i2 = iTest-1;
      }
    }
    assert( i2>=-1 && i2<pPg->pg.nEntry );
    assert( i2>=0  || pPg->aEntry[0].iRowid>iKey );
    assert( i2==-1 || pPg->aEntry[i2].iRowid<=iKey );

    if( i2<0 ){
      iChild = pPg->pg.iPeerPg;
    }else{
      iChild = pPg->aEntry[i2].iChildPgno;
    }
    p1->aiCell[p1->iPg] = i2;
    if( iChild==0 ){
      break;
    }

    p1->iPg++;
    rc = sqlite3HctFilePageGetPhysical(
        pCsr->pDb->pFile, iChild, &p1->aPg[p1->iPg]
    );
  }while( rc==SQLITE_OK );

  if( rc==SQLITE_OK ){
    switch( pCsr->eDir ){
      case BTREE_DIR_FORWARD:
        rc = hctDbEdksCsr1Advance(pCsr, p1, +1);
        assert( rc!=SQLITE_OK || p1->iPg<0 || p1->aiCell[p1->iPg]>=0 );
        break;
      case BTREE_DIR_NONE:
        /* For a BTREE_DIR_NONE seek, if this is not an exact hit, move
        ** the cursor to EOF. At this point it is known that the seek is
        ** not an exact hit, as in that case the code jumps directly to
        ** csr1_seek_done below.  */
        while( p1->iPg>=0 ){
          sqlite3HctFilePageRelease(&p1->aPg[p1->iPg]);
          p1->iPg--;
        }
        break;

      default: assert( pCsr->eDir==BTREE_DIR_REVERSE );
        while( p1->iPg>=0 && p1->aiCell[p1->iPg]<0 ){
          sqlite3HctFilePageRelease(&p1->aPg[p1->iPg]);
          p1->iPg--;
        }

        /* If bSkip is set, check if the current entry (a delete-key) is 
        ** visible. If not, back up to the previous entry.  */
        if( pCsr->bSkip && p1->iPg>0 ){
          u64 iTid = hctDbEdksCsr1Tid(p1);
          if( hctDbTidIsVisible(pCsr->pDb, iTid) ){
            rc = hctDbEdksCsr1Advance(pCsr, p1, -1);
          }
        }
        break;
    }
  }

 csr1_seek_done:
  return rc;
}


/*
** Return true if the EDKS cursor is at EOF, false otherwise.
*/
static int hctDbEdksCsrEof(HctDbEdksCsr *pCsr){
  return pCsr->iCurrent<0;
}

static int hctDbEdksCsrSeek(
  HctDatabase *pDb,
  HctDbEdksCsr *pCsr,
  i64 iKey,
  int eDir
){
  int rc = SQLITE_OK;
  int i1;

  pCsr->eDir = eDir;
  for(i1=0; rc==SQLITE_OK && i1<pCsr->nCsr; i1++){
    HctDbEdksCsr1 *p1 = &pCsr->aCsr[i1];
    rc = hctDbEdksCsr1Seek(pCsr, p1, iKey);
  }
  if( rc==SQLITE_OK ){
    rc = hctDbEdksCsrSetCurrent(pCsr);
  }

  return rc;
}

/*
** Open a new EDKS cursor with physical root page iRoot.
*/
static int hctDbEdksCsrInit(
  HctDatabase *pDb,               /* Database handle */
  u32 iRoot,                      /* Physical root page number */
  u64 iTid,                       /* TID associated with EDKS root page */
  int eDir,                       /* BTREE_DIR_FORWARD/REVERSE/NONE */
  i64 iStartKey,                  /* Key to start at */
  HctDbEdksCsr **ppCsr            /* OUT: New HctDbEdksCsr object */
){
  int rc;
  HctDbEdksCsr *pRet = 0;

  // u64 iCid;
  // iCid = hctDbTMapLookup(pDb, iTid);
  // if( iCid<=pCsr->pDb->iSnapshotId )

  assert( eDir==BTREE_DIR_FORWARD 
       || eDir==BTREE_DIR_REVERSE 
       || eDir==BTREE_DIR_NONE 
  );

  /* Allocate the HctDbEdksCsr structure and the array of HctDbEdksCsr1 
  ** cursors. */
  rc = hctDbEdksCsrBuild(pDb, iRoot, &pRet);
  if( rc==SQLITE_OK ){
    pRet->bSkip = (eDir!=BTREE_DIR_NONE);
    rc = hctDbEdksCsrSeek(pDb, pRet, iStartKey, eDir);
  }

  if( rc!=SQLITE_OK || hctDbEdksCsrEof(pRet) ){
    hctDbEdksCsrClose(pRet);
    pRet = 0;
  }
  *ppCsr = pRet;
  return rc;
}

static void hctDbEdksCsrAdvance(HctDbEdksCsr *pCsr, int iDir){
  HctDbEdksCsr1 *p1 = &pCsr->aCsr[pCsr->iCurrent];
  i64 iKey = hctDbEdksCsr1Intkey(p1);
  int ii;

  assert( iDir==1 || iDir==-1 );

  for(ii=0; ii<pCsr->nCsr; ii++){
    HctDbEdksCsr1 *p2 = &pCsr->aCsr[ii];
    if( hctDbEdksCsr1Eof(p2)==0 && hctDbEdksCsr1Intkey(p2)==iKey ){
      hctDbEdksCsr1Advance(pCsr, p2, iDir);
    }
  }

  hctDbEdksCsrSetCurrent(pCsr);
}

static void hctDbEdksCsrNext(HctDbEdksCsr *pCsr){
  hctDbEdksCsrAdvance(pCsr, 1);
}
static void hctDbEdksCsrPrev(HctDbEdksCsr *pCsr){
  hctDbEdksCsrAdvance(pCsr, -1);
}

static u32 hctDbEdksCsrEntry(HctDbEdksCsr *pCsr, i64 *piKey, u64 *piTid){
  HctDbEdksCsr1 *p1 = &pCsr->aCsr[pCsr->iCurrent];
  HctDbIntkeyEdksLeaf *pLeaf = (HctDbIntkeyEdksLeaf*)p1->aPg[p1->iPg].aOld;
  HctDbIntkeyEdksEntry *pEntry = &pLeaf->aEntry[p1->aiCell[p1->iPg]];
  if( piKey ) *piKey = pEntry->iRowid;
  if( piTid ) *piTid = pEntry->iTid;
  return pEntry->iOldPgno;
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
    int rc;
    int bExact;
    HctDbIndexEntry *p = &((HctDbIndexLeaf*)pCsr->pg.aOld)->aEntry[pCsr->iCell];
    u8 *aCell = &pCsr->pg.aOld[hctDbOffset(p->iOff, p->flags)];

    rc = hctDbAllocateUnpacked(pCsr);
    if( rc==SQLITE_OK ){
      sqlite3VdbeRecordUnpack(pCsr->pKeyInfo, p->nSize, aCell, pCsr->pRec);
      rc = hctDbIndexLeafSearch(pCsr->pg.aOld, pCsr->pRec, &iCell, &bExact);
    }
    if( rc ) return rc;
    if( bExact==0 ) iCell = -1;
  }else{
    i64 iKey;
    if( pCsr->eEdks==HCT_EDKS_NO ){
      iKey = ((HctDbIntkeyLeaf*)pCsr->pg.aOld)->aEntry[pCsr->iCell].iKey;
    }else{
      hctDbEdksCsrEntry(pCsr->pEdks, &iKey, 0);
    }
    iCell = hctDbFindKeyInPage((HctDbIntkeyLeaf*)pCsr->oldpg.aOld, iKey);
  }

  pCsr->iOldCell = iCell;
  return (iCell<0 ? SQLITE_CORRUPT_BKPT : SQLITE_OK);
}

/*
** Read the current 64-bit integer key from the main cursor of cursor pCsr.
*/
static void hctDbCsrKey(HctDbCsr *pCsr, i64 *piKey){
  HctDbIntkeyLeaf *pPg = (HctDbIntkeyLeaf*)pCsr->pg.aOld;
  assert( pCsr->iCell>=0 && pCsr->iCell<pPg->pg.nEntry );
  assert( hctPagetype(pPg)==HCT_PAGETYPE_INTKEY );
  *piKey = pPg->aEntry[pCsr->iCell].iKey;
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
  if( pCsr->iCell<0 && pCsr->eEdks==HCT_EDKS_NO ) return 1;    /* EOF */

  if( pCsr->eEdks==HCT_EDKS_NO ){
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
    ** version of the key. Or, if there is no old-page pointer, check if 
    ** there is an EDKS entry with the same key. If there is, modify the
    ** internal state of the cursor so that the EDKS entry is the current
    ** entry. Code below will handle this.  
    **
    ** If there are neither of these things - old-page pointer or EDKS entry,
    ** return 0 to indicate that no visible entry with the current key
    ** could be found.  */
    if( flags & HCTDB_HAS_OLD ){
      iOld = hctGetU32(&aPage[iOff+8]);
    }else if( pCsr->pEdks ){
      i64 iEdksKey;
      i64 iMainKey;
      assert( hctPagetype(aPage)==HCT_PAGETYPE_INTKEY );
      hctDbEdksCsrEntry(pCsr->pEdks, &iEdksKey, 0);
      hctDbCsrKey(pCsr, &iMainKey);
      if( iEdksKey==iMainKey ){
        pCsr->eEdks = HCT_EDKS_YES;
      }else{
        return 0;
      }
    }else{
      return 0;
    }
  }

  if( pCsr->eEdks!=HCT_EDKS_NO ){
    u64 iTid;
    iOld = hctDbEdksCsrEntry(pCsr->pEdks, 0, &iTid);

    /* If this EDKS entry is visible to the client, return 0 (because it
    ** is a delete-key). */
    if( hctDbTidIsVisible(pCsr->pDb, iTid) ) return 0;
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
  hctDbEdksCsrClose(pCsr->pEdks);
  pCsr->pEdks = 0;
  pCsr->eEdks = HCT_EDKS_NO;
  /* pCsr->eDir = 0; */
}

/*
** If the leaf page that pCsr currently points to has an EDKS pointer,
** initialize an EDKS cursor for it.
*/
static int hctDbCsrInitEdks(HctDbCsr *pCsr, i64 iLimit){
  int rc = SQLITE_OK;
  HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)pCsr->pg.aOld;

  assert( pCsr->pEdks==0 );
  assert( pCsr->eDir==BTREE_DIR_FORWARD 
       || pCsr->eDir==BTREE_DIR_REVERSE 
       || pCsr->eDir==BTREE_DIR_NONE 
  );
  if( pLeaf && pLeaf->hdr.iEdksPg ){
    i64 iVal;
    if( pCsr->eDir!=BTREE_DIR_REVERSE ){
      i64 iPg = hctDbIntkeyFPKey(pLeaf);
      iVal = MAX(iPg, iLimit);
    }else{
      iVal = iLimit;
    }

    rc = hctDbEdksCsrInit(pCsr->pDb, 
        pLeaf->hdr.iEdksPg, pLeaf->hdr.iEdksVal, pCsr->eDir, iVal, &pCsr->pEdks
    );
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
  int rc;
  int bExact;

  hctDbCsrReset(pCsr);
  rc = hctDbCsrSeek(pCsr, 0, pRec, iKey, &bExact);

  /* The main cursor now points to the largest entry less than or equal 
  ** to the supplied key (pRec or iKey). If the supplied key is smaller 
  ** than all entries in the table, then pCsr->iCell is set to -1.  */
  if( rc==SQLITE_OK ){

    if( pCsr->iCell<0 ){
      /* If the cursor is BTREE_DIR_REVERSE or NONE, then leave it as it is
      ** at EOF. Otherwise, if the cursor is BTREE_DIR_FORWARD, attempt
      ** to move it to the first valid entry. */
      if( pCsr->eDir==BTREE_DIR_FORWARD ){
        rc = sqlite3HctDbCsrFirst(pCsr);
        *pRes = sqlite3HctDbCsrEof(pCsr) ? -1 : +1;
      }else{
        *pRes = -1;
      }
    }else{

      if( pRec==0 && (pCsr->eDir!=BTREE_DIR_NONE || bExact==0) ){
        rc = hctDbCsrInitEdks(pCsr, iKey);
        if( rc==SQLITE_OK && pCsr->pEdks ){
          i64 iEdksKey;
          i64 iMainKey;
          hctDbEdksCsrEntry(pCsr->pEdks, &iEdksKey, 0);
          hctDbCsrKey(pCsr, &iMainKey);
          switch( pCsr->eDir ){
            case BTREE_DIR_FORWARD:
              assert( iMainKey<=iKey && iEdksKey>=iKey );
              assert( iMainKey<=iEdksKey );
              if( iKey==iEdksKey && iMainKey<iKey ){
                assert( bExact==0 );
                sqlite3HctDbCsrNext(pCsr);
                if( sqlite3HctDbCsrEof(pCsr)==0 ){
                  i64 iCsrKey;
                  sqlite3HctDbCsrKey(pCsr, &iCsrKey);
                  bExact = (iCsrKey==iKey);
                  *pRes = (iCsrKey==iKey) ? 0 : +1;
                }else{
                  *pRes = -1;
                }
                return SQLITE_OK;
              }
              break;
            case BTREE_DIR_REVERSE:
              assert( iMainKey<=iKey && iEdksKey<=iKey );
              if( iMainKey<iEdksKey ){
                bExact = (iEdksKey==iKey);
                pCsr->eEdks = HCT_EDKS_YES;
              }
              break;
            case BTREE_DIR_NONE:
              if( iEdksKey==iKey && bExact==0 ){
                bExact = 1;
                pCsr->eEdks = HCT_EDKS_YES;
              }
              break;
          }
        }
      }

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

static void hctDbCsrInit(HctDatabase *pDb, u32 iRoot, HctDbCsr *pCsr){
  memset(pCsr, 0, sizeof(HctDbCsr));
  pCsr->pDb = pDb;
  pCsr->iRoot = iRoot;
}

/*
** Return number of bytes required by a cell...
**
** TODO: Add more parameters to support other types of cells.
*/
#if 0
static int hctDbCellSize(u64 iTid, u32 iOld, int nLocal){
  return (iTid ? sizeof(u64) : 0) + (iOld ? sizeof(u32) : 0) + nLocal;
}
#endif

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

static void hctDbInsertDiscard(HctDbWriter *p){
  int ii;
  for(ii=0; ii<p->nWritePg; ii++){
    sqlite3HctFilePageUnwrite(&p->aWritePg[ii]);
    sqlite3HctFilePageRelease(&p->aWritePg[ii]);
  }
  for(ii=0; ii<p->nDiscard; ii++){
    sqlite3HctFilePageRelease(&p->aDiscard[ii]);
  }
  if( p->writecsr.pRec ){
    sqlite3DbFree(p->writecsr.pKeyInfo->db, p->writecsr.pRec);
  }
  p->nWritePg = 0;
  p->nDiscard = 0;
  sqlite3_free(p->aWriteFpKey);
  p->aWriteFpKey = 0;
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


static int hctDbInsertFlushWrite(HctDatabase *pDb, HctDbWriter *p){
  int rc = SQLITE_OK;
  int ii;
  int eType = hctPagetype(p->aWritePg[0].aNew);
  HctFilePage root;

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
    rc = sqlite3HctFilePageCommit(&p->aWritePg[ii]);
  }

  /* If there is one, write the new root page to disk */
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFilePageRelease(&root);
  }

  if( (p->nWritePg>1 || p->nDiscard>0) && rc==SQLITE_OK ){
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
      }

      rc = hctDbInsert(pDb, &wr, iRoot, pRec, iKey, pPg->iPg, bDel, nFP, aFP);
    }
    hctBufferFree(&buf);

    if( rc==SQLITE_OK ){
      rc = hctDbInsertFlushWrite(pDb, &wr);
    }
  }

  for(ii=0; ii<p->nDiscard; ii++){
    sqlite3HctFilePageZero(&p->aDiscard[ii]);
  }

  hctDbInsertDiscard(p);
  return rc;
}

void sqlite3HctDbRollbackMode(HctDatabase *pDb, int bRollback){
  assert( bRollback==0 || bRollback==1 );
  assert( pDb->bRollback==0 || pDb->bRollback==1 );
  assert( pDb->bRollback==0 || bRollback==0 );
  pDb->bRollback = bRollback;
}

static HctDbIntkeyEntry *hctDbIntkeyEntry(u8 *aPg, int iCell){
  return iCell<0 ? 0 : (&((HctDbIntkeyLeaf*)aPg)->aEntry[iCell]);
}

static int hctDbFindRollbackData(
  HctDatabase *pDb,
  i64 iKey,
  u32 iOld,
  int *pbDel,                     /* OUT: Value of bDel for rollback entry */
  int *pnData,                    /* OUT: Value of nData for rollback entry */
  const u8 **paData               /* OUT: Value of aData for rollback entry */
){
  int rc;
  
  rc = sqlite3HctFilePageGetPhysical(pDb->pFile, iOld, &pDb->pa.writecsr.oldpg);
  if( rc==SQLITE_OK ){
    HctDbIntkeyEntry *pEntry;
    int b;
    int iCell = hctDbIntkeyLeafSearch(pDb->pa.writecsr.oldpg.aOld, iKey, &b);
    pEntry = hctDbIntkeyEntry(pDb->pa.writecsr.oldpg.aOld, iCell);
    if( (pEntry->flags & HCTDB_IS_DELETE) ){
      *pbDel = 1;
      *pnData = 0;
      *paData = 0;
    }else{
      int iOff = pEntry->iOff 
        + ((pEntry->flags & HCTDB_HAS_TID) ? 8 : 0)
        + ((pEntry->flags & HCTDB_HAS_OLD) ? 4 : 0)
        + ((pEntry->flags & HCTDB_HAS_OVFL) ? 4 : 0);
      *pbDel = 0;
      *pnData = pEntry->nSize;
      *paData = &pDb->pa.writecsr.oldpg.aOld[iOff];
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

#ifdef SQLITE_DEBUG
static void assert_page_is_ok(const u8 *aData, int nData){
  HctDbPageHdr *pPg = (HctDbPageHdr*)aData;

  if( hctPagetype(pPg)==HCT_PAGETYPE_INTKEY && pPg->nHeight==0 ){
    HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)pPg;
    int iEnd = nData;
    int iStart = sizeof(HctDbIntkeyLeaf) + sizeof(HctDbIntkeyEntry)*pPg->nEntry;
    int ii;
    int nRecTotal = 0;

    /* Assert that the nFreeGap header field looks right. */
    for(ii=0; ii<pLeaf->pg.nEntry; ii++){
      iEnd = MIN(iEnd, (int)pLeaf->aEntry[ii].iOff);
      nRecTotal += hctDbIntkeyEntrySize(&pLeaf->aEntry[ii], nData);
    }

    assert( pLeaf->hdr.nFreeGap==(iEnd - iStart) );
    assert( pLeaf->hdr.nFreeBytes==nData - (
          sizeof(HctDbIntkeyLeaf) 
        + sizeof(HctDbIntkeyEntry)*pLeaf->pg.nEntry 
        + nRecTotal
    ));
  }
}
#else
# define assert_page_is_ok(x)
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
      p->aDiscard[p->nDiscard++] = p->aWritePg[iRem];
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
static int hctDbLoadPeers(HctDatabase *pDb, HctDbWriter *p, int *piPg){
  int rc = SQLITE_OK;
  int iPg = *piPg;

  if( p->nWritePg==1 ){
    HctFilePage *pLeft = &p->aWritePg[0];

    assert( iPg==0 );
    if( 0==hctIsLeftmost(pLeft->aNew) ){
      HctDbCsr csr;
      int bDummy;
      HctFilePage *pCopy = &p->aDiscard[p->nDiscard++];

      hctDbCsrInit(pDb, p->writecsr.iRoot, &csr);
      if( hctPagetype(pLeft->aNew)==HCT_PAGETYPE_INTKEY ){
        i64 iKey = hctDbIntkeyFPKey(pLeft->aNew);
        assert( iKey!=SMALLEST_INT64 );
        rc = hctDbCsrSeek(&csr, p->iHeight, 0, iKey-1, &bDummy);
      }else{
        UnpackedRecord *pRec = 0;
        HctBuffer buf;
        int nData = 0;
        const u8 *aData = 0;
        memset(&buf, 0, sizeof(buf));
        rc = hctDbLoadRecord(pDb, &buf, pLeft->aNew, 0, &nData, &aData);
        if( rc==SQLITE_OK ){
          rc = hctDbAllocateUnpacked(&p->writecsr);
        }
        if( rc==SQLITE_OK ){
          pRec = p->writecsr.pRec;
          sqlite3VdbeRecordUnpack(p->writecsr.pKeyInfo, nData, aData, pRec);
          pRec->default_rc = 1;
          hctDbCsrSeek(&csr, p->iHeight, pRec, 0, &bDummy);
          pRec->default_rc = 0;
        }
        hctBufferFree(&buf);
      }

      if( rc==SQLITE_OK ){
        assert( csr.pg.iPg!=pLeft->iPg );
        *pCopy = *pLeft;
        *pLeft = csr.pg;
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

#if 1
    if( rc==SQLITE_OK ){
      HctDbPageHdr *pHdr = (HctDbPageHdr*)p->aWritePg[p->nWritePg-1].aNew;
      if( pHdr->iPeerPg ){
        HctFilePage *pPg = &p->aWritePg[p->nWritePg++];
        HctFilePage *pCopy = &p->aDiscard[p->nDiscard++];
        rc = sqlite3HctFilePageGet(pDb->pFile, pHdr->iPeerPg, pCopy);
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
#endif
  }

  return rc;
}


typedef struct HctDbEWriter HctDbEWriter;

/*
** An instance of this structure is used to write an EDKS. The structure
** itself is allocated on the stack and initialized by hctDbEWriterInit().
**
*/
struct HctDbEWriter {
  HctDatabase *pDb;
  u64 iMaxTid;
  int nPg;                        /* Number of valid entries in aPg[] */
  int rc;                         /* Return code */
  HctFilePage fanpg;              /* Fan page, if any */
  int iPg;                        /* Previous entry was written here */
  HctFilePage aPg[HCTDB_EKDS_MAX_HEIGHT];
};

static void hctDbEWriterInit(HctDbEWriter *p, HctDatabase *pDb){
  memset(p, 0, sizeof(HctDbEWriter));
  p->pDb = pDb;
}

static void hctDbEWriterNewPage(HctDbEWriter *p, int iPg){
  if( p->rc==SQLITE_OK ){
    p->rc = sqlite3HctFilePageNewPhysical(p->pDb->pFile, &p->aPg[iPg]);
    if( p->rc==SQLITE_OK ){
      HctDbIntkeyEdksLeaf *pPg;
      memset(p->aPg[p->iPg].aNew, 0, p->pDb->pgsz);
      pPg = (HctDbIntkeyEdksLeaf*)(p->aPg[p->iPg].aNew);
      pPg->pg.nHeight = p->iPg;
      pPg->pg.hdrFlags = HCT_PAGETYPE_INTKEY_EDKS;
    }
  }
}

static void hctDbEWriterIntkey(
  HctDbEWriter *p,                /* EWriter object */
  i64 iRowid,                     /* Rowid for new entry */
  u64 iTid,                       /* TID value for new entry */
  u32 iOldPgno                    /* Old page pointer for new entry */
){
  if( p->rc==SQLITE_OK ){
    HctDbIntkeyEdksLeaf *pPg;
    HctDbIntkeyEdksEntry *pEntry;
    const int nMax = (p->pDb->pgsz - sizeof(*pPg)) / sizeof(*pEntry);

    /* If the previous entry was written to an internal node, then that
    ** previous entry currently has an iChildPgno value of 0. Allocate a
    ** new child page and set said child page number. The new entry
    ** will become the first on the new child */
    if( p->iPg>0 ){
      pPg = (HctDbIntkeyEdksLeaf*)(p->aPg[p->iPg].aNew);
      p->iPg--;
      sqlite3HctFilePageRelease(&p->aPg[p->iPg]);
      hctDbEWriterNewPage(p, p->iPg);
      assert( pPg->aEntry[pPg->pg.nEntry-1].iChildPgno==0 );
      pPg->aEntry[pPg->pg.nEntry-1].iChildPgno = p->aPg[p->iPg].iNewPg;
    }

    while( 1 ){
      if( p->iPg==p->nPg ){
        hctDbEWriterNewPage(p, p->iPg);
        if( p->iPg>0 ){
          pPg = (HctDbIntkeyEdksLeaf*)(p->aPg[p->iPg].aNew);
          pPg->pg.iPeerPg = p->aPg[p->iPg-1].iNewPg;
        }
        p->nPg++;
      }
      pPg = (HctDbIntkeyEdksLeaf*)(p->aPg[p->iPg].aNew);

      if( pPg->pg.nEntry<nMax ){
        /* Entry fits on current page. Write it here and quit the loop. */
        pEntry = &pPg->aEntry[pPg->pg.nEntry++];
        pEntry->iRowid = iRowid;
        pEntry->iTid = iTid;
        pEntry->iOldPgno = iOldPgno;
        pEntry->iChildPgno = 0;
        break;
      }else{
        /* Entry does not fit on current page. Move to its parent. */
        p->iPg++;
      }
    }

    p->iMaxTid = MAX(iTid, p->iMaxTid);
  }
}

static int hctDbEWriterFlush(HctDbEWriter*, u64*, u32*);

static void hctDbEWriterMerge(
  HctDbEWriter *p, 
  HctDbEdksFanEntry *aEntry, 
  int *pnEntry
){
  int aHist[16] = {0, 0, 0, 0,  0, 0, 0, 0,  0, 0, 0, 0,  0, 0, 0, 0};
  int ii;
  int nEntry = *pnEntry;
  int iLvl;
  HctDbEdksCsr *pCsr = 0;         /* Cursor for merge inputs */
  int rc = SQLITE_OK;
  HctDbEWriter merge; 

  u64 iEdksVal = 0;
  u32 iEdksRoot = 0;
  int iOut = 0;

  memset(&merge, 0, sizeof(merge));

  /* Each entry in aEntry[] has an associated level - the number of times
  ** it has already been merged. Find the level with the largest number of
  ** entries. All EDKS on this level will be merge to a single structure. */
  for(ii=0; ii<nEntry; ii++){
    int nLevel = hctEdksValMergeCount(aEntry[ii].iEdksVal);
    if( nLevel>=ArraySize(aHist) ){
      rc = SQLITE_CORRUPT_BKPT;
      break;
    }
    aHist[nLevel]++;
  }
  iLvl = 0;
  for(ii=1; ii<ArraySize(aHist); ii++){
    if( aHist[ii]>aHist[iLvl] ) ii = iLvl;
  }

  hctDbEWriterInit(&merge, p->pDb);

  /* Open an EDKS cursor to merge and iterate through the contents of all
  ** EDKS structures with level=iLvl.  */
  for(ii=0; rc==SQLITE_OK && ii<nEntry; ii++){
    if( hctEdksValMergeCount(aEntry[ii].iEdksVal)==iLvl ){
      rc = hctDbEdksCsrBuild(p->pDb, aEntry[ii].iRoot, &pCsr);
    }
  }
  if( rc==SQLITE_OK ){
    rc = hctDbEdksCsrSeek(p->pDb, pCsr, SMALLEST_INT64, BTREE_DIR_FORWARD);
  }
  while( rc==SQLITE_OK && !hctDbEdksCsrEof(pCsr) ){
    u64 iTid = 0;
    i64 iKey = 0;
    u32 iOld = hctDbEdksCsrEntry(pCsr, &iKey, &iTid);
    hctDbEWriterIntkey(&merge, iKey, iTid, iOld);
    hctDbEdksCsrNext(pCsr); /* todo - error code? */
  }
  hctDbEdksCsrClose(pCsr);

  if( rc==SQLITE_OK ){
    rc = hctDbEWriterFlush(&merge, &iEdksVal, &iEdksRoot);
  }

  for(ii=0; ii<nEntry; ii++){
    if( hctEdksValMergeCount(aEntry[ii].iEdksVal)==iLvl ){
      if( iEdksVal ){
        aEntry[iOut].iEdksVal = iEdksVal;
        aEntry[iOut].iRoot = iEdksRoot;
        iEdksVal = 0;
        iOut++;
      }
    }else{
      aEntry[iOut++] = aEntry[ii];
    }
  }

  *pnEntry = iOut;
  p->rc = rc;
}

/*
** Add a pointer to the EDKS with root page iNewRoot and max TID iNewEdksVal to 
** the EDKS being written by (*p).
*/
static void hctDbEWriterEDKS(HctDbEWriter *p, u64 iNewEdksVal, u32 iNewRoot){
  if( p->rc==SQLITE_OK ){
    HctDbEdksFan *pFan;
    HctDatabase *pDb = p->pDb;

#if 0
    const int nMax = (pDb->pgsz - sizeof(*pFan)) / sizeof(HctDbEdksFanEntry);
#else
    const int nMax = 32;   /* TODO - this should be a configurable test param */
#endif

    /* If there is no fan-page, allocate it now */
    pFan = (HctDbEdksFan*)p->fanpg.aNew;
    if( pFan==0 ){
      p->rc = sqlite3HctFilePageNewPhysical(pDb->pFile, &p->fanpg);
      if( p->rc ) return;
      pFan = (HctDbEdksFan*)p->fanpg.aNew;
      memset(pFan, 0, sizeof(*pFan));
      pFan->pg.hdrFlags = HCT_PAGETYPE_EDKS_FAN;
    }

    if( hctEdksValIsFan(iNewEdksVal) ){
      HctFilePage pg;
      p->rc = sqlite3HctFilePageGetPhysical(pDb->pFile, iNewRoot, &pg);
      if( p->rc==SQLITE_OK ){
        HctDbEdksFan *pNew = (HctDbEdksFan*)pg.aOld;
        HctDbEdksFanEntry *aOut = 0;

        /* Allocate the aOut[] array so that it is large enough to merge the
        ** contents of both fan pages even if there are no duplicates.  */
        int nByte = sizeof(*aOut) * (pFan->pg.nEntry + pNew->pg.nEntry);
        if( nByte ){
          aOut = (HctDbEdksFanEntry*)sqlite3Malloc(nByte);
          if( !aOut ) p->rc = SQLITE_NOMEM;
        }

        /* Merge the contents of the two fan pages into the aOut[] array.
        ** Then, if it is already small enough, copy aOut[] back into the
        ** body of page pFan. Or, if aOut[] is too large, merge EDKS 
        ** structures until that is no longer the case.  */
        if( p->rc==SQLITE_OK ){
          int iNew = 0;
          int iFan = 0;
          int iOut = 0;

          while( iNew<pNew->pg.nEntry || iFan<pFan->pg.nEntry ){
            u32 iNewRoot = iNew<pNew->pg.nEntry ? pNew->aEntry[iNew].iRoot :0;
            u32 iFanRoot = iFan<pFan->pg.nEntry ? pFan->aEntry[iFan].iRoot :0;
            if( iFanRoot==0 || (iNewRoot && iNewRoot<iFanRoot) ){
              aOut[iOut] = pNew->aEntry[iNew++];
            }else{
              aOut[iOut] = pFan->aEntry[iFan++];
              if( iFanRoot==iNewRoot ) iNew++;
            }
            iOut++;
          }

          while( p->rc==SQLITE_OK && iOut>nMax ){
            hctDbEWriterMerge(p, aOut, &iOut);
          }
          memcpy(pFan->aEntry, aOut, sizeof(HctDbEdksFanEntry)*iOut);
          pFan->pg.nEntry = iOut;
        }

        sqlite3HctFilePageRelease(&pg);
        sqlite3_free(aOut);
      }
    }else{
      HctDbEdksFanEntry *pEntry;
      HctDbEdksFanEntry *pEof;

      /* If fan page is full, merge some EDKS together */
      if( pFan->pg.nEntry>=nMax ){
        assert( 0 );
      }

      /* Insert the new entry so that entries are sorted by iRoot field */
      pEof = &pFan->aEntry[pFan->pg.nEntry];
      for(pEntry=pFan->aEntry; pEntry<pEof; pEntry++){
        if( pEntry->iRoot>iNewRoot ) break;
      }
      if( pEntry!=pEof ){
        memmove(&pEntry[1], pEntry, (pEof-pEntry)*sizeof(*pEntry));
      }
      pEntry->iEdksVal = iNewEdksVal;
      pEntry->iRoot = iNewRoot;
      pFan->pg.nEntry++;
    }
  }
}

static int hctDbEWriterFlush(
  HctDbEWriter *p,
  u64 *piEdksVal,
  u32 *piEdksRoot
){
  int rc = p->rc;
  u32 iRoot = 0;
  int ii;
  if( p->fanpg.aNew ){
    HctDbEdksFan *pFan = (HctDbEdksFan*)p->fanpg.aNew;
    iRoot = p->fanpg.iNewPg;
    if( p->nPg ){
      /* Add an entry to the fan page for the new EDKS */
      pFan->aEntry[pFan->pg.nEntry].iEdksVal = p->iMaxTid;
      pFan->aEntry[pFan->pg.nEntry].iRoot = p->aPg[0].iNewPg;
      pFan->pg.nEntry++;
    }
    for(ii=0; ii<pFan->pg.nEntry; ii++){
      p->iMaxTid = MAX(p->iMaxTid, hctEdksValTid(pFan->aEntry[ii].iEdksVal));
    }
    p->iMaxTid = hctEdksValNew(p->iMaxTid, 0xFF);
  }else if( p->nPg ){
    iRoot = p->aPg[p->nPg-1].iNewPg;
  }

  if( rc==SQLITE_OK && p->fanpg.aNew ){
    rc = sqlite3HctFilePageRelease(&p->fanpg);
  }
  for(ii=0; rc==SQLITE_OK && ii<p->nPg; ii++){
    rc = sqlite3HctFilePageRelease(&p->aPg[ii]);
  }
  assert( rc==SQLITE_OK ); /* TODO */

  assert( (iRoot==0)==(p->iMaxTid==0) );
  *piEdksRoot = iRoot;
  *piEdksVal = p->iMaxTid;
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
*/
static void hctDbBalanceGetCellSz(
  HctDatabase *pDb,
  HctDbWriter *p,
  int iPg,
  int iInsert, int bClobber,
  int bDel,
  u32 pgOvfl,
  int nWrite,                     /* Bytes stored on page for new cell */
  u8 **aPgCopy,
  int nPgCopy,
  HctDbCellSz *aSz
){
  int ii;
  int iSz = 0;
  int szEntry = hctDbPageEntrySize(aPgCopy[0]);

  /* Deal with the special case of a new entry smaller than all existing
  ** entries in the page array being inserted. This may only happen with
  ** the first page in a list */
  if( bClobber==0 && iInsert==0 ){
    HctDbCellSz *pSz = &aSz[iSz++];
    assert( iPg==0 && hctIsLeftmost(aPgCopy[0]) );
    pSz->nByte = szEntry + hctDbEntrySize(nWrite, bClobber, pgOvfl);
    pSz->iPg = -1;
    pSz->bEdks = 0;
  }

  for(ii=0; ii<nPgCopy; ii++){
    int iEntry;
    HctDbPageHdr *pPg = (HctDbPageHdr*)aPgCopy[ii];
    for(iEntry=0; iEntry<pPg->nEntry; iEntry++){

      if( ii!=iPg || iEntry!=iInsert || bClobber==0 ){
        HctDbCellSz *pSz = &aSz[iSz++];
        int flags = 0;
        hctDbEntryInfo(pPg, iEntry, 0, &flags);
        pSz->nByte = szEntry + hctDbPageRecordSize(pPg, pDb->pgsz, iEntry);
        pSz->iPg = ii;
        pSz->iEntry = iEntry;
        pSz->bEdks = (flags & HCTDB_IS_DELETE) ? 1 : 0;
      }

      if( ii==iPg && (iEntry+!bClobber)==iInsert ){
        if( bDel==0 || p->iHeight==0 ){
          HctDbCellSz *pSz = &aSz[iSz++];
          pSz->nByte = szEntry + hctDbEntrySize(nWrite, bClobber, pgOvfl);
          pSz->iPg = -1;
          pSz->bEdks = bDel;
          //pSz->bEdks = 0;         /* TODO: Handle this key going to EKDS */
        }
      }
    }
  }

  /* First cell in the balance is an existing FP key. Not eligible for EKDS. */
  aSz[0].bEdks = 0;

  // assert( iSz==nSz );
}


/*
** Rebalance routine for intkey leaves, index leaves and index nodes.
*/
static int hctDbBalance(
  HctDatabase *pDb,
  HctDbWriter *p,
  int iPg,
  int iInsert, int bClobber,
  i64 iKey, 
  u32 iChildPg, 
  int bDel,
  u32 pgOvfl,
  int nWrite,
  int nData, 
  const u8 *aData
){
  int rc = SQLITE_OK;             /* Return Code */
  int iLeftPg;                    /* Index of leftmost page used in balance */
  int nIn = 1;                    /* Number of input peers for balance */
  int ii;                         /* Iterator used for various things */

  HctDbCellSz *aSz = 0;
  int nSz = 0;
  int iSz = 0;

  int nOut = 1;                   /* Number of output peers */
  int nRem;

  int aPgRem[5];
  int aPgFirst[6];
  u8 *aPgCopy[5];
  u8 *pFree = 0;
  u32 iOld = 0;
  int szEntry;
  int ePagetype;
  int bCreateEdks = 0;

  assert( bClobber || bDel==0 );

  if( bClobber ){
    iOld = p->iOldPgno;
  }

  /* If the HctDbWriter.aWritePg[] array still contains a single page, load
  ** some peer pages into it. */
  rc = hctDbLoadPeers(pDb, p, &iPg);
  if( rc!=SQLITE_OK ){
    return rc;
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

  /* Figure out how many cells there are. */
  for(ii=iLeftPg; ii<iLeftPg+nIn; ii++){
    HctDbPageHdr *pPg = (HctDbPageHdr*)p->aWritePg[ii].aNew;
    nSz += pPg->nEntry;
  }
  if( iInsert>=0 && bClobber==0 ) nSz++;
  if( bDel && p->iHeight>0 ) nSz--;

  /* Allocate enough space for the cell-size array and for a copy of 
  ** each input page. */
  pFree = (u8*)sqlite3Malloc(nIn*pDb->pgsz + nSz*sizeof(HctDbCellSz));
  if( pFree==0 ) return SQLITE_NOMEM;
  aSz = (HctDbCellSz*)&pFree[pDb->pgsz * nIn];

  /* Make a copy of each input page */
  for(ii=0; ii<nIn; ii++){
    aPgCopy[ii] = &pFree[pDb->pgsz * ii];
    memcpy(aPgCopy[ii], p->aWritePg[iLeftPg+ii].aNew, pDb->pgsz);
  }
  ePagetype = hctPagetype(aPgCopy[0]);

  /* Populate the aSz[] array with the sizes and locations of each cell */
  hctDbBalanceGetCellSz(
      pDb, p, iPg-iLeftPg, iInsert, bClobber, bDel, 
      pgOvfl, nWrite, aPgCopy, nIn, aSz
  );

  /* Determine whether or not this balance operation will generate EDKS */
  bCreateEdks = (ePagetype==HCT_PAGETYPE_INTKEY);

  /* Figure out how many output pages will be required. This loop calculates
  ** a mapping heavily biased to the left. */
  assert( sizeof(HctDbIntkeyLeaf)==sizeof(HctDbIndexLeaf) );
  aPgFirst[0] = 0;
  nRem = pDb->pgsz - sizeof(HctDbIntkeyLeaf);
  for(iSz=0; iSz<nSz; iSz++){
    if( bCreateEdks && aSz[iSz].bEdks ) continue;
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
  aPgFirst[nOut] = nSz;

  /* Adjust the packing calculated by the previous loop. */
  for(ii=nOut-1; ii>0; ii--){
    /* Try to shift cells from output page (ii-1) to output page (ii). Shift
    ** cells for as long as (a) there is more free space on page (ii) than on
    ** page (ii-1), and (b) there is enough free space on page (ii) to fit
    ** the last cell from page (ii-1).  */
    while( aPgRem[ii]>aPgRem[ii-1] ){       /* condition (a) */
      HctDbCellSz *pLast = &aSz[aPgFirst[ii]-1];
      while( bCreateEdks && pLast->bEdks ) pLast--;
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
  szEntry = hctDbPageEntrySize(aPgCopy[0]);
  for(ii=0; ii<nOut; ii++){
    HctDbEWriter edks;
    int iIdx = ii+iLeftPg;
    HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)p->aWritePg[iIdx].aNew;
    int iOff = pDb->pgsz;
    int iSz2;
    int iLast = (ii==(nOut-1) ? nSz : aPgFirst[ii+1]);
    int iEntry = 0;
    int nFree;
    int iIn;

    /* Initialize an EDKS writer, in case it is required */
    hctDbEWriterInit(&edks, pDb);
    for(iIn=0; iIn<nIn; iIn++){
      HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)aPgCopy[iIn];
      if( pLeaf->hdr.iEdksPg ){
        hctDbEWriterEDKS(&edks, pLeaf->hdr.iEdksVal, pLeaf->hdr.iEdksPg);
      }
    }

    memset(&pLeaf->hdr, 0, sizeof(pLeaf->hdr));

    for(iSz2=aPgFirst[ii]; iSz2<iLast; iSz2++){
      HctDbCellSz *pSz = &aSz[iSz2];
      int nCopy = (pSz->nByte - szEntry);

      iOff -= nCopy;
      if( ePagetype==HCT_PAGETYPE_INTKEY ){
        if( bCreateEdks && pSz->bEdks ){
          u64 iTid;
          u32 iOldPg;
          i64 iOldKey;
          if( pSz->iPg<0 ){
            iTid = pDb->iTid;
            iOldPg = iOld;
            iOldKey = iKey;
          }else{
            HctDbIntkeyLeaf *pSrc = (HctDbIntkeyLeaf*)aPgCopy[pSz->iPg];
            HctDbIntkeyEntry *pOld = &pSrc->aEntry[pSz->iEntry];
            assert( pSz->iPg>=0 );
            assert( pOld->flags==(HCTDB_HAS_TID|HCTDB_HAS_OLD|HCTDB_IS_DELETE));
            iTid = hctGetU64(&((u8*)pSrc)[pOld->iOff]);
            iOldPg = hctGetU32(&((u8*)pSrc)[pOld->iOff+8]);
            iOldKey = pOld->iKey;
          }
          hctDbEWriterIntkey(&edks, iOldKey, iTid, iOldPg);
          iOff += nCopy;
        }else{
          HctDbIntkeyEntry *pEntry = &pLeaf->aEntry[iEntry++];
          if( pSz->iPg<0 ){
            u8 *a = &((u8*)pLeaf)[iOff];
            hctDbCellPut(a, pDb->iTid, iOld, pgOvfl, aData, nWrite);
            pEntry->iKey = iKey;
            pEntry->nSize = nData;
            pEntry->flags = HCTDB_HAS_TID 
              | (bClobber?HCTDB_HAS_OLD:0) | (bDel?HCTDB_IS_DELETE:0)
              | (pgOvfl?HCTDB_HAS_OVFL:0);
          }else{
            HctDbIntkeyLeaf *pSrc = (HctDbIntkeyLeaf*)aPgCopy[pSz->iPg];
            HctDbIntkeyEntry *pOld = &pSrc->aEntry[pSz->iEntry];
            memcpy(&((u8*)pLeaf)[iOff], &((u8*)pSrc)[pOld->iOff], nCopy);
            *pEntry = *pOld;
          }
          pEntry->iOff = iOff;
        }
      }else if( p->iHeight==0 ){
        HctDbIndexEntry *pEntry = &((HctDbIndexLeaf*)pLeaf)->aEntry[iEntry];
        iEntry++;
        if( pSz->iPg<0 ){
          u8 *a = &((u8*)pLeaf)[iOff];
          hctDbCellPut(a, pDb->iTid, iOld, pgOvfl, aData, nWrite);
          pEntry->nSize = nData;
          pEntry->flags = HCTDB_HAS_TID 
            | (bClobber?HCTDB_HAS_OLD:0) | (bDel?HCTDB_IS_DELETE:0)
            | (pgOvfl?HCTDB_HAS_OVFL:0);
        }else{
          HctDbIndexLeaf *pSrc = (HctDbIndexLeaf*)aPgCopy[pSz->iPg];
          HctDbIndexEntry *pOld = &pSrc->aEntry[pSz->iEntry];
          memcpy(&((u8*)pLeaf)[iOff], &((u8*)pSrc)[pOld->iOff], nCopy);
          *pEntry = *pOld;
        }
        pEntry->iOff = iOff;
      }else{
        HctDbIndexNodeEntry *pEntry = &((HctDbIndexNode*)pLeaf)->aEntry[iEntry];
        iEntry++;
        if( pSz->iPg<0 ){
          assert( bDel==0 );
          hctDbCellPut(&((u8*)pLeaf)[iOff], 0, 0, pgOvfl, aData, nWrite);
          pEntry->nSize = nData;
          pEntry->flags = (pgOvfl?HCTDB_HAS_OVFL:0);
          pEntry->iChildPg = iChildPg; 
        }else{
          HctDbIndexNode *pSrc = (HctDbIndexNode*)aPgCopy[pSz->iPg];
          HctDbIndexNodeEntry *pOld = &pSrc->aEntry[pSz->iEntry];
          memcpy(&((u8*)pLeaf)[iOff], &((u8*)pSrc)[pOld->iOff], nCopy);
          *pEntry = *pOld;
        }
        pEntry->iOff = iOff;
      }
    }
    
    if( ePagetype==HCT_PAGETYPE_INTKEY ){
      nFree = iOff - sizeof(HctDbIntkeyLeaf) - iEntry*sizeof(HctDbIntkeyEntry);
    }else if( p->iHeight==0 ){
      nFree = iOff - sizeof(HctDbIndexLeaf) - iEntry*sizeof(HctDbIndexEntry);
    }else{
      nFree = iOff - sizeof(HctDbIndexNode) -iEntry*sizeof(HctDbIndexNodeEntry);
    }
    pLeaf->pg.nEntry = iEntry;
    pLeaf->hdr.nFreeBytes = pLeaf->hdr.nFreeGap = (u16)nFree;

    if( p->iHeight==0 ){
      rc = hctDbEWriterFlush(&edks, &pLeaf->hdr.iEdksVal, &pLeaf->hdr.iEdksPg);
    }
  }

  sqlite3_free(pFree);
  return rc;
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
    nIn = 3;
    iLeftPg--;
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

  pNode = (HctDbIntkeyNode*)p->aWritePg[iPg].aNew;
  if( (pNode->pg.nEntry>=nMax && bClobber==0) ){
    /* Need to do a balance operation to make room for the new entry */
    rc = hctDbBalanceIntkeyNode(pDb, p, iPg, iInsert, iKey, iChildPg);
  }else if( bDel ){
    assert( iInsert<pNode->pg.nEntry );
    if( iInsert==0 ){
      hctDbLoadPeers(pDb, p, &iPg);
      pNode = (HctDbIntkeyNode*)p->aWritePg[iPg].aNew;
    }
    if( iInsert<(pNode->pg.nEntry-1) ){
      int nByte = sizeof(HctDbIntkeyNodeEntry) * (pNode->pg.nEntry-1-iInsert);
      memmove(&pNode->aEntry[iInsert], &pNode->aEntry[iInsert+1], nByte);
    }
    pNode->pg.nEntry--;
    if( iInsert==0 || pNode->pg.nEntry<nMin ){
      rc = hctDbBalanceIntkeyNode(pDb, p, iPg, -1, 0, 0);
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
  int nMax = pDb->pgsz-sizeof(HctDbIntkeyLeaf)-sizeof(HctDbIntkeyEntry)-12;

  if( nData<=nMax ){
    *pnWrite = nData;
    *ppgOvfl = 0;
  }else{
    const int sz = (pDb->pgsz - sizeof(HctDbPageHdr));
    int nLocal = hctDbLocalsize(aTarget, pDb->pgsz, nData);
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
  int iPg = 0;
  int bClobber = 0;
  u8 *aTarget;                    /* Page to write new entry to */
  int nReq;
  int nDataReq;
  int nDataFree;
  u32 pgOvfl = 0;
  int nWrite = nData;
  u16 nRecField = (pRec ? pRec->nField : 0);

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

  if( pRec && pRec->pKeyInfo->nUniqField ){
    int ii;
    u16 nUniqField = pRec->pKeyInfo->nUniqField;
    for(ii=0; ii<nUniqField; ii++){
      if( pRec->aMem[ii].flags & MEM_Null ){
        nUniqField = nRecField;
        break;
      }
    }
    pRec->nField = nUniqField;
  }

  /* If the page array is empty, seek the write cursor to find the leaf
  ** page on which to insert this new entry or delete key. */
  if( p->nWritePg==0 ){
    hctDbCsrInit(pDb, iRoot, &p->writecsr);
    if( pRec ) p->writecsr.pKeyInfo = pRec->pKeyInfo;
    rc = hctDbCsrSeek(&p->writecsr, p->iHeight, pRec, iKey, &bClobber);
    if( rc ) return rc;

    p->aWritePg[0] = p->writecsr.pg;
    memset(&p->writecsr.pg, 0, sizeof(HctFilePage));
    p->nWritePg = 1;
    rc = sqlite3HctFilePageWrite(&p->aWritePg[0]);
    if( rc ) return rc;
    memcpy(p->aWritePg[0].aNew, p->aWritePg[0].aOld, pDb->pgsz);
    rc = hctDbSetWriteFpKey(pDb, p);
    if( rc ) return rc;
    p->iOldPgno = (p->aWritePg[0].iPagemap & 0xFFFFFFFF);
  }
  assert( p->nWritePg>0 && p->aWritePg[0].aNew );

  /* Figure out which page in the aWritePg[] array the new entry belongs
  ** on. This can be optimized later - by remembering which page the 
  ** previous key was stored on.  */
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
    if( p->iHeight==0 ){
      rc = hctDbIndexSearch(pDb,
          p->aWritePg[iPg].aNew, pRec, &iInsert, &bClobber
      );
      if( rc!=SQLITE_OK ) return rc;
    }else{
      iInsert = hctDbIndexNodeSearch(p->aWritePg[iPg].aNew, pRec, &bClobber);
    }
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
  if( pRec ) pRec->nField = nRecField;

  /* Writes to an intkey internal node are handled separately. They are
  ** different because they used fixed size key/data pairs. All other types
  ** of page use variably sized key/data entries. */
  if( pRec==0 && p->iHeight>0 ){
    return hctDbInsertIntkeyNode(
        pDb, p, iPg, iInsert, iKey, iChildPg, bClobber, bDel
    );
  }

  /* At this point it is known that the new entry should be inserted into 
  ** page pDb->aWritePg[iPg]. */
  aTarget = p->aWritePg[iPg].aNew;

  /* If this write is to a leaf page and the search above failed to find an
  ** exact match, search any EDKS for a match. This is required for both
  ** rollback (to find the data for the original entry) and for regular
  ** writes (to see if there is a conflict).  */
  if( p->iHeight==0 ){
    u32 iEdksPgno = 0;
    u64 iEdksTid = 0;

    if( bClobber ){
      int flags;
      int iOff = hctDbEntryInfo(aTarget, iInsert, 0, &flags);
      if( flags & HCTDB_HAS_TID ){
        iEdksTid = hctGetU64(&aTarget[iOff]);
      }
      if( flags & HCTDB_HAS_OLD ){
        iEdksPgno = hctGetU32(&aTarget[iOff+8]);
      }
    }else if( pRec==0 ){
      HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)aTarget;
      if( pLeaf->hdr.iEdksPg ){
        HctDbEdksCsr *pEdks = 0;
        i64 iEdksKey = 0;
        rc = hctDbEdksCsrInit(pDb, pLeaf->hdr.iEdksPg, 
            pLeaf->hdr.iEdksVal, BTREE_DIR_NONE, iKey, &pEdks
        );
        assert( rc==SQLITE_OK );  /* todo */
        if( pEdks ){
          iEdksPgno = hctDbEdksCsrEntry(pEdks, &iEdksKey, &iEdksTid);
          if( iEdksKey!=iKey ){
            iEdksTid = 0;
            iEdksPgno = 0;
          }
          hctDbEdksCsrClose(pEdks);
        }
      }
    }

    if( pDb->bRollback ){
      if( iEdksTid!=pDb->iTid ){
        rc = hctDbInsertFlushWrite(pDb, p);
        if( rc==SQLITE_OK ) rc = SQLITE_DONE;
      }else if( iEdksPgno==0 ){
        bDel = 1;
        aData = 0;
        nData = 0;
      }else{
        assert( pRec==0 );          /* TODO */
        rc = hctDbFindRollbackData(pDb, iKey, iEdksPgno, &bDel, &nData, &aData);
      }

      if( rc!=SQLITE_OK ) return rc;

    }else if( iEdksTid ){
      /* Check for a write conflict */
      u64 iCid = hctDbTMapLookup(pDb, iEdksTid);
      if( iCid>pDb->iSnapshotId ){
        hctDbInsertDiscard(p);
        return SQLITE_BUSY;
      }
    }

  }

  /* If any overflow pages are required, create them now. */
  rc = hctDbInsertOverflow(pDb, aTarget, nData, aData, &nWrite, &pgOvfl);
assert( rc==SQLITE_OK );

  nDataReq = 8 + (pgOvfl?4:0) + nWrite;
  if( bClobber ){
    if( pDb->bRollback==0 ){
      if( p->iHeight>0 ){
        /* A delete from an internal index node */
        assert( bDel );
        nDataReq = nReq = -4;
      }else{
        int flags;
        int iOff = hctDbEntryInfo(aTarget, iInsert, 0, &flags);
        if( flags & HCTDB_HAS_TID ){
          u64 iTid = hctGetU64(&aTarget[iOff]);
          u64 iCid = hctDbTMapLookup(pDb, iTid);
          if( iCid>pDb->iSnapshotId ){
            hctDbInsertDiscard(p);
            return SQLITE_BUSY;
          }
        }
      }
    }
    nDataReq += 4;                /* Space for "old page" number */
    nReq = nDataReq;

    /* Figure out how many bytes of data being freed by removing the 
    ** old record from the data area.  */
    nDataFree = hctDbPageRecordSize(aTarget, pDb->pgsz, iInsert);
  }else{
    nReq = nDataReq + hctDbPageEntrySize(aTarget);
    nDataFree = 0;
  }

  if( hctDbFreegap(aTarget)<nReq 
   || (iInsert==0 && p->iHeight>0) 
   || ( 
       hctDbFreebytes(aTarget)+nDataFree-nDataReq>(2*pDb->pgsz/3)
    && (((HctDbPageHdr*)aTarget)->iPeerPg || hctIsLeftmost(aTarget)==0)
   )
  ){
    rc = hctDbBalance(pDb, p, iPg, iInsert, bClobber, 
        iKey, iChildPg, bDel, pgOvfl, nWrite, nData, aData
    );
  }else{
    u8 f = HCTDB_HAS_TID|(bClobber?HCTDB_HAS_OLD:0)|(bDel?HCTDB_IS_DELETE:0);
    u32 iOld = 0;                 /* "old" page number for cell, or zero */
    int iOff;                     /* Offset of cell within leaf page */
    i64 iTid = pDb->iTid;

    assert_page_is_ok(aTarget, pDb->pgsz);

    f |= (pgOvfl ? HCTDB_HAS_OVFL : 0);

#if 0
    if( pLeaf->hdr.nFreeGap<nReq ){
      /* TODO: repack cells on page */
      assert( 0 );
    }
#endif

    if( pRec==0 ){
      HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)aTarget;
      HctDbIntkeyEntry *pEntry; 

      iOff = sizeof(HctDbIntkeyLeaf) 
        + pLeaf->pg.nEntry*sizeof(HctDbIntkeyEntry)
        + pLeaf->hdr.nFreeGap 
        - nDataReq;

      if( bClobber==0 ){
        if( iInsert<pLeaf->pg.nEntry ){
          int nByte = sizeof(HctDbIntkeyEntry) * (pLeaf->pg.nEntry-iInsert);
          memmove(&pLeaf->aEntry[iInsert+1], &pLeaf->aEntry[iInsert], nByte);
        }
        pLeaf->pg.nEntry++;
      }else{
        pLeaf->hdr.nFreeBytes += 
          hctDbIntkeyEntrySize(&pLeaf->aEntry[iInsert], pDb->pgsz);
        iOld = p->iOldPgno;
      }

      pEntry = &pLeaf->aEntry[iInsert];
      pEntry->iKey = iKey;
      pEntry->nSize = nData;
      pEntry->iOff = iOff;
      pEntry->flags = f;
    }else if( p->iHeight==0 ){
      HctDbIndexLeaf *pLeaf = (HctDbIndexLeaf*)aTarget;
      HctDbIndexEntry *pEntry;

      iOff = sizeof(HctDbIndexLeaf) 
        + pLeaf->pg.nEntry*sizeof(HctDbIndexEntry)
        + pLeaf->hdr.nFreeGap 
        - nDataReq;

      if( bClobber==0 ){
        if( iInsert<pLeaf->pg.nEntry ){
          int nByte = sizeof(HctDbIndexEntry) * (pLeaf->pg.nEntry-iInsert);
          memmove(&pLeaf->aEntry[iInsert+1], &pLeaf->aEntry[iInsert], nByte);
        }
        pLeaf->pg.nEntry++;
      }else{
        iOld = p->iOldPgno;
      }

      pEntry = &pLeaf->aEntry[iInsert];
      pEntry->nSize = nData;
      pEntry->iOff = iOff;
      pEntry->flags = f;
    }else{
      HctDbIndexNode *pINode = (HctDbIndexNode*)aTarget;
      HctDbIndexNodeEntry *pEntry;

      iOff = sizeof(HctDbIndexNode) 
        + pINode->pg.nEntry*sizeof(HctDbIndexNodeEntry)
        + pINode->hdr.nFreeGap 
        - nDataReq;

      pEntry = &pINode->aEntry[iInsert];
      if( bClobber==0 ){
        if( iInsert<pINode->pg.nEntry ){
          int nByte = sizeof(HctDbIndexNodeEntry) * (pINode->pg.nEntry-iInsert);
          memmove(&pINode->aEntry[iInsert+1], &pINode->aEntry[iInsert], nByte);
        }
        pINode->pg.nEntry++;
      }else if( bDel ){
        int nLocal = hctDbIndexNodeEntrySize(pEntry, pDb->pgsz);
        assert( nDataReq==0 );
        if( pEntry->iOff==iOff ){
          pINode->hdr.nFreeGap += nLocal + sizeof(HctDbIndexNodeEntry);
        }else{
          pINode->hdr.nFreeGap += sizeof(HctDbIndexNodeEntry);
        }
        pINode->hdr.nFreeBytes += nLocal + sizeof(HctDbIndexNodeEntry);
        if( iInsert<pINode->pg.nEntry-1 ){
          int nByte = sizeof(HctDbIndexNodeEntry)*(pINode->pg.nEntry-iInsert-1);
          memmove(&pINode->aEntry[iInsert], &pINode->aEntry[iInsert+1], nByte);
        }
        pINode->pg.nEntry--;
        return SQLITE_OK;
      }else{
        iOld = p->iOldPgno;
      }

      pEntry->nSize = nData;
      pEntry->iOff = iOff;
      pEntry->flags = (pgOvfl ? HCTDB_HAS_OVFL : 0);
      pEntry->iChildPg = iChildPg;
      iTid = 0;
      assert( iChildPg!=0 );
    }

    hctDbCellPut(&aTarget[iOff], iTid, iOld, pgOvfl, aData, nWrite);
    ((HctDbIndexNode*)aTarget)->hdr.nFreeGap -= nReq;
    ((HctDbIndexNode*)aTarget)->hdr.nFreeBytes -= nReq;
    assert_page_is_ok(aTarget, pDb->pgsz);
  }

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
  rc = hctDbInsert(pDb, &pDb->pa, iRoot, pRec, iKey, 0, bDel, nData, aData);
  if( rc==SQLITE_LOCKED ){
    rc = SQLITE_OK;
    *pnRetry = pDb->pa.nWriteKey;
    pDb->pa.nWriteKey = 0;
  }else{
    *pnRetry = 0;
  }
  return rc;
}

/*
** Start the write-phase of a transaction.
*/
int sqlite3HctDbStartWrite(HctDatabase *p){
  int rc = SQLITE_OK;
  HctTMapClient *pTMapClient = sqlite3HctFileTMapClient(p->pFile);

  assert( p->iTid==0 );
  assert( p->bRollback==0 );

  p->iTid = sqlite3HctFileAllocateTransid(p->pFile);
  rc = sqlite3HctTMapNewTID(pTMapClient, p->iSnapshotId, p->iTid, &p->pTmap);
  return rc;
}

/*
** This is called once the current transaction has been completely 
** written to disk.
*/
int sqlite3HctDbEndWrite(HctDatabase *p){
  /* HctTMapClient *pTMapClient = sqlite3HctFileTMapClient(p->pFile); */
  HctTMap *pTmap = p->pTmap;
  int rc = SQLITE_OK;
  u64 iCID;                       /* Commit ID for transaction */
  int iMap;
  int iEntry;

  assert( p->bRollback==0 );
  assert( p->pa.nWritePg==0 );
  assert( p->pa.aWriteFpKey==0 );
  assert( pTmap->iFirstTid<=p->iTid );
  assert( pTmap->iFirstTid+(pTmap->nMap*HCT_TMAP_PAGESIZE)>p->iTid );

  iCID = sqlite3HctFileAllocateCID(p->pFile);
  iMap = (p->iTid - pTmap->iFirstTid) / HCT_TMAP_PAGESIZE;
  iEntry = (p->iTid - pTmap->iFirstTid) % HCT_TMAP_PAGESIZE;
  HctAtomicStore(&pTmap->aaMap[iMap][iEntry], iCID);

  /* sqlite3HctTMapNewTID(pTMapClient, iCID, p->iTid, &p->pTmap); */
  p->iTid = 0;
  return rc;
}

int sqlite3HctDbEndRead(HctDatabase *pDb){
  HctTMapClient *pTMapClient = sqlite3HctFileTMapClient(pDb->pFile);
  assert( (pDb->iSnapshotId==0)==(pDb->pTmap==0) );
  if( pDb->iSnapshotId ){
    sqlite3HctTMapEnd(pTMapClient, pDb->iSnapshotId);
    pDb->pTmap = 0;
    pDb->iSnapshotId = 0;
  }
  return SQLITE_OK;
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

  p = (HctDbCsr*)sqlite3MallocZero(sizeof(HctDbCsr));
  if( p==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    p->pDb = pDb;
    p->iRoot = iRoot;
    p->iCell = -1;
    p->pCsrNext = pDb->pCsrList;
    p->pKeyInfo = pKeyInfo;
    pDb->pCsrList = p;
    hctDbSnapshotOpen(pDb);
  }
  *ppCsr = p;
  return rc;
}

void sqlite3HctDbCsrClose(HctDbCsr *pCsr){
  if( pCsr ){
    HctDatabase *pDb = pCsr->pDb;
    HctDbCsr **pp;
    hctDbCsrReset(pCsr);
    for(pp=&pDb->pCsrList; *pp!=pCsr; pp=&(*pp)->pCsrNext);
    *pp = pCsr->pCsrNext;
    if( pCsr->pRec ) sqlite3DbFree(pCsr->pKeyInfo->db, pCsr->pRec);
    hctBufferFree(&pCsr->rec);
    sqlite3_free(pCsr);
  }
}

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

int sqlite3HctDbCsrEof(HctDbCsr *pCsr){
  return pCsr==0 || (pCsr->iCell<0 && pCsr->eEdks==HCT_EDKS_NO);
}

int sqlite3HctDbCsrFirst(HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  HctFile *pFile = pCsr->pDb->pFile;
  HctFilePage pg;
  HctDbPageHdr *pPg;
  u32 iPg = pCsr->iRoot;

  hctDbCsrReset(pCsr);
  pCsr->eDir = BTREE_DIR_FORWARD;

  /* Starting at the root of the tree structure, follow the left-most 
  ** pointers to find the left-most node in the list of leaves. */
  while( 1 ){
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

  /* Open an EDKS cursor for the new leaf page, if required. */
  if( rc==SQLITE_OK ){
    memcpy(&pCsr->pg, &pg, sizeof(pg));
    rc = hctDbCsrInitEdks(pCsr, 0);
  }

  /* Skip forward to the first visible entry, if any. */
  if( rc==SQLITE_OK ){
    assert( pCsr->eEdks==HCT_EDKS_NO );
    pCsr->iCell = -1;
    rc = sqlite3HctDbCsrNext(pCsr);
  }

  return rc;
}

int sqlite3HctDbCsrLast(HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  HctFile *pFile = pCsr->pDb->pFile;
  u32 iPg = pCsr->iRoot;
  HctDbPageHdr *pPg = 0;
  HctFilePage pg;

  hctDbCsrReset(pCsr);
  pCsr->eDir = BTREE_DIR_REVERSE;

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

  /* Open an EDKS cursor for the new leaf page, if required. */
  if( rc==SQLITE_OK ){
    assert( pPg->nHeight==0 && pPg->iPeerPg==0 );
    memcpy(&pCsr->pg, &pg, sizeof(pg));
    rc = hctDbCsrInitEdks(pCsr, LARGEST_INT64);
  }

  if( rc==SQLITE_OK ){
    pCsr->iCell = pPg->nEntry;
    rc = sqlite3HctDbCsrPrev(pCsr);
  }
  return rc;
}

int sqlite3HctDbCsrNext(HctDbCsr *pCsr){
  int rc = SQLITE_OK;

  sqlite3HctFilePageRelease(&pCsr->oldpg);

  do {
    if( pCsr->eEdks==HCT_EDKS_NO ){
      /* Advance the main cursor */
      HctDbPageHdr *pPg = (HctDbPageHdr*)pCsr->pg.aOld;
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
        if( pCsr->pEdks ){
          pCsr->eEdks = HCT_EDKS_TRAIL;
        }else{
          /* If there is no EDKS cursor for the previous leaf page, load 
          ** any EDKS cursor for this new leaf page now */
          rc = hctDbCsrInitEdks(pCsr, SMALLEST_INT64);
        }
      }
    }else{
      assert( pCsr->eEdks==HCT_EDKS_YES || pCsr->eEdks==HCT_EDKS_TRAIL );
      hctDbEdksCsrNext(pCsr->pEdks);
      if( hctDbEdksCsrEof(pCsr->pEdks) ){
        hctDbEdksCsrClose(pCsr->pEdks);
        pCsr->pEdks = 0;
        if( pCsr->eEdks==HCT_EDKS_TRAIL ){
          rc = hctDbCsrInitEdks(pCsr, SMALLEST_INT64);
        }
        pCsr->eEdks = HCT_EDKS_NO;
      }
    }

    if( rc==SQLITE_OK && pCsr->pEdks && pCsr->iCell>=0 ){
      i64 iEdksKey;
      i64 iMainKey;
      hctDbEdksCsrEntry(pCsr->pEdks, &iEdksKey, 0);
      hctDbCsrKey(pCsr, &iMainKey);
      if( iMainKey<=iEdksKey ){
        if( pCsr->eEdks==HCT_EDKS_TRAIL ){
          hctDbEdksCsrClose(pCsr->pEdks);
          pCsr->pEdks = 0;
          rc = hctDbCsrInitEdks(pCsr, SMALLEST_INT64);
        }
        pCsr->eEdks = HCT_EDKS_NO;
      }else if( pCsr->eEdks==HCT_EDKS_NO ){
        pCsr->eEdks = HCT_EDKS_YES;
      }
    }

  }while( hctDbCsrFindVersion(&rc, pCsr)==0 );

  return rc;
}

int sqlite3HctDbCsrPrev(HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  assert( pCsr->eEdks==HCT_EDKS_NO || pCsr->pEdks );
  assert( pCsr->eEdks==HCT_EDKS_NO || pCsr->eEdks==HCT_EDKS_YES );

  sqlite3HctFilePageRelease(&pCsr->oldpg);

  do {
    
    if( pCsr->eEdks==HCT_EDKS_NO ){
      /* Advance the main cursor */
      pCsr->iCell--;
      if( pCsr->iCell<0 ){

        hctDbEdksCsrClose(pCsr->pEdks);
        pCsr->pEdks = 0;

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
          if( rc==SQLITE_OK ){
            rc = hctDbCsrInitEdks(pCsr, iKey-1);
          }
        }
      }
    }else{
      hctDbEdksCsrPrev(pCsr->pEdks);
      if( hctDbEdksCsrEof(pCsr->pEdks) ){
        hctDbEdksCsrClose(pCsr->pEdks);
        pCsr->pEdks = 0;
        pCsr->eEdks = HCT_EDKS_NO;
      }
    }

    /* If there is an EDKS cursor and the main cursor is currently valid,
    ** set HctDbCsr.eEdks to indicate which represents the current cursor
    ** entry.  */
    if( rc==SQLITE_OK && pCsr->pEdks && pCsr->iCell>=0 ){
      i64 iEdksKey;
      i64 iMainKey;
      hctDbEdksCsrEntry(pCsr->pEdks, &iEdksKey, 0);
      hctDbCsrKey(pCsr, &iMainKey);
      if( iMainKey>=iEdksKey ){
        pCsr->eEdks = HCT_EDKS_NO;
      }else{
        pCsr->eEdks = HCT_EDKS_YES;
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

/*************************************************************************
**************************************************************************
** Below are the virtual table implementations.
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
    pBody += sqlite3VdbeSerialGet(pBody, (u32)iSerialType, &mem);
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

      default:
        zRet = sqlite3_mprintf("%z%sunsupported(%d)", zRet, zSep, 
            sqlite3_value_type(&mem)
        );
        break;
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
  HctDbIntkeyEdksEntry *pEdks = 0;
  HctDbEdksFanEntry *pFan = 0;

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

    case HCT_PAGETYPE_EDKS_FAN:
      pFan = &((HctDbEdksFan*)pCur->pg.aOld)->aEntry[pCur->iEntry];
      break;

    default:
      assert( eType==HCT_PAGETYPE_INTKEY_EDKS );
      pEdks = &((HctDbIntkeyEdksLeaf*)pCur->pg.aOld)->aEntry[pCur->iEntry];
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
      if( pEdks ) sqlite3_result_int64(ctx, pEdks->iRowid);
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
      if( pFan ){
        if( i==7 ) sqlite3_result_int64(ctx, (i64)pFan->iEdksVal);
        if( i==8 ) sqlite3_result_int64(ctx, (i64)pFan->iRoot);
      }
      else if( pEdks ){
        if( i==7 ) sqlite3_result_int64(ctx, (i64)pEdks->iTid);
        if( i==8 ) sqlite3_result_int64(ctx, (i64)pEdks->iOldPgno);
      }
      else if( pIntkeyNode==0 ){
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
      if( pEdks==0 && pIntkeyNode==0 && pFan==0 ){
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

  int rc;

  rc = sqlite3_create_module(db, "hctdb", &hctdbModule, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_module(db, "hctentry", &hctentryModule, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFileVtabInit(db);
  }
  return rc;
}
