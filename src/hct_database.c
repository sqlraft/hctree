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
#include <string.h>
#include <assert.h>

typedef struct HctDatabase HctDatabase;
typedef struct HctDbGlobal HctDbGlobal;
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
typedef struct HctDbTMap HctDbTMap;

struct HctDbCsr {
  HctDatabase *pDb;               /* Database that owns this cursor */
  HctDbCsr *pCsrNext;             /* Next cursor in list belonging to pDb */
  u32 iRoot;                      /* Root page cursor is opened on */
  KeyInfo *pKeyInfo;
  UnpackedRecord *pRec;
  int eDir;                       /* Direction cursor will step after Seek() */
  int iCell;                      /* Current cell within page */
  HctFilePage pg;                 /* Current database page */

  int iOldCell;                   /* Cell within old page */
  HctFilePage oldpg;              /* Old page, if required */
};

#define HCTDB_MAX_DIRTY 8
struct HctDbWriter {
  int iHeight;                    /* Height to write at (0==leaves) */
  HctFilePage aWritePg[HCTDB_MAX_DIRTY+2];
  int nWritePg;                   /* Number of valid entries in aWritePg[] */
  int nWriteKey;                  /* Number of new keys in aWritePg[] array */
  i64 iWriteFpKey;
  u8 *aWriteFpKey;
  HctDbCsr writecsr;
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

  u64 iSnapshotId;                /* Snapshot id for reading */
  HctDbTMap *pTMap;               /* Transaction map */
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
#define HCT_PAGETYPE_INTKEY   0x01
#define HCT_PAGETYPE_INDEX    0x02
#define HCT_PAGETYPE_OVERFLOW 0x03

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
  u64 iEdksTid;                   /* EDKS TID value, if any. 0 otherwise */
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

/*
** Flags for HctDbIntkeyEntry.flags
*/
#define HCTDB_HAS_TID   0x01
#define HCTDB_HAS_OLD   0x02
#define HCTDB_HAS_OVFL  0x04
#define HCTDB_IS_DELETE 0x08

/*
** Flags used in the spare 8-bits of the transaction-id fields on each
** non-overflow page of the db.
*/
#define HCT_IS_DELETED   (((u64)0x01)<<56)
#define HCT_HAS_OVERFLOW (((u64)0x02)<<56)

#define HCT_TID_MASK     ((((u64)0x00FFFFFF)<<32)|0xFFFFFFFF)

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

/*
** Update connection pDb to use the latest transaction-map.
*/
static int hctDbTMapGet(HctDatabase *pDb, int nMap){
  int rc = SQLITE_OK;
  HctDbGlobal *pGlobal = pDb->pGlobal;
  HctDbTMap *pTMap = pDb->pTMap;
  pDb->pTMap = 0;
  sqlite3_mutex_enter(pGlobal->pMutex);
  if( pTMap ){
    if( pTMap->nRef==1 ){
      sqlite3_free(pTMap);
    }else{
      pTMap->nRef--;
    }
  }
  pTMap = pGlobal->pTMap;
  if( pTMap->nMap<nMap ){
    int nByte = sizeof(HctDbTMap) + sizeof(u64*)*nMap;
    HctDbTMap *pNew = sqlite3MallocZero(nByte);
    if( pNew==0 ){
      rc = SQLITE_NOMEM_BKPT;
    }else{
      int ii;
      pNew->nRef = 1;
      pNew->iFirstTid = pTMap->iFirstTid;
      pNew->nMap = nMap;
      pNew->aaMap = (u64**)&pNew[1];
      for(ii=0; rc==SQLITE_OK && ii<nMap; ii++){
        if( ii<pTMap->nMap ){
          pNew->aaMap[ii] = pTMap->aaMap[ii];
        }else{
          u64 *aNew = sqlite3_malloc(sizeof(u64)*HCTDB_TMAP_SIZE);
          if( aNew==0 ){
            rc = SQLITE_NOMEM_BKPT;
          }else{
            memset(aNew, 0xFF, sizeof(u64)*HCTDB_TMAP_SIZE);
            pNew->aaMap[ii] = aNew;
          }
        }
      }
      if( rc==SQLITE_OK ){
        if( pTMap->nRef==1 ){
          sqlite3_free(pTMap);
        }else{
          pTMap->nRef--;
        }
        pGlobal->pTMap = pTMap = pNew;
      }
    }
  }
  if( rc==SQLITE_OK ){
    pTMap->nRef++;
    pDb->pTMap = pTMap;
  }
  sqlite3_mutex_leave(pGlobal->pMutex);
  assert( rc==SQLITE_OK || nMap>0 );
  return rc;
}

/*
** Release connection pDb's transaction-map, if it has one.
*/
static void hctDbTMapRelease(HctDatabase *pDb){
  if( pDb->pTMap ){
    HctDbGlobal *pGlobal = pDb->pGlobal;
    sqlite3_mutex_enter(pGlobal->pMutex);
    pDb->pTMap->nRef--;
    if( pDb->pTMap->nRef==0 ){
      sqlite3_free(pDb->pTMap);
    }
    sqlite3_mutex_leave(pGlobal->pMutex);
    pDb->pTMap = 0;
  }
}

static int hctDbTMapInit(HctDatabase *pDb){
  int rc = SQLITE_OK;
  HctDbGlobal *pGlobal = pDb->pGlobal;
  sqlite3_mutex_enter(pGlobal->pMutex);
  if( pGlobal->pTMap==0 ){
    HctDbTMap *pNew;
    u64 *aPage;
    pNew = (HctDbTMap*)sqlite3MallocZero(sizeof(HctDbTMap) + sizeof(u64*));
    aPage = (u64*)sqlite3Malloc(sizeof(u64) * HCTDB_TMAP_SIZE);
    if( pNew==0 || aPage==0 ){
      sqlite3_free(pNew);
      sqlite3_free(aPage);
      rc = SQLITE_NOMEM_BKPT;
    }else{
      pNew->nRef = 1;
      pNew->iFirstTid = sqlite3HctFileGetTransid(pDb->pFile) + 1;
      pNew->nMap = 1;
      pNew->aaMap = (u64**)&pNew[1];
      pNew->aaMap[0] = aPage;
      memset(aPage, 0xFF, sizeof(u64) * HCTDB_TMAP_SIZE);
      pGlobal->pTMap = pNew;
    }
  }
  sqlite3_mutex_leave(pGlobal->pMutex);
  return rc;
}

static u64 hctDbTMapLookup(HctDatabase *pDb, u64 iTid){
  HctDbTMap *pTMap = pDb->pTMap;
  int iMap = (iTid - pTMap->iFirstTid) / HCTDB_TMAP_SIZE;
  if( iMap>=0 && iMap<pTMap->nMap ){
    int iOff = (iTid - pTMap->iFirstTid) % HCTDB_TMAP_SIZE;
    return pTMap->aaMap[iMap][iOff];
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
    }else{
      rc = hctDbTMapInit(pNew);
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
    hctDbTMapRelease(p);
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
      pNode->hdr.nFreeBytes = szPage - sizeof(HctDbIndexNode);
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
  if( pDb->iSnapshotId==0 ){
    u64 iMaxTid = 0;
    HctDbTMap *pTMap = pDb->pTMap;

    pDb->iSnapshotId = sqlite3HctFileGetSnapshotid(pDb->pFile, &iMaxTid);
    if( pTMap==0 || iMaxTid>=(pTMap->iFirstTid + pTMap->nMap*HCTDB_TMAP_SIZE) ){
      hctDbTMapGet(pDb, 0);
    }
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

static i64 hctDbIntkeyFPKey(void *aPg){
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


static int hctDbIndexFPKey(
  u8 *aPg, int nPg,               /* IN: Page data and size */
  const u8 **ppKey, int *pnKey,   /* OUT: pointer to key buffer and size */
  u8 **ppFree                     /* OUT: caller must sqlite3_free() this */
){
  if( hctPageheight(aPg)==0 ){
    HctDbIndexLeaf *pLeaf = (HctDbIndexLeaf*)aPg;
    HctDbIndexEntry *pEntry = &pLeaf->aEntry[0];
    *ppKey = &aPg[ hctDbOffset(pEntry->iOff, pEntry->flags) ];
    *pnKey = pEntry->nSize;
    *ppFree = 0;
  }else{
    HctDbIndexNode *pNode = (HctDbIndexNode*)aPg;
    HctDbIndexNodeEntry *pEntry = &pNode->aEntry[0];
    *ppKey = &aPg[ hctDbOffset(pEntry->iOff, pEntry->flags) ];
    *pnKey = pEntry->nSize;
    *ppFree = 0;
  }

  return SQLITE_OK;
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
  int *pbExact
){
  HctDbIndexLeaf *pLeaf = (HctDbIndexLeaf*)aPg;
  int i1 = 0;
  int i2 = pLeaf->pg.nEntry;

  assert( pLeaf->pg.nHeight==0 );
  while( i2>i1 ){
    int iTest = (i1+i2)/2;
    HctDbIndexEntry *pEntry = &pLeaf->aEntry[iTest];
    int iOff = hctDbOffset(pEntry->iOff, pEntry->flags);
    int res = sqlite3VdbeRecordCompare(pEntry->nSize, &aPg[iOff], pRec);

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
  u8 *aPg,
  int *pbGe
){
  const u8 *aFP = 0;
  int nFP = 0;
  u8 *pFree = 0;
  int res;
  int rc;

  rc = hctDbIndexFPKey(aPg, pDb->pgsz, &aFP, &nFP, &pFree);
assert( rc==SQLITE_OK );
  res = sqlite3VdbeRecordCompare(nFP, aFP, pRec);
  sqlite3_free(pFree);

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

static int nCall = 0;
nCall++;

  while( rc==SQLITE_OK ){
    if( iPg ) rc = sqlite3HctFilePageGet(pFile, iPg, &pCsr->pg);
    if( rc==SQLITE_OK ){
      HctDbPageHdr *pHdr = (HctDbPageHdr*)pCsr->pg.aOld;
      int i2 = pHdr->nEntry-1;
      int bExact;
      if( pHdr->nHeight==0 ){
        if( pRec ){
          i2 = hctDbIndexLeafSearch(pCsr->pg.aOld, pRec, &bExact);
        }else{
          i2 = hctDbIntkeyLeafSearch(pCsr->pg.aOld, iKey, &bExact);
        }
        if( bExact==0 ) i2--;
      }else if( pRec ){
        HctDbIndexNode *pNode = (HctDbIndexNode*)pCsr->pg.aOld;
        i2 = hctDbIndexNodeSearch(pNode, pRec, &bExact);
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
** Find the cell in page HctDbCsr.oldpg with a key identical to the current
** cell in HctDbCsr.pg. Set HctDbCsr.iOldCell to the cell index.
**
** Return SQLITE_OK if successful, or SQLITE_CORRUPT if the required key
** cannot be found in HctDbCsr.oldpg.
*/
static int hctDbFindKeyInOldPage(HctDbCsr *pCsr){
  int iCell;
  if( pCsr->pKeyInfo ){
    int bExact;
    HctDbIndexEntry *p = &((HctDbIndexLeaf*)pCsr->pg.aOld)->aEntry[pCsr->iCell];
    u8 *aCell = &pCsr->pg.aOld[hctDbOffset(p->iOff, p->flags)];
    if( pCsr->pRec==0 ){
      pCsr->pRec = sqlite3VdbeAllocUnpackedRecord(pCsr->pKeyInfo);
      if( pCsr->pRec==0 ) return SQLITE_NOMEM_BKPT;
    }
    sqlite3VdbeRecordUnpack(pCsr->pKeyInfo, p->nSize, aCell, pCsr->pRec);
    iCell = hctDbIndexLeafSearch(pCsr->pg.aOld, pCsr->pRec, &bExact);
    if( bExact==0 ) iCell = -1;
  }else{
    iCell = hctDbFindKeyInPage(
        (HctDbIntkeyLeaf*)pCsr->oldpg.aOld, 
        ((HctDbIntkeyLeaf*)pCsr->pg.aOld)->aEntry[pCsr->iCell].iKey
    );
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
  if( *pRc==SQLITE_OK && pCsr->iCell>=0 ){
    u8 *aPage = pCsr->pg.aOld;
    u8 flags = 0;
    int iOff = hctDbCellOffset(aPage, pCsr->iCell, &flags);
    assert( pCsr->oldpg.aOld==0 );
    while( flags & HCTDB_HAS_TID ){
      u64 iTid = hctGetU64(&aPage[iOff]);
      u64 iCid = hctDbTMapLookup(pCsr->pDb, iTid);
      if( iCid<=pCsr->pDb->iSnapshotId ){
        break;
      }

      if( (flags & HCTDB_HAS_OLD)==0 ){
        sqlite3HctFilePageRelease(&pCsr->oldpg);
        return 0;
      }else{
        int iCell;
        int rc;
        HctFile *pFile = pCsr->pDb->pFile;
        u32 iOld = hctGetU32(&aPage[iOff+8]);
        
        sqlite3HctFilePageRelease(&pCsr->oldpg);
        rc = sqlite3HctFilePageGetPhysical(pFile, iOld, &pCsr->oldpg);
        if( rc==SQLITE_OK ){
          iCell = hctDbFindKeyInOldPage(pCsr);
        }

        if( rc==SQLITE_OK ){
          if( iCell<0 ){
            rc = SQLITE_CORRUPT_BKPT;
          }else{
            aPage = pCsr->oldpg.aOld;
            iOff = hctDbCellOffset(pCsr->oldpg.aOld, iCell, &flags);
            pCsr->iOldCell = iCell;
          }
        }
        if( rc!=SQLITE_OK ){
          *pRc = rc;
          return 1;
        }
      }
    }

    /* This entry is visible to the snapshot. Return true if it is a
    ** real version, or false if it is a delete key. */
    return ((flags & HCTDB_IS_DELETE) ? 0 : 1);
  }
  return 1;
}

static void hctDbCsrReset(HctDbCsr *pCsr){
  sqlite3HctFilePageRelease(&pCsr->pg);
  sqlite3HctFilePageRelease(&pCsr->oldpg);
  pCsr->iCell = -1;
  /* pCsr->eDir = 0; */
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

  /* Cursor now points to the largest entry less than or equal to the
  ** supplied key (pRec or iKey). If the supplied key is smaller than all
  ** entries in the table, then pCsr->iCell is set to -1.  */
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
    }else if( 0==hctDbCsrFindVersion(&rc, pCsr) ){
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

  return rc;
}

static void hctDbCsrInit(HctDatabase *pDb, u32 iRoot, HctDbCsr *pCsr){
  memset(pCsr, 0, sizeof(HctDbCsr));
  pCsr->pDb = pDb;
  pCsr->iRoot = iRoot;
}

#if 0
static void hctDbSplitcopy(
  void *pTo,                      /* Target buffer */
  void *pFrom,                    /* Source buffer */
  int nEntry,                     /* Number of entries in source array */
  int szEntry,                    /* Size of each array entry in bytes */
  int iNew                        /* Index at which to insert gap */
){
  u8 *pDest = (u8*)pTo;
  u8 *pSrc = (u8*)pFrom;
  if( iNew>0 ){
    memcpy(pDest, pSrc, iNew*szEntry);
  }
  if( iNew<nEntry ){
    int nByte = (nEntry-iNew) * szEntry;
    memcpy(&pDest[(iNew+1)*szEntry], &pSrc[iNew*szEntry], nByte);
  }
}
#endif

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

static void hctDbInsertDiscard(HctDbWriter *p){
  int ii;
  for(ii=0; ii<p->nWritePg; ii++){
    sqlite3HctFilePageUnwrite(&p->aWritePg[ii]);
    sqlite3HctFilePageRelease(&p->aWritePg[ii]);
  }
  if( p->writecsr.pRec ){
    sqlite3DbFree(p->writecsr.pKeyInfo->db, p->writecsr.pRec);
  }
  p->nWritePg = 0;
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

static int hctDbInsertFlushWrite(HctDatabase *pDb, HctDbWriter *p){
  int rc = SQLITE_OK;
  int ii;
  if( p->nWritePg==1 ){
    rc = sqlite3HctFilePageRelease(&p->aWritePg[0]);
    p->nWritePg = 0;
  }else if( p->nWritePg>1 ){
    int eType = hctPagetype(p->aWritePg[0].aNew);
    HctFilePage root;

    memset(&root, 0, sizeof(root));

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

    if( rc==SQLITE_OK ){
      HctDbWriter wr;
      memset(&wr, 0, sizeof(wr));
      wr.iHeight = p->iHeight + 1;
      for(ii=1; ii<p->nWritePg && rc==SQLITE_OK; ii++){
        HctFilePage *pPg = &p->aWritePg[ii];
        if( eType==HCT_PAGETYPE_INTKEY ){
          i64 iKey = hctDbIntkeyFPKey(pPg->aOld);
          rc = hctDbInsert(pDb, &wr, p->writecsr.iRoot, 0, iKey,pPg->iPg,0,0,0);
        }else{
          const u8 *aFPKey = 0;
          int nFPKey = 0;
          u8 *pFree = 0;
          rc = hctDbIndexFPKey(pPg->aOld, pDb->pgsz, &aFPKey, &nFPKey, &pFree);
          if( rc==SQLITE_OK ){
            UnpackedRecord *pRec = p->writecsr.pRec;
            if( pRec==0 ){
              pRec = sqlite3VdbeAllocUnpackedRecord(p->writecsr.pKeyInfo);
              if( pRec==0 ){
                rc = SQLITE_NOMEM_BKPT;
                break;
              }
              p->writecsr.pRec = pRec;
            }
            sqlite3VdbeRecordUnpack(p->writecsr.pKeyInfo, nFPKey, aFPKey, pRec);
            rc = hctDbInsert(pDb, &wr, p->writecsr.iRoot, 
                pRec, 0, pPg->iPg, 0, nFPKey, aFPKey
            );
          }
          sqlite3_free(pFree);
        }
      }

      if( rc==SQLITE_OK ){
        rc = hctDbInsertFlushWrite(pDb, &wr);
      }
    }

  }else{
    assert( p->nWriteKey>0 );
    for(ii=0; rc==SQLITE_OK && ii<p->nWritePg; ii++){
      HctFilePage *pPg = &p->aWritePg[p->nWritePg-1-ii];
      rc = sqlite3HctFilePageRelease(pPg);
    }
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

static int hctDbFindRollback(
  HctDatabase *pDb,
  i64 iKey,
  u8 *aPg,
  int iCell,
  int *pbDel,                     /* OUT: Value of bDel for rollback entry */
  int *pnData,                    /* OUT: Value of nData for rollback entry */
  const u8 **paData               /* OUT: Value of aData for rollback entry */
){
  HctDbIntkeyEntry *pEntry = hctDbIntkeyEntry(aPg, iCell);
  int rc = SQLITE_OK;
  if( pEntry==0 
   || pEntry->iKey!=iKey
   || (pEntry->flags & HCTDB_HAS_TID)==0
   || hctGetU64(&aPg[pEntry->iOff])!=pDb->iTid
  ){
    return SQLITE_DONE;
  }

  if( (pEntry->flags & HCTDB_HAS_OLD)==0 ){
    *pbDel = 1;
    *pnData = 0;
    *paData = 0;
  }else{
    u32 iOld = hctGetU32(&aPg[pEntry->iOff+8]);
    rc = sqlite3HctFilePageGetPhysical(pDb->pFile, iOld, &pDb->pa.writecsr.oldpg);
    if( rc==SQLITE_OK ){
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
  }

  return rc;
}

int sqlite3HctDbInsertFlush(HctDatabase *pDb, int *pnRetry){
  int rc = hctDbInsertFlushWrite(pDb, &pDb->pa);
  if( rc==SQLITE_LOCKED ){
    *pnRetry = pDb->pa.nWriteKey;
    rc = SQLITE_OK;
  }else{
    *pnRetry = 0;
  }
  pDb->pa.nWriteKey = 0;
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
        HctDbIndexEntry *pEntry = &((HctDbIndexLeaf*)pg.aOld)->aEntry[0];
        p->aWriteFpKey = sqlite3_malloc(pEntry->nSize);
        if( p->aWriteFpKey==0 ){
          rc = SQLITE_NOMEM_BKPT;
        }else{
          int iOff = hctDbOffset(pEntry->iOff, pEntry->flags);
          memcpy(p->aWriteFpKey, &pg.aOld[iOff], pEntry->nSize);
        }
      }
      sqlite3HctFilePageRelease(&pg);
    }
  }

  return rc;
}

static int hctDbIntkeyEntrySize(HctDbIntkeyEntry *pEntry){
  int sz = pEntry->nSize + 
    + ((pEntry->flags & HCTDB_HAS_TID) ? 8 : 0)
    + ((pEntry->flags & HCTDB_HAS_OLD) ? 4 : 0);
  return sz;
}

static int hctDbIndexEntrySize(HctDbIndexEntry *pEntry){
  int sz = pEntry->nSize + 
    + ((pEntry->flags & HCTDB_HAS_TID) ? 8 : 0)
    + ((pEntry->flags & HCTDB_HAS_OLD) ? 4 : 0);
  return sz;
}

static int hctDbIndexNodeEntrySize(HctDbIndexNodeEntry *pEntry){
  int sz = pEntry->nSize;
  return sz;
}

static int hctDbIntkeyNewSize(int nData, int bClobber){
  int sz = nData + 8 + (bClobber ? 4 : 0);
  return sz;
}

/*
** The pointer passed as the first argument is a pointer to a buffer
** containing a page that uses variable sized records. That is, an
** intkey leaf page, or an index leaf or node page. This function
** returns the number of bytes of record-area space consumed by
** entry iEntry on the page.
*/
static int hctDbPageRecordSize(void *aPg, int iEntry){
  int eType = hctPagetype(aPg);
  if( eType==HCT_PAGETYPE_INTKEY ){
    assert( hctPageheight(aPg)==0 );
    return hctDbIntkeyEntrySize(&((HctDbIntkeyLeaf*)aPg)->aEntry[iEntry]);
  }else if( hctPageheight(aPg)==0 ){
    return hctDbIndexEntrySize(&((HctDbIndexLeaf*)aPg)->aEntry[iEntry]);
  }
  return hctDbIndexNodeEntrySize(&((HctDbIndexNode*)aPg)->aEntry[iEntry]);
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
      nRecTotal += hctDbIntkeyEntrySize(&pLeaf->aEntry[ii]);
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

  return rc;
}


static int hctDbBalance(
  HctDatabase *pDb,
  HctDbWriter *p,
  int iPg,
  int iInsert, int bClobber,
  i64 iKey, 
  u32 iChildPg, 
  int bDel,
  int nData, const u8 *aData
){
  typedef struct HctDbCellSz HctDbCellSz;
  struct HctDbCellSz {
    int nByte;                      /* Size of cell in bytes */
    i16 iPg;                        /* Index of input page */
    u16 iEntry;                     /* Entry of cell on input page */
  };

  int rc = SQLITE_OK;
  int iLeftPg = iPg;              /* Index of leftmost page used in balance */
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

  if( bClobber ){
    iOld = (p->aWritePg[iPg].iPagemap & 0xFFFFFFFF);
  }

  /* TODO: Load in more peers as required! */

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

  /* Populate the aSz[] array with the sizes and locations of each cell */
  iSz = 0;
  szEntry = hctDbPageEntrySize(aPgCopy[0]);
  ePagetype = hctPagetype(aPgCopy[0]);
  if( bClobber==0 && iInsert==0 ){
    HctDbCellSz *pSz = &aSz[iSz++];
    assert( iPg==0 );
    pSz->nByte = szEntry + hctDbIntkeyNewSize(nData, bClobber);
    pSz->iPg = -1;
  }
  for(ii=0; ii<nIn; ii++){
    int iEntry;
    HctDbPageHdr *pPg = (HctDbPageHdr*)aPgCopy[ii];
    for(iEntry=0; iEntry<pPg->nEntry; iEntry++){
      if( (ii+iLeftPg)!=iPg || iEntry!=iInsert || bClobber==0 ){
        HctDbCellSz *pSz = &aSz[iSz++];
        pSz->nByte = szEntry + hctDbPageRecordSize(pPg, iEntry);
        pSz->iPg = ii;
        pSz->iEntry = iEntry;
      }
      if( (ii+iLeftPg)==iPg && (iEntry+!bClobber)==iInsert ){
        HctDbCellSz *pSz = &aSz[iSz++];
        pSz->nByte = szEntry + hctDbIntkeyNewSize(nData, bClobber);
        pSz->iPg = -1;
      }
    }
  }
  assert( iSz==nSz );

  /* Figure out how many output pages will be required. This loop calculates
  ** a mapping heavily biased to the left. */
  assert( sizeof(HctDbIntkeyLeaf)==sizeof(HctDbIndexLeaf) );
  aPgFirst[0] = 0;
  nRem = pDb->pgsz - sizeof(HctDbIntkeyLeaf);
  for(iSz=0; iSz<nSz; iSz++){
    if( aSz[iSz].nByte>nRem ){
      aPgRem[nOut-1] = nRem;
      aPgFirst[nOut] = iSz;
      nOut++;
      nRem = pDb->pgsz - sizeof(HctDbIntkeyLeaf);
      assert( nOut<ArraySize(aPgRem) );
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
      int nSz = aSz[aPgFirst[ii]-1].nByte;  /* Size of last cell on (ii-1) */
      if( nSz>aPgRem[ii] ) break;           /* condition (b) */
      aPgRem[ii] -= nSz;
      aPgRem[ii-1] += nSz;
      aPgFirst[ii]--;
    }
  }

  assert( nOut>=nIn );            /* todo: handle this case */

  /* Allocate any required new pages and link them into the list. */
  if( nOut>nIn ){
    rc = hctDbExtendWriteArray(pDb, p, iLeftPg+nIn, nOut-nIn);
assert( rc==SQLITE_OK );
  }

  /* Populate the output pages */
  for(ii=0; ii<nOut; ii++){
    int iIdx = ii+iLeftPg;
    HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)p->aWritePg[iIdx].aNew;
    int iEntry;
    int iOff = pDb->pgsz;

    pLeaf->pg.nEntry = aPgFirst[ii+1] - aPgFirst[ii];
    memset(&pLeaf->hdr, 0, sizeof(pLeaf->hdr));
    pLeaf->hdr.nFreeBytes = pDb->pgsz - sizeof(HctDbIntkeyLeaf);

    for(iEntry=0; iEntry<pLeaf->pg.nEntry; iEntry++){
      HctDbCellSz *pSz = &aSz[iEntry + aPgFirst[ii]];
      int nCopy = (pSz->nByte - szEntry);

      iOff -= nCopy;
      if( ePagetype==HCT_PAGETYPE_INTKEY ){
        HctDbIntkeyEntry *pEntry = &pLeaf->aEntry[iEntry];
        if( pSz->iPg<0 ){
          hctDbCellPut(&((u8*)pLeaf)[iOff], pDb->iTid, iOld, aData, nData);
          pEntry->iKey = iKey;
          pEntry->nSize = nData;
          pEntry->flags = HCTDB_HAS_TID 
            | (bClobber?HCTDB_HAS_OLD:0) | (bDel?HCTDB_IS_DELETE:0);
        }else{
          HctDbIntkeyLeaf *pSrc = (HctDbIntkeyLeaf*)aPgCopy[pSz->iPg];
          HctDbIntkeyEntry *pOld = &pSrc->aEntry[pSz->iEntry];
          memcpy(&((u8*)pLeaf)[iOff], &((u8*)pSrc)[pOld->iOff], nCopy);
          *pEntry = *pOld;
        }
        pEntry->iOff = iOff;
      }else if( p->iHeight==0 ){
        HctDbIndexEntry *pEntry = &((HctDbIndexLeaf*)pLeaf)->aEntry[iEntry];
        if( pSz->iPg<0 ){
          hctDbCellPut(&((u8*)pLeaf)[iOff], pDb->iTid, iOld, aData, nData);
          pEntry->nSize = nData;
          pEntry->flags = HCTDB_HAS_TID 
            | (bClobber?HCTDB_HAS_OLD:0) | (bDel?HCTDB_IS_DELETE:0);
        }else{
          HctDbIndexLeaf *pSrc = (HctDbIndexLeaf*)aPgCopy[pSz->iPg];
          HctDbIndexEntry *pOld = &pSrc->aEntry[pSz->iEntry];
          memcpy(&((u8*)pLeaf)[iOff], &((u8*)pSrc)[pOld->iOff], nCopy);
          *pEntry = *pOld;
        }
        pEntry->iOff = iOff;
      }else{
        HctDbIndexNodeEntry *pEntry = &((HctDbIndexNode*)pLeaf)->aEntry[iEntry];
        if( pSz->iPg<0 ){
          hctDbCellPut(&((u8*)pLeaf)[iOff], 0, 0, aData, nData);
          pEntry->nSize = nData;
          pEntry->flags = 0; 
          pEntry->iChildPg = iChildPg; 
        }else{
          HctDbIndexNode *pSrc = (HctDbIndexNode*)aPgCopy[pSz->iPg];
          HctDbIndexNodeEntry *pOld = &pSrc->aEntry[pSz->iEntry];
          memcpy(&((u8*)pLeaf)[iOff], &((u8*)pSrc)[pOld->iOff], nCopy);
          *pEntry = *pOld;
        }
        pEntry->iOff = iOff;
      }
      pLeaf->hdr.nFreeBytes -= pSz->nByte;
    }
    
    pLeaf->hdr.nFreeGap = pLeaf->hdr.nFreeBytes;
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
  int iLeftPg = 0;                /* Index of left-most page in balance */
  int ii;                         /* Iterator variable */
  int nTotal = 0;                 /* Total number of keys for balance */
  u8 *aPgCopy[3];
  u8 *pFree = 0;

  /* TODO: Load in more peers as required! */

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
assert( nOut>=nIn );
  if( nOut>nIn ){
    rc = hctDbExtendWriteArray(pDb, p, iLeftPg+nIn, nOut-nIn);
assert( rc==SQLITE_OK );
  }

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
  HctDbIntkeyNode *pNode;
  int rc = SQLITE_OK;

assert( bDel==0 );
assert( bClobber==0 );

  pNode = (HctDbIntkeyNode*)p->aWritePg[iPg].aNew;
  if( pNode->pg.nEntry>=nMax && bClobber==0 ){
    /* Need to do a balance operation to make room for the new entry */
    rc = hctDbBalanceIntkeyNode(pDb, p, iPg, iInsert, iKey, iChildPg);
  }else{
    if( iInsert<pNode->pg.nEntry && bClobber==0 ){
      int nByte = sizeof(HctDbIntkeyNodeEntry) * pNode->pg.nEntry-iInsert;
      memmove(&pNode->aEntry[iInsert+1], &pNode->aEntry[iInsert], nByte);
    }
    pNode->aEntry[iInsert].iKey = iKey;
    pNode->aEntry[iInsert].iChildPg = iChildPg;
    pNode->aEntry[iInsert].unused = 0;
    pNode->pg.nEntry++;
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
  HctDbIntkeyLeaf *pLeaf = 0;
  int nReq;
  int nDataReq;

  p->nWriteKey++;

assert( bDel==0 || p->iHeight==0 );

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

  if( p->nWritePg==0 ){
    /* Page array is empty. Seek the write cursor. */
    hctDbCsrInit(pDb, iRoot, &p->writecsr);
    if( pRec ) p->writecsr.pKeyInfo = pRec->pKeyInfo;
    rc = hctDbCsrSeek(&p->writecsr, p->iHeight, pRec, iKey, &bClobber);
    assert( rc==SQLITE_OK );
    iInsert = p->writecsr.iCell;

    if( pDb->bRollback && p->iHeight==0 ){
      assert( pRec==0 );
      rc = hctDbFindRollback(
          pDb, iKey, p->writecsr.pg.aOld, iInsert, &bDel, &nData, &aData
      );
      if( rc!=SQLITE_OK ) return rc;
      assert( bClobber );
    }else{
      if( bDel && bClobber==0 ){
        hctDbCsrReset(&p->writecsr);
        return SQLITE_OK;
      }
    }

    p->aWritePg[0] = p->writecsr.pg;
    memset(&p->writecsr.pg, 0, sizeof(HctFilePage));
    p->nWritePg = 1;
    rc = sqlite3HctFilePageWrite(&p->aWritePg[0]);
    assert( rc==SQLITE_OK );      /* todo! */
    memcpy(p->aWritePg[0].aNew, p->aWritePg[0].aOld, pDb->pgsz);

    rc = hctDbSetWriteFpKey(pDb, p);
    if( bClobber==0 ) iInsert++;
  }else{
    /* Figure out which page in the aWritePg[] array the new entry belongs
    ** on. This can be optimized later - by remembering which page the 
    ** previous key was stored on.  */
    if( pRec ){
      for(iPg=0; iPg<p->nWritePg-1; iPg++){
        const u8 *aK;
        int nK;
        u8 *pFree = 0;
        rc = hctDbIndexFPKey(p->aWritePg[iPg+1].aNew,pDb->pgsz,&aK,&nK,&pFree);
        if( rc!=SQLITE_OK ) return rc;
        if( sqlite3VdbeRecordCompare(nK, aK, pRec)>0 ) break;
      }
      if( p->iHeight==0 ){
        iInsert = hctDbIndexLeafSearch(p->aWritePg[iPg].aNew, pRec, &bClobber);
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

    if( pDb->bRollback && p->iHeight==0 ){
      assert( pRec==0 );
      rc = hctDbFindRollback(
          pDb, iKey, p->aWritePg[iPg].aNew, iInsert, &bDel, &nData, &aData
      );
      if( rc!=SQLITE_OK ) return rc;
    }else{
      if( bDel && bClobber==0 ) return SQLITE_OK;
    }
  }

  /* 
  ** At this point it is known that the new entry should be inserted into 
  ** page pDb->aWritePg[iPg]. If bClobber is true then it deletes 
  ** or clobbers existing entry iInsert. Or, if bClobber is false, then the
  ** new key is inserted to the right of entry iInsert.  
  */

  if( pRec==0 && p->iHeight>0 ){
    return hctDbInsertIntkeyNode(
        pDb, p, iPg, iInsert, iKey, iChildPg, bClobber, bDel
    );
  }

  pLeaf = (HctDbIntkeyLeaf*)p->aWritePg[iPg].aNew;
  nDataReq = 8 + nData;
  if( bClobber ){
    HctDbIntkeyEntry *pEntry = &pLeaf->aEntry[iInsert];
assert( pRec==0 );  /* TODO  - index support */
assert( p->iHeight==0 );
    if( pDb->bRollback==0 && (pEntry->flags & HCTDB_HAS_TID) ){
      u64 iTid = hctGetU64(&((u8*)pLeaf)[pEntry->iOff]);
      u64 iCid = hctDbTMapLookup(pDb, iTid);
      if( iCid>pDb->iSnapshotId ){
        hctDbInsertDiscard(p);
        return SQLITE_BUSY;
      }
    }
    nDataReq += 4;
    nReq = nDataReq;
  }else{
    if( pRec ){
      if( p->iHeight>0 ){
        nReq = nDataReq + sizeof(HctDbIndexNodeEntry);
      }else{
        nReq = nDataReq + sizeof(HctDbIndexEntry);
      }
    }else{
      nReq = nDataReq + sizeof(HctDbIntkeyEntry);
    }
  }

  if( pLeaf->hdr.nFreeGap<nReq ){
    rc = hctDbBalance(
        pDb, p, iPg, iInsert, bClobber, iKey, iChildPg, bDel, nData, aData
    );
  }else{
    u8 f = HCTDB_HAS_TID|(bClobber?HCTDB_HAS_OLD:0)|(bDel?HCTDB_IS_DELETE:0);
    u32 iOld = 0;                 /* "old" page number for cell, or zero */
    int iOff;                     /* Offset of cell within leaf page */
    i64 iTid = pDb->iTid;

    assert_page_is_ok((u8*)pLeaf, pDb->pgsz);

    if( pLeaf->hdr.nFreeGap<nReq ){
      /* TODO: repack cells on page */
      assert( 0 );
    }

    if( pRec==0 ){
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
        pLeaf->hdr.nFreeBytes += hctDbIntkeyEntrySize(&pLeaf->aEntry[iInsert]);
        iOld = (p->aWritePg[0].iPagemap & 0xFFFFFFFF);
      }

      pEntry = &pLeaf->aEntry[iInsert];
      pEntry->iKey = iKey;
      pEntry->nSize = nData;
      pEntry->iOff = iOff;
      pEntry->flags = f;
    }else if( p->iHeight==0 ){
      HctDbIndexLeaf *pILeaf = (HctDbIndexLeaf*)pLeaf;
      HctDbIndexEntry *pEntry;

      iOff = sizeof(HctDbIndexLeaf) 
        + pILeaf->pg.nEntry*sizeof(HctDbIndexEntry)
        + pILeaf->hdr.nFreeGap 
        - nDataReq;

      if( bClobber==0 ){
        if( iInsert<pILeaf->pg.nEntry ){
          int nByte = sizeof(HctDbIndexEntry) * (pILeaf->pg.nEntry-iInsert);
          memmove(&pILeaf->aEntry[iInsert+1], &pILeaf->aEntry[iInsert], nByte);
        }
        pILeaf->pg.nEntry++;
      }else{
        iOld = (p->aWritePg[0].iPagemap & 0xFFFFFFFF);
      }

      pEntry = &pILeaf->aEntry[iInsert];
      pEntry->nSize = nData;
      pEntry->iOff = iOff;
      pEntry->flags = f;
    }else{
      HctDbIndexNode *pINode = (HctDbIndexNode*)pLeaf;
      HctDbIndexNodeEntry *pEntry;

      iOff = sizeof(HctDbIndexNode) 
        + pINode->pg.nEntry*sizeof(HctDbIndexNodeEntry)
        + pINode->hdr.nFreeGap 
        - nDataReq;

      if( bClobber==0 ){
        if( iInsert<pINode->pg.nEntry ){
          int nByte = sizeof(HctDbIndexNodeEntry) * (pINode->pg.nEntry-iInsert);
          memmove(&pINode->aEntry[iInsert+1], &pINode->aEntry[iInsert], nByte);
        }
        pINode->pg.nEntry++;
      }else{
        iOld = (p->aWritePg[0].iPagemap & 0xFFFFFFFF);
      }

      pEntry = &pINode->aEntry[iInsert];
      pEntry->nSize = nData;
      pEntry->iOff = iOff;
      pEntry->flags = 0;
      pEntry->iChildPg = iChildPg;
      iTid = 0;
      assert( iChildPg!=0 );
    }

    hctDbCellPut(&((u8*)pLeaf)[iOff], iTid, iOld, aData, nData);
    pLeaf->hdr.nFreeGap -= nReq;
    pLeaf->hdr.nFreeBytes -= nReq;
    assert_page_is_ok((u8*)pLeaf, pDb->pgsz);
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
  HctDbTMap *pTMap;
  assert( p->iTid==0 );
  assert( p->bRollback==0 );
  p->iTid = sqlite3HctFileAllocateTransid(p->pFile);
  pTMap = p->pTMap;
  if( p->iTid>=(pTMap->iFirstTid + pTMap->nMap*HCTDB_TMAP_SIZE) ){
    rc = hctDbTMapGet(p, 1+((p->iTid - pTMap->iFirstTid) / HCTDB_TMAP_SIZE));
  }
  return rc;
}

int sqlite3HctDbEndWrite(HctDatabase *p){
  int rc = SQLITE_OK;
  assert( p->bRollback==0 );
  assert( p->pa.nWritePg==0 );
  assert( p->pa.aWriteFpKey==0 );
  if( rc==SQLITE_OK ){
    HctDbTMap *pTMap = p->pTMap;
    u64 iSnapshotid = sqlite3HctFileAllocateSnapshotid(p->pFile);
    int iMap;
    int iEntry;

    assert( pTMap->iFirstTid<=p->iTid );
    assert( pTMap->iFirstTid+(pTMap->nMap*HCTDB_TMAP_SIZE)>p->iTid );

    iMap = (p->iTid - pTMap->iFirstTid) / HCTDB_TMAP_SIZE;
    iEntry = (p->iTid - pTMap->iFirstTid) % HCTDB_TMAP_SIZE;

    HctAtomicStore(&pTMap->aaMap[iMap][iEntry], iSnapshotid);
  }
  p->iTid = 0;
  return rc;
}

int sqlite3HctDbEndRead(HctDatabase *p){
  p->iSnapshotId = 0;
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
    for(pp=&pDb->pCsrList; *pp!=pCsr; pp=&(*pp)->pCsrNext);
    *pp = pCsr->pCsrNext;
    sqlite3_free(pCsr);
  }
}

void sqlite3HctDbCsrKey(HctDbCsr *pCsr, i64 *piKey){
  HctDbIntkeyLeaf *pPg = (HctDbIntkeyLeaf*)pCsr->pg.aOld;
  assert( pCsr->iCell>=0 && pCsr->iCell<pPg->pg.nEntry );
  assert( hctPagetype(pPg)==HCT_PAGETYPE_INTKEY );
  *piKey = pPg->aEntry[pCsr->iCell].iKey;
}

int sqlite3HctDbCsrEof(HctDbCsr *pCsr){
  return pCsr==0 || pCsr->iCell<0;
}

int sqlite3HctDbCsrFirst(HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  HctFile *pFile = pCsr->pDb->pFile;
  HctFilePage pg;
  HctDbPageHdr *pPg;
  u32 iPg = pCsr->iRoot;

  hctDbCsrReset(pCsr);
  pCsr->eDir = BTREE_DIR_FORWARD;

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

  if( rc==SQLITE_OK ){
    memcpy(&pCsr->pg, &pg, sizeof(pg));
    assert( pPg->nHeight==0 );
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

  if( rc==SQLITE_OK ){
    assert( pPg->nHeight==0 && pPg->iPeerPg==0 );
    memcpy(&pCsr->pg, &pg, sizeof(pg));
    pCsr->iCell = pPg->nEntry;
    rc = sqlite3HctDbCsrPrev(pCsr);
  }
  return rc;
}


int sqlite3HctDbCsrNext(HctDbCsr *pCsr){
  int rc = SQLITE_OK;

  sqlite3HctFilePageRelease(&pCsr->oldpg);
  do {
    HctDbPageHdr *pPg = (HctDbPageHdr*)pCsr->pg.aOld;
    assert( pCsr->iCell>=-1 && pCsr->iCell<pPg->nEntry );
    assert( pPg->nHeight==0 );
    pCsr->iCell++;
    if( pCsr->iCell==pPg->nEntry ){
      u32 iPeerPg = pPg->iPeerPg;
      if( iPeerPg==0 ){
        pCsr->iCell = -1;
      }else{
        /* Jump to peer page */
        rc = sqlite3HctFilePageRelease(&pCsr->pg);
        if( rc==SQLITE_OK ){
          rc = sqlite3HctFilePageGet(pCsr->pDb->pFile, iPeerPg, &pCsr->pg);
          pCsr->iCell = 0;
        }
      }
    }
  }while( rc==SQLITE_OK && hctDbCsrFindVersion(&rc, pCsr)==0 );

  return rc;
}

int sqlite3HctDbCsrPrev(HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  sqlite3HctFilePageRelease(&pCsr->oldpg);
  do {
    pCsr->iCell--;
    if( pCsr->iCell<0 ){
      /* TODO: This can be made much faster by iterating through the 
      ** parent list (which is in reverse order) instead of seeking 
      ** for each new page.  */
      if( pCsr->pKeyInfo ){
assert( 0 );
      }else{
        HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)pCsr->pg.aOld;
        i64 iKey = pLeaf->aEntry[0].iKey;
        if( iKey!=SMALLEST_INT64 ){
          sqlite3HctFilePageRelease(&pCsr->pg);
          rc = hctDbCsrSeek(pCsr, 0, 0, iKey-1, 0);
        }
      }
    }
  }while( rc==SQLITE_OK && hctDbCsrFindVersion(&rc, pCsr)==0 );

  return rc;
}


int sqlite3HctDbCsrData(HctDbCsr *pCsr, int *pnData, const u8 **paData){
  u8 *pPg;
  int iCell;
  int iOff;
  int nSize;

  if( pCsr->oldpg.aOld ){
    pPg = pCsr->oldpg.aOld;
    iCell = pCsr->iOldCell;
  }else{
    pPg = pCsr->pg.aOld;
    iCell = pCsr->iCell;
  }

  if( pCsr->pKeyInfo ){
    HctDbIndexEntry *pEntry = &((HctDbIndexLeaf*)pPg)->aEntry[iCell];
    nSize = pEntry->nSize;
    iOff = hctDbOffset(pEntry->iOff, pEntry->flags);
  }else{
    HctDbIntkeyEntry *pEntry = &((HctDbIntkeyLeaf*)pPg)->aEntry[iCell];
    nSize = pEntry->nSize;
    iOff = hctDbOffset(pEntry->iOff, pEntry->flags);
  }

  if( paData ){
    *paData = &pPg[iOff];
  }
  *pnData = nSize;
  return SQLITE_OK;
}

/*************************************************************************
**************************************************************************
** Below is the virtual table implementation.
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
  char *zKeys;
  char *zData;
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
        "peer INTEGER, nentry INTEGER, keys TEXT,"
        "data TEXT"
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
  sqlite3_free(pCur->zKeys);
  sqlite3_free(pCur->zData);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

#include "vdbeInt.h"

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
        assert( 0 );
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
    0,                          /* 0x00 */
    "intkey",                   /* 0x01 */
    "index",                    /* 0x02 */
    "overflow",                 /* 0x03 */
  };
  int rc = SQLITE_OK;
  HctDbPageHdr *pHdr = (HctDbPageHdr*)aPg;
  sqlite3 *db = ((hctdb_vtab*)pCur->base.pVtab)->db;

  sqlite3_free(pCur->zKeys);
  pCur->zKeys = 0;
  sqlite3_free(pCur->zData);
  pCur->zData = 0;

  pCur->zPgtype = azType[hctPagetype(pHdr)];
  pCur->iPeerPg = pHdr->iPeerPg;
  pCur->nEntry = pHdr->nEntry;
  pCur->nHeight = pHdr->nHeight;

  if( hctPagetype(pHdr)==HCT_PAGETYPE_INTKEY ){
    if( pHdr->nHeight==0 ){
      HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)aPg;
      char *zKeys = 0;
      int i;
      for(i=0; rc==SQLITE_OK && i<pHdr->nEntry; i++){
        HctDbIntkeyEntry *pEntry = &pLeaf->aEntry[i];
        zKeys = sqlite3_mprintf("%z%s%lld(%s%s%s%s)",
            zKeys, zKeys ? " " : "", pEntry->iKey, 
            ((pEntry->flags & HCTDB_IS_DELETE) ? "x" : ""),
            ((pEntry->flags & HCTDB_HAS_TID) ? "t" : ""),
            ((pEntry->flags & HCTDB_HAS_OLD) ? "o" : ""),
            ((pEntry->flags & HCTDB_HAS_OVFL) ? ">" : "")
            );
        if( zKeys==0 ) rc = SQLITE_NOMEM_BKPT;
      }
      pCur->zKeys = zKeys;
    }else{
      HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)aPg;
      char *zKeys = 0;
      int i;
      for(i=0; rc==SQLITE_OK && i<pHdr->nEntry; i++){
        HctDbIntkeyNodeEntry *pEntry = &pNode->aEntry[i];
        zKeys = sqlite3_mprintf("%z%s%lld(%lld)",
            zKeys, zKeys ? " " : "", pEntry->iKey, (i64)pEntry->iChildPg
        );
        if( zKeys==0 ) rc = SQLITE_NOMEM_BKPT;
      }
      pCur->zKeys = zKeys;
    }
  }else if( hctPagetype(pHdr)==HCT_PAGETYPE_INDEX ){
    if( pHdr->nHeight==0 ){
      HctDbIndexLeaf *pLeaf = (HctDbIndexLeaf*)aPg;
      char *zKeys = 0;
      int i;
      for(i=0; rc==SQLITE_OK && i<pHdr->nEntry; i++){
        HctDbIndexEntry *pEntry = &pLeaf->aEntry[i];
        const u8 *aRec = &aPg[hctDbOffset(pEntry->iOff, pEntry->flags)];
        char *zRec = hctDbRecordToText(db, aRec, pEntry->nSize);

        zKeys = sqlite3_mprintf("%z%s[%z](%s%s%s%s)",
            zKeys, zKeys ? " " : "", zRec,
            ((pEntry->flags & HCTDB_IS_DELETE) ? "x" : ""),
            ((pEntry->flags & HCTDB_HAS_TID) ? "t" : ""),
            ((pEntry->flags & HCTDB_HAS_OLD) ? "o" : ""),
            ((pEntry->flags & HCTDB_HAS_OVFL) ? ">" : "")
        );
        if( zKeys==0 ) rc = SQLITE_NOMEM_BKPT;
      }
      pCur->zKeys = zKeys;
    }else{
      HctDbIndexNode *pNode = (HctDbIndexNode*)aPg;
      char *zKeys = 0;
      int i;
      for(i=0; rc==SQLITE_OK && i<pHdr->nEntry; i++){
        HctDbIndexNodeEntry *pEntry = &pNode->aEntry[i];
        const u8 *aRec = &aPg[hctDbOffset(pEntry->iOff, pEntry->flags)];
        char *zRec = hctDbRecordToText(db, aRec, pEntry->nSize);

        zKeys = sqlite3_mprintf("%z%s[%z](%u)",
            zKeys, zKeys ? " " : "", zRec, pEntry->iChildPg
        );
        if( zKeys==0 ) rc = SQLITE_NOMEM_BKPT;
      }
      pCur->zKeys = zKeys;
    }
  }
  return rc;
}

#define HCTDB_HAS_TID   0x01
#define HCTDB_HAS_OLD   0x02
#define HCTDB_HAS_OVFL  0x04
#define HCTDB_IS_DELETE 0x08


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
    case 2: /* peer */
      sqlite3_result_int64(ctx, (i64)pCur->nHeight);
      break;
    case 3: /* peer */
      sqlite3_result_int64(ctx, (i64)pCur->iPeerPg);
      break;
    case 4: /* nEntry */
      sqlite3_result_int64(ctx, (i64)pCur->nEntry);
      break;
    case 5: /* keys */
      sqlite3_result_text(ctx, pCur->zKeys, -1, SQLITE_TRANSIENT);
      break;
    case 6: /* data */
      sqlite3_result_text(ctx, pCur->zData, -1, SQLITE_TRANSIENT);
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
  int rc;

  rc = sqlite3_create_module(db, "hctdb", &hctdbModule, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFileVtabInit(db);
  }
  return rc;
}
