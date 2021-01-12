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
typedef struct HctDbTMap HctDbTMap;

typedef struct HctDbPageHdr HctDbPageHdr;
typedef struct HctDbFreespaceHdr HctDbFreespaceHdr;

typedef struct HctDbIntkeyLeaf HctDbIntkeyLeaf;
typedef struct HctDbIntkeyEntry HctDbIntkeyEntry;
typedef struct HctDbIntkeyNodeEntry HctDbIntkeyNodeEntry;
typedef struct HctDbIntkeyNode HctDbIntkeyNode;

struct HctDbCsr {
  HctDatabase *pDb;               /* Database that owns this cursor */
  HctDbCsr *pCsrNext;             /* Next cursor in list belonging to pDb */
  u32 iRoot;                      /* Root page cursor is opened on */
  int eDir;                       /* Direction cursor will step after Seek() */
  int iCell;                      /* Current cell within page */
  HctFilePage pg;                 /* Current database page */

  int iOldCell;                   /* Cell within old page */
  HctFilePage oldpg;              /* Old page, if required */
};

#define HCTDB_MAX_DIRTY 8

/*
*/
struct HctDatabase {
  HctFile *pFile;
  HctDbGlobal *pGlobal;           /* Shared by all connections to same db */
  int pgsz;                       /* Page size in bytes */
  HctDbCsr *pCsrList;             /* List of open cursors */

  u64 iSnapshotId;                /* Snapshot id for reading */
  HctDbTMap *pTMap;               /* Transaction map */
  u64 iTid;                       /* Transaction id for writing */

  HctFilePage aWritePg[HCTDB_MAX_DIRTY+2];
  int nWritePg;                   /* Number of valid entries in aWritePg[] */
  int nWriteKey;                  /* Number of new keys in aWritePg[] array */
  i64 iWriteFpKey;
  HctDbCsr writecsr;
  int bRollback;                  /* True for rollback mode */
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
** Structure used for all database pages.
*/
struct HctDbPageHdr {
  /* Placeholder recovery header fields */
  u64 a;
  u64 b;
  u64 c;

  /* 8-byte page header fields */
  u8 ePagetype;
  u8 nHeight;                     /* 0 for leaves, 1 for parents etc. */
  u16 nEntry;
  u32 iPeerPg;
};

/* 
** Free-space header. Used by index leaf and node pages, and by intkey leaves.
*/
struct HctDbFreespaceHdr {
  u16 nFreeSpace;                 /* Size of free space area */
  u16 nFree;                      /* Total free bytes on page */
  u32 unused2;                    /* Padding to multiple of 8 bytes */
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
  HctDbFreespaceHdr fs;
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

void sqlite3HctDbRootPageInit(int bIndex, u8 *aPage, int szPage){
  HctDbIntkeyLeaf *pPg = (HctDbIntkeyLeaf*)aPage;
  memset(aPage, 0, szPage);
  pPg->pg.ePagetype = bIndex ? HCT_PAGETYPE_INDEX : HCT_PAGETYPE_INTKEY;
  pPg->fs.nFree = pPg->fs.nFreeSpace = szPage - sizeof(HctDbIntkeyLeaf);
}

void sqlite3HctDbMetaPageInit(u8 *aPage, int szPage){
  HctDbIntkeyLeaf *pPg = (HctDbIntkeyLeaf*)aPage;
  HctDbIntkeyEntry *pEntry = &pPg->aEntry[0];
  const int nRecord = 8 + 4 + SQLITE_N_BTREE_META*4;
  int nFree;

  memset(aPage, 0, szPage);
  pPg->pg.ePagetype = HCT_PAGETYPE_INTKEY;
  pPg->pg.nEntry = 1;
  nFree = szPage - sizeof(HctDbIntkeyLeaf) - sizeof(HctDbIntkeyEntry) - nRecord;
  pPg->fs.nFree = pPg->fs.nFreeSpace = nFree;
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

/*
** Seek for key iKey (if pRec is null) or pRec within the table opened
** by cursor pCsr.
*/
int hctDbCsrSeek(
  HctDbCsr *pCsr,                 /* Cursor to seek */
  int iHeight,                    /* Height to seek at (0==leaf, 1==parent) */
  UnpackedRecord *pRec,           /* Key for index tables */
  i64 iKey                        /* Key for intkey tables */
){
  HctFile *pFile = pCsr->pDb->pFile;
  int rc;

  assert( pRec==0 ); /* TODO! Support index tables! */
  rc = sqlite3HctFilePageGet(pFile, pCsr->iRoot, &pCsr->pg);
  while( rc==SQLITE_OK ){
    HctDbIntkeyLeaf *pPg = (HctDbIntkeyLeaf*)pCsr->pg.aOld;
    int i1 = -1;
    int i2 = pPg->pg.nEntry-1;

    /* Seek within the page. Goal is to find the entry that is less than or
    ** equal to iKey. If all entries on the page are larger than iKey, find 
    ** the "entry" -1. */
    while( i2>i1 ){
      int iTest = (i1+i2+1)/2;
      i64 iPgkey = pPg->aEntry[iTest].iKey;

      if( iKey<iPgkey ){
        i2 = iTest-1;
      }else if( iKey>iPgkey ){
        i1 = iTest;
      }else{
        i1 = i2 = iTest;
      }
    }

    /* Assert that we appear to have landed on the correct entry. */
    assert( i1==i2 );
    assert( i2==-1 || iKey>=pPg->aEntry[i2].iKey );
    assert( i2+1==pPg->pg.nEntry || iKey<pPg->aEntry[i2+1].iKey );

    /* Test if it is necessary to skip to the peer node. */
    if( i2>=0 && i2==pPg->pg.nEntry-1 && pPg->pg.iPeerPg!=0 ){
      HctFilePage peer;
      rc = sqlite3HctFilePageGet(pFile, pPg->pg.iPeerPg, &peer);
      if( rc==SQLITE_OK ){
        HctDbIntkeyLeaf *pPeer = (HctDbIntkeyLeaf*)peer.aOld;
        if( pPeer->aEntry[0].iKey<=iKey ){
          SWAP(HctFilePage, pCsr->pg, peer);
          sqlite3HctFilePageRelease(&peer);
          continue;
        }
        sqlite3HctFilePageRelease(&peer);
      }
    }

    pCsr->iCell = i2;
    break;
  }

  return rc;
}

static i64 hctDbIntkeyFPKey(u8 *aPg){
  i64 iRet;
  if( ((HctDbPageHdr*)aPg)->nHeight==0 ){
    HctDbIntkeyLeaf *p = (HctDbIntkeyLeaf*)aPg;
    iRet = p->aEntry[0].iKey;
  }else{
    HctDbIntkeyNode *p = (HctDbIntkeyNode*)aPg;
    iRet = p->aEntry[0].iKey;
  }
  return iRet;
}


int hctDbCsrIntkeySeek(
  HctDbCsr *pCsr,                 /* Cursor to seek */
  int iHeight,                    /* Height to seek at (0==leaf, 1==parent) */
  i64 iKey                        /* Key for intkey tables */
){
  HctFile *pFile = pCsr->pDb->pFile;
  u32 iPg = pCsr->iRoot;
  int rc = SQLITE_OK;

  while( rc==SQLITE_OK ){
    if( iPg ) rc = sqlite3HctFilePageGet(pFile, iPg, &pCsr->pg);
    if( rc==SQLITE_OK ){
      HctDbPageHdr *pHdr = (HctDbPageHdr*)pCsr->pg.aOld;
      int i2 = pHdr->nEntry-1;
      int bReverse = (pHdr->nHeight % 2);
      if( pHdr->nHeight==0 ){
        HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)pHdr;
        int i1 = -1;

        /* Seek within the page. Goal is to find the entry that is less than or
        ** equal to iKey. If all entries on the page are larger than iKey, find
        ** the "entry" -1. */
        while( i2>i1 ){
          int iTest = (i1+i2+1)/2;
          i64 iPgkey = pLeaf->aEntry[iTest].iKey;
          if( iKey<iPgkey ){
            i2 = iTest-1;
          }else if( iKey>iPgkey ){
            i1 = iTest;
          }else{
            i1 = i2 = iTest;
          }
        }

        /* Assert that we appear to have landed on the correct entry. */
        assert( i1==i2 );
        assert( i2==-1 || iKey>=pLeaf->aEntry[i2].iKey );
        assert( i2+1==pLeaf->pg.nEntry || iKey<pLeaf->aEntry[i2+1].iKey );
      }else{
        HctDbIntkeyNodeEntry *aEntry = ((HctDbIntkeyNode*)pHdr)->aEntry;
        int i1 = 0;

        /* Seek within the page. Goal is to find the cell containing the key
        ** that is less than (in the sort order) or equal to iKey.  */
        while( i2>i1 ){
          int iTest = (i1+i2)/2;
          i64 iPgkey = aEntry[iTest].iKey;
          if( (bReverse==0 && iKey<iPgkey) || (bReverse && iKey>iPgkey) ){
            i2 = iTest;
          }else if( (bReverse==0 && iKey>iPgkey) || (bReverse && iKey<iPgkey) ){
            i1 = iTest+1;
          }else{
            i1 = i2 = iTest;
          }
        }

        /* Assert that we appear to have landed on the correct entry. */
        assert( i1==i2 && i2>=0 && i2<pHdr->nEntry );
        assert( bReverse==0 || (i2==pHdr->nEntry-1 || iKey>=aEntry[i2].iKey) );
        assert( bReverse==1 || (i2==pHdr->nEntry-1 || iKey<=aEntry[i2].iKey) );

        if( bReverse && iKey<aEntry[i2].iKey ){
          assert( pHdr->iPeerPg );
          assert( i2==pHdr->nEntry-1 );
          iPg = pHdr->iPeerPg;
        }else{
          iPg = aEntry[i2].iChildPg;
        }
      }

      /* Test if it is necessary to skip to the peer node. */
      if( bReverse==0 && i2>=0 && i2==pHdr->nEntry-1 && pHdr->iPeerPg!=0 ){
        HctFilePage peer;
        rc = sqlite3HctFilePageGet(pFile, pHdr->iPeerPg, &peer);
        if( rc==SQLITE_OK ){
          i64 iFP = hctDbIntkeyFPKey(peer.aOld);
          if( (bReverse==0 && iFP<=iKey) || (bReverse && iFP>=iKey) ){
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
    HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)aPage;
    HctDbIntkeyEntry *pEntry = &pLeaf->aEntry[pCsr->iCell];

    assert( pCsr->oldpg.aOld==0 );
    while( pEntry->flags & HCTDB_HAS_TID ){
      i64 iKey;
      u64 iTid = hctGetU64(&aPage[pEntry->iOff]);
      u64 iCid = hctDbTMapLookup(pCsr->pDb, iTid);
      if( iCid<=pCsr->pDb->iSnapshotId ){
        break;
      }

      iKey = pEntry->iKey;
      if( (pEntry->flags & HCTDB_HAS_OLD)==0 ){
        sqlite3HctFilePageRelease(&pCsr->oldpg);
        return 0;
      }else{
        int rc;
        HctFile *pFile = pCsr->pDb->pFile;
        u32 iOld = hctGetU32(&pCsr->pg.aOld[pEntry->iOff+8]);
        
        sqlite3HctFilePageRelease(&pCsr->oldpg);
        rc = sqlite3HctFilePageGetPhysical(pFile, iOld, &pCsr->oldpg);
        if( rc==SQLITE_OK ){
          int iCell;
          aPage = pCsr->oldpg.aOld;
          iCell = hctDbFindKeyInPage((HctDbIntkeyLeaf*)aPage, iKey);
          if( iCell<0 ){
            rc = SQLITE_CORRUPT_BKPT;
          }else{
            pEntry = &((HctDbIntkeyLeaf*)aPage)->aEntry[iCell];
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
    return ((pEntry->flags & HCTDB_IS_DELETE) ? 0 : 1);
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

  hctDbCsrReset(pCsr);
  rc = hctDbCsrIntkeySeek(pCsr, 0, iKey);

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
      i64 iCsrKey;
      sqlite3HctDbCsrKey(pCsr, &iCsrKey);
      *pRes = iCsrKey<iKey ? -1 : 0;
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
static int hctDbCellSize(u64 iTid, u32 iOld, int nLocal){
  return (iTid ? sizeof(u64) : 0) + (iOld ? sizeof(u32) : 0) + nLocal;
}

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
static int hctDbLocalSize(HctDatabase *pDb, int nData){
  int nOther = sizeof(HctDbIntkeyLeaf) + sizeof(HctDbIntkeyEntry) + 12;
  if( nData<=(pDb->pgsz-nOther) ){
    return nData;
  }
  assert( !"todo" );
  return 0;
}

static i64 hctDbIntkeyGetKey(u8 *aPg, int ii){
  HctDbIntkeyLeaf *p = (HctDbIntkeyLeaf*)aPg;
  return p->aEntry[ii].iKey;
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
** Buffer aPg contains an intkey node page.
**
** This function searches the node page for key iKey. If found, it returns
** the index of the matching key within the page and sets output variable
** (*pbExact) to 1. If there is no match for key iKey, this function returns
** the index of the smallest key on the page that is larger than iKey, or 
** (nEntry) if all keys on the page are smaller than iKey. (*pbExact) is 
** set to 0 before returning in this case.
*/
static int hctDbIntkeyNodeSearch(
  HctDbIntkeyNode *pNode,
  i64 iKey,
  int *pbExact
){
  int i1 = 0;
  int i2 = pNode->pg.nEntry;
  int bRev = (pNode->pg.nHeight & 0x01);

  assert( bRev==0 || bRev==1 );
  while( i2>i1 ){
    int iTest = (i1+i2)/2;
    i64 iPgKey = pNode->aEntry[iTest].iKey;
    if( iPgKey==iKey ){
      *pbExact = 1;
      return iTest;
    }else if( (bRev==0 && iPgKey<iKey) || (bRev!=0 && iPgKey>iKey) ){
      i1 = iTest+1;
    }else{
      i2 = iTest;
    }
  }
  assert( i1==i2 );

  assert( i2>=0 );
  assert( bRev==1 || i2==pNode->pg.nEntry || iKey<pNode->aEntry[i2].iKey );
  assert( bRev==1 || i2==0 || iKey>pNode->aEntry[i2-1].iKey );
  assert( bRev==0 || i2==pNode->pg.nEntry || iKey>pNode->aEntry[i2].iKey );
  assert( bRev==0 || i2==0 || iKey<pNode->aEntry[i2-1].iKey );

  *pbExact = 0;
  return i2;
}

static int hctDbInsertFPKey(
  HctDatabase *pDb, 
  int nKey,                       /* Number of keys in aKey[] */
  HctDbIntkeyNodeEntry *aKey,     /* Array of keys to insert */
  int iHeight                     /* Insert at this height */
){
  int rc = SQLITE_OK;
  HctDbCsr csr;
  int nRem = nKey;
  int nMax;
  HctDbIntkeyNodeEntry *aRem = aKey;

  hctDbCsrInit(pDb, pDb->writecsr.iRoot, &csr);
  nMax = (pDb->pgsz - sizeof(HctDbIntkeyNode)) / sizeof(HctDbIntkeyNodeEntry);
  while( rc==SQLITE_OK && nRem>0 ){
    HctDbIntkeyNode *pNew;
    HctDbIntkeyNodeEntry pkey = {0, 0};
    HctFilePage pg2;
    HctDbIntkeyNode *pNew2 = 0;
    int iRem;

    memset(&pg2, 0, sizeof(HctFilePage));
    rc = hctDbCsrIntkeySeek(&csr, iHeight, aRem[0].iKey);
    if( rc==SQLITE_OK ){
      rc = sqlite3HctFilePageWrite(&csr.pg);
    }
    if( rc==SQLITE_OK ){
      memcpy(csr.pg.aNew, csr.pg.aOld, pDb->pgsz);
      pNew = (HctDbIntkeyNode*)csr.pg.aNew;

      for(iRem=0; iRem<nRem; iRem++){
        HctDbIntkeyNode *pIns = 0;
        int bExact;
        int iPos;

        if( pNew->pg.nEntry==nMax ){
          int nByte;
          rc = sqlite3HctFilePageNew(pDb->pFile, 0, &pg2);
          assert( rc==SQLITE_OK );
          assert( pNew2==0 );
          pNew2 = (HctDbIntkeyNode*)pg2.aNew;

          if( csr.pg.iPg==csr.iRoot ){
            i64 iNewKey = (iHeight & 0x01) ? LARGEST_INT64 : SMALLEST_INT64;
            memcpy(pg2.aNew, csr.pg.aNew, pDb->pgsz);
            pNew->pg.nHeight++;
            pNew->pg.nEntry = 1;
            pNew->pg.iPeerPg = 0;
            pNew->aEntry[0].iChildPg = pg2.iPg;
            pNew->aEntry[0].iKey = iNewKey;
            break;
          }else{
            pNew2->pg.ePagetype = HCT_PAGETYPE_INTKEY;
            pNew2->pg.nHeight = iHeight;
            pNew2->pg.nEntry = nMax/2;
            pNew2->pg.iPeerPg = pNew->pg.iPeerPg;

            pNew->pg.iPeerPg = pg2.iPg;
            pNew->pg.nEntry = nMax - pNew2->pg.nEntry;

            nByte = sizeof(HctDbIntkeyNodeEntry) * pNew2->pg.nEntry;
            memcpy(pNew2->aEntry, &pNew->aEntry[pNew->pg.nEntry], nByte);

            pkey.iChildPg = pg2.iPg;
            pkey.iKey = pNew2->aEntry[0].iKey;
          }
        }

        if( pNew2 ){
          int bRev = (iHeight & 0x01);
          i64 iFP = pNew2->aEntry[0].iKey;
          i64 iIns = aRem[iRem].iKey;
          if( (bRev && iIns<=iFP) || (bRev==0 && iIns>=iFP) ){
            pIns = pNew2;
          }else{
            pIns = pNew;
          }
        }else{
          pIns = pNew;
        }

        iPos = hctDbIntkeyNodeSearch(pIns, aRem[iRem].iKey, &bExact);
        assert( bExact==0 );

        if( iPos<pIns->pg.nEntry ){
          int nByte = sizeof(HctDbIntkeyNodeEntry) * (pIns->pg.nEntry-iPos);
          memmove(&pIns->aEntry[iPos+1], &pIns->aEntry[iPos], nByte);
        }
        pIns->aEntry[iPos] = aRem[iRem];
        pIns->pg.nEntry++;
      }

      rc = sqlite3HctFilePageRelease(&csr.pg);
      assert( rc==SQLITE_OK );
      rc = sqlite3HctFilePageRelease(&pg2);
      assert( rc==SQLITE_OK );

      if( pkey.iChildPg ){
        rc = hctDbInsertFPKey(pDb, 1, &pkey, iHeight+1);
        assert( rc==SQLITE_OK );
      }

      nRem -= iRem;
      aRem += iRem;
    }
    hctDbCsrReset(&csr);
  }

  return rc;
}

static void hctDbInsertDiscard(HctDatabase *pDb){
  int ii;
  for(ii=0; ii<pDb->nWritePg; ii++){
    sqlite3HctFilePageUnwrite(&pDb->aWritePg[ii]);
    sqlite3HctFilePageRelease(&pDb->aWritePg[ii]);
  }
  pDb->nWritePg = 0;
}

static int hctDbInsertFlushWrite(HctDatabase *pDb){
  int rc = SQLITE_OK;
  if( pDb->nWritePg ){
    HctFilePage root;
    int bNewRoot = 0;
    HctDbIntkeyNodeEntry aKey[HCTDB_MAX_DIRTY+2];
    int ii;

    if( pDb->nWritePg>1 && pDb->aWritePg[0].iPg==pDb->writecsr.iRoot ){
      /* This is a split of the root page of a tree. */
      bNewRoot = 1;
      memcpy(&root, &pDb->aWritePg[0], sizeof(HctFilePage));
      memset(&pDb->aWritePg[0], 0, sizeof(HctFilePage));
      rc = sqlite3HctFilePageNew(pDb->pFile, 0, &pDb->aWritePg[0]);
      if( rc==SQLITE_OK ){
        memcpy(pDb->aWritePg[0].aNew, root.aNew, pDb->pgsz);
      }
    }

    /* Loop through the set of pages to write out. */
    assert( pDb->nWriteKey>0 );
    for(ii=0; rc==SQLITE_OK && ii<pDb->nWritePg; ii++){
      HctFilePage *pPg = &pDb->aWritePg[pDb->nWritePg-1-ii];
      HctDbIntkeyLeaf *pNew = (HctDbIntkeyLeaf*)(pPg->aNew);
      aKey[ii].iChildPg = pPg->iPg;
      aKey[ii].iKey = pNew->aEntry[0].iKey;
      rc = sqlite3HctFilePageRelease(pPg);
    }

    if( rc==SQLITE_OK ){
      if( bNewRoot ){
        HctDbIntkeyNode *pRoot = (HctDbIntkeyNode*)root.aNew;
        memset(root.aNew, 0, pDb->pgsz);
        pRoot->pg.nEntry = pDb->nWritePg;
        pRoot->pg.ePagetype = HCT_PAGETYPE_INTKEY;
        pRoot->pg.nHeight = 1;
        pRoot->pg.iPeerPg = 0;
        aKey[pDb->nWritePg-1].iKey = SMALLEST_INT64;
        memcpy(pRoot->aEntry, aKey, sizeof(aKey[0])*pDb->nWritePg);
        rc = sqlite3HctFilePageRelease(&root);
      }else if( pDb->nWritePg>1 ){
        rc = hctDbInsertFPKey(pDb, pDb->nWritePg-1, &aKey[0], 1);
      }
    }

    if( rc==SQLITE_LOCKED ){
      hctDbInsertDiscard(pDb);
    }
    pDb->nWritePg = 0;
  }
  return rc;
}

/*
** Split page pDb->aWritePg[iPg] into either two or three pages and add 
** the new peer or peers to the aWritePg[] array. At the same time, add
** new entry [iKey -> (nData/aData)] to the array of peer pages. If
** parameter bClobber is non-zero, this key clobbers key iInsert from
** the initial page. Or, if bClobber is zero, the new key follows key
** iInsert in the new data.
**
** Return SQLITE_OK if successful, or an SQLite error code if an error
** occurs.
*/
static int hctDbSplitPage(
  HctDatabase *pDb, 
  int iPg, 
  int iInsert,
  int bDel,
  int bClobber,
  int nReq,
  i64 iKey, int nData, const u8 *aData
){
  int rc = SQLITE_OK;
  u8 *aTmpPg = 0;                 /* Copy of page iPg data */

  assert( bClobber==1 || bClobber==0 );

  aTmpPg = sqlite3_malloc(pDb->pgsz);
  if( aTmpPg==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    HctDbIntkeyLeaf *pIn = (HctDbIntkeyLeaf*)aTmpPg;
    HctDbIntkeyLeaf *pOut = (HctDbIntkeyLeaf*)pDb->aWritePg[iPg].aNew;
    int nRem;                     /* Remaining space required */
    int ii;                       /* Input key index */

    memcpy(pIn, pOut, pDb->pgsz);
    pOut->pg.nEntry = 0;
    pOut->fs.nFree = pOut->fs.nFreeSpace = pDb->pgsz - sizeof(HctDbIntkeyLeaf);
    nRem = pDb->pgsz - pIn->fs.nFree - sizeof(HctDbIntkeyLeaf);
    nRem += nReq;

    for(ii=0; ii<(pIn->pg.nEntry + !bClobber); ii++){
      HctDbIntkeyEntry *pEntry;
      HctDbIntkeyEntry *pNew;
      u8 *aNewCell = 0;
      int nSpace;
      if( ii==iInsert ){
        pEntry = 0;
        nSpace = (nData + sizeof(HctDbIntkeyEntry) + 8 + (bClobber ? 4 : 0));
      }else{
        int iIn = (ii>iInsert ? ii-!bClobber : ii);
        pEntry = &pIn->aEntry[iIn];

        nSpace = pEntry->nSize + sizeof(HctDbIntkeyEntry) 
          + ((pEntry->flags & HCTDB_HAS_TID) ? 8 : 0)
          + ((pEntry->flags & HCTDB_HAS_OLD) ? 4 : 0);
      }

      assert( pOut->fs.nFree==pOut->fs.nFreeSpace );
      if( nSpace>pOut->fs.nFree ){
        int iNew = iPg+1;
        HctDbIntkeyLeaf *pNewOut;
        if( iNew<pDb->nWritePg ){
          int nByte = sizeof(HctFilePage) * (pDb->nWritePg-iNew);
          memmove(&pDb->aWritePg[iNew+1], &pDb->aWritePg[iNew], nByte);
        }
        pDb->nWritePg++;
        memset(&pDb->aWritePg[iNew], 0, sizeof(HctFilePage));
        rc = sqlite3HctFilePageNew(pDb->pFile, 0, &pDb->aWritePg[iNew]);
        assert( rc==SQLITE_OK );
        pNewOut = (HctDbIntkeyLeaf*)pDb->aWritePg[iNew].aNew;

        pNewOut->pg.nEntry = 0;
        pNewOut->pg.nHeight = 0;
        pNewOut->pg.ePagetype = pOut->pg.ePagetype;
        pNewOut->pg.iPeerPg = pOut->pg.iPeerPg;
        pNewOut->fs.nFree = pDb->pgsz - sizeof(HctDbIntkeyLeaf);
        pNewOut->fs.nFreeSpace = pNewOut->fs.nFree;

        pOut->pg.iPeerPg = pDb->aWritePg[iNew].iPg;
        pOut = pNewOut;
      }

      pNew = &pOut->aEntry[pOut->pg.nEntry++];
      pOut->fs.nFree -= nSpace;
      pOut->fs.nFreeSpace -= nSpace;
      pNew->iOff = sizeof(HctDbIntkeyLeaf) 
                 + pOut->pg.nEntry*sizeof(HctDbIntkeyEntry)
                 + pOut->fs.nFree;
      aNewCell = &((u8*)pOut)[pNew->iOff];
      if( pEntry ){
        int nCopy = nSpace - sizeof(HctDbIntkeyEntry);
        pNew->iKey = pEntry->iKey;
        pNew->nSize = pEntry->nSize;
        pNew->flags = pEntry->flags;
        memcpy(aNewCell, &aTmpPg[pEntry->iOff], nCopy);
      }else{
        u32 iOld = (bClobber ? (pDb->aWritePg[0].iPagemap & 0xFFFFFFFF) : 0);
        pNew->iKey = iKey;
        pNew->nSize = nData;
        pNew->flags = HCTDB_HAS_TID 
          | (bClobber ? HCTDB_HAS_OLD : 0)
          | (bDel     ? HCTDB_IS_DELETE : 0);
        hctDbCellPut(aNewCell, pDb->iTid, iOld, aData, nData);
      }
    }
  }

  sqlite3_free(aTmpPg);
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
    rc = sqlite3HctFilePageGetPhysical(pDb->pFile, iOld, &pDb->writecsr.oldpg);
    if( rc==SQLITE_OK ){
      int b;
      int iCell = hctDbIntkeyLeafSearch(pDb->writecsr.oldpg.aOld, iKey, &b);
      pEntry = hctDbIntkeyEntry(pDb->writecsr.oldpg.aOld, iCell);
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
        *paData = &pDb->writecsr.oldpg.aOld[iOff];
      }
    }
  }

  return rc;
}

int sqlite3HctDbInsertFlush(HctDatabase *pDb, int *pnRetry){
  int rc = hctDbInsertFlushWrite(pDb);
  if( rc==SQLITE_LOCKED ){
    *pnRetry = pDb->nWriteKey;
    rc = SQLITE_OK;
  }else{
    *pnRetry = 0;
  }
  pDb->nWriteKey = 0;
  return rc;
}

int sqlite3HctDbInsert(
  HctDatabase *pDb,
  u32 iRoot,
  UnpackedRecord *pRec,
  i64 iKey,
  int bDel, int nData, const u8 *aData,
  int *pnRetry
){
  int rc = SQLITE_OK;
  int iInsert;
  int iPg = 0;
  int bClobber = 0;
  HctDbIntkeyLeaf *pLeaf = 0;
  int nReq;
  int nDataReq;

  *pnRetry = 0;
  pDb->nWriteKey++;

  /* Check if any existing dirty pages need to be flushed to disk before 
  ** this key can be inserted. */
  assert( pDb->nWritePg==0 || iRoot==pDb->writecsr.iRoot );
  if( pDb->nWritePg ){
    if( pDb->nWritePg>HCTDB_MAX_DIRTY || iKey>=pDb->iWriteFpKey ){
      rc = sqlite3HctDbInsertFlush(pDb, pnRetry);
      if( rc || *pnRetry ) return rc;
      pDb->nWriteKey = 1;
    }
  }

  if( pDb->nWritePg==0 ){
    hctDbCsrInit(pDb, iRoot, &pDb->writecsr);
    rc = hctDbCsrIntkeySeek(&pDb->writecsr, 0, iKey);
    assert( rc==SQLITE_OK );
    iInsert = pDb->writecsr.iCell;

    if( pDb->bRollback ){
      rc = hctDbFindRollback(
          pDb, iKey, pDb->writecsr.pg.aOld, iInsert, &bDel, &nData, &aData
      );
      if( rc!=SQLITE_OK ) return rc;
      bClobber = 1;
    }else{
      if( iInsert>=0 
       && hctDbIntkeyGetKey(pDb->writecsr.pg.aOld,iInsert)==iKey 
      ){
        bClobber = 1;
      }
      if( bDel && bClobber==0 ){
        hctDbCsrReset(&pDb->writecsr);
        return SQLITE_OK;
      }
    }

    pDb->aWritePg[0] = pDb->writecsr.pg;
    memset(&pDb->writecsr.pg, 0, sizeof(HctFilePage));
    pDb->nWritePg = 1;
    rc = sqlite3HctFilePageWrite(&pDb->aWritePg[0]);
    assert( rc==SQLITE_OK );
    memcpy(pDb->aWritePg[0].aNew, pDb->aWritePg[0].aOld, pDb->pgsz);

    pLeaf = (HctDbIntkeyLeaf*)pDb->aWritePg[iPg].aNew;
    if( pLeaf->pg.iPeerPg==0 ){
      pDb->iWriteFpKey = LARGEST_INT64;
    }else{
      HctFilePage pg;
      memset(&pg, 0, sizeof(HctFilePage));
      rc = sqlite3HctFilePageGet(pDb->pFile, pLeaf->pg.iPeerPg, &pg);
      if( rc==SQLITE_OK ){
        HctDbIntkeyLeaf *pPeer = (HctDbIntkeyLeaf*)pg.aOld;
        pDb->iWriteFpKey = pPeer->aEntry[0].iKey;
        sqlite3HctFilePageRelease(&pg);
      }
    }

    if( bClobber==0 ) iInsert++;
  }else{
    /* Figure out which page in the aWritePg[] array the new entry belongs
    ** on. This can be optimized later - by remembering which page the 
    ** previous key was stored on.  */
    for(iPg=0; iPg<pDb->nWritePg-1; iPg++){
      HctDbIntkeyLeaf *pPg = (HctDbIntkeyLeaf*)pDb->aWritePg[iPg+1].aNew;
      if( pPg->aEntry[0].iKey>iKey ) break;
    }

    iInsert = hctDbIntkeyLeafSearch(pDb->aWritePg[iPg].aNew, iKey, &bClobber);
    if( pDb->bRollback ){
      rc = hctDbFindRollback(
          pDb, iKey, pDb->aWritePg[iPg].aNew, iInsert, &bDel, &nData, &aData
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
  pLeaf = (HctDbIntkeyLeaf*)pDb->aWritePg[iPg].aNew;

  nDataReq = 8 + nData;
  if( bClobber ){
    HctDbIntkeyEntry *pEntry = &pLeaf->aEntry[iInsert];
    if( pDb->bRollback==0 && (pEntry->flags & HCTDB_HAS_TID) ){
      u64 iTid = hctGetU64(&((u8*)pLeaf)[pEntry->iOff]);
      u64 iCid = hctDbTMapLookup(pDb, iTid);
      if( iCid>pDb->iSnapshotId ){
        hctDbInsertDiscard(pDb);
        return SQLITE_BUSY;
      }
    }
    nDataReq += 4;
    nReq = nDataReq;
  }else{
    nReq = nDataReq + sizeof(HctDbIntkeyEntry);
  }

  if( pLeaf->fs.nFreeSpace<nReq ){
    rc = hctDbSplitPage(pDb, iPg, iInsert, bDel,bClobber,nReq,iKey,nData,aData);
  }else{
    u32 iOld = 0;
    int iOff;
    HctDbIntkeyEntry *pEntry;

    iOff = sizeof(HctDbIntkeyLeaf) 
         + pLeaf->pg.nEntry*sizeof(HctDbIntkeyEntry)
         + pLeaf->fs.nFreeSpace 
         - nDataReq;

    if( bClobber==0 ){
      if( iInsert<pLeaf->pg.nEntry ){
        int nByte = sizeof(HctDbIntkeyEntry) * (pLeaf->pg.nEntry-iInsert);
        memmove(&pLeaf->aEntry[iInsert+1], &pLeaf->aEntry[iInsert], nByte);
      }
      pLeaf->pg.nEntry++;
    }else{
      iOld = (pDb->aWritePg[0].iPagemap & 0xFFFFFFFF);
    }
    pEntry = &pLeaf->aEntry[iInsert];

    pEntry->iKey = iKey;
    pEntry->nSize = nData;
    pEntry->iOff = iOff;
    pEntry->flags = HCTDB_HAS_TID 
      | (bClobber ? HCTDB_HAS_OLD : 0)
      | (bDel     ? HCTDB_IS_DELETE : 0);

    hctDbCellPut(&((u8*)pLeaf)[iOff], pDb->iTid, iOld, aData, nData);

    pLeaf->fs.nFreeSpace -= nReq;
    pLeaf->fs.nFree -= nReq;
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
  assert( p->nWritePg==0 );
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
int sqlite3HctDbCsrOpen(HctDatabase *pDb, u32 iRoot, HctDbCsr **ppCsr){
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
  assert( pPg->pg.ePagetype==HCT_PAGETYPE_INTKEY );
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
    if( (pPg->nHeight % 2) && pPg->iPeerPg ){
      iPg = pPg->iPeerPg;
    }
    else if( pPg->nHeight==0 ){
      break;
    }
    else if( (pPg->nHeight % 2) ){
      iPg = ((HctDbIntkeyNode*)pPg)->aEntry[pPg->nEntry-1].iChildPg;
    }else{
      iPg = ((HctDbIntkeyNode*)pPg)->aEntry[0].iChildPg;
    }
    sqlite3HctFilePageRelease(&pg);
  }

  if( rc==SQLITE_OK ){
    memcpy(&pCsr->pg, &pg, sizeof(pg));
    assert( pPg->ePagetype==HCT_PAGETYPE_INTKEY && pPg->nHeight==0 );
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
    if( pPg->iPeerPg && (pPg->nHeight % 2)==0 ){
      iPg = pPg->iPeerPg;
    }else if( pPg->nHeight==0 ){
      break;
    }else{
      HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)pPg;
      if( (pPg->nHeight % 2) ){
        iPg = pNode->aEntry[0].iChildPg;
      }else{
        iPg = pNode->aEntry[pPg->nEntry-1].iChildPg;
      }
    }
    sqlite3HctFilePageRelease(&pg);
  }

  if( rc==SQLITE_OK ){
    assert( pPg->ePagetype==HCT_PAGETYPE_INTKEY && pPg->nHeight==0 );
    assert( pPg->iPeerPg==0 );
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
    assert( pPg->ePagetype==HCT_PAGETYPE_INTKEY && pPg->nHeight==0 );
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
      HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)pCsr->pg.aOld;
      i64 iKey = pLeaf->aEntry[0].iKey;
      if( iKey!=SMALLEST_INT64 ){
        sqlite3HctFilePageRelease(&pCsr->pg);
        rc = hctDbCsrIntkeySeek(pCsr, 0, iKey-1);
      }
    }
  }while( rc==SQLITE_OK && hctDbCsrFindVersion(&rc, pCsr)==0 );

  return rc;
}


int sqlite3HctDbCsrData(HctDbCsr *pCsr, int *pnData, const u8 **paData){
  HctDbIntkeyLeaf *pPg;
  int iCell;
  HctDbIntkeyEntry *pEntry;
  int iOff;

  if( pCsr->oldpg.aOld ){
    pPg = (HctDbIntkeyLeaf*)pCsr->oldpg.aOld;
    iCell = pCsr->iOldCell;
  }else{
    pPg = (HctDbIntkeyLeaf*)pCsr->pg.aOld;
    iCell = pCsr->iCell;
  }
  pEntry = &pPg->aEntry[iCell];

  assert( iCell>=0 && iCell<pPg->pg.nEntry );
  assert( pPg->pg.ePagetype==HCT_PAGETYPE_INTKEY && pPg->pg.nHeight==0 );
  assert( (pEntry->flags & HCTDB_HAS_OVFL)==0 );

  iOff = pEntry->iOff 
    + ((pEntry->flags & HCTDB_HAS_TID) ? 8 : 0)
    + ((pEntry->flags & HCTDB_HAS_OLD) ? 4 : 0)
    + ((pEntry->flags & HCTDB_HAS_OVFL) ? 4 : 0);

  if( paData ){
    *paData = &((u8*)pPg)[iOff];
  }
  *pnData = pEntry->nSize;

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

  sqlite3_free(pCur->zKeys);
  pCur->zKeys = 0;
  sqlite3_free(pCur->zData);
  pCur->zData = 0;

  pCur->zPgtype = azType[pHdr->ePagetype];
  pCur->iPeerPg = pHdr->iPeerPg;
  pCur->nEntry = pHdr->nEntry;
  pCur->nHeight = pHdr->nHeight;

  if( pHdr->ePagetype==HCT_PAGETYPE_INTKEY ){
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
  pCur->pgno = 0;
  pCur->iMaxPgno = sqlite3HctFileMaxpage(pCur->pDb->pFile);
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
  pIdxInfo->estimatedCost = (double)10;
  pIdxInfo->estimatedRows = 10;
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
