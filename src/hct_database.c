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
typedef struct HctDatabasePage HctDatabasePage;

struct HctDatabase {
  HctFile *pFile;
  int pgsz;                       /* Page size in bytes */
  u64 iTid;                       /* Current transaction id (or zero) */
};

struct HctDbCsr {
  HctDatabase *pDb;               /* Database that owns this cursor */
  u32 iRoot;                      /* Root page cursor is opened on */
  int iCell;                      /* Current cell within page */
  HctFilePage pg;                 /* Current database page */
};

/* 
** Structure used for all database pages.
*/
struct HctDatabasePage {
  /* Recovery header fields */
  u64 iCksum;
  u64 iTid;
  u64 iLargestTid;
  u32 iLogicId;
  u32 iPrevId;

  /* Page header fields */
  u8 ePagetype;
  u8 unused;
  u16 nEntry;
  u32 iPeerPg;

  /* Second page header - for index leaf and node pages and intkey leaves. */
  u16 nFreeSpace;                 /* Size of free space area */
  u16 nFree;                      /* Total free bytes on page */
  u32 unused2;                    /* Padding to multiple of 8 bytes */
};

typedef struct HctIntkeyTid HctIntkeyTid;
struct HctIntkeyTid {
  i64 iKey;
  u64 iTidFlags;
};

/*
** Flags used in the spare 8-bits of the transaction-id fields on each
** non-overflow page of the db.
*/
#define HCT_IS_DELETED   (((u64)0x01)<<56)
#define HCT_HAS_OVERFLOW (((u64)0x02)<<56)

#define HCT_TID_MASK     ((((u64)0x00FFFFFF)<<32)|0xFFFFFFFF)

int sqlite3HctDbOpen(const char *zFile, HctDatabase **ppDb){
  int rc = SQLITE_OK;
  HctDatabase *pNew;

  pNew = (HctDatabase*)sqlite3MallocZero(sizeof(*pNew));
  if( pNew ){
    pNew->pgsz = 4096;
    rc = sqlite3HctFileOpen(zFile, &pNew->pFile);
  }else{
    rc = SQLITE_NOMEM_BKPT;
  }

  if( rc!=SQLITE_OK ){
    sqlite3HctDbClose(pNew);
    pNew = 0;
  }

  *ppDb = pNew;
  return rc;
}

void sqlite3HctDbClose(HctDatabase *p){
  if( p ){
    sqlite3HctFileClose(p->pFile);
    p->pFile = 0;
    sqlite3_free(p);
  }
}

int sqlite3HctDbRootNew(HctDatabase *p, u32 *piRoot){
  return sqlite3HctFileRootNew(p->pFile, piRoot);
}

int sqlite3HctDbRootFree(HctDatabase *p, u32 iRoot){
  return sqlite3HctFileRootFree(p->pFile, iRoot);
}

static void hctDbStartTrans(HctDatabase *p){
  if( p->iTid==0 ){
    p->iTid = sqlite3HctFileStartTrans(p->pFile);
  }
}

void sqlite3HctDbRootPageInit(int bIndex, u8 *aPage, int szPage){
  HctDatabasePage *pPg = (HctDatabasePage*)aPage;
  memset(aPage, 0, szPage);
  pPg->ePagetype = bIndex?HCT_PAGETYPE_INDEX_LEAF:HCT_PAGETYPE_INTKEY_LEAF;
  pPg->nFree = pPg->nFreeSpace = szPage - sizeof(HctDatabasePage);
}

int sqlite3HctDbRootInit(HctDatabase *p, int bIndex, u32 iRoot){
  HctFilePage pg;
  int rc;

  hctDbStartTrans(p);
  rc = sqlite3HctFilePageNew(p->pFile, iRoot, &pg);
  if( rc==SQLITE_OK ){
    sqlite3HctDbRootPageInit(bIndex, pg.aNew, p->pgsz);
    rc = sqlite3HctFilePageRelease(&pg);
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
  HctFile *pFile = pCsr->pDb->pFile;
  u64 iTid = pCsr->pDb->iTid;
  int res = -1;
  int rc;

  assert( pRec==0 ); /* TODO! Support index tables! */
  rc = sqlite3HctFilePageGet(pFile, pCsr->iRoot, &pCsr->pg);
  while( rc==SQLITE_OK ){
    HctDatabasePage *pPg = (HctDatabasePage*)pCsr->pg.aOld;
    HctIntkeyTid *aKey = (HctIntkeyTid*)&pPg[1];
    int i1 = 0;
    int i2 = pPg->nEntry-1;

    /* Seek within the page. Goal is to find the entry that is less than or
    ** equal to (iKey/iTid).  */
    while( i2>i1 ){
      int iTest = (i1+i2+1)/2;
      i64 iPageKey = aKey[iTest].iKey;
      assert( iTest>i1 );
      if( iPageKey<iKey ){
        i1 = iTest;
      }else if( iPageKey>iKey ){
        i2 = iTest-1;
      }else{
        /* Keys match. Compare tid values. */
        u64 iPageTid = (aKey[iTest].iTidFlags & HCT_TID_MASK);
        if( iPageTid<iTid ){
          i1 = iTest;
        }else if( iPageTid>iTid ){
          i2 = iTest-1;
        }else{
          i1 = i2 = iTest;
        }
      }
    }

    /* Assert that we appear to have landed on the correct entry. */
    assert( i1==i2 || (pPg->nEntry==0 && i2==-1) );
    assert( i2==-1 || aKey[i2].iKey<iKey 
        || (aKey[i2].iKey==iKey && (aKey[i2].iTidFlags & HCT_TID_MASK)<=iTid) 
    );
    assert( i2+1==pPg->nEntry || aKey[i2+1].iKey>iKey 
        || (aKey[i2+1].iKey==iKey && (aKey[i2+1].iTidFlags & HCT_TID_MASK)>iTid)
    );

    /* Test if it is necessary to skip to the peer node. */
    if( i2>=0 && i2==pPg->nEntry-1 && pPg->iPeerPg!=0 ){
      assert( 0 );
    }

    if( pPg->ePagetype==HCT_PAGETYPE_INTKEY_LEAF ){
      pCsr->iCell = i2;
      break;
    }else{
      /* Descend to the next list */
      assert( 0 );
    }
  }

  if( pRes ) *pRes = res;
  return rc;
}

static void hctDbCsrInit(HctDatabase *pDb, u32 iRoot, HctDbCsr *pCsr){
  memset(pCsr, 0, sizeof(HctDbCsr));
  pCsr->pDb = pDb;
  pCsr->iRoot = iRoot;
}


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

int sqlite3HctDbInsert(
  HctDatabase *pDb,
  u32 iRoot, 
  UnpackedRecord *pRec, 
  i64 iKey, 
  int nData, const u8 *aData
){
  int rc;
  HctDbCsr csr;

  assert( pRec==0 );
  hctDbStartTrans(pDb);

  hctDbCsrInit(pDb, iRoot, &csr);
  rc = sqlite3HctDbCsrSeek(&csr, pRec, iKey, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFilePageWrite(&csr.pg);
  }
  if( rc==SQLITE_OK ){
    /* Create the new version of the page */
    int iNewCell = csr.iCell+1;
    HctDatabasePage *pOld = (HctDatabasePage*)csr.pg.aOld;
    HctDatabasePage *pNew = (HctDatabasePage*)csr.pg.aNew;
    HctIntkeyTid *aNewKey = (HctIntkeyTid*)&pNew[1];
    HctIntkeyTid *aOldKey = (HctIntkeyTid*)&pOld[1];
    u16 *aNewOff = (u16*)&aNewKey[pOld->nEntry+1];
    u16 *aOldOff = (u16*)&aOldKey[pOld->nEntry];

    int nReq = 16 + 2 + sqlite3VarintLen(nData) + nData;

    /* Check if there is enough space for the new entry on the page. */
    if( pOld->nFree>=nReq ){
      int iOff;
      if( pOld->nFreeSpace<nReq ){
        /* shuffle up entries on page to make room */
        assert( 0 );
      }

      /* Create the new database page header */
      memcpy(pNew, pOld, sizeof(HctDatabasePage));
      pNew->nEntry++;
      pNew->nFreeSpace -= nReq;
      pNew->nFree -= nReq;

      /* Create the new key/tidflags array */
      hctDbSplitcopy(aNewKey, aOldKey, pOld->nEntry, 16, iNewCell);
      aNewKey[iNewCell].iKey = iKey;
      aNewKey[iNewCell].iTidFlags = pDb->iTid;

      /* Create the new offset array (new entry populated below) */
      hctDbSplitcopy(aNewOff, aOldOff, pOld->nEntry, 2, iNewCell);

      /* Create the new record body area */
      iOff = sizeof(HctDatabasePage) + (16+2) * pOld->nEntry + pOld->nFreeSpace;
      if( pDb->pgsz>iOff ){
        memcpy(&csr.pg.aNew[iOff], &csr.pg.aOld[iOff], pDb->pgsz - iOff);
      }
      iOff -= (nReq - (16+2));
      aNewOff[iNewCell] = (u16)iOff;
      iOff += sqlite3PutVarint(&csr.pg.aNew[iOff], nData);
      memcpy(&csr.pg.aNew[iOff], aData, nData);
    }else{
      /* split page */
      assert( 0 );
    }

    rc = sqlite3HctFilePageRelease(&csr.pg);
  }else{
    sqlite3HctFilePageRelease(&csr.pg);
  }

  return rc;
}

int sqlite3HctDbDelete(
  HctDatabase *p, 
  u32 iRoot, 
  UnpackedRecord *pUnpacked, 
  i64 iKey
){
  hctDbStartTrans(p);
  assert( 0 );
}

int sqlite3HctDbCommit(HctDatabase *p){
  p->iTid = 0;
  return SQLITE_OK;
}

