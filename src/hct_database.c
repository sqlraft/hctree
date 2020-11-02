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
  HctDbCsr *pCsrList;
};

struct HctDbCsr {
  HctDatabase *pDb;               /* Database that owns this cursor */
  u32 iRoot;                      /* Root page cursor is opened on */
  int iCell;                      /* Current cell within page */
  int eDir;                       /* Direction cursor will step after Seek() */
  HctDbCsr *pCsrNext;
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

static void hctDbIntkeyLeaf(
  HctDatabasePage *pPg, 
  HctIntkeyTid **paKey,
  u16 **paOff
){
  *paKey = (HctIntkeyTid*)&pPg[1];
  if( paOff ) *paOff = (u16*)&(*paKey)[pPg->nEntry];
}


/*
** Seek for key iKey (if pRec is null) or pRec within the table opened
** by cursor pCsr.
*/
int hctDbCsrSeek(
  HctDbCsr *pCsr,                 /* Cursor to seek */
  UnpackedRecord *pRec,           /* Key for index tables */
  i64 iKey,                       /* Key for intkey tables */
  int *pbMatch                    /* Result of seek (see above) */
){
  HctFile *pFile = pCsr->pDb->pFile;
  u64 iTid = pCsr->pDb->iTid;
  int rc;
  int res = 0;

  assert( pRec==0 ); /* TODO! Support index tables! */
  rc = sqlite3HctFilePageGet(pFile, pCsr->iRoot, &pCsr->pg);
  while( rc==SQLITE_OK ){
    HctDatabasePage *pPg = (HctDatabasePage*)pCsr->pg.aOld;
    HctIntkeyTid *aKey = (HctIntkeyTid*)&pPg[1];
    int i1 = -1;
    int i2 = pPg->nEntry-1;

    /* Seek within the page. Goal is to find the entry that is less than or
    ** equal to (iKey/iTid). If all entries on the page are larger than
    ** (iKey/iTid), find the "entry" -1. */
    res = 0;
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
        res = 1;
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
    assert( i1==i2 );
    assert( i2==-1 || aKey[i2].iKey<iKey 
        || (aKey[i2].iKey==iKey && (aKey[i2].iTidFlags & HCT_TID_MASK)<=iTid) 
    );
    assert( i2+1==pPg->nEntry || aKey[i2+1].iKey>iKey 
        || (aKey[i2+1].iKey==iKey && (aKey[i2+1].iTidFlags & HCT_TID_MASK)>iTid)
    );

    /* Test if it is necessary to skip to the peer node. */
    if( i2>=0 && i2==pPg->nEntry-1 && pPg->iPeerPg!=0 ){
      HctFilePage peer;
      rc = sqlite3HctFilePageGet(pFile, pPg->iPeerPg, &peer);
      if( rc==SQLITE_OK ){
        HctIntkeyTid *aPKey;
        u16 *aPOff;
        hctDbIntkeyLeaf((HctDatabasePage*)peer.aOld, &aPKey, &aPOff);
        if( aPKey[0].iKey<iKey 
        || (aPKey[0].iKey==iKey && (aPKey[0].iTidFlags & HCT_TID_MASK)<=iTid) ){
          SWAP(HctFilePage, pCsr->pg, peer);
          sqlite3HctFilePageRelease(&peer);
          continue;
        }
        sqlite3HctFilePageRelease(&peer);
      }
    }

    if( pPg->ePagetype==HCT_PAGETYPE_INTKEY_LEAF ){
      pCsr->iCell = i2;
      break;
    }else{
      /* Descend to the next list */
      assert( 0 );
    }
  }

  if( pbMatch ) *pbMatch = res;
  return rc;
}

void sqlite3HctDbCsrDir(HctDbCsr *pCsr, int eDir){
  pCsr->eDir = eDir;
}


/*
** Return true if the entry that pCsr currently points at should be 
** visible to the user. Or false if it should not. If the cursor already
** points at EOF, return true.
*/
static int hctDbCsrVisible(HctDbCsr *pCsr){
  if( pCsr->iCell>=0 ){
    HctDatabasePage *pPg = (HctDatabasePage*)pCsr->pg.aOld;
    HctIntkeyTid *aTid;
    hctDbIntkeyLeaf(pPg, &aTid, 0);
    if( (aTid[pCsr->iCell].iTidFlags & HCT_TID_MASK)>pCsr->pDb->iTid ){
      return 0;
    }
  }
  return 1;
}

static void hctDbCsrReset(HctDbCsr *pCsr){
  sqlite3HctFilePageRelease(&pCsr->pg);
  pCsr->iCell = -1;
  pCsr->eDir = 0;
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
  int bMatch;
  rc = hctDbCsrSeek(pCsr, pRec, iKey, &bMatch);

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
    }else if( 0==hctDbCsrVisible(pCsr) ){
      switch( pCsr->eDir ){
        case BTREE_DIR_FORWARD:
          *pRes = 1;
          rc = sqlite3HctDbCsrNext(pCsr);
          break;
        case BTREE_DIR_REVERSE:
          assert( 0 );
          break;
        default: assert( pCsr->eDir==BTREE_DIR_NONE );
          hctDbCsrReset(pCsr);
          *pRes = -1;
          break;
      }
    }else{
      *pRes = bMatch ? 0 : -1;
    }
  }

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

/*
** Return number of bytes required by a cell...
**
** TODO: Add more parameters to support other types of cells.
*/
static int hctDbCellSize(int nData){
  return sqlite3VarintLen(nData) + nData;
}

static int hctDbCellPut(u8 *aBuf, int nData, const u8 *aData){
  int iOff;
  iOff = sqlite3PutVarint(aBuf, nData);
  memcpy(&aBuf[iOff], aData, nData);
  return iOff+nData;
}

static int hctDbSplitPage(
  HctDbCsr *pCsr,
  UnpackedRecord *pRec, 
  i64 iKey, 
  int nData, const u8 *aData
){
  HctDatabase *pDb = pCsr->pDb;
  int pgsz = pDb->pgsz;
  int iNewCell = pCsr->iCell+1;     /* Position of new entry in cell array */
  HctDatabasePage *pOld = (HctDatabasePage*)pCsr->pg.aOld;
  int nTotal = 0;                 /* Total bytes of data to distribute */
  int nLeft = 0;                  /* Number of cells on left-hand-page */
  HctIntkeyTid *aOldKey;
  u16 *aOldOff;
  int iNew;
  int iOld;
  HctFilePage peer;               /* New peer page */
  int rc;

  hctDbIntkeyLeaf(pOld, &aOldKey, &aOldOff);

  /* Figure out how much content, in total, there is. */
  nTotal = (pgsz - pOld->nFreeSpace - sizeof(*pOld));
  nTotal += hctDbCellSize(nData) + (16+2);

  /* Work out how many cells to store on the left-hand-page. Store this 
  ** number in stack variable nLeft. */
  nLeft = sizeof(HctDatabasePage);
  for(iNew=0, iOld=0; nLeft<=(nTotal/2); iNew++){
    nLeft += (16+2);
    if( iNew==iNewCell ){
      nLeft += hctDbCellSize(nData);
    }else{
      int nCellData;
      getVarint32(&pCsr->pg.aOld[aOldOff[iOld]], nCellData);
      nLeft += hctDbCellSize(nCellData);
      iOld++;
    }
    if( nLeft>pgsz ) break;
  }
  nLeft = iNew;

// printf("splitting page %d (%d/%d)\n", pCsr->pg.iPg, nLeft, pOld->nEntry+1-nLeft);

  /* Allocate the peer page */
  rc = sqlite3HctFilePageNew(pDb->pFile, 0, &peer);
  if( rc==SQLITE_OK ){
    int iOff;
    HctDatabasePage *pLeft = (HctDatabasePage*)pCsr->pg.aNew;
    HctDatabasePage *pRight = (HctDatabasePage*)peer.aNew;
    HctIntkeyTid *aLeftKey, *aRightKey;
    u16 *aLeftOff, *aRightOff;

    memset(pLeft, 0, sizeof(*pLeft));
    pLeft->ePagetype = HCT_PAGETYPE_INTKEY_LEAF;
    pLeft->nEntry = nLeft;
    pLeft->iPeerPg = peer.iPg;
    hctDbIntkeyLeaf(pLeft, &aLeftKey, &aLeftOff);

    memset(pRight, 0, sizeof(*pRight));
    pRight->ePagetype = HCT_PAGETYPE_INTKEY_LEAF;
    pRight->nEntry = pOld->nEntry+1-nLeft;
    pRight->iPeerPg = pOld->iPeerPg;
    hctDbIntkeyLeaf(pRight, &aRightKey, &aRightOff);

    iOff = pgsz;
    for(iNew=0, iOld=0; iNew<pOld->nEntry+1; iNew++){
      int iCellOff;
      HctIntkeyTid newKey;
      int nNewData;
      const u8 *aNewData;
      if( iNew==iNewCell ){
        newKey.iKey = iKey;
        newKey.iTidFlags = pDb->iTid;
        nNewData = nData;
        aNewData = aData;
      }else{
        int iCellOff = aOldOff[iOld];
        newKey = aOldKey[iOld];
        iCellOff += getVarint32(&pCsr->pg.aOld[iCellOff], nNewData);
        aNewData = &pCsr->pg.aOld[iCellOff];
        iOld++;
      }

      if( iNew<nLeft ){
        int iCellOff;
        aLeftKey[iNew] = newKey;
        iOff -= hctDbCellSize(nNewData);
        iCellOff = iOff + sqlite3PutVarint(&pCsr->pg.aNew[iOff], nNewData);
        memcpy(&pCsr->pg.aNew[iCellOff], aNewData, nNewData);
        aLeftOff[iNew] = (u16)iOff;
      }else{
        if( iNew==nLeft ) iOff = pgsz;
        aRightKey[iNew-nLeft] = newKey;
        iOff -= hctDbCellSize(nNewData);
        iCellOff = iOff + sqlite3PutVarint(&peer.aNew[iOff], nNewData);
        memcpy(&peer.aNew[iCellOff], aNewData, nNewData);
        aRightOff[iNew-nLeft] = (u16)iOff;
      }
    }

    pLeft->nFree = pLeft->nFreeSpace = 
      aLeftOff[pLeft->nEntry-1] - sizeof(*pLeft) - (16+2)*pLeft->nEntry;
    pRight->nFree = pRight->nFreeSpace = 
      aRightOff[pRight->nEntry-1] - sizeof(*pRight) - (16+2)*pRight->nEntry;

    /* Release peer and original pages */
    rc = sqlite3HctFilePageRelease(&peer);
    if( rc==SQLITE_OK ){
      rc = sqlite3HctFilePageRelease(&pCsr->pg);
      /* TODO: If this fails - reclaim the allocated peer page */
    }
  }

  return SQLITE_OK;
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
  rc = hctDbCsrSeek(&csr, pRec, iKey, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFilePageWrite(&csr.pg);
  }
  if( rc==SQLITE_OK ){
    /* Create the new version of the page */
    HctDatabasePage *pOld = (HctDatabasePage*)csr.pg.aOld;
    int nReq;                     /* Required space on page in bytes */
    
    /* Check if there is enough space for the new entry on the page. */
    nReq = 16 + 2 + hctDbCellSize(nData);
    if( pOld->nFree>=nReq ){
      int iNewCell = csr.iCell+1; /* Position of new entry in cell array */
      HctDatabasePage *pNew = (HctDatabasePage*)csr.pg.aNew;
      HctIntkeyTid *aNewKey = (HctIntkeyTid*)&pNew[1];
      HctIntkeyTid *aOldKey = (HctIntkeyTid*)&pOld[1];
      u16 *aNewOff = (u16*)&aNewKey[pOld->nEntry+1];
      u16 *aOldOff = (u16*)&aOldKey[pOld->nEntry];
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
      hctDbCellPut(&csr.pg.aNew[iOff], nData, aData);
    }else{
      /* split page */
      rc = hctDbSplitPage(&csr, pRec, iKey, nData, aData);
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

int sqlite3HctDbEndTransaction(HctDatabase *p){
  p->iTid = 0;
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
    if( pDb->iTid==0 ){
      pDb->iTid = sqlite3HctFileGetTransid(pDb->pFile);
      assert( pDb->iTid<50000 );
    }
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
  HctDatabasePage *pPg = (HctDatabasePage*)pCsr->pg.aOld;
  HctIntkeyTid *aTid = (HctIntkeyTid*)&pPg[1];

  assert( pCsr->iCell>=0 && pCsr->iCell<pPg->nEntry );
  assert( pPg->ePagetype==HCT_PAGETYPE_INTKEY_LEAF );

  *piKey = aTid[pCsr->iCell].iKey;
}

int sqlite3HctDbCsrEof(HctDbCsr *pCsr){
  return pCsr==0 || pCsr->iCell<0;
}

int sqlite3HctDbCsrFirst(HctDbCsr *pCsr){
  int rc;
  HctFile *pFile = pCsr->pDb->pFile;

  rc = sqlite3HctFilePageGet(pFile, pCsr->iRoot, &pCsr->pg);
  if( rc==SQLITE_OK ){
    HctDatabasePage *pPg = (HctDatabasePage*)pCsr->pg.aOld;
    assert( pPg->ePagetype==HCT_PAGETYPE_INTKEY_LEAF );

    pCsr->iCell = 0;
    if( pPg->nEntry==0 ){
      pCsr->iCell = -1;
    }

    if( hctDbCsrVisible(pCsr)==0 ){
      rc = sqlite3HctDbCsrNext(pCsr);
    }
  }

  return rc;
}

int sqlite3HctDbCsrLast(HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  HctFile *pFile = pCsr->pDb->pFile;
  u32 iPg = pCsr->iRoot;
  HctDatabasePage *pPg = 0;

  hctDbCsrReset(pCsr);
  while( rc==SQLITE_OK && iPg ) {
    sqlite3HctFilePageRelease(&pCsr->pg);
    rc = sqlite3HctFilePageGet(pFile, iPg, &pCsr->pg);
    if( rc==SQLITE_OK ){
      pPg = (HctDatabasePage*)pCsr->pg.aOld;
      iPg = pPg->iPeerPg;
    }
  }

  if( rc==SQLITE_OK ){
    assert( pPg->ePagetype==HCT_PAGETYPE_INTKEY_LEAF );
    assert( pPg->iPeerPg==0 );
    pCsr->iCell = pPg->nEntry-1;
    /* TODO - avoid landing on an invisible entry. Deal with this after
    ** parent lists and xPrev() are working properly. */
  }

  return rc;
}

int sqlite3HctDbCsrNext(HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  HctDatabasePage *pPg = (HctDatabasePage*)pCsr->pg.aOld;

  assert( pCsr->iCell>=0 && pCsr->iCell<pPg->nEntry );
  assert( pPg->ePagetype==HCT_PAGETYPE_INTKEY_LEAF );

  do {
    pCsr->iCell++;
    if( pCsr->iCell==pPg->nEntry ){
      u32 iPeerPg = pPg->iPeerPg;
      /* TODO - jump to peer page */
      if( iPeerPg==0 ){
        pCsr->iCell = -1;
      }else{
        rc = sqlite3HctFilePageRelease(&pCsr->pg);
        if( rc==SQLITE_OK ){
          rc = sqlite3HctFilePageGet(pCsr->pDb->pFile, iPeerPg, &pCsr->pg);
          pCsr->iCell = 0;
        }
      }
    }
  }while( rc==SQLITE_OK && hctDbCsrVisible(pCsr)==0 );

  return rc;
}


int sqlite3HctDbCsrData(HctDbCsr *pCsr, int *pnData, const u8 **paData){
  HctDatabasePage *pPg = (HctDatabasePage*)pCsr->pg.aOld;
  int iOff;
  int nData;
  u16 *aOff;

  assert( pCsr->iCell>=0 && pCsr->iCell<pPg->nEntry );
  assert( pPg->ePagetype==HCT_PAGETYPE_INTKEY_LEAF );

  aOff = (u16*)&pCsr->pg.aOld[sizeof(HctDatabasePage) + 16*pPg->nEntry];
  iOff = (int)aOff[pCsr->iCell];
  iOff += getVarint32(&pCsr->pg.aOld[iOff], nData);
  if( paData ){
    *paData = &pCsr->pg.aOld[iOff];
  }

  *pnData = nData;
  return SQLITE_OK;
}

