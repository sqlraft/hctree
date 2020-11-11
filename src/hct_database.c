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
  u8 nHeight;                     /* 0 for leaves, 1 for parents etc. */
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
    rc = sqlite3HctFileOpen(zFile, &pNew->pFile);
  }else{
    rc = SQLITE_NOMEM_BKPT;
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

static void hctDbStartTrans(HctDatabase *p){
  if( p->iTid==0 ){
    p->iTid = sqlite3HctFileStartTrans(p->pFile);
  }
}

void sqlite3HctDbRootPageInit(int bIndex, u8 *aPage, int szPage){
  HctDatabasePage *pPg = (HctDatabasePage*)aPage;
  memset(aPage, 0, szPage);
  pPg->ePagetype = bIndex ? HCT_PAGETYPE_INDEX : HCT_PAGETYPE_INTKEY;
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

static void hctDbIntkeyNode(
  HctDatabasePage *pPg, 
  HctIntkeyTid **paKey,
  u32 **paChild
){
  *paKey = (HctIntkeyTid*)&pPg->nFreeSpace;
  if( paChild ) *paChild = (u32*)&(*paKey)[pPg->nEntry];
}

static HctDatabasePage *hctDbIntkeyPage(
  u8 *aPg,
  HctIntkeyTid **paKey,
  u16 **paOff,
  u32 **paChild
){
  HctDatabasePage *pPg = (HctDatabasePage*)aPg;
  HctIntkeyTid *aKey;

  if( pPg->nHeight==0 ){
    hctDbIntkeyLeaf(pPg, &aKey, paOff);
  }else{
    hctDbIntkeyNode(pPg, &aKey, paChild);
  }
  if( paKey ) *paKey = aKey;

  return pPg;
}

static HctIntkeyTid *hctDbCsrKey(u8 *aPg, int iCell){
  HctIntkeyTid *aKey;
  hctDbIntkeyPage(aPg, &aKey, 0, 0);
  return &aKey[iCell];
}

/*
** Return (*p1 - *p2).
*/
static int hctDbIntkeyCmp(
  int bReverse,
  HctIntkeyTid *p1, 
  i64 iKey,
  u64 iTid
){
  int ret = 0;
  if( p1->iKey<iKey ){
    ret = -1;
  }else if( p1->iKey>iKey ){
    ret = 1;
  }else{
    u64 iP1Tid = (p1->iTidFlags & HCT_TID_MASK);
    if( iP1Tid<iTid ){
      ret = -1;
    }else if( iP1Tid>iTid ){
      ret = 1;
    }
  }

  return ret * (bReverse ? -1 : 1);
}

/*
** Seek for key iKey (if pRec is null) or pRec within the table opened
** by cursor pCsr.
*/
int hctDbCsrSeek(
  HctDbCsr *pCsr,                 /* Cursor to seek */
  int iHeight,                    /* Height to seek at (0==leaf, 1==parent) */
  UnpackedRecord *pRec,           /* Key for index tables */
  i64 iKey,                       /* Key for intkey tables */
  u64 iTid 
){
  HctFile *pFile = pCsr->pDb->pFile;
  int rc;

  assert( pRec==0 ); /* TODO! Support index tables! */
  rc = sqlite3HctFilePageGet(pFile, pCsr->iRoot, &pCsr->pg);
  while( rc==SQLITE_OK ){
    HctDatabasePage *pPg = (HctDatabasePage*)pCsr->pg.aOld;
    int bReverse = (pPg->nHeight % 2);
    HctIntkeyTid *aKey = 0;
    u32 *aChild = 0;
    int i1 = -1;
    int i2 = pPg->nEntry-1;

    if( pPg->nHeight>0 ){
      hctDbIntkeyNode(pPg, &aKey, &aChild);
    }else{
      hctDbIntkeyLeaf(pPg, &aKey, 0);
    }

    /* Seek within the page. Goal is to find the entry that is less than or
    ** equal to (iKey/iTid). If all entries on the page are larger than
    ** (iKey/iTid), find the "entry" -1. */
    while( i2>i1 ){
      int iTest = (i1+i2+1)/2;
      int ret = hctDbIntkeyCmp(bReverse, &aKey[iTest], iKey, iTid);
      if( ret<0 ){
        i1 = iTest;
      }else if( ret>0 ){
        i2 = iTest-1;
      }else{
        i1 = i2 = iTest;
      }
    }

    /* Assert that we appear to have landed on the correct entry. */
    assert( i1==i2 );
    assert( i2==-1 || hctDbIntkeyCmp(bReverse, &aKey[i2], iKey, iTid)<=0 );
    assert( i2+1==pPg->nEntry 
        || hctDbIntkeyCmp(bReverse, &aKey[i2+1], iKey, iTid)>0 
    );

    /* Test if it is necessary to skip to the peer node. */
    if( i2>=0 && i2==pPg->nEntry-1 && pPg->iPeerPg!=0 ){
      HctFilePage peer;
      rc = sqlite3HctFilePageGet(pFile, pPg->iPeerPg, &peer);
      if( rc==SQLITE_OK ){
        HctIntkeyTid *aPKey;
        u16 *aPOff;
        hctDbIntkeyLeaf((HctDatabasePage*)peer.aOld, &aPKey, &aPOff);
        if( pPg->nHeight 
         || hctDbIntkeyCmp(bReverse, &aPKey[0], iKey, iTid)<=0 
        ){
          SWAP(HctFilePage, pCsr->pg, peer);
          sqlite3HctFilePageRelease(&peer);
          continue;
        }
        sqlite3HctFilePageRelease(&peer);
      }
    }

    if( rc==SQLITE_OK ){
      if( pPg->nHeight==iHeight ){
        pCsr->iCell = i2;
        break;
      }else{
        /* Descend to the next list */
        u32 iPg = aChild[i2+1];
        assert( (i2+1)>=0 && (i2+1)<pPg->nEntry );
        sqlite3HctFilePageRelease(&pCsr->pg);
        rc = sqlite3HctFilePageGet(pFile, iPg, &pCsr->pg);
      }
    }
  }

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
    i64 iTid = pCsr->pDb->iTid;
    int iCell = pCsr->iCell;
    HctDatabasePage *pPg = (HctDatabasePage*)pCsr->pg.aOld;
    HctIntkeyTid *aTid;
    u16 *aOff;
    hctDbIntkeyLeaf(pPg, &aTid, &aOff);

    if( aOff[iCell]==0 || (aTid[iCell].iTidFlags & HCT_TID_MASK)>iTid ){
      return 0;
    }
    if( aTid[iCell].iTidFlags & HCT_IS_DELETED ){
      i64 iDelTid = LARGEST_INT64;
      if( iCell+1<pPg->nEntry ){
        iDelTid = aTid[iCell+1].iTidFlags & HCT_TID_MASK;
      }else{
        HctIntkeyTid *pKey;
        HctFilePage pg;
        sqlite3HctFilePageGet(pCsr->pDb->pFile, pPg->iPeerPg, &pg);
        pKey = hctDbCsrKey(pg.aOld, 0);
        if( pKey->iKey==aTid[iCell].iKey ){
          iDelTid = pKey->iTidFlags & HCT_TID_MASK;
        }
      }
      if( iDelTid<=iTid ) return 0;
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
  rc = hctDbCsrSeek(pCsr, 0, pRec, iKey, pCsr->pDb->iTid);

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

static int hctDbRedistribute(
  HctDbCsr *pCsr,
  int bClobber,
  UnpackedRecord *pRec, 
  i64 iKey, 
  int nData, const u8 *aData,
  HctFilePage *pPg1,
  HctFilePage *pPg2
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

  int iOff;
  HctDatabasePage *pLeft = (HctDatabasePage*)pPg1->aNew;
  HctDatabasePage *pRight = (HctDatabasePage*)pPg2->aNew;
  HctIntkeyTid *aLeftKey, *aRightKey;
  u16 *aLeftOff, *aRightOff;

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
      if( aOldOff[iOld] ){
        int nCellData;
        getVarint32(&pCsr->pg.aOld[aOldOff[iOld]], nCellData);
        nLeft += hctDbCellSize(nCellData);
      }
      iOld++;
    }
    if( nLeft>pgsz ) break;
  }
  nLeft = iNew;

// printf("splitting page %d (%d/%d)\n", pCsr->pg.iPg, nLeft, pOld->nEntry+1-nLeft);

  memset(pLeft, 0, sizeof(*pLeft));
  pLeft->ePagetype = HCT_PAGETYPE_INTKEY;
  pLeft->nEntry = nLeft;
  pLeft->iPeerPg = pPg2->iPg;
  hctDbIntkeyLeaf(pLeft, &aLeftKey, &aLeftOff);

  memset(pRight, 0, sizeof(*pRight));
  pRight->ePagetype = HCT_PAGETYPE_INTKEY;
  pRight->nEntry = pOld->nEntry+1-nLeft;
  pRight->iPeerPg = pOld->iPeerPg;
  hctDbIntkeyLeaf(pRight, &aRightKey, &aRightOff);

  iOff = pgsz;
  for(iNew=0, iOld=0; iNew<pOld->nEntry+1; iNew++){
    int iCellOff;
    HctIntkeyTid newKey;
    int nNewData;
    const u8 *aNewData = 0;
    if( iNew==iNewCell ){
      newKey.iKey = iKey;
      newKey.iTidFlags = pDb->iTid;
      nNewData = nData;
      aNewData = aData;
    }else{
      int iCellOff = aOldOff[iOld];
      newKey = aOldKey[iOld];
      if( iNew==iNewCell-1 && bClobber ){
        newKey.iTidFlags |= HCT_IS_DELETED;
      }
      if( iCellOff ){
        iCellOff += getVarint32(&pCsr->pg.aOld[iCellOff], nNewData);
        aNewData = &pCsr->pg.aOld[iCellOff];
      }
      iOld++;
    }

    if( iNew<nLeft ){
      int iCellOff;
      aLeftKey[iNew] = newKey;
      if( aNewData ){
        iOff -= hctDbCellSize(nNewData);
        iCellOff = iOff + sqlite3PutVarint(&pPg1->aNew[iOff], nNewData);
        memcpy(&pPg1->aNew[iCellOff], aNewData, nNewData);
        aLeftOff[iNew] = (u16)iOff;
      }else{
        aLeftOff[iNew] = 0;
      }
    }else{
      if( iNew==nLeft ) iOff = pgsz;
      aRightKey[iNew-nLeft] = newKey;
      if( aNewData ){
        iOff -= hctDbCellSize(nNewData);
        iCellOff = iOff + sqlite3PutVarint(&pPg2->aNew[iOff], nNewData);
        memcpy(&pPg2->aNew[iCellOff], aNewData, nNewData);
        aRightOff[iNew-nLeft] = (u16)iOff;
      }else{
        aRightOff[iNew-nLeft] = 0;
      }
    }
  }

  pLeft->nFree = pLeft->nFreeSpace = 
    aLeftOff[pLeft->nEntry-1] - sizeof(*pLeft) - (16+2)*pLeft->nEntry;
  pRight->nFree = pRight->nFreeSpace = 
    aRightOff[pRight->nEntry-1] - sizeof(*pRight) - (16+2)*pRight->nEntry;

  return SQLITE_OK;
}

static int hctDbInsertFP(HctDatabase*, u32, int, HctIntkeyTid*, u32);
static int hctDbSplitPage(
  HctDbCsr *pCsr,
  int bClobber,
  UnpackedRecord *pRec, 
  i64 iKey, 
  int nData, const u8 *aData
){
  HctDatabase *pDb = pCsr->pDb;
  HctDatabasePage *pOld = (HctDatabasePage*)pCsr->pg.aOld;
  int rc;                         /* Return code */
  HctFilePage peer;               /* New peer page */
  HctIntkeyTid fpk = {0,0};       /* Fencepost key for new page */
  u32 iNew = 0;                   /* Logical page number of new page */
  int iHeight = 0;

  assert( pOld->ePagetype==HCT_PAGETYPE_INTKEY && pOld->nHeight==0 );

  /* Allocate a peer page. This is required whether or not this is a split
  ** of the root node.  */
  rc = sqlite3HctFilePageNew(pDb->pFile, 0, &peer);
  if( rc!=SQLITE_OK ) return rc;

  if( pCsr->iRoot==pCsr->pg.iPg ){
    HctFilePage peer1;               /* New peer page */
    /* This is a split the root page. Allocate a second peer. */
    rc = sqlite3HctFilePageNew(pDb->pFile, 0, &peer1);
    if( rc==SQLITE_OK ){
      rc = hctDbRedistribute(
          pCsr, bClobber, pRec, iKey, nData, aData, &peer1, &peer
      );
    }
    if( rc==SQLITE_OK ){
      HctIntkeyTid *aKey = 0;
      HctIntkeyTid *aRightKey = 0;
      u32 *aChild = 0;
      HctDatabasePage *pRoot = (HctDatabasePage*)pCsr->pg.aNew;
      HctDatabasePage *pRight = (HctDatabasePage*)peer.aNew;

      memset(pCsr->pg.aNew, 0, pDb->pgsz);
      pRoot->ePagetype = HCT_PAGETYPE_INTKEY;
      /* TODO: Allow for trees with more than one level of internal node */
      pRoot->nEntry = 2;
      pRoot->nHeight = 1;
      hctDbIntkeyNode(pRoot, &aKey, &aChild);
      hctDbIntkeyLeaf(pRight, &aRightKey, 0);

      aKey[0].iKey = aRightKey[0].iKey;
      aKey[0].iTidFlags = (aRightKey[0].iTidFlags & HCT_TID_MASK);
      aChild[0] = peer.iPg;
      aKey[1].iKey = SMALLEST_INT64;
      aKey[1].iTidFlags = 0;
      aChild[1] = peer1.iPg;

      rc = sqlite3HctFilePageRelease(&peer1);
    }
  }else{
    HctDatabasePage *pPg;
    HctIntkeyTid *aKey = 0;
    rc = hctDbRedistribute(
        pCsr, bClobber, pRec, iKey, nData, aData, &pCsr->pg, &peer
    );
    pPg = hctDbIntkeyPage(peer.aNew, &aKey, 0, 0);
    fpk.iTidFlags = aKey[0].iTidFlags & HCT_TID_MASK;
    fpk.iKey = aKey[0].iKey;
    iNew = peer.iPg;
    iHeight = pPg->nHeight+1;
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3HctFilePageRelease(&peer);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFilePageRelease(&pCsr->pg);
    /* TODO: If this fails - reclaim the allocated peer page(s) */
  }

  if( iNew && rc==SQLITE_OK ){
    rc = hctDbInsertFP(pCsr->pDb, pCsr->iRoot, iHeight, &fpk, iNew);
  }

  return rc;
}

int sqlite3HctDbInsert(
  HctDatabase *pDb,
  u32 iRoot, 
  UnpackedRecord *pRec, 
  i64 iKey, 
  int bDel,
  int nData, const u8 *aData
){
  int rc;
  int bClobber = 0;
  HctDbCsr csr;

  assert( bDel==0 || bDel==1 );
  assert( bDel==(aData==0) );
  assert( pRec==0 );
  hctDbStartTrans(pDb);

  hctDbCsrInit(pDb, iRoot, &csr);
  rc = hctDbCsrSeek(&csr, 0, pRec, iKey, pDb->iTid);
  if( rc==SQLITE_OK ){
    if( csr.iCell>=0 ){
      HctIntkeyTid *pKey = hctDbCsrKey(csr.pg.aOld, csr.iCell);
      bClobber = pKey->iKey==iKey;
    }
    if( bDel && !bClobber ){
      /* This is a delete operation but the key does not appear to be
      ** present in the database. Return early.  */
      sqlite3HctFilePageRelease(&csr.pg);
      return SQLITE_OK;
    }
    rc = sqlite3HctFilePageWrite(&csr.pg);
  }

  if( rc==SQLITE_OK ){
    /* Create the new version of the page */
    HctDatabasePage *pOld = (HctDatabasePage*)csr.pg.aOld;
    int nReq;                     /* Required space on page in bytes */
    
    /* Check if there is enough space for the new entry on the page. */
    nReq = 16 + 2 + (bDel ? 0 : hctDbCellSize(nData));
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
      if( bDel ){
        aNewOff[iNewCell] = 0;
      }else{
        aNewOff[iNewCell] = (u16)iOff;
        hctDbCellPut(&csr.pg.aNew[iOff], nData, aData);
      }
      if( bClobber ){
        assert( iNewCell>0 );
        aNewKey[iNewCell-1].iTidFlags |= HCT_IS_DELETED;
      }
    }else{
      /* split page */
      rc = hctDbSplitPage(&csr, bClobber, pRec, iKey, nData, aData);
    }

    rc = sqlite3HctFilePageRelease(&csr.pg);
  }else{
    sqlite3HctFilePageRelease(&csr.pg);
  }

  return rc;
}

/*
** Given a page size of pgsz bytes, return the number of cells that fit
** on a single intkey internal node.
*/
static int hctDbIntkeyNodeSize(int pgsz){
  return (pgsz - sizeof(HctDatabasePage)) / (16+4);
}

static void hctDbRedisNode(
  HctDbCsr *pCsr,
  HctIntkeyTid *pKey,
  u32 iChild,
  HctFilePage *pPg1,
  HctFilePage *pPg2
){
  HctDatabasePage *pOld = (HctDatabasePage*)pCsr->pg.aOld;
  int iNewCell = pCsr->iCell+1;
  int nTotal = pOld->nEntry+1;
  HctIntkeyTid *aOldKey = 0;
  u32 *aOldChild = 0;
  int nLeft = (nTotal+1)/2;
  int i, iOld;

  HctDatabasePage *pLeft, *pRight;
  HctIntkeyTid *aLKey, *aRKey;
  u32 *aLChild, *aRChild;

  assert( pPg1->aNew && pPg2->aNew );
  assert( pOld->nHeight>0 && pOld->ePagetype==HCT_PAGETYPE_INTKEY );

  hctDbIntkeyNode(pOld, &aOldKey, &aOldChild);
  pLeft = (HctDatabasePage*)pPg1->aNew;
  pRight = (HctDatabasePage*)pPg2->aNew;

  pLeft->ePagetype = HCT_PAGETYPE_INTKEY;
  pLeft->nHeight = pOld->nHeight;
  pLeft->nEntry = nLeft;
  pLeft->iPeerPg = pPg2->iPg;
  hctDbIntkeyNode(pLeft, &aLKey, &aLChild);

  pRight->ePagetype = HCT_PAGETYPE_INTKEY;
  pRight->nHeight = pOld->nHeight;
  pRight->nEntry = nTotal - nLeft;
  pRight->iPeerPg = pOld->iPeerPg;
  hctDbIntkeyNode(pRight, &aRKey, &aRChild);

  for(i=0, iOld=0; i<nTotal; i++){
    u32 *pChild;
    HctIntkeyTid *pPageKey;
    if( i<nLeft ){
      pChild = &aLChild[i];
      pPageKey = &aLKey[i];
    }else{
      pChild = &aRChild[i-nLeft];
      pPageKey = &aRKey[i-nLeft];
    }
    if( i==iNewCell ){
      *pChild = iChild;
      *pPageKey = *pKey;
    }else{
      *pChild = aOldChild[iOld];
      *pPageKey = aOldKey[iOld];
      iOld++;
    }
  }
}

static int hctDbSplitNode(HctDbCsr *pCsr, HctIntkeyTid *pKey, u32 iChild){
  HctDatabase *pDb = pCsr->pDb;
  HctFilePage peer;
  u32 iNew = 0;
  int iHeight = 0;
  HctIntkeyTid fpk = {0, 0};
  int rc;

  /* Allocate a peer page. This is required whether or not this is a split
  ** of the root node.  */
  rc = sqlite3HctFilePageNew(pDb->pFile, 0, &peer);
  if( rc!=SQLITE_OK ) return rc;

  if( pCsr->iRoot==pCsr->pg.iPg ){
    HctFilePage peer1;               /* New peer page */
    /* This is a split the root page. Allocate a second peer. */
    rc = sqlite3HctFilePageNew(pDb->pFile, 0, &peer1);
    if( rc==SQLITE_OK ){
      hctDbRedisNode(pCsr, pKey, iChild, &peer1, &peer);
    }
    if( rc==SQLITE_OK ){
      HctIntkeyTid *aKey = 0;
      HctIntkeyTid *aRightKey = 0;
      u32 *aChild = 0;
      HctDatabasePage *pRoot = (HctDatabasePage*)pCsr->pg.aNew;
      HctDatabasePage *pRight = (HctDatabasePage*)peer.aNew;

      memset(pCsr->pg.aNew, 0, pDb->pgsz);
      pRoot->ePagetype = HCT_PAGETYPE_INTKEY;
      pRoot->nEntry = 2;
      pRoot->nHeight = pRight->nHeight+1;

      hctDbIntkeyNode(pRoot, &aKey, &aChild);
      hctDbIntkeyNode(pRight, &aRightKey, 0);

      if( (pRight->nHeight % 2) ){
        aKey[0].iKey = aRightKey[0].iKey;
        aKey[0].iTidFlags = (aRightKey[0].iTidFlags & HCT_TID_MASK);
        aChild[0] = peer.iPg;

        aKey[1].iKey = LARGEST_INT64;
        aKey[1].iTidFlags = (LARGEST_INT64 & HCT_TID_MASK);
        aChild[1] = peer1.iPg;
      }else{
        aKey[0].iKey = aRightKey[0].iKey;
        aKey[0].iTidFlags = (aRightKey[0].iTidFlags & HCT_TID_MASK);
        aChild[0] = peer.iPg;

        aKey[1].iKey = SMALLEST_INT64;
        aKey[1].iTidFlags = 0;
        aChild[1] = peer1.iPg;
      }

      rc = sqlite3HctFilePageRelease(&peer1);
    }
  }else{
    HctDatabasePage *pPg;
    HctIntkeyTid *aKey = 0;
    hctDbRedisNode(pCsr, pKey, iChild, &pCsr->pg, &peer);
    pPg = hctDbIntkeyPage(peer.aNew, &aKey, 0, 0);
    fpk.iTidFlags = aKey[0].iTidFlags & HCT_TID_MASK;
    fpk.iKey = aKey[0].iKey;
    iNew = peer.iPg;
    iHeight = pPg->nHeight+1;
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3HctFilePageRelease(&peer);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFilePageRelease(&pCsr->pg);
    /* TODO: If this fails - reclaim the allocated peer page(s) */
  }

  if( iNew && rc==SQLITE_OK ){
    rc = hctDbInsertFP(pCsr->pDb, pCsr->iRoot, iHeight, &fpk, iNew);
  }

  return SQLITE_OK;
}

static int hctDbInsertFP(
  HctDatabase *pDb,               /* Database to write to */
  u32 iRoot,
  int iHeight,                    /* Height of list to insert new entry into */
  HctIntkeyTid *pKey,             /* New key to insert */
  u32 iChild                      /* Associated child page number */
){
  int rc;
  HctDbCsr csr;

  assert( pKey->iTidFlags==(pKey->iTidFlags & HCT_TID_MASK) );
  hctDbCsrInit(pDb, iRoot, &csr);
  rc = hctDbCsrSeek(&csr, iHeight, 0, pKey->iKey, pKey->iTidFlags);
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFilePageWrite(&csr.pg);
  }
  if( rc==SQLITE_OK ){
    /* Create the new version of the page */
    HctDatabasePage *pOld = (HctDatabasePage*)csr.pg.aOld;
    int nMax = hctDbIntkeyNodeSize(pDb->pgsz);
    
    /* Check if there is enough space for the new entry on the page. */
    if( pOld->nEntry<nMax ){
      int iNewCell = csr.iCell+1; /* Position of new entry in cell array */
      HctDatabasePage *pNew = (HctDatabasePage*)csr.pg.aNew;
      HctIntkeyTid *aNewKey, *aOldKey;
      u32 *aNewChild, *aOldChild;

      pNew->ePagetype = pOld->ePagetype;
      pNew->nHeight = pOld->nHeight;
      pNew->nEntry = pOld->nEntry+1;
      pNew->iPeerPg = pOld->iPeerPg;

      hctDbIntkeyPage(csr.pg.aNew, &aNewKey, 0, &aNewChild);
      hctDbIntkeyPage(csr.pg.aOld, &aOldKey, 0, &aOldChild);

      /* Create the new key/tidflags array */
      hctDbSplitcopy(aNewKey, aOldKey, pOld->nEntry, 16, iNewCell);
      aNewKey[iNewCell] = *pKey;

      /* Create the new child array */
      hctDbSplitcopy(aNewChild, aOldChild, pOld->nEntry, 4, iNewCell);
      aNewChild[iNewCell] = iChild;
    }else{
      /* split page */
      rc = hctDbSplitNode(&csr, pKey, iChild);
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
  HctIntkeyTid *aKey = 0;

  assert( pCsr->iCell>=0 && pCsr->iCell<pPg->nEntry );
  assert( pPg->ePagetype==HCT_PAGETYPE_INTKEY );
  hctDbIntkeyLeaf(pPg, &aKey, 0);

  *piKey = aKey[pCsr->iCell].iKey;
}

int sqlite3HctDbCsrEof(HctDbCsr *pCsr){
  return pCsr==0 || pCsr->iCell<0;
}

int sqlite3HctDbCsrFirst(HctDbCsr *pCsr){
  HctFile *pFile = pCsr->pDb->pFile;
  int rc;
  HctFilePage pg;
  HctDatabasePage *pPg;
  u32 iPg = pCsr->iRoot;

  hctDbCsrReset(pCsr);

  while( 1 ){
    HctIntkeyTid *aDummy;
    u32 *aChild;
    rc = sqlite3HctFilePageGet(pFile, iPg, &pg);
    if( rc!=SQLITE_OK ) break;
    pPg = (HctDatabasePage*)pg.aOld;
    if( pPg->nHeight==0 ) break;
    hctDbIntkeyNode(pPg, &aDummy, &aChild);
    if( (pPg->nHeight % 2) ){
      /* TODO - goto peer node if it exists... */
      iPg = aChild[pPg->nEntry-1];
    }else{
      iPg = aChild[0];
    }
    sqlite3HctFilePageRelease(&pg);
  }

  if( rc==SQLITE_OK ){
    memcpy(&pCsr->pg, &pg, sizeof(pg));
    assert( pPg->ePagetype==HCT_PAGETYPE_INTKEY && pPg->nHeight==0 );
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
  HctFilePage pg;

  hctDbCsrReset(pCsr);
  while( 1 ){
    HctIntkeyTid *aDummy;
    u32 *aChild;

    rc = sqlite3HctFilePageGet(pFile, iPg, &pg);
    if( rc!=SQLITE_OK ) break;

    pPg = (HctDatabasePage*)pg.aOld;
    if( pPg->iPeerPg && (pPg->nHeight % 2)==0 ){
      iPg = pPg->iPeerPg;
    }else if( pPg->nHeight==0 ){
      break;
    }else{
      hctDbIntkeyNode(pPg, &aDummy, &aChild);
      if( (pPg->nHeight % 2) ){
        iPg = aChild[0];
      }else{
        iPg = aChild[pPg->nEntry-1];
      }
    }
    sqlite3HctFilePageRelease(&pg);
  }

  if( rc==SQLITE_OK ){
    assert( pPg->ePagetype==HCT_PAGETYPE_INTKEY && pPg->nHeight==0 );
    assert( pPg->iPeerPg==0 );
    memcpy(&pCsr->pg, &pg, sizeof(pg));
    pCsr->iCell = pPg->nEntry-1;
    /* TODO - avoid landing on an invisible entry. Deal with this after
    ** parent lists and xPrev() are working properly. */
  }

  return rc;
}

int sqlite3HctDbCsrNext(HctDbCsr *pCsr){
  int rc = SQLITE_OK;

  do {
    HctDatabasePage *pPg = (HctDatabasePage*)pCsr->pg.aOld;
    assert( pCsr->iCell>=0 && pCsr->iCell<pPg->nEntry );
    assert( pPg->ePagetype==HCT_PAGETYPE_INTKEY && pPg->nHeight==0 );
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

int sqlite3HctDbCsrPrev(HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  do {
    pCsr->iCell--;
    if( pCsr->iCell<0 ){
      /* TODO: This can be made much faster by iterating through the 
      ** parent list (which is in reverse order) instead of seeking 
      ** for each new page.  */
      HctIntkeyTid *pKey = hctDbCsrKey(pCsr->pg.aOld, 0);
      i64 iKey = pKey->iKey;
      i64 iTid = (pKey->iTidFlags & HCT_TID_MASK);
      sqlite3HctFilePageRelease(&pCsr->pg);
      assert( iTid>0 );
      rc = hctDbCsrSeek(pCsr, 0, 0, iKey, iTid-1);
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
  assert( pPg->ePagetype==HCT_PAGETYPE_INTKEY && pPg->nHeight==0 );

  aOff = (u16*)&pCsr->pg.aOld[sizeof(HctDatabasePage) + 16*pPg->nEntry];
  iOff = (int)aOff[pCsr->iCell];
  iOff += getVarint32(&pCsr->pg.aOld[iOff], nData);
  if( paData ){
    *paData = &pCsr->pg.aOld[iOff];
  }

  *pnData = nData;
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
  HctDatabasePage *pPg
){
  static const char *azType[] = {
    0,                          /* 0x00 */
    "intkey",                   /* 0x01 */
    "index",                    /* 0x02 */
    "overflow",                 /* 0x03 */
  };
  int rc = SQLITE_OK;
  HctIntkeyTid *aKey = 0;

  sqlite3_free(pCur->zKeys);
  pCur->zKeys = 0;
  sqlite3_free(pCur->zData);
  pCur->zData = 0;

  pCur->zPgtype = azType[pPg->ePagetype];
  pCur->iPeerPg = pPg->iPeerPg;
  pCur->nEntry = pPg->nEntry;
  pCur->nHeight = pPg->nHeight;

  if( pPg->ePagetype==HCT_PAGETYPE_INTKEY ){
    if( pPg->nHeight==0 ){
      hctDbIntkeyLeaf(pPg, &aKey, 0);
    }else{
      u32 *aChild = 0;
      hctDbIntkeyNode(pPg, &aKey, &aChild);
      char *zData = 0;
      int i;
      for(i=0; rc==SQLITE_OK && i<pPg->nEntry; i++){
        zData = sqlite3_mprintf("%z%s%lld",
            zData, zData ? " " : "", (i64)aChild[i]
        );
        if( zData==0 ) rc = SQLITE_NOMEM_BKPT;
      }
      pCur->zData = zData;
    }
  }

  if( aKey ){
    char *zKeys = 0;
    int i;
    for(i=0; rc==SQLITE_OK && i<pPg->nEntry; i++){
      zKeys = sqlite3_mprintf("%z%s{%lld %lld%s}",
          zKeys, zKeys ? " " : "", aKey[i].iKey, 
          (aKey[i].iTidFlags & HCT_TID_MASK),
          (aKey[i].iTidFlags & HCT_IS_DELETED) ? "*" : ""
      );
      if( zKeys==0 ) rc = SQLITE_NOMEM_BKPT;
    }
    pCur->zKeys = zKeys;
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
    rc = hctdbLoadPage(pCur, (HctDatabasePage*)pg.aOld);
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
