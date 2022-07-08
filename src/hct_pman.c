/*
** 2022 April 10
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

typedef struct HctPManPageset HctPManPageset;
typedef struct HctPManTree HctPManTree;

#define PAGESET_INIT_SIZE 1000


/*
** A basket of free page ids - a pageset - is represented by an instance 
** of the following type.
**
** nAlloc:
**   Allocated size of aPg[] array, in entries (not bytes).
**
** nPg:
**   Number of valid entries in aPg[].
**
** aPg:
**   Array of free logical or physical page ids.
**
** iMaxTid:
**   When a page is freed, it is associated with a TID. Such that the page
**   may be reused once it is guaranteed that all current and future readers
**   include in their snapshots all transactions with TID values less than
**   the associated TID. The maximum of all these values for pages in the
**   page set is stored in this variable.
**
** pNext:
**   Used to link the HctPManServer.apList[] lists together.
*/
struct HctPManPageset {
  i64 iMaxTid;                    /* Max associated TID of aPg[] entries */
  int nAlloc;                     /* Allocated size of aPg[] array */
  int nPg;                        /* Number of valid entries in aPg[] */
  u32 *aPg;                       /* Array of page numbers */
  HctPManPageset *pNext;          /* Next in list */
};

/*
** A tree of free logical and physical pages.
*/
struct HctPManTree {
  u32 iRoot;                      /* Logical root of free tree */
  i64 iTid;                       /* Associated TID value */
};

/*
** Indexes into HctPManServer.apList[], HctPManClient.apAcc[] and
** HctPManClient.apUse[] arrays. 
*/
#define PAGESET_PHYSICAL 0
#define PAGESET_LOGICAL  1

/*
** aList[]:
**   aList[0].pHead is a pointer to the first element of a singly-linked
**   list of pagesets containing free physical page ids. aList[0].pTail
**   always points to the last element of this list.
**
**   aList[1] is similar, but for logical page ids.
**
** aTree[]:
**   Array of tree structures to eventually walk and free
*/
struct HctPManServer {
  sqlite3_mutex *pMutex;          /* Mutex to protect this object */
  HctFileServer *pFileServer;     /* Associated file-server object */
  struct HctPManServerList {
    HctPManPageset *pHead;
    HctPManPageset *pTail;
  } aList[2];

  int nTree;
  HctPManTree *aTree;
};

/*
** apAcc[]:
**   These two pagesets are used to accumulate physical (apAcc[0]) and
**   logical (apAcc[1]) page ids as they are freed by the client. Once
**   sufficient page ids have been accumulated the pageset will be handed
**   to the server object.
**
** apUse[]:
**   These two pagesets are guaranteed to contain page ids that can be
**   reused immediately. For the client to use as it requires.
*/
struct HctPManClient {
  HctConfig *pConfig;
  HctPManServer *pServer;
  HctFile *pFile;
  HctPManPageset *apAcc[2];       /* Accumulating physical, logical sets */
  HctPManPageset *apUse[2];       /* Physical, logical sets for using */
};

/*
** Utility malloc function for hct.
*/
void *sqlite3HctMalloc(int *pRc, i64 nByte){
  void *pRet = 0;
  assert( nByte!=0 );
  if( *pRc==SQLITE_OK ){
    pRet = sqlite3MallocZero(nByte);
    if( pRet==0 ){
      *pRc = SQLITE_NOMEM_BKPT;
    }
  }
  return pRet;
}


/*
** Allocate and return a new HctPManServer object.
*/
HctPManServer *sqlite3HctPManServerNew(
  int *pRc, 
  HctFileServer *pFileServer
){
  int rc = *pRc;
  HctPManServer *pRet = 0;
  pRet = sqlite3HctMalloc(&rc, sizeof(*pRet));
  if( pRet ){
    pRet->pFileServer = pFileServer;
    pRet->pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);
    if( pRet->pMutex==0 ){
      rc = SQLITE_NOMEM_BKPT;
    }
  }

  if( rc!=SQLITE_OK ){
    sqlite3HctPManServerFree(pRet);
    pRet = 0;
  }
  *pRc = rc;
  return pRet;
}


/*
** Free an HctPManServer object allocated by an earlier call to
** sqlite3HctPManServerNew().
*/
void sqlite3HctPManServerFree(HctPManServer *pServer){
  if( pServer ){
    int ii = 0;
    for(ii=0; ii<2; ii++){
      HctPManPageset *pNext = pServer->aList[ii].pHead;
      while( pNext ){
        HctPManPageset *pDel = pNext;
        pNext = pNext->pNext;
        sqlite3_free(pDel);
      }
    }
    sqlite3_mutex_free(pServer->pMutex);
    sqlite3_free(pServer);
  }
}

/*
** Allocate and return a pointer to a new pageset object with enough
** space for up to nAlloc page ids.
*/
static HctPManPageset *hctPManPagesetNew(int *pRc, int nAlloc){
  const int nByte = sizeof(HctPManPageset) + nAlloc*sizeof(u32);
  HctPManPageset *pRet = 0;

  pRet = (HctPManPageset*)sqlite3HctMalloc(pRc, nByte);
  if( pRet ){
    pRet->aPg = (u32*)&pRet[1];
    pRet->nAlloc = nAlloc;
  }

  return pRet;
}

/*
** Add page iPg directly to the list of free pages managed by server pServer.
** iPg may be either a logical (if bLogical==1) or a physical (if bLogical==0) 
** page id. It is available for reuse immediately.
**
** This function is not threadsafe. It is only called during initialization,
** when there is only one thread that may be accessing object pServer.
*/
void sqlite3HctPManServerInit(
  int *pRc, 
  HctPManServer *pServer, 
  u32 iPg, 
  int bLogical
){
  struct HctPManServerList *p = &pServer->aList[bLogical];
  assert( bLogical==0 || bLogical==1 );
  if( p->pHead==0 || p->pHead->nPg==p->pHead->nAlloc ){
    HctPManPageset *pNew = hctPManPagesetNew(pRc, PAGESET_INIT_SIZE);
    if( pNew==0 ) return;
    pNew->pNext = p->pHead;
    p->pHead = pNew;
    if( p->pTail==0 ) p->pTail = pNew;
  }
  p->pHead->aPg[p->pHead->nPg++] = iPg;
}

/*
** Allocate a new page-manager client.
*/
HctPManClient *sqlite3HctPManClientNew(
  int *pRc,                       /* IN/OUT: Error code */
  HctConfig *pConfig,             /* Connection configuration object */
  HctPManServer *pServer,         /* Page-manager server to connect to */
  HctFile *pFile                  /* File object */
){
  HctPManClient *pClient = 0;
  pClient = (HctPManClient*)sqlite3HctMalloc(pRc, sizeof(HctPManClient));
  if( pClient ){
    pClient->pConfig = pConfig;
    pClient->pServer = pServer;
    pClient->pFile = pFile;
  }
  return pClient;
}

/*
** Hand off a page-set object to the server passed as the first argument.
*/
static void hctPManServerHandoff(
  HctPManServer *p,               /* Server object */
  HctPManPageset *pPageSet,       /* Pageset to pass to the server */
  int bLogical,                   /* True for logical, false for physical ids */
  int bUsable                     /* Page ids are immediately usable */
){
  if( pPageSet ){
    struct HctPManServerList *pList = &p->aList[bLogical];
    if( bUsable ){
      pPageSet->pNext = pList->pHead;
      pList->pHead = pPageSet;
      if( pList->pTail==0 ) pList->pTail = pPageSet;
    }else{
      pPageSet->pNext = 0;
      if( pList->pTail==0 ){
        pList->pTail = pList->pHead = pPageSet;
      }else{
        pList->pTail->pNext = pPageSet;
        pList->pTail = pPageSet;
      }
    }
  }
}

/*
** Free a page-manager client.
*/
void sqlite3HctPManClientFree(HctPManClient *pClient){
  if( pClient ){
    HctPManServer *p = pClient->pServer;

    /* Return all 4 pagesets to the server object under protection of the
    ** server mutex. The two apUse[] pagesets go to the front of the lists - 
    ** so that they can be reused immediately. The two apAcc[] pagesets go on
    ** the end of the lists. */
    sqlite3_mutex_enter(p->pMutex);
    hctPManServerHandoff(p, pClient->apUse[0], 0, 1);
    hctPManServerHandoff(p, pClient->apUse[1], 1, 1);
    hctPManServerHandoff(p, pClient->apAcc[0], 0, 0);
    hctPManServerHandoff(p, pClient->apAcc[1], 1, 0);
    sqlite3_mutex_leave(p->pMutex);

    sqlite3_free(pClient);
  }
}


typedef struct FreeTreeCtx FreeTreeCtx; 
struct FreeTreeCtx {
  HctFile *pFile;
  HctPManClient *pPManClient;
};

static int pmanFreeTreeCb(void *pCtx, u32 iLogic, u32 iPhys){
  FreeTreeCtx *p = (FreeTreeCtx*)pCtx;
  int rc = SQLITE_OK;

  if( iLogic && !sqlite3HctFilePageIsFree(p->pFile, iLogic, 1) ){
    rc = sqlite3HctFilePageClearInUse(p->pFile, iLogic, 1);
    sqlite3HctPManFreePg(&rc, p->pPManClient, 0, iLogic, 1);
  }
  if( iPhys && !sqlite3HctFilePageIsFree(p->pFile, iPhys, 0) && rc==SQLITE_OK ){
    rc = sqlite3HctFilePageClearInUse(p->pFile, iPhys, 0);
    sqlite3HctPManFreePg(&rc, p->pPManClient, 0, iPhys, 0);
  }

  return rc;
}

static int hctPManFreeTreeNow(
  HctPManClient *p, 
  HctFile *pFile, 
  u32 iRoot
){
  int rc = SQLITE_OK;
  FreeTreeCtx ctx;
  ctx.pPManClient = p;
  ctx.pFile = pFile;
  rc = sqlite3HctDbWalkTree(pFile, iRoot, pmanFreeTreeCb, (void*)&ctx);
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFilePageClearIsRoot(pFile, iRoot);
  }
  return rc;
}

#if 0
static void pman_debug(
  HctPManClient *pClient, 
  const char *zOp, 
  int bLogical, 
  u32 iPg, 
  i64 iTid
){
  printf("pman: (%p) %s %s page %d - tid=%lld\n", pClient,
    zOp, bLogical ? "LOGICAL" : "PHYSICAL", (int)iPg, iTid
  );
}

static void pman_debug_new_pageset(
  HctPManPageset *pPageSet,
  int bLogical,
  u64 iSafeTid,
  u64 iServerTid
){
  printf(
      "pman: new %s pageset - safetid=%lld servertid=%lld\n",
      bLogical ? "LOGICAL" : "PHYSICAL", iSafeTid, iServerTid
  );
}
#else

# define pman_debug(a,b,c,d,e)
# define pman_debug_new_pageset(a,b,c,d)

#endif

/*
** Allocate a new logical or physical page.
*/
u32 sqlite3HctPManAllocPg(
  int *pRc,                       /* IN/OUT: Error code */
  HctPManClient *pClient,         /* page-manager client handle */
  HctFile *pFile,
  int bLogical
){
  HctPManPageset *pUse = pClient->apUse[bLogical];
  u32 iRet = 0;

  if( pUse==0 ){
    const int nPageSet = pClient->pConfig->nPageSet;
    HctPManServer *p = pClient->pServer;
    struct HctPManServerList *pList = &p->aList[bLogical];
    u64 iSafeTid = sqlite3HctFileSafeTID(pClient->pFile);
    u64 iServerTid = 0;
    u32 iRoot = 0;

    /* First try to obtain a new pageset object from the server */
    sqlite3_mutex_enter(p->pMutex);
    if( p->nTree>0 && p->aTree[0].iTid<=iSafeTid ){
      iRoot = p->aTree[0].iRoot;
      p->nTree--;
      memmove(&p->aTree[0], &p->aTree[1], (p->nTree)*sizeof(HctPManTree));
    }else if( pList->pHead ){
      iServerTid = pList->pHead->iMaxTid;
      if( iServerTid<=iSafeTid ){
        pUse = pList->pHead;
        pList->pHead = pUse->pNext;
        if( pList->pHead==0 ) pList->pTail = 0;
      }
    }
    sqlite3_mutex_leave(p->pMutex);

    if( iRoot && *pRc==SQLITE_OK ){
      *pRc = hctPManFreeTreeNow(pClient, pFile, iRoot);
      pUse = pClient->apUse[bLogical];
    }

    /* If there is still no new pageset, allocate a new one at the end 
    ** of the current domain. */
    if( pUse==0 ){
      pUse = hctPManPagesetNew(pRc, nPageSet); 
      if( pUse ){
        HctFile *pFile = pClient->pFile;
        int ii;
        u32 iPg = sqlite3HctFilePageRangeAlloc(pFile, bLogical, nPageSet);
        for(ii=0; ii<nPageSet; ii++){
          pUse->aPg[ii] = iPg + (nPageSet - 1 - ii);
        }
        pUse->nPg = nPageSet;
      }
    }
    pman_debug_new_pageset(pUse, bLogical, iSafeTid, iServerTid);

    pClient->apUse[bLogical] = pUse;
  }

  assert( pUse || *pRc!=SQLITE_OK );
  if( pUse ){
    assert( pUse->nPg>0 );
    iRet = pUse->aPg[pUse->nPg-1];
    pman_debug(pClient, "using", bLogical, iRet, pUse->iMaxTid);
    pUse->nPg--;
    if( pUse->nPg==0 ){
      sqlite3_free(pUse);
      pClient->apUse[bLogical] = 0;
    }
  }

  return iRet;
}

/*
** Free a physical or logical page.
*/
void sqlite3HctPManFreePg(
  int *pRc,                       /* IN/OUT: Error code */
  HctPManClient *pClient,         /* page-manager client handle */
  i64 iTid,                       /* Associated TID value */
  u32 iPg,                        /* Page number */
  int bLogical                    /* True for logical, false for physical */
){
  const int nPageSet = pClient->pConfig->nPageSet;
  HctPManPageset *pAcc = pClient->apAcc[bLogical];
  HctPManPageset *pUse = pClient->apUse[bLogical];

  pman_debug(pClient, "freeing", bLogical, iPg, iTid);

  if( iTid==0 && pUse && pUse->nPg<pUse->nAlloc ){
    pUse->aPg[pUse->nPg++] = iPg;
    return;
  }

  if( pAcc && (pAcc->nPg==pAcc->nAlloc || pAcc->nPg>=nPageSet) ){
    HctPManServer *p = pClient->pServer;

    /* Hand pAcc off to the server. */
    sqlite3_mutex_enter(p->pMutex);
    hctPManServerHandoff(p, pAcc, bLogical, 0);
    sqlite3_mutex_leave(p->pMutex);

    pAcc = 0;
  }

  if( pAcc==0 ){
    pAcc = hctPManPagesetNew(pRc, nPageSet);
    pClient->apAcc[bLogical] = pAcc;
  }

  if( pAcc ){
    assert( pAcc->nPg<pAcc->nAlloc );
    pAcc->aPg[pAcc->nPg++] = iPg;
    if( iTid>pAcc->iMaxTid ) pAcc->iMaxTid = iTid;
  }
}

int sqlite3HctPManFreeTree(
  HctPManClient *p, 
  HctFile *pFile, 
  u32 iRoot, 
  u64 iTid
){
  int rc = SQLITE_OK;
  if( iTid==0 ){
    rc = hctPManFreeTreeNow(p, pFile, iRoot);
  }else{
    HctPManServer *pServer = p->pServer;
    int nNew;
    HctPManTree *aNew;

    sqlite3_mutex_enter(pServer->pMutex);

    nNew = pServer->nTree + 1;
    aNew = (HctPManTree*)sqlite3_realloc(
        pServer->aTree, nNew*sizeof(HctPManTree)
    );
    if( aNew==0 ){
      rc = SQLITE_NOMEM_BKPT;
    }else{
      aNew[pServer->nTree].iRoot = iRoot;
      aNew[pServer->nTree].iTid = iTid;
      pServer->nTree++;
      pServer->aTree = aNew;
    }

    sqlite3_mutex_leave(pServer->pMutex);
  }
  return rc;
}

/*************************************************************************
** Beginning of vtab implemetation.
*************************************************************************/

#define HCT_PMAN_SCHEMA         \
"  CREATE TABLE hctpman("       \
"    type TEXT,"                \
"    location TEXT,"            \
"    pgno INTEGER,"             \
"    tid INTEGER"               \
"  );"

typedef struct pman_vtab pman_vtab;
typedef struct pman_cursor pman_cursor;
typedef struct HctPmanRow HctPmanRow;

/* 
** Virtual table type for "hctpman".
*/
struct pman_vtab {
  sqlite3_vtab base;              /* Base class - must be first */
  sqlite3 *db;
};

/* 
** Virtual cursor type for "hctpman".
*/
struct pman_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  int nRow;
  int iRow;
  HctPmanRow *aRow;
};

/*
** Values to return for a single row of the hctpman table.
*/
struct HctPmanRow {
  u8 eType;                       /* HCT_PMAN_TYPE_* value */
  u8 eLoc;                        /* HCT_PMAN_LOC_* value */
  u32 pgno;                       /* Page number */
  i64 iTid;                       /* Associated TID */
};

#define HCT_PMAN_TYPE_PHYSICAL 0
#define HCT_PMAN_TYPE_LOGICAL  1

#define HCT_PMAN_LOC_USE       0
#define HCT_PMAN_LOC_ACC       1
#define HCT_PMAN_LOC_SERVER    2

/*
** This xConnect() method is invoked to create a new hctpman virtual table.
*/
static int pmanConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  pman_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db, HCT_PMAN_SCHEMA);
  pNew = (pman_vtab*)sqlite3HctMalloc(&rc, sizeof(*pNew));
  if( pNew ){
    pNew->db = db;
  }

  *ppVtab = (sqlite3_vtab*)pNew;
  return rc;
}

/*
** This method is the destructor for pman_vtab objects.
*/
static int pmanDisconnect(sqlite3_vtab *pVtab){
  pman_vtab *p = (pman_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new pman_cursor object.
*/
static int pmanOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  pman_cursor *pCur;
  pCur = sqlite3MallocZero(sizeof(*pCur));
  if( pCur==0 ) return SQLITE_NOMEM;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a pman_cursor.
*/
static int pmanClose(sqlite3_vtab_cursor *cur){
  pman_cursor *pCur = (pman_cursor*)cur;
  sqlite3_free(pCur->aRow);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int pmanEof(sqlite3_vtab_cursor *cur){
  pman_cursor *pCur = (pman_cursor*)cur;
  return pCur->iRow>=pCur->nRow;
}

/*
** Advance a pman_cursor to its next row of output.
*/
static int pmanNext(sqlite3_vtab_cursor *cur){
  pman_cursor *pCur = (pman_cursor*)cur;
  pCur->iRow++;
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the pgmap_cursor
** is currently pointing.
*/
static int pmanColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  const char *aType[] = {"physical", "logical"};
  const char *aLoc[] = {"use", "acc", "server"};
  pman_cursor *pCur = (pman_cursor*)cur;

  HctPmanRow *pRow = &pCur->aRow[pCur->iRow];
  switch( i ){
    case 0: {  /* type */
      sqlite3_result_text(ctx, aType[pRow->eType], -1, SQLITE_STATIC);
      break;
    }
    case 1: {  /* location */
      sqlite3_result_text(ctx, aLoc[pRow->eLoc], -1, SQLITE_STATIC);
      break;
    }
    case 2: {  /* pgno */
      sqlite3_result_int64(ctx, pRow->pgno);
      break;
    }
    case 3: {  /* tid */
      sqlite3_result_int64(ctx, pRow->iTid);
      break;
    }
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the slotno value.
*/
static int pmanRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  pman_cursor *pCur = (pman_cursor*)cur;
  *pRowid = pCur->iRow+1;
  return SQLITE_OK;
}

static int hctPagesetSize(HctPManPageset *pPageset){
  return pPageset ? pPageset->nPg : 0;
}

static void hctPagesetRows(
  pman_cursor *pCur, 
  HctPManPageset *pPageset, 
  u8 eType,
  u8 eLoc
){
  if( pPageset ){
    int ii;
    for(ii=0; ii<pPageset->nPg; ii++){
      HctPmanRow *pRow = &pCur->aRow[pCur->nRow++];
      pRow->eType = eType;
      pRow->eLoc = eLoc;
      pRow->pgno = pPageset->aPg[ii];
      pRow->iTid = pPageset->iMaxTid;
    }
  }
}

/*
** This method is called to "rewind" the pman_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to pmanColumn() or pmanRowid() or 
** pmanEof().
*/
static int pmanFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  pman_cursor *pCur = (pman_cursor*)pVtabCursor;
  pman_vtab *pTab = (pman_vtab*)(pCur->base.pVtab);
  HctPManClient *pClient = 0;
  int nRow = 0;
  int ii = 0;
  HctPManPageset *pSet = 0;
  int rc = SQLITE_OK;

  pCur->iRow = 0;
  pCur->nRow = 0;
  sqlite3_free(pCur->aRow);
  pCur->aRow = 0;
 
  pClient = sqlite3HctFilePManClient(
      sqlite3HctDbFile(sqlite3HctDbFind(pTab->db, 0))
  );

  sqlite3_mutex_enter(pClient->pServer->pMutex);
  for(ii=0; ii<2; ii++){
    nRow += hctPagesetSize(pClient->apAcc[ii]);
    nRow += hctPagesetSize(pClient->apUse[ii]);
    for(pSet=pClient->pServer->aList[ii].pHead; pSet; pSet=pSet->pNext){
      nRow += hctPagesetSize(pSet);
    }
  }
  pCur->aRow = sqlite3HctMalloc(&rc, sizeof(HctPmanRow) * nRow);
  if( pCur->aRow ){
    for(ii=0; ii<2; ii++){
      hctPagesetRows(pCur, pClient->apAcc[ii], ii, HCT_PMAN_LOC_ACC);
      hctPagesetRows(pCur, pClient->apUse[ii], ii, HCT_PMAN_LOC_USE);
      for(pSet=pClient->pServer->aList[ii].pHead; pSet; pSet=pSet->pNext){
        hctPagesetRows(pCur, pSet, ii, HCT_PMAN_LOC_SERVER);
      }
    }
  }
  sqlite3_mutex_leave(pClient->pServer->pMutex);

  return rc;
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int pmanBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  pIdxInfo->estimatedCost = (double)10;
  pIdxInfo->estimatedRows = 10;
  return SQLITE_OK;
}

int sqlite3HctPManVtabInit(sqlite3 *db){
  static sqlite3_module pmanModule = {
    /* iVersion    */ 0,
    /* xCreate     */ 0,
    /* xConnect    */ pmanConnect,
    /* xBestIndex  */ pmanBestIndex,
    /* xDisconnect */ pmanDisconnect,
    /* xDestroy    */ 0,
    /* xOpen       */ pmanOpen,
    /* xClose      */ pmanClose,
    /* xFilter     */ pmanFilter,
    /* xNext       */ pmanNext,
    /* xEof        */ pmanEof,
    /* xColumn     */ pmanColumn,
    /* xRowid      */ pmanRowid,
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

  return sqlite3_create_module(db, "hctpman", &pmanModule, 0);
}



