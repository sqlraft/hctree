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

typedef struct HctPManFreePg HctPManFreePg;
typedef struct HctPManFreePgSet HctPManFreePgSet;

struct HctPManFreePg {
  i64 pgno;                       /* The free page number */
  i64 iTid;                       /* TID of transaction that freed page */
};

struct HctPManFreePgSet {
  HctPManFreePg *aPg;             /* Page buffer */
  int nAlloc;                     /* Allocated size of aPg[] */
  int iFirst;                     /* Index of first entry in aPg[] */
  int nPg;                        /* Number of valid pages in aPg[] */ 
};




/****************************************************************/

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
**   always points to the last element of this list. The list is sorted
**   in order of HctPManPageset.iMaxTid values.
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
** Event counters used by the hctstats virtual table.
*/
typedef struct HctPManStats HctPManStats;
struct HctPManStats {
  i64 nMutex;
  i64 nMutexBlock;
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

  HctPManFreePgSet aPgSet[2];     /* Free physical and logical pages */

  HctPManStats stats;
};

static void hctPManMutexEnter(HctPManClient *pClient){
  sqlite3_mutex *pMutex = pClient->pServer->pMutex;
  pClient->stats.nMutex++;
  if( sqlite3_mutex_try(pMutex)!=SQLITE_OK ){
    pClient->stats.nMutexBlock++;
    sqlite3_mutex_enter(pMutex);
  }
}


#define ENTER_PMAN_MUTEX(pClient) hctPManMutexEnter(pClient)
#define LEAVE_PMAN_MUTEX(pClient) sqlite3_mutex_leave(pClient->pServer->pMutex)

/*
** Utility malloc function for hct. Allocate nByte bytes of zeroed memory.
*/
void *sqlite3HctMallocRc(int *pRc, i64 nByte){
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
** Equivalent to sqlite3MallocZero(nByte). Except that if the call
** to sqlite3MallocZero() fails, the process aborts.
*/
void *sqlite3HctMalloc(i64 nByte){
  void *pRet = sqlite3MallocZero(nByte);
  assert( nByte>0 );
  if( pRet==0 ){
    sqlite3_log(SQLITE_ABORT, 
        "FATAL: attempt to sqlite3MallocZero() %d bytes failed, exiting...",
        (int)nByte
    );
    abort();
  }
  return pRet;
}

/*
** Equivalent to sqlite3_realloc64(pIn, nByte). Except that if the call
** to sqlite3_realloc64() fails, the process aborts.
*/
void *sqlite3HctRealloc(void *pIn, i64 nByte){
  void *pRet = sqlite3_realloc(pIn, nByte);
  assert( nByte>0 );
  if( pRet==0 ){
    sqlite3_log(SQLITE_ABORT, 
        "FATAL: attempt to sqlite3_realloc() %d bytes failed, exiting...",
        (int)nByte
    );
    abort();
  }
  return pRet;
}

/*
** Wrapper around sqlite3_mprintf(). Except that this version abort()s if
** a memory allocation fails.
*/
char *sqlite3HctMprintf(char *zFmt, ...){
  char *zRet = 0;
  va_list ap;

  va_start(ap, zFmt);
  zRet = sqlite3_vmprintf(zFmt, ap);
  va_end(ap);

  if( !zRet ){
    sqlite3_log(SQLITE_ABORT,
        "FATAL: attempt to sqlite3_vmprintf() failed, exiting..."
    );
    abort();
  }
  return zRet;
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
  pRet = sqlite3HctMallocRc(&rc, sizeof(*pRet));
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


void sqlite3HctPManServerReset(HctPManServer *pServer){
  int ii = 0;
  for(ii=0; ii<2; ii++){
    HctPManPageset *pNext = pServer->aList[ii].pHead;
    while( pNext ){
      HctPManPageset *pDel = pNext;
      pNext = pNext->pNext;
      sqlite3_free(pDel);
    }
    memset(&pServer->aList[ii], 0, sizeof(struct HctPManServerList));
  }
}

/*
** Free an HctPManServer object allocated by an earlier call to
** sqlite3HctPManServerNew().
*/
void sqlite3HctPManServerFree(HctPManServer *pServer){
  if( pServer ){
    sqlite3HctPManServerReset(pServer);
    sqlite3_mutex_free(pServer->pMutex);
    sqlite3_free(pServer->aTree);
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

  pRet = (HctPManPageset*)sqlite3HctMallocRc(pRc, nByte);
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
  u64 iTid,
  u32 iPg, 
  int bLogical
){
  struct HctPManServerList *p = &pServer->aList[bLogical];
  assert( bLogical==0 || bLogical==1 );

  if( p->pHead==0 || p->pHead->nPg==p->pHead->nAlloc ){
    HctPManPageset *pNew = hctPManPagesetNew(pRc, PAGESET_INIT_SIZE);
    if( pNew==0 ) return;
    pNew->pNext = p->pHead;
    pNew->iMaxTid = iTid;
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
  pClient = (HctPManClient*)sqlite3HctMallocRc(pRc, sizeof(HctPManClient));
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
**
*/
static int hctPManHandback(
  HctPManClient *pClient,         /* Client to hand pages back from */
  int bLogical,                   /* True for logical pages, false for phys. */
  int nPg                         /* Number of pages to hand back */
){
  u64 iSafeTid = sqlite3HctFileSafeTID(pClient->pFile);
  const int nPageSet = pClient->pConfig->nPageSet;
  HctPManFreePgSet *pSet = &pClient->aPgSet[bLogical];
  int nRem = nPg;
  int rc = SQLITE_OK;

  HctPManPageset *pList = 0;

  assert( bLogical==0 || bLogical==1 );
  assert( nPg<=pSet->nPg );

  while( nRem>0 ){
    int ii = 0;
    HctPManPageset *pNew = 0;
    int nCopy = MIN(nRem, nPageSet);

    nRem -= nCopy;
    pNew = hctPManPagesetNew(&rc, nCopy);
    if( !pNew ) break;
    for(ii=0; ii<nCopy; ii++){
      int iPg = (pSet->iFirst + ii) % pSet->nAlloc;
      pNew->aPg[pNew->nPg++] = (u32)(pSet->aPg[iPg].pgno);
      pNew->iMaxTid = pSet->aPg[iPg].iTid;
    }
    pSet->iFirst = (pSet->iFirst+nCopy) % pSet->nAlloc;
    pSet->nPg -= nCopy;

    pNew->pNext = pList;
    pList = pNew;
  }
  assert( pList || nPg==0 || rc!=SQLITE_OK );

  ENTER_PMAN_MUTEX(pClient);
  while( pList ){
    int bSafe = (pList->iMaxTid<=iSafeTid);
    HctPManPageset *pNext = pList->pNext;
    pList->pNext = 0;
    hctPManServerHandoff(pClient->pServer, pList, bLogical, bSafe);
    pList = pNext;
  }
  LEAVE_PMAN_MUTEX(pClient);

  return rc;
}

/*
** Free a page-manager client.
*/
void sqlite3HctPManClientFree(HctPManClient *pClient){
  if( pClient ){
    /* Return all pages to the server object */
    hctPManHandback(pClient, 0, pClient->aPgSet[0].nPg);
    hctPManHandback(pClient, 1, pClient->aPgSet[1].nPg);

    /* Free allocations */
    sqlite3_free(pClient->aPgSet[0].aPg);
    sqlite3_free(pClient->aPgSet[1].aPg);
    sqlite3_free(pClient);
  }
}


typedef struct FreeTreeCtx FreeTreeCtx; 
struct FreeTreeCtx {
  HctFile *pFile;
  HctPManClient *pPManClient;
  u32 iRoot;                      /* If not zero, preserve this page */
};

static int pmanFreeTreeCb(void *pCtx, u32 iLogic, u32 iPhys){
  FreeTreeCtx *p = (FreeTreeCtx*)pCtx;
  int rc = SQLITE_OK;

  if( iLogic!=p->iRoot ){
    if( iLogic && !sqlite3HctFilePageIsFree(p->pFile, iLogic, 1) ){
      rc = sqlite3HctFilePageClearInUse(p->pFile, iLogic, 1);
      sqlite3HctPManFreePg(&rc, p->pPManClient, 0, iLogic, 1);
    }
    if( iPhys && !sqlite3HctFilePageIsFree(p->pFile,iPhys,0) && rc==SQLITE_OK ){
      rc = sqlite3HctFilePageClearInUse(p->pFile, iPhys, 0);
      sqlite3HctPManFreePg(&rc, p->pPManClient, 0, iPhys, 0);
    }
  }

  return rc;
}

static int hctPManFreeTreeNow(
  HctPManClient *p, 
  HctFile *pFile, 
  u32 iRoot,
  int bSaveRoot
){
  int rc = SQLITE_OK;
  FreeTreeCtx ctx;
  ctx.pPManClient = p;
  ctx.pFile = pFile;
  ctx.iRoot = bSaveRoot ? iRoot : 0;
  rc = sqlite3HctDbWalkTree(pFile, iRoot, pmanFreeTreeCb, (void*)&ctx);
  if( rc==SQLITE_OK && !bSaveRoot ){
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
  fflush(stdout);
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
  fflush(stdout);
}
#else

# define pman_debug(a,b,c,d,e)
# define pman_debug_new_pageset(a,b,c,d)

#endif

/* 
** Ensure that the circular buffer identified by bLogical has at least
** nPg free slots in it.  
*/
static int hctPManMakeSpace(
  HctPManClient *pClient,
  int bLogical,
  int nPg
){
  int rc = SQLITE_OK;
  HctPManFreePgSet *pSet = &pClient->aPgSet[bLogical];

  if( (pSet->nAlloc-pSet->nPg)<nPg ){
    int nNew = pSet->nPg + nPg;
    int nByte = nNew * sizeof(HctPManFreePg);
    HctPManFreePg *aNew = (HctPManFreePg*)sqlite3_realloc(pSet->aPg, nByte);

    if( aNew==0 ){
      rc = SQLITE_NOMEM;
    }else{
      pSet->aPg = aNew;
      if( (pSet->iFirst + pSet->nPg)>pSet->nAlloc ){
        int nExtra = nNew - pSet->nAlloc;
        int nStart = pSet->nPg - (pSet->nAlloc - pSet->iFirst);

        if( nExtra>=nStart ){
          memcpy(&aNew[pSet->nAlloc], aNew, nStart*sizeof(HctPManFreePg));
        }else{
          memcpy(&aNew[pSet->nAlloc], aNew, nExtra*sizeof(HctPManFreePg));
          memmove(aNew, &aNew[nExtra], (nStart-nExtra)*sizeof(HctPManFreePg));
        }
      }
      pSet->nAlloc = nNew;
    }
  }

  return rc;
}

static void hctPManAddFree(
  HctPManClient *pClient,
  int bLogical,
  i64 iPg,
  i64 iTid
){
  HctPManFreePgSet *pSet = &pClient->aPgSet[bLogical];
  int iIdx = 0;

  assert( pSet->nPg<pSet->nAlloc );
  if( iTid==0 ){
    if( pSet->iFirst==0 ) pSet->iFirst = pSet->nAlloc;
    pSet->iFirst--;
    iIdx = pSet->iFirst;
  }else{
    iIdx = (pSet->iFirst + pSet->nPg) % pSet->nAlloc;
  }

  pSet->nPg++;
  pSet->aPg[iIdx].pgno = iPg;
  pSet->aPg[iIdx].iTid = iTid;
}


/*
** Allocate a new logical or physical page.
*/
u32 sqlite3HctPManAllocPg(
  int *pRc,                       /* IN/OUT: Error code */
  HctPManClient *pClient,         /* page-manager client handle */
  HctFile *pFile,
  int bLogical
){
  HctPManServer *p = pClient->pServer;
  u64 iSafeTid = sqlite3HctFileSafeTID(pFile);
  HctPManFreePgSet *pSet = &pClient->aPgSet[bLogical];
  u32 iRoot = 0;
  HctPManPageset *pPgset = 0;
  int rc = SQLITE_OK;

  /* Check if the client has a usable page already. If so, return early. */
  if( pSet->nPg>0 && pSet->aPg[pSet->iFirst].iTid<=iSafeTid ){
    u32 pgno = pSet->aPg[pSet->iFirst].pgno;

    pman_debug(pClient, "alloc", bLogical, pgno, pSet->aPg[pSet->iFirst].iTid);

    pSet->iFirst = (pSet->iFirst+1) % pSet->nAlloc;
    pSet->nPg--;
    return pgno;
  }

  do{
    iRoot = 0;

    /* Attempt to allocate a page from the page-manager server. */
    ENTER_PMAN_MUTEX(pClient);
    if( p->nTree>0 && p->aTree[0].iTid<=iSafeTid ){
      /* A tree structure that can be traversed to find free pages. */
      iRoot = p->aTree[0].iRoot;
      p->nTree--;
      memmove(&p->aTree[0], &p->aTree[1], (p->nTree)*sizeof(HctPManTree));
    }else{
      struct HctPManServerList *pList = &p->aList[bLogical];
      if( pList->pHead && pList->pHead->iMaxTid<=iSafeTid ){
        /* A page-set object full of usable pages */
        pPgset = pList->pHead;
        pList->pHead = pList->pHead->pNext;
        if( pList->pHead==0 ) pList->pTail = 0;
      }
    }
    LEAVE_PMAN_MUTEX(pClient);

    /* If a free tree structure was found, iterate through it, returning
    ** all physical and logical pages to the server. Then retry the above.
    */
    if( iRoot ){
      rc = hctPManFreeTreeNow(pClient, pFile, iRoot, 0);
    }
  }while( iRoot );
  
  if( rc==SQLITE_OK ){
    int ii;
    if( pPgset ){
      pman_debug_new_pageset(pPgset, bLogical, iSafeTid, pPgset->iMaxTid);
      rc = hctPManMakeSpace(pClient, bLogical, pPgset->nPg);
      if( rc==SQLITE_OK ){
        for(ii=pPgset->nPg-1; ii>=0; ii--){
          hctPManAddFree(pClient, bLogical, pPgset->aPg[ii], 0);
        }
      }
    }else{
      const int nPageSet = pClient->pConfig->nPageSet;
      rc = hctPManMakeSpace(pClient, bLogical, nPageSet);
      if( rc==SQLITE_OK ){
        u32 iPg = sqlite3HctFilePageRangeAlloc(pFile, bLogical, nPageSet);
        pman_debug_new_pageset(0, bLogical, iSafeTid, -1);
        for(ii=nPageSet-1; ii>=0; ii--){
          hctPManAddFree(pClient, bLogical, iPg+ii, 0);
        }
      }
    }
  }
  sqlite3_free(pPgset);

  if( rc==SQLITE_OK ){
    assert( pSet->nPg>0 && pSet->aPg[pSet->iFirst].iTid<=iSafeTid );
    return sqlite3HctPManAllocPg(pRc, pClient, pFile, bLogical);
  }

  /* An error has occurred. Return 0. */
  *pRc = rc;
  return 0;
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
  int rc = SQLITE_OK;
  pman_debug(pClient, "free", bLogical, iPg, iTid);
  assert( iPg>0 );
  rc = hctPManMakeSpace(pClient, bLogical, 1);
  if( rc==SQLITE_OK ){
    hctPManAddFree(pClient, bLogical, iPg, iTid);
  }
}

void sqlite3HctPManClientHandoff(HctPManClient *pClient){
  hctPManHandback(pClient, 0, pClient->aPgSet[0].nPg);
  hctPManHandback(pClient, 1, pClient->aPgSet[1].nPg);
}

int sqlite3HctPManFreeTree(
  HctPManClient *p, 
  HctFile *pFile, 
  u32 iRoot, 
  u64 iTid,
  int bSaveRoot
){
  int rc = SQLITE_OK;
  assert( bSaveRoot==0 || iTid==0 );
  if( iTid==0 ){
    rc = hctPManFreeTreeNow(p, pFile, iRoot, bSaveRoot);
  }else{
    HctPManServer *pServer = p->pServer;
    int nNew;
    HctPManTree *aNew;

    ENTER_PMAN_MUTEX(p);
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
    LEAVE_PMAN_MUTEX(p);
  }
  return rc;
}

typedef struct InitRootCtx InitRootCtx; 
struct InitRootCtx {
  HctFile *pFile;
  HctPManServer *pServer;
  u64 iTid;
  u64 iRoot;                      /* Logical root page of this tree */
};

static int pmanInitRootCb(void *pCtx, u32 iLogic, u32 iPhys){
  InitRootCtx *p = (InitRootCtx*)pCtx;
  int rc = SQLITE_OK;

  if( iLogic && !sqlite3HctFilePageIsFree(p->pFile, iLogic, 1) ){
    rc = sqlite3HctFilePageClearInUse(p->pFile, iLogic, 1);
    if( iLogic<p->iRoot ){
      sqlite3HctPManServerInit(&rc, p->pServer, p->iTid, iLogic, 1);
    }
  }
  if( iPhys && !sqlite3HctFilePageIsFree(p->pFile, iPhys, 0) && rc==SQLITE_OK ){
    rc = sqlite3HctFilePageClearInUse(p->pFile, iPhys, 0);
    if( iPhys<p->iRoot ){
      sqlite3HctPManServerInit(&rc, p->pServer, p->iTid, iPhys, 0);
    }
  }

  return rc;
}

int sqlite3HctPManServerInitRoot(
  int *pRc, 
  HctPManServer *pServer, 
  u64 iTid,
  HctFile *pFile, 
  u32 iRoot
){
  int rc = SQLITE_OK;
  InitRootCtx ctx;
  ctx.pServer = pServer;
  ctx.pFile = pFile;
  ctx.iTid = iTid;
  ctx.iRoot = iRoot;
  rc = sqlite3HctDbWalkTree(pFile, iRoot, pmanInitRootCb, (void*)&ctx);
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFilePageClearIsRoot(pFile, iRoot);
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
  pNew = (pman_vtab*)sqlite3HctMallocRc(&rc, sizeof(*pNew));
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

  ENTER_PMAN_MUTEX(pClient);
  for(ii=0; ii<2; ii++){
    nRow += pClient->aPgSet[ii].nPg;
    for(pSet=pClient->pServer->aList[ii].pHead; pSet; pSet=pSet->pNext){
      nRow += hctPagesetSize(pSet);
    }
  }
  pCur->aRow = sqlite3HctMallocRc(&rc, sizeof(HctPmanRow) * nRow);
  if( pCur->aRow ){
    for(ii=0; ii<2; ii++){
      int i2;
      HctPManFreePgSet *pPgSet = &pClient->aPgSet[ii];
      for(i2=0; i2<pPgSet->nPg; i2++){
        HctPmanRow *pRow = &pCur->aRow[pCur->nRow++];
        int idx = (pPgSet->iFirst + i2) % pPgSet->nAlloc;
        pRow->eType = ii;
        pRow->eLoc = HCT_PMAN_LOC_USE;
        pRow->pgno = pPgSet->aPg[idx].pgno;
        pRow->iTid = pPgSet->aPg[idx].iTid;
      }
      for(pSet=pClient->pServer->aList[ii].pHead; pSet; pSet=pSet->pNext){
        hctPagesetRows(pCur, pSet, ii, HCT_PMAN_LOC_SERVER);
      }
    }
  }
  LEAVE_PMAN_MUTEX(pClient);

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


i64 sqlite3HctPManStats(sqlite3 *db, int iStat, const char **pzStat){
  HctPManClient *pClient = 0;
  i64 iVal = -1;

  pClient = sqlite3HctFilePManClient(sqlite3HctDbFile(sqlite3HctDbFind(db, 0)));
  switch( iStat ){
    case 0:
      *pzStat = "mutex_attempt";
      iVal = pClient->stats.nMutex;
      break;
    case 1:
      *pzStat = "mutex_block";
      iVal = pClient->stats.nMutexBlock;
      break;
    default:
      break;
  }

  return iVal;
}


