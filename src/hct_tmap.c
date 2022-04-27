/*
** 2021 February 28
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

/*
** NOTES ON LOCKING
**
** Each time a new HctTMap object is allocated, the locking related 
** variables are set:
**
**     HctTMap.iMinTid
**     HctTMap.iMinCid
**
** New HctTMap objects are always allocated by writers during the
** WRITING phase of a transaction. The iMinCid variable is set to
** the CID value associated with the snapshot on which the writer
** based its transaction. The iMinTid value is set to the largest
** TID value for which it and all smaller TID values map to fully
** committed transactions with CID values smaller than or equal
** to iMinCid. This means that:
**
**   * The new object may be used by any client accessing a snapshot
**     with a snapshot-id >= iMinCid.
**
**   * So long as this object exists, it is not safe to reuse any
**     page ids (logical or physical) freed by transactions with
**     TID values > iMinTid.
**
** The HctTMap object may then be used to access any snapshot with
** a CID value greater than or equal to iMinCid. While the HctTMap
** is still in use, it is not safe to reuse any logical or physical
** page id freed by a transaction with a TID value greater than
** iMinTid.
**
** A new HctTMap object is created by a writer after it is allocated
** its TID iff:
**
**   *  The expression (iNewTid % HctTMapServer.nTidStep)==0 is true, or
**   *  The existing transaction map is too small to contain an entry
**      for iNewTid.
**
** The first time a client obtains a new HctTMap object, it remembers 
** the CID of the first snapshot it accesses using it. The HctTMap 
** is released at the end of the first transaction for which the CID is 
** greater than or equal to (iFirstCid + HctTMapServer.nTidStep). This
** happens even if a new HctTMap has been obtained since then. TODO: There 
** is probably a role for some randomness here.
**
** The above creates a problem - a single dormant connection can prevent
** all reuse of freed logical and physical pages. This is addressed by
** using smart reference objects of type HctTMapRef that support the
** reference being revoked by the server at any time. See comments above
** struct HctTMapRef for details.
*/

#include "hctInt.h"

typedef struct HctTMapFull HctTMapFull;
typedef struct HctTMapRef HctTMapRef;

/*
** The following object type represents a reference to an HctTMapFull
** object. The reference is taken and released under the cover of the
** associated HctTMapServer.mutex mutex.
*/
struct HctTMapRef {
  u32 refMask;
  HctTMapFull *pMap;
  HctTMapRef *pRefNext;
  HctTMapRef *pRefPrev;
};

/*
** Bits from HctTMapRef.refMask.
*/
#define HCT_TMAPREF_CLIENT 0x01
#define HCT_TMAPREF_SERVER 0x02
#define HCT_TMAPREF_BOTH   0x03


struct HctTMapClient {
  HctTMapServer *pServer;
  int eState;
  u64 iDerefCID;                  /* Drop all references at/after this CID */
  int iRef;                       /* Current entry in aRef[2], or -1 for none */
  HctTMapRef aRef[2];             /* Pair of tmap references */
};

/*
** Values for HctTMapClient.eState
*/
#define HCT_CLIENT_NONE 0
#define HCT_CLIENT_OPEN 1
#define HCT_CLIENT_UP   2

/*
** iMinMinTid:
**   This value is set only when the mutex is held, using AtomicStore().
**   It may be read, using AtomicLoad(), at any time.
*/
struct HctTMapServer {
  sqlite3_mutex *pMutex;          /* Mutex to protect this object */
  int nTidStep;
  u64 iMinMinTid;                 /* Smallest iMinTid value in pList */
  HctTMapFull *pList;             /* List of tmaps. Newest first */
};

#define HCT_TMAP_NTIDSTEP 2

struct HctTMapFull {
  HctTMap m;
  HctTMapRef *pRefList;
  HctTMapFull *pNext;             /* Next entry in HctTMapServer.pList */
};

/*
** Atomic version of:
**
**   if( *pPtr!=iOld ){
**     return 0;
**   }
**   *pPtr = iNew;
**   return 1;
*/
static int hctTMapBoolCAS32(u32 *pPtr, u32 iOld, u32 iNew){
  return (int)(__sync_bool_compare_and_swap(pPtr, iOld, iNew));
}

/*
** Allocate the initial HctTMapFull object for the server passed as the
** only argument.
*/
static int hctTMapInit(HctTMapServer *p, u64 iFirstTid){
  int rc = SQLITE_OK;
  HctTMapFull *pNew;

  assert( p->pList==0 );
  assert( (iFirstTid & HCT_TID_MASK)==iFirstTid );

  pNew = (HctTMapFull*)sqlite3MallocZero(sizeof(HctTMapFull) * sizeof(u64*)*3);
  if( pNew==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    int i;
    pNew->m.iMinCid = 1;
    pNew->m.iMinTid = iFirstTid-1;
    pNew->m.iFirstTid = iFirstTid;
    pNew->m.nMap = 3;
    pNew->m.aaMap = (u64**)&pNew[1];
    for(i=0; i<pNew->m.nMap; i++){
      u64 *a = (u64*)sqlite3MallocZero(sizeof(u64)*HCT_TMAP_PAGESIZE);
      if( a==0 ){
        rc = SQLITE_NOMEM_BKPT;
        break;
      }else{
        pNew->m.aaMap[i] = a;
      }
    }

    if( rc!=SQLITE_OK ){
      for(i=0; i<pNew->m.nMap; i++){
        sqlite3_free(pNew->m.aaMap[i]);
      }
      sqlite3_free(pNew);
    }else{
      p->pList = pNew;
    }
  }

  return rc;
}

int sqlite3HctTMapServerNew(u64 iFirstTid, HctTMapServer **pp){
  int rc = SQLITE_OK;
  HctTMapServer *pNew;

  pNew = sqlite3MallocZero(sizeof(HctTMapServer));
  if( pNew==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    pNew->pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
    if( pNew->pMutex==0 ){
      rc = SQLITE_NOMEM_BKPT;
    }else{
      pNew->iMinMinTid = iFirstTid;
      pNew->nTidStep = HCT_TMAP_NTIDSTEP;
      rc = hctTMapInit(pNew, iFirstTid);
    }
  }

  if( rc!=SQLITE_OK ){
    sqlite3HctTMapServerFree(pNew);
    pNew = 0;
  }

  *pp = pNew;
  return rc;
}

void sqlite3HctTMapServerFree(HctTMapServer *p){
  if( p ){
    sqlite3_mutex_free(p->pMutex);
    if( p->pList ){
      int i;
      HctTMapFull *pIter;
      HctTMapFull *pNext;
      for(i=0; i<p->pList->m.nMap; i++){
        sqlite3_free(p->pList->m.aaMap[i]);
      }
      for(pIter=p->pList; pIter; pIter=pNext){
        pNext = pIter->pNext;
        assert( pIter->pRefList==0 );
        sqlite3_free(pIter);
      }
    }
    sqlite3_free(p);
  }
}

int sqlite3HctTMapClientNew(HctTMapServer *p, HctTMapClient **ppClient){
  int rc = SQLITE_OK;
  HctTMapClient *pNew;
  pNew = (HctTMapClient*)sqlite3MallocZero(sizeof(HctTMapClient));
  if( pNew==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    pNew->pServer = p;
  }
  *ppClient = pNew;
  return rc;
}

static void hctTMapGetRef(HctTMapServer *p, HctTMapRef *pRef){
  HctTMapFull *pMap;

  assert( sqlite3_mutex_held(p->pMutex) );
  assert( pRef->pMap==0 );
  assert( pRef->refMask==0 );
  assert( pRef->pRefNext==0 );
  assert( pRef->pRefPrev==0 );

  pMap = p->pList;
  pRef->pRefNext = pMap->pRefList;
  pMap->pRefList = pRef;
  if( pRef->pRefNext ){
    assert( pRef->pRefNext->pRefPrev==0 );
    pRef->pRefNext->pRefPrev = pRef;
  }
  pRef->refMask = HCT_TMAPREF_SERVER;
  pRef->pMap = pMap;
}

static void hctTMapDropMap(HctTMapServer *p, HctTMapFull *pMap){
  HctTMapFull **pp;
  u64 iVal = 0;

  assert( sqlite3_mutex_held(p->pMutex) );
  assert( pMap->pRefList==0 && pMap!=p->pList );

  for(pp=&p->pList; *pp!=pMap; pp=&(*pp)->pNext){
    iVal = (*pp)->m.iMinTid;
  }
  *pp = pMap->pNext;
#if 0
  printf("dropping tmap object - iVal = %lld, last=%s\n", (i64)iVal,
      pMap->pNext ? "no" : "yes"
  );
#endif
  if( pMap->pNext==0 ){
    AtomicStore(&p->iMinMinTid, iVal);
  }
  sqlite3_free(pMap);
}

/*
** Called to drop the reference held by object pRef, if any. The server 
** mutex must be held to call this function.
*/
static void hctTMapDropRef(HctTMapServer *p, HctTMapRef *pRef){
  HctTMapFull *pMap = pRef->pMap;
  assert( sqlite3_mutex_held(p->pMutex) );
  if( pMap ){

    /* Check list pointers look Ok */
    assert( pRef->pRefNext==0 || pRef->pRefNext->pRefPrev==pRef );
    assert( pRef->pRefPrev==0 || pRef->pRefPrev->pRefNext==pRef );
    assert( pRef->pRefPrev!=0 || pRef==pMap->pRefList );

    /* Unlink the reference from the linked-list at HctTMapFull.pList */
    if( pRef->pRefNext ){
      pRef->pRefNext->pRefPrev = pRef->pRefPrev;
    }
    if( pRef->pRefPrev ){
      pRef->pRefPrev->pRefNext = pRef->pRefNext;
    }else{
      pMap->pRefList = pRef->pRefNext;
    }

    /* Zero the reference object */
    memset(pRef, 0, sizeof(HctTMapRef));

    /* If the HctTMapFull object is no longer required: 
    **
    **   1. remove it from the HctTMapServer.pList list,
    **   2. update HctTMapServer.iMinMinTid if possible, and
    **   3. free the object itself.
    */
    if( pMap->pRefList==0 && pMap!=p->pList ){
      hctTMapDropMap(p, pMap);
    }
  }
  assert( pRef->refMask==0 );
  assert( pRef->pRefNext==0 );
  assert( pRef->pRefPrev==0 );
  assert( pRef->pMap==0 );
}

int sqlite3HctTMapBegin(HctTMapClient *pClient, HctTMap **ppMap){
  HctTMapRef *pRef;
  assert( pClient->eState==HCT_CLIENT_NONE );

  pRef = &pClient->aRef[pClient->iRef];
  while( 1 ){
    if( hctTMapBoolCAS32(&pRef->refMask, HCT_TMAPREF_SERVER,HCT_TMAPREF_BOTH) ){
      *ppMap = &pRef->pMap->m;
      pClient->eState = HCT_CLIENT_OPEN;
      break;
    }else{
      assert( AtomicLoad(&pRef->refMask)==0 );
      sqlite3_mutex_enter(pClient->pServer->pMutex);
      hctTMapGetRef(pClient->pServer, pRef);
      sqlite3_mutex_leave(pClient->pServer->pMutex);
    }
  }

  return SQLITE_OK;
}

static void hctTMapUpdateSafe(HctTMapClient *pClient){
  assert( sqlite3_mutex_held(pClient->pServer->pMutex) );
  if( pClient->eState==HCT_CLIENT_UP ){
    hctTMapDropRef(pClient->pServer, &pClient->aRef[pClient->iRef]);
  }else{
    assert( pClient->eState==HCT_CLIENT_OPEN );
    assert( pClient->aRef[!pClient->iRef].refMask==0 );
    pClient->iRef = !pClient->iRef;
    pClient->eState = HCT_CLIENT_UP;
  }

  hctTMapGetRef(pClient->pServer, &pClient->aRef[pClient->iRef]);
  pClient->aRef[pClient->iRef].refMask = HCT_TMAPREF_BOTH;
}

/*
** This is called by a reader if it needs to look-up a TID for which its
** current HctTMap object is not large enough. This function sets output
** parameter (*ppMap) to point to the latest HctTMap object, which,
** unless the db is corrupt, is guaranteed to be large enough.
**
** SQLITE_OK is returned if successful.
*/
int sqlite3HctTMapUpdate(HctTMapClient *pClient, HctTMap **ppMap){
  assert( pClient->eState==HCT_CLIENT_OPEN || pClient->eState==HCT_CLIENT_UP );
  sqlite3_mutex_enter(pClient->pServer->pMutex);
  hctTMapUpdateSafe(pClient);
  sqlite3_mutex_leave(pClient->pServer->pMutex);
  *ppMap = &pClient->aRef[pClient->iRef].pMap->m;
  return SQLITE_OK;
}

/*
** Called to signal the end of a read or write a transaction. Parameter
** iCID is passed the CID of the snapshot on which the transaction was
** based.
*/
int sqlite3HctTMapEnd(HctTMapClient *pClient, u64 iCID){
  HctTMapRef *pRef;
  assert( pClient->eState==HCT_CLIENT_OPEN || pClient->eState==HCT_CLIENT_UP );

  if( pClient->iDerefCID==0 ){
    pClient->iDerefCID = iCID + pClient->pServer->nTidStep;
  }

  /* If the client currently holds two references, drop the older of the two */
  if( pClient->eState==HCT_CLIENT_UP ){
    pRef = &pClient->aRef[!pClient->iRef];
    assert( AtomicLoad(&pRef->refMask) & HCT_TMAPREF_CLIENT );
    sqlite3_mutex_enter(pClient->pServer->pMutex);
    hctTMapDropRef(pClient->pServer, pRef);
    sqlite3_mutex_leave(pClient->pServer->pMutex);
  }

  /* Check if the current reference also needs to be dropped. It needs to
  ** dropped if either:
  **
  **   * The CID value passed as the second argument to this function
  **     indicates it is time to do so, or
  **   * The HCT_TMAPREF_SERVER flag has been cleared on the reference.
  */
  pRef = &pClient->aRef[pClient->iRef];
  if( iCID>=pClient->iDerefCID
   || 0==hctTMapBoolCAS32(&pRef->refMask, HCT_TMAPREF_BOTH, HCT_TMAPREF_SERVER)
  ){
    sqlite3_mutex_enter(pClient->pServer->pMutex);
    hctTMapDropRef(pClient->pServer, pRef);
    sqlite3_mutex_leave(pClient->pServer->pMutex);
    pClient->iDerefCID = 0;
  }

  pClient->eState = HCT_CLIENT_NONE;
  return SQLITE_OK;
}

void sqlite3HctTMapClientFree(HctTMapClient *p){
  if( p ){
    assert( p->eState==HCT_CLIENT_NONE );
    sqlite3_mutex_enter(p->pServer->pMutex);
    hctTMapDropRef(p->pServer, &p->aRef[0]);
    hctTMapDropRef(p->pServer, &p->aRef[1]);
    sqlite3_mutex_leave(p->pServer->pMutex);
    sqlite3_free(p);
  }
}

static u64 *hctTMapFind(HctTMapFull *pMap, u64 iTid){
  int iOff = iTid - pMap->m.iFirstTid;
  int iMap = iOff / HCT_TMAP_PAGESIZE;
  return &pMap->m.aaMap[iMap][iOff % HCT_TMAP_PAGESIZE];
}

static HctTMapFull *hctTMapNewObject(
  HctTMapFull *pPrev, 
  u64 iCid,
  int nMapReq
){
  HctTMapFull *pNew;
  int nMap = MAX(nMapReq, pPrev->m.nMap);

  pNew = (HctTMapFull*)sqlite3MallocZero(sizeof(HctTMapFull)+nMap*sizeof(u64*));
  if( pNew ){
    int i;
    u64 iMinTid;
    u64 iEof = pPrev->m.iFirstTid + pPrev->m.nMap*HCT_TMAP_PAGESIZE;
    int nSkip;
    assert( (pPrev->m.iMinTid+1)>=pPrev->m.iFirstTid );
    for(iMinTid=pPrev->m.iMinTid; iMinTid<(iEof-1); iMinTid++){
      u64 iVal = AtomicLoad( hctTMapFind(pPrev, iMinTid+1) );
      if( (iVal & HCT_TMAP_STATE_MASK)!=HCT_TMAP_COMMITTED
       || (iVal & HCT_TMAP_CID_MASK)>iCid
      ){
        break;
      }
    }
    pNew->m.iFirstTid = pPrev->m.iFirstTid;
    pNew->m.iMinCid = MAX(pPrev->m.iMinCid, iCid);
    pNew->m.iMinTid = iMinTid;

    nSkip = (pNew->m.iMinTid - pNew->m.iFirstTid) / HCT_TMAP_PAGESIZE;
    if( nSkip<0 ) nSkip = 0;
    pNew->m.iFirstTid += (nSkip * HCT_TMAP_PAGESIZE);

    pNew->m.nMap = nMap-nSkip;
    pNew->m.aaMap = (u64**)&pNew[1];
    for(i=nSkip; i<nMap; i++){
      if( i<pPrev->m.nMap ){
        pNew->m.aaMap[i-nSkip] = pPrev->m.aaMap[i];
      }else{
        int sz = sizeof(u64) * HCT_TMAP_PAGESIZE;
        pNew->m.aaMap[i-nSkip] = (u64*)sqlite3MallocZero(sz);
        if( pNew->m.aaMap[i-nSkip]==0 ){
          for(i=pPrev->m.nMap; i<nMap; i++){
            sqlite3_free(pNew->m.aaMap[i-nSkip]);
          }
          sqlite3_free(pNew);
          pNew = 0;
          break;
        }
      }
    }

  }

  return pNew;
}

/*
** Return the largest TID for which it is safe to reuse freed pages.
*/
u64 sqlite3HctTMapSafeTID(HctTMapClient *p){
  return AtomicLoad(&p->pServer->iMinMinTid);
}

/*
** This is called by write transactions immediately after obtaining
** the transactions TID value (at the start of the commit process).
** A new HctTMapFull object is created if either:
**
**     *  (iTid % nTidStep)==0 is true, or
**     *  the current HctTMapFull object is too small to fit the new TID.
**
** A pointer to the new HctTMap object is returned to the user via
** output parameter *ppMap.
*/
int sqlite3HctTMapNewTID(
  HctTMapClient *p,               /* Transaction map client */
  u64 iCid,                       /* Snapshot transaction was based on */
  u64 iTid,                       /* TID for write transaction */
  HctTMap **ppMap                 /* OUT: (possibly) new transaction map */
){
  int rc = SQLITE_OK;
  HctTMapFull *pMap = p->aRef[p->iRef].pMap;
  int nTidStep = p->pServer->nTidStep;
  int nMapReq = (
      (iTid - pMap->m.iFirstTid + HCT_TMAP_PAGESIZE) / HCT_TMAP_PAGESIZE
  );

  assert( p->eState!=HCT_CLIENT_NONE );
  assert( (p->aRef[p->iRef].refMask & HCT_TMAPREF_CLIENT) );

  if( (iTid % nTidStep)==0 || nMapReq>pMap->m.nMap ){
    sqlite3_mutex_enter(p->pServer->pMutex);
    pMap = p->pServer->pList;
    if( (iTid % nTidStep)==0 || nMapReq>pMap->m.nMap ){
      /* Create a new HctTMapFull object! */
      HctTMapFull *pNew = hctTMapNewObject(pMap, iCid, nMapReq);
      if( pNew==0 ){
        rc = SQLITE_NOMEM_BKPT;
      }else{
        HctTMapFull *pOld = pNew->pNext = p->pServer->pList;
        p->pServer->pList = pNew;
        if( pOld->pRefList==0 ){
          hctTMapDropMap(p->pServer, pOld);
        }
      }
    }
    hctTMapUpdateSafe(p);
    sqlite3_mutex_leave(p->pServer->pMutex);
  }

  *ppMap = &p->aRef[p->iRef].pMap->m;
  return rc;
}





