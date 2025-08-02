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
** TODO: This all needs updating!!!
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


/*
** Event counters used by the hctstats virtual table.
*/
typedef struct HctTMapStats HctTMapStats;
struct HctTMapStats {
  i64 nMutex;
  i64 nMutexBlock;
};

/* How many new tmap pages to allocate at a time. */
#define TMAP_NPAGE_ALLOC 16

/*
** iLockValue:
**   This field contains two things - a flag and a safe-tid value. The flag
**   is set whenever a read transaction is active, and clear otherwise.
**   The safe-tid value is set to a TID value for which itself an all smaller
**   TID values are included in the connection's transactions - current and
**   future.
**
**   Pages freed by the transaction with the safe-tid value may be reused
**   without disturbing this client.
**
** pNextClient:
**   Linked list of all clients associated with pServer.
**
** pBuild:
**   This is used by the sqlite3HctTMapRecoveryXXX() API when constructing
**   a new tmap object as part of sqlite_hct_journal recovery.
*/
struct HctTMapClient {
  HctTMapServer *pServer;
  HctConfig *pConfig;
  u64 iLockValue;
  HctTMapClient *pNextClient;
  HctTMapFull *pMap;
  HctTMapStats stats;

  HctTMapFull *pBuild;
  u64 iBuildMin;                  /* Min TID value explicitly set in pBuild */
};

#define HCT_LOCKVALUE_ACTIVE (((u64)0x01) << 56)

/*
** Values for HctTMapClient.eState
*/
#define HCT_CLIENT_NONE 0
#define HCT_CLIENT_OPEN 1
#define HCT_CLIENT_UP   2

/*
** iMinMinTid:
**   This value is set only when the mutex is held, using HctAtomicStore().
**   It may be read, using HctAtomicLoad(), at any time.
*/
struct HctTMapServer {
  sqlite3_mutex *pMutex;          /* Mutex to protect this object */
  int nClient;                    /* Number of connected clients */
  u64 iMinMinTid;                 /* Smallest iLockValue value in pClientList */
  HctTMapFull *pList;             /* List of tmaps. Newest first */
  HctTMapClient *pClientList;     /* List of clients */
};

/*
** nRef:
**   Number of clients that hold a pointer to this object.
*/
struct HctTMapFull {
  HctTMap m;
  int nRef;                       /* Number of pointers to this object */
  HctTMapFull *pNext;             /* Next entry in HctTMapServer.pList */
};

/*
** ENTER_TMAP_MUTEX(pClient) implementation.
**
** Grab the server mutex. And update client-stats as required at the same
** time.
*/
static void hctTMapMutexEnter(HctTMapClient *pClient){
  sqlite3_mutex *pMutex = pClient->pServer->pMutex;
  pClient->stats.nMutex++;
  if( sqlite3_mutex_try(pMutex)!=SQLITE_OK ){
    pClient->stats.nMutexBlock++;
    sqlite3_mutex_enter(pMutex);
  }
}

#if 0
#define ENTER_TMAP_MUTEX(pClient) sqlite3_mutex_enter(pClient->pServer->pMutex)
#endif
#define ENTER_TMAP_MUTEX(pClient) hctTMapMutexEnter(pClient)
#define LEAVE_TMAP_MUTEX(pClient) sqlite3_mutex_leave(pClient->pServer->pMutex)

/*
** Atomic version of:
**
**   if( *pPtr!=iOld ){
**     return 0;
**   }
**   *pPtr = iNew;
**   return 1;
*/
#if 0
static int hctTMapBoolCAS32(u32 *pPtr, u32 iOld, u32 iNew){
  return HctCASBool(pPtr, iOld, iNew);
}
#endif
static int hctTMapBoolCAS64(u64 *pPtr, u64 iOld, u64 iNew){
  return HctCASBool(pPtr, iOld, iNew);
}

/*
** Return a pointer to the slot in pMap associated with TID iTid.
*/
static u64 *hctTMapFind(HctTMapFull *pMap, u64 iTid){
  int iOff = iTid - pMap->m.iFirstTid;
  int iMap = iOff / HCT_TMAP_PAGESIZE;
  iOff = HCT_TMAP_ENTRYSLOT( (iOff % HCT_TMAP_PAGESIZE) );
  return &pMap->m.aaMap[iMap][iOff % HCT_TMAP_PAGESIZE];
}

/*
** Allocate the initial HctTMapFull object for the server passed as the
** only argument. This is called as part of sqlite3HctTMapServerNew().
*/
static int hctTMapInit(HctTMapServer *p, u64 iFirstTid, u64 iLastTid){
  int rc = SQLITE_OK;
  int nMap = 0;
  int nByte = 0;
  u64 iFirst = (iFirstTid / HCT_TMAP_PAGESIZE) * HCT_TMAP_PAGESIZE;
  HctTMapFull *pNew = 0;

  assert( p->pList==0 );
  assert( (iFirstTid & HCT_TMAP_CID_MASK)==iFirstTid );

  nMap = (iLastTid / HCT_TMAP_PAGESIZE) 
       - (iFirst / HCT_TMAP_PAGESIZE) 
       + TMAP_NPAGE_ALLOC;
  nByte = sizeof(HctTMapFull) + sizeof(u64*)*nMap;
  pNew = (HctTMapFull*)sqlite3HctMallocRc(&rc, nByte);
  if( pNew ){
    int i;
    pNew->m.iFirstTid = iFirst;
    pNew->m.nMap = nMap;
    pNew->m.aaMap = (u64**)&pNew[1];
    for(i=0; i<pNew->m.nMap; i++){
      u64 *a = (u64*)sqlite3HctMallocRc(&rc, sizeof(u64)*HCT_TMAP_PAGESIZE);
      pNew->m.aaMap[i] = a;
    }

    if( rc!=SQLITE_OK ){
      assert( 0 );   /* OOM case */
      for(i=0; i<pNew->m.nMap; i++){
        sqlite3_free(pNew->m.aaMap[i]);
      }
      sqlite3_free(pNew);
    }else{
      u64 t;
      for(t=iFirst; t<iLastTid; t++){
        u64 *pEntry = hctTMapFind(pNew, t);
        *pEntry = (u64)1 | HCT_TMAP_COMMITTED;
      }
      p->pList = pNew;
      pNew->nRef = 1;             /* Server reference */
    }
  }

  return rc;
}

int sqlite3HctTMapServerNew(u64 iFirstTid, u64 iLastTid, HctTMapServer **pp){
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
      pNew->iMinMinTid = iFirstTid-1;
      rc = hctTMapInit(pNew, iFirstTid, iLastTid);
    }
  }

  if( rc!=SQLITE_OK ){
    sqlite3HctTMapServerFree(pNew);
    pNew = 0;
  }

  *pp = pNew;
  return rc;
}

int sqlite3HctTMapServerSet(HctTMapServer *pServer, u64 iTid, u64 iCid){
  u64 *pEntry = hctTMapFind(pServer->pList, iTid);
  *pEntry = iCid;
  return SQLITE_OK;
}

/*
** Argument pMap is an HctTMapFull object that is currently linked
** into the list at HctTMapServer.pList. This function removes pMap
** from that list and frees all associated allocations.
*/
static void hctTMapFreeMap(HctTMapServer *p, HctTMapFull *pMap){
  int iFirst = 0;                 /* First in pMap->m.aaMap[] to free */
  int iSave = 0;                  /* First in pMap->m.aaMap[] to preserve */
  int ii;

  assert( pMap && pMap->nRef==0 );
  if( pMap==p->pList ){
    if( pMap->pNext==0 ) iSave = pMap->m.nMap;
    p->pList = pMap->pNext;
  }else{
    HctTMapFull *pPrev;
    HctTMapFull *pNext = pMap->pNext;

    for(pPrev=p->pList; pPrev->pNext!=pMap; pPrev=pPrev->pNext);
    for(iSave=0; iSave<pMap->m.nMap; iSave++){
      if( pMap->m.aaMap[iSave]==pPrev->m.aaMap[0] ) break;
    }

    if( pNext ){
      u64 *aDoNotDel = pNext->m.aaMap[pNext->m.nMap-1];
      for(iFirst=pMap->m.nMap; iFirst>0; iFirst--){
        if( pMap->m.aaMap[iFirst-1]==aDoNotDel ) break;
      }
    }

    pPrev->pNext = pMap->pNext;
  }

  for(ii=iFirst; ii<iSave; ii++){
    sqlite3_free(pMap->m.aaMap[ii]);
  }
  sqlite3_free(pMap);

}

/*
** Free a tmap-server object.
*/
void sqlite3HctTMapServerFree(HctTMapServer *p){
  if( p ){
    assert( p->pClientList==0 );
    sqlite3_mutex_free(p->pMutex);

    assert( p->pList==0 || p->pList->nRef==1 );
    if( p->pList ) p->pList->nRef--;
    while( p->pList ){
      HctTMapFull *pMap = p->pList;
      while( pMap->pNext ) pMap = pMap->pNext;
      hctTMapFreeMap(p, pMap);
    }

    sqlite3_free(p);
  }
}

int sqlite3HctTMapClientNew(
  HctTMapServer *p, 
  HctConfig *pConfig,
  HctTMapClient **ppClient
){
  int rc = SQLITE_OK;
  HctTMapClient *pNew;

  pNew = (HctTMapClient*)sqlite3HctMallocRc(&rc, sizeof(HctTMapClient));
  if( pNew ){
    pNew->pServer = p;
    pNew->pConfig = pConfig;
    ENTER_TMAP_MUTEX(pNew);
    /* Under cover of the server mutex, link this new client into the 
    ** list of clients associated with the server. The minimum TID value
    ** for the client is set to the current global minimum. */
    pNew->iLockValue = p->iMinMinTid;
    pNew->pNextClient = p->pClientList;
    pNew->pMap = p->pList;
    pNew->pMap->nRef++;
    p->pClientList = pNew;
    LEAVE_TMAP_MUTEX(pNew);
  }
  *ppClient = pNew;
  return rc;
}

void sqlite3HctTMapClientFree(HctTMapClient *pClient){
  if( pClient ){
    HctTMapClient **pp;
    ENTER_TMAP_MUTEX(pClient);

    pClient->pMap->nRef--;
    if( pClient->pMap->nRef==0 ){
      hctTMapFreeMap(pClient->pServer, pClient->pMap);
    }

    /* Remove this client from the HctTMapServer.pClientList list */
    for(pp=&pClient->pServer->pClientList;*pp!=pClient;pp=&(*pp)->pNextClient);
    *pp = pClient->pNextClient;

    LEAVE_TMAP_MUTEX(pClient);
    sqlite3_free(pClient);
  }
}


static void hctTMapUpdateSafe(HctTMapClient *pClient){
  assert( sqlite3_mutex_held(pClient->pServer->pMutex) );
  if( pClient->pMap!=pClient->pServer->pList ){
    pClient->pMap->nRef--;
    if( pClient->pMap->nRef==0 ){
      hctTMapFreeMap(pClient->pServer, pClient->pMap);
    }
    pClient->pMap = pClient->pServer->pList;
    pClient->pMap->nRef++;
  }
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
  ENTER_TMAP_MUTEX(pClient);
  hctTMapUpdateSafe(pClient);
  LEAVE_TMAP_MUTEX(pClient);
  *ppMap = (HctTMap*)pClient->pMap;
  return SQLITE_OK;
}

HctTMap *sqlite3HctTMapBegin(
  HctTMapClient *pClient, 
  u64 iSnapshot, 
  u64 iPeekTid
){
  HctTMapFull *pMap = pClient->pMap;
  u64 iEof = pMap->m.iFirstTid + pMap->m.nMap*HCT_TMAP_PAGESIZE;

  /* If the current mapping does not cover up to TID value iPeekTid, then
  ** attempt to update the mapping. */
  if( iEof<iPeekTid ){
    ENTER_TMAP_MUTEX(pClient);
    hctTMapUpdateSafe(pClient);
    LEAVE_TMAP_MUTEX(pClient);
    pMap = pClient->pMap;
    iEof = pMap->m.iFirstTid + pMap->m.nMap*HCT_TMAP_PAGESIZE;
  }

  while( 1 ){
    u64 iOrigLockValue = HctAtomicLoad(&pClient->iLockValue);
    u64 iLockValue;

    /* Find the new "safe-tid" value */
    u64 iSafe = (iOrigLockValue & HCT_TMAP_CID_MASK);
    u64 iMinMinTid = HctAtomicLoad(&pClient->pServer->iMinMinTid);
    if( iSafe<iMinMinTid ){
      iSafe = iMinMinTid;
    }

    while( iSafe<(iEof-1) ){
      u64 iVal = HctAtomicLoad( hctTMapFind(pMap, iSafe+1) );
      u64 eState = (iVal & HCT_TMAP_STATE_MASK);
      if( eState!=HCT_TMAP_COMMITTED && eState!=HCT_TMAP_ROLLBACK ) break;
      if( iSnapshot!=0 && (iVal & HCT_TMAP_CID_MASK)>iSnapshot ) break;
      iSafe++;
    }

    /* Set the lock-value. If this fails, it means some writer process
    ** has increased the safe-tid value for us. */
    assert( (iOrigLockValue & HCT_LOCKVALUE_ACTIVE)==0 );
    iLockValue = iSafe | HCT_LOCKVALUE_ACTIVE;
    if( hctTMapBoolCAS64(&pClient->iLockValue, iOrigLockValue, iLockValue) ){
      break;
    }
  }

  return (HctTMap*)pMap;
}

u64 sqlite3HctTMapCommitedTID(HctTMapClient *pClient){
  return (pClient->iLockValue & HCT_TMAP_CID_MASK);
}

/*
** Called to signal the end of a read or write a transaction. Parameter
** iCID is passed the CID of the snapshot on which the transaction was
** based.
*/
int sqlite3HctTMapEnd(HctTMapClient *pClient, u64 iCID){
  while( 1 ){
    u64 iOrigLockValue = pClient->iLockValue;
    u64 iLockValue;

    assert( (iOrigLockValue & HCT_LOCKVALUE_ACTIVE)!=0 );
    iLockValue = (iOrigLockValue & ~HCT_LOCKVALUE_ACTIVE);
    if( hctTMapBoolCAS64(&pClient->iLockValue, iOrigLockValue, iLockValue) ){
      break;
    }
  }
  return SQLITE_OK;
}

/*
** Allocate a new HctTMapFull object and link it into the list 
** belonging to server pServer. The new map object is based on
** the server's current newest - pServer->pList. Relative to this
** object, the new map:
**
**   * appends one mapping page to the end of the map, and
**
**   * may remove one or more mapping pages from the start of the
**     map, based on the current value of HctTMapServer.iMinMinTid.
**
** The server mutex must be held to call this function.
*/
static int hctTMapNewObject(HctTMapServer *pServer){
  u64 iFirst = 0;
  HctTMapFull *pOld = pServer->pList;
  HctTMapFull *pNew = 0;
  int nMap = 0;
  int nDiscard = 0;
  int nByte = 0;
  int rc = SQLITE_OK;

  /* Figure out the first TID in the new map object. The new map object
  ** must include all TID values greater than or equal to iMinMinTid for
  ** all clients. Writers in LEADER mode require TID values greater than
  ** or equal to (iMinMinTid - HCT_MAX_LEADING_WRITE).  */
  iFirst = (u64)MAX(
      (i64)pOld->m.iFirstTid, ((i64)pServer->iMinMinTid - HCT_MAX_LEADING_WRITE)
  );
  iFirst = (iFirst / HCT_TMAP_PAGESIZE) * HCT_TMAP_PAGESIZE;

  assert( sqlite3_mutex_held(pServer->pMutex) );
  assert( (iFirst % HCT_TMAP_PAGESIZE)==0 );
  assert( (pOld->m.iFirstTid % HCT_TMAP_PAGESIZE)==0 );
  assert( (pServer->iMinMinTid & HCT_TMAP_CID_MASK)==pServer->iMinMinTid );
  assert( (iFirst & HCT_TMAP_CID_MASK)==iFirst );

  nDiscard = (iFirst - pOld->m.iFirstTid) / HCT_TMAP_PAGESIZE;
  nMap = pOld->m.nMap + TMAP_NPAGE_ALLOC - nDiscard;
  nByte = sizeof(HctTMapFull) + nMap*sizeof(u64*);
  pNew = (HctTMapFull*)sqlite3HctMallocRc(&rc, nByte);

  if( pNew ){
    int ii;
    pNew->m.iFirstTid = iFirst;
    pNew->m.nMap = nMap;
    pNew->m.aaMap = (u64**)&pNew[1];
    pNew->nRef = 1;
    for(ii=0; ii<(nMap-TMAP_NPAGE_ALLOC); ii++){
      pNew->m.aaMap[ii] = pOld->m.aaMap[ii+nDiscard];
    }
    for(/* noop */; ii<nMap; ii++){
      pNew->m.aaMap[ii] = (u64*)sqlite3HctMalloc(sizeof(u64)*HCT_TMAP_PAGESIZE);
    }

    pServer->pList->nRef--;
    if( pServer->pList->nRef==0 ){
      hctTMapFreeMap(pServer, pServer->pList);
    }
    pNew->pNext = pServer->pList;
    pServer->pList = pNew;
  }

  return rc;
}

/*
** Return the largest TID for which it is safe to reuse freed pages.
*/
u64 sqlite3HctTMapSafeTID(HctTMapClient *p){
  /* TODO: -1? */
  return HctAtomicLoad(&p->pServer->iMinMinTid);
}

/*
** This is called by write transactions immediately after obtaining
** the transaction's TID value (at the start of the commit process).
*/
int sqlite3HctTMapNewTID(
  HctTMapClient *p,               /* Transaction map client */
  u64 iTid,                       /* TID for write transaction */
  HctTMap **ppMap                 /* OUT: (possibly) new transaction map */
){
  int rc = SQLITE_OK;
  HctTMapFull *pMap = p->pMap;
  u64 iEof = pMap->m.iFirstTid + ((u64)pMap->m.nMap*HCT_TMAP_PAGESIZE);

  /* If it is time to do so, allocate a new transaction-map */
  if( iTid>=iEof || iTid==(iEof - HCT_TMAP_PAGESIZE/2) ){
    ENTER_TMAP_MUTEX(p);
    hctTMapUpdateSafe(p);
    pMap = p->pMap;
    iEof = pMap->m.iFirstTid + ((u64)pMap->m.nMap*HCT_TMAP_PAGESIZE);
    if( iTid>=iEof || iTid==(iEof - HCT_TMAP_PAGESIZE/2) ){
      hctTMapNewObject(p->pServer);
      hctTMapUpdateSafe(p);
    }
    LEAVE_TMAP_MUTEX(p);
  }

  *ppMap = (HctTMap*)p->pMap;
  return rc;
}

/*
** Loop through all clients connected to the same transaction-map as the
** client passed as the only argument. Adjust HctTMapClient.iLockValue
** and HctTMapServer.iMinMinTid values as required.
*/
void sqlite3HctTMapScan(HctTMapClient *p){
  HctTMapClient *pClient = 0;
  u64 iSafe = p->iLockValue & HCT_TMAP_CID_MASK;

  ENTER_TMAP_MUTEX(p);
  for(pClient=p->pServer->pClientList; pClient; pClient=pClient->pNextClient){
    u64 iVal = HctAtomicLoad(&pClient->iLockValue);
    u64 iTid = (iVal & HCT_TMAP_CID_MASK);

    if( (iVal & HCT_LOCKVALUE_ACTIVE)==0 && iTid<iSafe ){
      hctTMapBoolCAS64(&pClient->iLockValue, iVal, iSafe);
      iVal = HctAtomicLoad(&pClient->iLockValue);
      iTid = (iVal & HCT_TMAP_CID_MASK);
    }

    iSafe = MIN(iSafe, iTid);
  }
  HctAtomicStore(&p->pServer->iMinMinTid, iSafe);
  LEAVE_TMAP_MUTEX(p);
}

i64 sqlite3HctTMapStats(sqlite3 *db, int iStat, const char **pzStat){
  HctTMapClient *pClient = 0;
  i64 iVal = -1;

  pClient = sqlite3HctFileTMapClient(sqlite3HctDbFile(sqlite3HctDbFind(db, 0)));
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

int sqlite3HctTMapRecoverySet(HctTMapClient *p, u64 iTid, u64 iCid){
  int rc = SQLITE_OK;
  HctTMapFull *pNew = p->pBuild;
  if( pNew==0 ){
    u64 iFirst = 0;
    u64 iEof = p->pServer->pList->m.iFirstTid;
    u64 iLast = iEof + (HCT_TMAP_PAGESIZE*2);
    int nMap = 0;
    if( iTid>=HCT_TMAP_PAGESIZE ){
      iFirst = ((iTid / HCT_TMAP_PAGESIZE) - 1) * HCT_TMAP_PAGESIZE;
    }
    nMap = ((iLast - iFirst) + HCT_TMAP_PAGESIZE-1) / HCT_TMAP_PAGESIZE;
    assert( nMap>0 );

    p->pBuild = pNew = (HctTMapFull*)sqlite3HctMallocRc(&rc,
        sizeof(HctTMapFull) + nMap*sizeof(u64*)
    );
    p->iBuildMin = iTid;
    if( pNew ){
      int ii;
      pNew->m.iFirstTid = iFirst;
      pNew->m.nMap = nMap;
      pNew->m.aaMap = (u64**)&pNew[1];
      pNew->nRef = 1;
      for(ii=0; ii<nMap; ii++){
        u64 *aMap = (u64*)sqlite3HctMallocRc(&rc,sizeof(u64) * HCT_TMAP_PAGESIZE);
        pNew->m.aaMap[ii] = aMap;
      }
      if( rc==SQLITE_OK ){
        u64 ee;
        for(ee=iFirst; ee<iEof; ee++){
          int iMap = (ee - iFirst) / HCT_TMAP_PAGESIZE;
          int iOff = (ee - iFirst) % HCT_TMAP_PAGESIZE;
          iOff = HCT_TMAP_ENTRYSLOT(iOff);
          pNew->m.aaMap[iMap][iOff] = ((u64)1 | HCT_TMAP_COMMITTED);
        }
      }
    }
  }
  p->iBuildMin = MIN(p->iBuildMin, iTid);

  assert( pNew->m.iFirstTid<=iTid );
  while( rc==SQLITE_OK && pNew->m.iFirstTid>iTid ){
    int ii;
    HctTMapFull *pAlloc = 0;
    int nMap = pNew->m.nMap + 1;

    pAlloc = (HctTMapFull*)sqlite3HctMallocRc(&rc,
        sizeof(HctTMapFull) + nMap*sizeof(u64*)
    );
    pAlloc->nRef = 1;
    pAlloc->m.nMap = nMap;
    pAlloc->m.aaMap = (u64**)&pAlloc[1];
    pAlloc->m.iFirstTid = pNew->m.iFirstTid - HCT_TMAP_PAGESIZE;
    memcpy(&pAlloc->m.aaMap[1], pNew->m.aaMap, pNew->m.nMap*sizeof(u64*));
    pAlloc->m.aaMap[0] = (u64*)sqlite3HctMallocRc(&rc,
        sizeof(u64) * HCT_TMAP_PAGESIZE
    );
    for(ii=0; ii<HCT_TMAP_PAGESIZE; ii++){
      pNew->m.aaMap[0][ii] = ((u64)1 | HCT_TMAP_COMMITTED);
    }

    assert( pNew->nRef==1 );
    sqlite3_free(pNew);
    p->pBuild = pNew = pAlloc;
  }

  if( rc==SQLITE_OK ){
    int iMap = (iTid - pNew->m.iFirstTid) / HCT_TMAP_PAGESIZE;
    int iOff = (iTid - pNew->m.iFirstTid) % HCT_TMAP_PAGESIZE;
    iOff = HCT_TMAP_ENTRYSLOT(iOff);
    pNew->m.aaMap[iMap][iOff] = (iCid | HCT_TMAP_COMMITTED);
  }

  return rc;
}

void sqlite3HctTMapRecoveryFinish(HctTMapClient *p, int rc){
  HctTMapFull *pNew = p->pBuild;
  if( pNew ){
    p->pBuild = 0;
    if( rc==SQLITE_OK ){
      HctTMapClient *pCli = 0;
      pNew->pNext = p->pServer->pList;
      p->pServer->pList = pNew;
      p->pServer->iMinMinTid = p->iBuildMin;
      if( pNew->pNext ){
        pNew->pNext->nRef--;
        if( pNew->pNext->nRef==0 ){
          hctTMapFreeMap(p->pServer, pNew->pNext);
        }
      }

      ENTER_TMAP_MUTEX(p);
      for(pCli=p->pServer->pClientList; pCli; pCli=pCli->pNextClient){
        hctTMapUpdateSafe(pCli);
      }
      LEAVE_TMAP_MUTEX(p);

    }else{
      int ii;
      for(ii=0; ii<pNew->m.nMap; ii++){
        sqlite3_free(pNew->m.aaMap[ii]);
      }
      sqlite3_free(pNew);
    }
    p->iBuildMin = 0;
  }
}

/*************************************************************************
** Beginning of vtab implemetation.
*************************************************************************/

#define HCT_TMAP_SCHEMA           \
"  CREATE TABLE hcttmap("         \
"    tid INTEGER,"                \
"    cid INTEGER,"                \
"    state TEXT"                  \
"  );"

typedef struct tmap_vtab tmap_vtab;
typedef struct tmap_cursor tmap_cursor;

struct tmap_vtab {
  sqlite3_vtab base;              /* Base class - must be first */
  HctTMapClient *pClient;
};

struct tmap_cursor {
  sqlite3_vtab_cursor base;       /* Base class - must be first */
  i64 iTid;
  i64 iEof;
  HctTMap *pMap;                  /* Map to iterate through */
};

static i64 tmapReadValue(HctTMap *pMap, i64 iTid){
  int iMap = (iTid - pMap->iFirstTid) / HCT_TMAP_PAGESIZE;
  int iOff = (iTid - pMap->iFirstTid) % HCT_TMAP_PAGESIZE;
  assert( iOff>=0 && iMap>=0 && iMap<=pMap->nMap );
  return pMap->aaMap[iMap][HCT_TMAP_ENTRYSLOT(iOff)];
}

/*
** This xConnect() method is invoked to create a new hcttmap virtual table.
*/
static int tmapConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  int rc = SQLITE_OK;
  tmap_vtab *pNew = 0;
  Btree *pBt = db->aDb[0].pBt;

  if( sqlite3IsHct(pBt)==0 ){
    rc = SQLITE_ERROR;
  }else{
    pNew = sqlite3MallocZero(sizeof(tmap_vtab));
    if( pNew==0 ){
      rc = SQLITE_NOMEM;
    }else{
      pNew->pClient = sqlite3HctFileTMapClient(
          sqlite3HctDbFile( sqlite3HctDbFind(db, 0) )
      );

      sqlite3_declare_vtab(db, HCT_TMAP_SCHEMA);
    }
  }

  *ppVtab = (sqlite3_vtab*)pNew;
  return rc;
}

/*
** This method is the destructor for tmap_vtab objects.
*/
static int tmapDisconnect(sqlite3_vtab *pVtab){
  tmap_vtab *p = (tmap_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new tmap_cursor object.
*/
static int tmapOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  tmap_cursor *pCur;
  pCur = sqlite3MallocZero(sizeof(*pCur));
  if( pCur==0 ) return SQLITE_NOMEM;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a tmap_cursor.
*/
static int tmapClose(sqlite3_vtab_cursor *cur){
  tmap_cursor *pCur = (tmap_cursor*)cur;
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int tmapEof(sqlite3_vtab_cursor *cur){
  tmap_cursor *pCur = (tmap_cursor*)cur;
  return pCur->iTid>=pCur->iEof;
}

/*
** Advance a tmap_cursor to its next row of output.
*/
static int tmapNext(sqlite3_vtab_cursor *cur){
  tmap_cursor *pCur = (tmap_cursor*)cur;
  pCur->iTid++;
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the pgmap_cursor
** is currently pointing.
*/
static int tmapColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  tmap_cursor *pCur = (tmap_cursor*)cur;

  if( i==0 ){
    /* column "tid" */
    sqlite3_result_int64(ctx, pCur->iTid);
  }else{
    i64 iVal = tmapReadValue(pCur->pMap, pCur->iTid);

    if( i==1 ){
      /* column "cid" */
      sqlite3_result_int64(ctx, iVal & HCT_TMAP_CID_MASK);
    }else if( iVal!=0 ){
      const char *zState = "???";
      /* column "state" */
      switch( iVal & HCT_TMAP_STATE_MASK ){
        case HCT_TMAP_WRITING:
          zState = "WRITING";
          break;
        case HCT_TMAP_VALIDATING:
          zState = "VALIDATING";
          break;
        case HCT_TMAP_ROLLBACK:
          zState = "ROLLBACK";
          break;
        case HCT_TMAP_COMMITTED:
          zState = "COMMITTED";
          break;
      }

      sqlite3_result_text(ctx, zState, -1, SQLITE_STATIC);
    }
  }

  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the tid value.
*/
static int tmapRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  tmap_cursor *pCur = (tmap_cursor*)cur;
  *pRowid = pCur->iTid;
  return SQLITE_OK;
}

/*
** This method is called to "rewind" the tmap_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to tmapColumn() or tmapRowid() or 
** tmapEof().
*/
static int tmapFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  i64 iEof;
  tmap_cursor *pCur = (tmap_cursor*)pVtabCursor;
  tmap_vtab *pTab = (tmap_vtab*)(pCur->base.pVtab);
  pCur->pMap = (HctTMap*)(pTab->pClient->pMap);
  pCur->iTid = pCur->pMap->iFirstTid;

  for(iEof=pCur->pMap->iFirstTid + (pCur->pMap->nMap * HCT_TMAP_PAGESIZE);
      iEof>pCur->pMap->iFirstTid;
      iEof--
  ){
    i64 iVal = tmapReadValue(pCur->pMap, iEof-1);
    if( iVal ) break;
  }
  pCur->iEof = iEof;

  return SQLITE_OK;
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int tmapBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  pIdxInfo->estimatedCost = (double)10;
  pIdxInfo->estimatedRows = 10;
  return SQLITE_OK;
}

int sqlite3HctTmapVtabInit(sqlite3 *db){
  static sqlite3_module tmapModule = {
    /* iVersion    */ 0,
    /* xCreate     */ 0,
    /* xConnect    */ tmapConnect,
    /* xBestIndex  */ tmapBestIndex,
    /* xDisconnect */ tmapDisconnect,
    /* xDestroy    */ 0,
    /* xOpen       */ tmapOpen,
    /* xClose      */ tmapClose,
    /* xFilter     */ tmapFilter,
    /* xNext       */ tmapNext,
    /* xEof        */ tmapEof,
    /* xColumn     */ tmapColumn,
    /* xRowid      */ tmapRowid,
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

  return sqlite3_create_module(db, "hcttmap", &tmapModule, 0);
}
