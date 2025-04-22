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

#define HCT_JOURNAL_TABLE                                                 \
"CREATE TABLE IF NOT EXISTS hct_journal("                                 \
    "cid INTEGER PRIMARY KEY,    /* Sequence number - 'Commit ID' */"     \
    "query TEXT,                 /* SQL commands for transaction */"      \
    "snapshot INTEGER,           /* 'depends upon' cid value */"          \
    "logptr BLOB                 /* Used by hctree internally */"         \
");"

#define SIZEOF_FOLLOWER_LOGPTR 16
#define SIZEOF_LEADER_LOGPTR    8


typedef struct HctJrnlServer HctJrnlServer;

/*
** One object of this type is shared by all connections to the same 
** database. Managed by the HctFileServer object (see functions
** sqlite3HctFileGetJrnlPtr() and SetJrnlPtr()).
**
** eMode:
**   The current database mode - either SQLITE_HCT_NORMAL, SQLITE_HCT_FOLLOWER
**   or SQLITE_HCT_LEADER.
**
** iJrnlRoot:
**   Root page number of hct_journal table. This is valid in either FOLLOWER
**   or LEADER mode.
**
** iSnapshot:
**   This is meaningful in FOLLOWER mode only.
**
**   This is set to a CID value for which it and all prior transactions are
**   committed. It may be written by any client using an atomic CAS operation,
**   but may only be increased, never decreased. No transaction with a CID
**   greater than (iSnapshot + HCT_MAX_LEADING_WRITE) may be started - 
**   iSnapshot must be increased first.
**
** nCommit:
**   Size of aCommit[] array. 0 if not in FOLLOWER mode.
**
** aCommit:
**   This array is only allocated and populated if the object is in FOLLOWER
**   mode.
**
**   Say the size of the array is N (actually HctJrnlServer.nCommit). Then,
**   when transaction X is committed, slot aCommit[X % N] is set to X.
*/
struct HctJrnlServer {
  int eMode;                      /* One of NORMAL, FOLLOWER, LEADER */
  i64 iJrnlRoot;                  /* Root page of journal table */
  u64 iSnapshot;                  /* Known valid snapshot in FOLLOWER mode */
  int nCommit;                    /* Size of aCommit[] array */
  u64 *aCommit;                   /* Array of size nCommit */

  /* Used by log sub-system (see hct_log.c) */
  sqlite3_mutex *pLogPtrMutex;    /* Mutex to protect pLogPtr */
  void *pLogPtr;
};

/*
** There is one instance of this structure for each database handle (HBtree*)
** open on a replication-enabled hctree database.
**
** eInWrite:
**   Set to true while the database connection is in a call to
**   sqlite3_hct_journal_write().
*/
struct HctJournal {
  HctDatabase *pDb;
  HctJrnlServer *pServer;

  int bInJrnlCommit;
  const u8 *pJrnlData;
  int nJrnlData;
  i64 iJrnlCid;
  i64 iJrnlSnapshot;
};

#define HCT_JOURNAL_NONE          0
#define HCT_JOURNAL_INWRITE       1
#define HCT_JOURNAL_INROLLBACK    2

static void hctJournalSetDbError(
  sqlite3 *db,                    /* Database on which to set error */
  int rc,                         /* Error code */
  const char *zFormat, ...        /* Printf() error string and arguments */
){
  char *zErr = 0;
  sqlite3_mutex_enter( sqlite3_db_mutex(db) );
  if( zFormat ){
    va_list ap;
    va_start(ap, zFormat);
    zErr = sqlite3_vmprintf(zFormat, ap);
    va_end(ap);
  }
  if( zErr ){
    sqlite3ErrorWithMsg(db, rc, "%s", zErr);
    sqlite3_free(zErr);
  }else{
    sqlite3ErrorWithMsg(db, rc, 0, 0);
  }
  sqlite3_mutex_leave( sqlite3_db_mutex(db) );
}

/*
** Use database connection db to run the SQL statement passed as the second
** argument. If successful, return SQLITE_OK and set (*piRes) to any integer
** value returned by the SQL, or to 0 if no value is returned. If an error
** occurs, return an SQLite error code and set (*piRes) to 0.
*/
static int hctDbExecForInt(sqlite3 *db, const char *zSql, i64 *piRes){
  int rc = SQLITE_OK;
  sqlite3_stmt *pStmt = 0;

  *piRes = 0;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc==SQLITE_OK ){
    if( sqlite3_step(pStmt)==SQLITE_ROW ){
      *piRes = sqlite3_column_int64(pStmt, 0);
    }
    rc = sqlite3_finalize(pStmt);
  }

  return rc;
}

/*
** Initialize the main database for replication.
*/
int sqlite3_hct_journal_init(sqlite3 *db){
  /* HBtree *p = (HBtree*)db->aDb[0].pBt; */
  int rc = SQLITE_OK;

  /* Test that there is not already an open transaction on this database. */
  if( sqlite3_get_autocommit(db)==0 ){
    hctJournalSetDbError(db, SQLITE_ERROR, "open transaction on database");
    return SQLITE_ERROR;
  }

  /* Test that the main db really is an hct database. Leave rc set to 
  ** something other than SQLITE_OK and an error message in the database
  ** handle if it is not. */
  if( rc==SQLITE_OK ){
    i64 nDbFile = 0;
    rc = hctDbExecForInt(db, "PRAGMA hct_ndbfile", &nDbFile);
    if( rc==SQLITE_OK && nDbFile==0 ){
      hctJournalSetDbError(db, SQLITE_ERROR, "not an hct database");
      rc = SQLITE_ERROR;
    }
  }

  /* Open a transaction on the db */
  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(db, "BEGIN", 0, 0, 0);
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(db, HCT_JOURNAL_TABLE, 0, 0, 0);
  }

  if( rc==SQLITE_OK ){
    i64 iMax;
    rc = hctDbExecForInt(db, "SELECT max(cid) FROM hct_journal", &iMax);
    if( iMax==0 ){
      rc = sqlite3_exec(db,
          "INSERT INTO hct_journal VALUES(1, '', 0, NULL)", 0, 0, 0
      );
    }
  }

  /* Find the root page number of the new (or possibly not new) table. 
  ** Set the page-map value to the root page number.  */
  if( rc==SQLITE_OK ){
    i64 iRoot = 0;                  /* Root page of hct_journal table */
    rc = hctDbExecForInt(db, 
        "SELECT rootpage FROM sqlite_schema WHERE name='hct_journal'", &iRoot
    );
    if( rc==SQLITE_OK ){
      assert( iRoot>0 );
      /* TODO: Something with this... */
    }
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
  }
  if( rc!=SQLITE_OK ){
    char *zErr = sqlite3_mprintf("%s", sqlite3_errmsg(db));
    sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
    hctJournalSetDbError(db, rc, "%s", zErr);
    sqlite3_free(zErr);
  }

  return rc;
}

/*
** Register a custom validation callback with the database handle.
*/
int sqlite3_hct_journal_hook(
  sqlite3 *db,
  void *pArg,
  int(*xValidate)(
    void *pCopyOfArg,
    sqlite3_int64 iCid,
    const char *zSchema,
    const void *pData, int nData,
    sqlite3_int64 iSchemaCid
  )
){
  db->xValidate = xValidate;
  db->pValidateArg = pArg;
  return SQLITE_OK;
}

/*
** Value iVal is to be stored as an integer in an SQLite record. This 
** function returns the number of bytes that it will use for storage.
*/
static int hctJrnlIntSize(u64 iVal){
#define MAX_6BYTE ((((i64)0x00008000)<<32)-1)
  if( iVal<=127 ) return 1;
  if( iVal<=32767 ) return 2;
  if( iVal<=8388607 ) return 3;
  if( iVal<=2147483647 ) return 4;
  if( iVal<=MAX_6BYTE ) return 6;
  return 8;
}

/*
** Store an (nByte*8) bit big-endian integer, value iVal, in buffer a[].
*/
static void hctJrnlIntPut(u8 *a, u64 iVal, int nByte){
  int i;
  for(i=1; i<=nByte; i++){
    a[nByte-i] = (iVal & 0xFF);
    iVal = (iVal >> 8);
  }
}

/*
** Return the byte value that should be stored in the SQLite record
** header for an nSize byte integer field.
*/
static u8 hctJrnlIntHdr(int nSize){
  if( nSize==8 ) return 6;
  if( nSize==6 ) return 5;
  return nSize;
}

/*
** Compose an SQLite record suitable for the sqlite_hct_journal table.
*/
static u8 *hctJrnlComposeRecord(
  u64 iCid,
  const u8 *pData, int nData,
  u64 iSnapshot,
  int nPtr, const u8 *aPtr,
  int *pnRec
){
  u8 *pRec = 0;
  int nRec = 0;
  int nHdr = 0;
  int nBody = 0;
  int nSnapshotByte = 0;

  nSnapshotByte = hctJrnlIntSize(iSnapshot);

  /* First figure out how large the eventual record will be */
  nHdr = 1                                     /* size of header varint */
       + 1                                     /* "cid" - always NULL */
       + sqlite3VarintLen((nData * 2) + 12)    /* "query" - BLOB */
       + 1                                     /* "snapshot" - INTEGER */
       + sqlite3VarintLen((nPtr * 2) + 12);    /* "logptr" - BLOB */

  nBody = 0                                    /* "cid" - always NULL */
       + nData                                 /* "query" - BLOB */
       + nSnapshotByte                         /* "snapshot" - INTEGER */
       + nPtr;                                 /* "logptr" - BLOB */

  nRec = nBody+nHdr;
  pRec = (u8*)sqlite3_malloc(nRec);
  if( pRec ){
    u8 *pHdr = pRec;
    u8 *pBody = &pRec[nHdr];

    *pHdr++ = (u8)nHdr;           /* size-of-header varint */
    *pHdr++ = 0x00;               /* "cid" - NULL */

    /* "query" field - BLOB */
    pHdr += sqlite3PutVarint(pHdr, (nData*2) + 12);
    if( nData>0 ){
      memcpy(pBody, pData, nData);
      pBody += nData;
    }

    /* "snapshot" field - INTEGER */
    *pHdr++ = hctJrnlIntHdr(nSnapshotByte);
    hctJrnlIntPut(pBody, iSnapshot, nSnapshotByte);
    pBody += nSnapshotByte;

    /* "logptr" field - BLOB or NULL */
    pHdr += sqlite3PutVarint(pHdr, (nPtr ? ((nPtr*2) + 12): 0));
    if( nPtr>0 ){
      memcpy(pBody, aPtr, nPtr);
      pBody += nPtr;
    }

    assert( pHdr==&pRec[nHdr] );
    assert( pBody==&pRec[nRec] );
  }else{
    nRec = 0;
  }

  *pnRec = nRec;
  return pRec;
}


static int hctJrnlWriteRecord(
  HctJournal *pJrnl,
  int bNotid,
  u64 iCid,
  const void *pData, int nData,
  u64 iSnapshot,
  int nPtr, const u8 *aPtr
){
  int rc = SQLITE_OK;
  u8 *pRec = 0;
  int nRec = 0;

  pRec = hctJrnlComposeRecord(iCid, pData, nData, iSnapshot, nPtr, aPtr, &nRec);

  if( pRec==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    HctDatabase *pDb = pJrnl->pDb;
    int nRetry = 0;
    u32 iR = (u32)pJrnl->pServer->iJrnlRoot;
    do {
      
      nRetry = 0;
      if( bNotid==0 ){
        rc = sqlite3HctDbInsert(pDb, iR, 0, iCid, 0, nRec, pRec, &nRetry);
      }else{
        rc = sqlite3HctDbJrnlWrite(pDb, iR, iCid, nRec, pRec, &nRetry);
      }
      if( rc!=SQLITE_OK ) break;
      assert( nRetry==0 || nRetry==1 );
      if( nRetry==0 ){
        rc = sqlite3HctDbInsertFlush(pDb, &nRetry);
        if( rc!=SQLITE_OK ) break;
      }
    }while( nRetry );
  }
  sqlite3_free(pRec);

  return rc;
}

int sqlite3HctJrnlLog(
  HctJournal *pJrnl, 
  u64 iCid, 
  u64 iSnapshot, 
  int nPtr,
  const u8 *aPtr,
  int rcin
){
  int rc = rcin;
  if( pJrnl->pJrnlData ){
    if( rcin==SQLITE_OK ){
      HctJrnlServer *pServer = pJrnl->pServer;
      if( pServer->eMode==SQLITE_HCT_FOLLOWER ){
        assert( iCid==pJrnl->iJrnlCid );
        rc = hctJrnlWriteRecord(pJrnl, 0, iCid, 
            pJrnl->pJrnlData, pJrnl->nJrnlData, pJrnl->iJrnlSnapshot, nPtr, aPtr
        );

        /* Set the entry in HctJrnlServer.aCommit[] to indicate that this
        ** transaction has been committed.  */
        HctAtomicStore(&pServer->aCommit[iCid % pServer->nCommit], iCid);

      }else{
        assert( pServer->eMode==SQLITE_HCT_LEADER );
        rc = hctJrnlWriteRecord(pJrnl, 0, iCid,
            pJrnl->pJrnlData, pJrnl->nJrnlData, iSnapshot, nPtr, aPtr
        );
        pJrnl->iJrnlSnapshot = iSnapshot;
      }
    }else{
      hctJrnlWriteRecord(pJrnl, 1, iCid, 0, 0, 0, 0, 0);
      pJrnl->iJrnlSnapshot = 0;
    }
    pJrnl->iJrnlCid = iCid;
  }
  return rc;
}

static void hctJrnlDelServer(void *p){
  if( p ){
    HctJrnlServer *pServer = (HctJrnlServer*)p;
    sqlite3HctLogFree(pServer->pLogPtr, 0);
    sqlite3_mutex_free(pServer->pLogPtrMutex);
    sqlite3_free(pServer->aCommit);
    sqlite3_free(pServer);
  }
}

typedef struct HctJournalRecord HctJournalRecord;
struct HctJournalRecord {
  i64 iCid;
  const char *zSchema; int nSchema;
  const void *pData; int nData;
  i64 iSchemaCid;
  const void *pHash;
  i64 iTid;
  i64 iValidCid;
};

int sqlite3HctJrnlSavePhysical( 
  sqlite3 *db,
  HctJournal *pJrnl, 
  int (*xSave)(void*, i64 iPhys), 
  void *pSave
){
  return SQLITE_OK;
}

static void hctJrnlTryToFreeLog(HctJournal *pJrnl){
  HctJrnlServer *pServer = pJrnl->pServer;
  if( pServer->pLogPtr ){
    u64 iSnap = sqlite3HctJrnlSnapshot(pJrnl);
    void *pFree = 0;
    sqlite3_mutex_enter(pServer->pLogPtrMutex);
    pFree = sqlite3HctLogFindLogToFree(&pServer->pLogPtr, iSnap);
    sqlite3_mutex_leave(pServer->pLogPtrMutex);
    sqlite3HctLogFree(pFree, 1);
  }
}

int sqlite3HctJournalNew(
  HctDatabase *pDb, 
  HctJournal **pp
){
  int rc = SQLITE_OK;
  HctJournal *pNew;

  assert( *pp==0 );
  pNew = sqlite3HctMallocRc(&rc, sizeof(HctJournal));
  if( pNew ){
    HctFile *pFile = sqlite3HctDbFile(pDb);
    pNew->pDb = pDb;
    pNew->pServer = (HctJrnlServer*)sqlite3HctFileGetJrnlPtr(pFile);
    *pp = pNew;
  }

  hctJrnlTryToFreeLog(pNew);

  return rc;
}

void sqlite3HctJournalClose(HctJournal *pJrnl){
  if( pJrnl ){
    hctJrnlTryToFreeLog(pJrnl);
    sqlite3_free(pJrnl);
  }
}

/*
** See description in hctJrnlInt.h.
*/
int sqlite3HctJournalIsNosnap(HctJournal *pJrnl, i64 iTable){
  HctJrnlServer *pServer = pJrnl->pServer;
  if( pServer->eMode!=SQLITE_HCT_NORMAL && pServer->iJrnlRoot==iTable ){
    return 1;
  }
  return 0;
}

/*
** Called during log file recovery to remove the entry with cid value iCid.
*/
int sqlite3HctJrnlRollbackEntry(HctJournal *pJrnl, i64 iCid){
  int rc = SQLITE_OK;
  int nRetry = 0;
  i64 iJrnlRoot = pJrnl->pServer->iJrnlRoot;

  rc = sqlite3HctDbInsert(pJrnl->pDb, iJrnlRoot, 0, iCid, 1, 0, 0, &nRetry);

  /* This is part of recovery, so this client should have exclusive access
  ** to the db. Therefore there is no chance of requiring retries. */
  assert( nRetry==0 );
  if( rc==SQLITE_OK ){
    rc = sqlite3HctDbInsertFlush(pJrnl->pDb, &nRetry);
    assert( nRetry==0 );
  }

  return rc;
}

/*
** Find the HctJournal object associated with the "main" database of the
** connection passed as the only argument. If successful, set (*ppJrnl)
** to point to said object and return SQLITE_OK. Or, if the database is
** not a replication-enabled db, set (*ppJrnl) to NULL and return SQLITE_OK.
** Or, if an error occurs, return an SQLite error code. The final value
** of (*ppJrnl) is undefined in this case.
*/
static int hctJrnlFind(sqlite3 *db, HctJournal **ppJrnl){
  int rc = SQLITE_OK;
  HctJournal *pJrnl = sqlite3HctJrnlFind(db);

  if( pJrnl==0 ){
    /* If the journal was not found, it might be because the database is
    ** not yet initialized. Run a query to ensure it is, then try to retrieve
    ** the journal object again.  */
    rc = sqlite3_exec(db, "SELECT 1 FROM sqlite_schema LIMIT 1", 0, 0, 0);
    if( rc==SQLITE_OK ){
      pJrnl = sqlite3HctJrnlFind(db);
    }
  }

  if( rc==SQLITE_OK && pJrnl==0 ){
    hctJournalSetDbError(db, SQLITE_ERROR, "not a journaled hct database");
    rc = SQLITE_ERROR;
  }

  *ppJrnl = pJrnl;
  return rc;
}


/*
** Return the current journal mode - SQLITE_HCT_JOURNAL_MODE_FOLLOWER or
** SQLITE_HCT_JOURNAL_MODE_LEADER - for the main database of the connection
** passed as the only argument. Or, if the main database is not an hct
** database, return -1;
*/
int sqlite3_hct_journal_mode(sqlite3 *db){
  int eRet = -1;
  HctJournal *pJrnl = sqlite3HctJrnlFind(db);
  if( pJrnl ){
    eRet = pJrnl->pServer->eMode;
  }
  return eRet;
}

static void hctJrnlFinalize(int *pRc, sqlite3_stmt *pStmt){
  int rc = sqlite3_finalize(pStmt);
  if( *pRc==SQLITE_OK ){
    *pRc = rc;
  }
}

/*
** An array of the following type is used to cache rows from the hct_journal
** within the sqlite3_hct_journal_setmode() function.
*/
typedef struct HctJrnlRow HctJrnlRow;
struct HctJrnlRow {
  i64 iCid;
  i64 iTid;
};

/*
** Set the NORMAL/LEADER/FOLLOWER setting of the main database of the 
** connection passed as the first argument.
**
** If the mode is changing from NORMAL to either FOLLOWER or LEADER:
**
**   1) 
*/
int sqlite3_hct_journal_setmode(sqlite3 *db, int eMode){
  int rc = SQLITE_OK;
  HctJournal *pJrnl = 0;
  int eCurrentMode = 0;
  HctJrnlServer *pServer = 0;
  HctFile *pFile = 0;

  /* Ensure the db is fully initialized, then find the HctJournal object
  ** that belongs to the main db of this connection.  */
  rc = sqlite3_exec(db, "SELECT 1 FROM sqlite_schema LIMIT 1", 0, 0, 0);
  if( rc!=SQLITE_OK ) return rc;
  pJrnl = sqlite3HctJrnlFind(db);
  pServer = pJrnl->pServer;
  pFile = sqlite3HctDbFile(pJrnl->pDb);

  /* If the mode is already set as desired, return early. */
  eCurrentMode = pServer->eMode;
  if( eCurrentMode==eMode ) return SQLITE_OK;

  /* Changing from NORMAL to either FOLLOWER or LEADER is the complicated
  ** case. We need to initialize both the transaction map, and the
  ** HctJrnlServer.aCommit[] array based on the contents of the hct_journal
  ** table.  
  **
  ** We assume that there are no other readers or writers of the database
  ** at this point. It is the callers responsibility to ensure thus.  
  */
  if( eCurrentMode==SQLITE_HCT_NORMAL ){
    i64 iCidMax = 0;
    u64 *aCommit = 0;


    rc = hctDbExecForInt(db, "SELECT max(cid) FROM hct_journal", &iCidMax);
    aCommit = (u64*)sqlite3HctMallocRc(&rc, HCT_MAX_LEADING_WRITE*sizeof(u64));

    if( rc==SQLITE_OK ){
      rc = hctDbExecForInt(db, 
          "SELECT rootpage FROM sqlite_schema WHERE name = 'hct_journal'", 
          &pServer->iJrnlRoot
      );
    }

    /* Populate a new transaction-map. */
    if( rc==SQLITE_OK ){
      HctTMapClient *pTClient = sqlite3HctFileTMapClient(pFile);
      i64 iLastTid = sqlite3HctFilePeekTransid(pFile);
      i64 iFirstTid = MAX(1, (iLastTid+1 - HCT_MAX_LEADING_WRITE));
      i64 ii;

      for(ii=iFirstTid; rc==SQLITE_OK && ii<=iLastTid; ii++){
        rc = sqlite3HctTMapRecoverySet(pTClient, ii, iCidMax);
      }

      sqlite3HctTMapRecoveryFinish(pTClient, rc);
    }

    if( rc==SQLITE_OK ){
      pServer->iSnapshot = iCidMax;
      pServer->nCommit = HCT_MAX_LEADING_WRITE;
      pServer->aCommit = aCommit;
      pServer->eMode = eMode;
      sqlite3HctFileSetCID(pFile, iCidMax);
    }

  }else{

    if( eCurrentMode==SQLITE_HCT_FOLLOWER ){
      u64 iSnap = pServer->iSnapshot;
      u64 iTest = 0;
      int bSeenMiss = 0;

      /* Check that the journal is contiguous. Set rc to SQLITE_ERROR and 
      ** leave an error message in the db handle if it is not. */
      for(iTest=pServer->iSnapshot+1; 
          iTest<pServer->iSnapshot+pServer->nCommit;
          iTest++
      ){
        u64 iVal = HctAtomicLoad(&pServer->aCommit[iTest % pServer->nCommit]);
        if( iVal==iTest ){
          if( bSeenMiss ){
            rc = SQLITE_ERROR;
            hctJournalSetDbError(db, rc, "non-contiguous journal table");
            break;
          }
          iSnap = iTest;
        }else{
          bSeenMiss = 1;
        }
      }

      assert( iSnap!=0 );
      sqlite3HctFileSetCID(pFile, iSnap);
    }

    if( rc==SQLITE_OK ){
      if( eMode==SQLITE_HCT_NORMAL ){
        /* Switching from LEADER or FOLLOWER back to NORMAL. */
        sqlite3_free(pServer->aCommit);
        pServer->iSnapshot = 0;
        pServer->nCommit = 0;
        pServer->aCommit = 0;
      }else

      if( eMode==SQLITE_HCT_FOLLOWER ){
        /* Switching from LEADER to FOLLOWER */
        assert( eCurrentMode==SQLITE_HCT_LEADER );
        HctAtomicStore(&pServer->iSnapshot, sqlite3HctFileGetSnapshotid(pFile));
      }else{ 
        /* Switching from FOLLOWER to LEADER */
        assert( eMode==SQLITE_HCT_LEADER );
        assert( eCurrentMode==SQLITE_HCT_FOLLOWER );
      }
    }
  }

  if( rc==SQLITE_OK ){
    HctAtomicStore(&pServer->eMode, eMode);
  }
  return rc;
}

void sqlite3HctJournalSchemaVersion(HctJournal *pJrnl, u32 *pSchemaVersion){
#if 0
  if( pJrnl && pJrnl->pServer ){
    *pSchemaVersion += HctAtomicLoad(&pJrnl->pServer->nSchemaVersionIncr);
  }
#endif
}

u64 sqlite3HctJrnlSnapshot(HctJournal *pJrnl){
  u64 iRet = 0;
  if( pJrnl ){
    HctJrnlServer *pServer = pJrnl->pServer;
    if( pServer->eMode==SQLITE_HCT_FOLLOWER ){
      u64 iTest = 0;

      u64 iSnap = HctAtomicLoad(&pServer->iSnapshot);
      iRet = iSnap;
      for(iTest=iRet+1; 1; iTest++){
        u64 iVal = HctAtomicLoad(&pServer->aCommit[iTest % pServer->nCommit]);
        if( iVal!=iTest ) break;
        iRet = iTest;
      }

      /* Update HctJrnlServer.iSnapshot if required and if possible */
      if( iRet>=iSnap+16 ){
        (void)HctCASBool(&pServer->iSnapshot, iSnap, iRet);
      }
    }
  }
  return iRet;
}

/*
** Set output variable (*piCid) to the CID of the newest available 
** database snapshot. Return SQLITE_OK if successful, or an SQLite
** error code if something goes wrong.
*/
int sqlite3_hct_journal_snapshot(sqlite3 *db, sqlite3_int64 *piCid){
  int rc = SQLITE_OK;
  HctJournal *pJrnl = 0;

  rc = hctJrnlFind(db, &pJrnl);
  if( rc==SQLITE_OK ){
    *piCid = (i64)sqlite3HctJrnlSnapshot(pJrnl);
  }else{
    *piCid = 0;
  }
  return rc;
}

int sqlite3HctJrnlInit(sqlite3 *db){
  int rc = SQLITE_OK;
  return rc;
}

int sqlite3HctJournalServerNew(void **pJrnlPtr){
  int rc = SQLITE_OK;
  HctJrnlServer *pNew = 0;
  pNew = (HctJrnlServer*)sqlite3HctMallocRc(&rc, sizeof(HctJrnlServer));
  pNew->pLogPtrMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  *pJrnlPtr = (void*)pNew;
  return rc;
}

void sqlite3HctJournalServerFree(void *pJrnlPtr){
  hctJrnlDelServer(pJrnlPtr);
}

int sqlite3_hct_journal_follower_commit(
  sqlite3 *db,                    /* Commit transaction for this db handle */
  const unsigned char *aData,     /* Data to write to "query" column */
  int nData,                      /* Size of aData[] in bytes */
  sqlite3_int64 iCid,             /* CID of committed transaction */
  sqlite3_int64 iSnapshot         /* Value for hct_journal.snapshot field */
){
  HctJournal *pJrnl = sqlite3HctJrnlFind(db);
  int rc = SQLITE_OK;

  /* Check the db really is in FOLLOWER mode */
  if( pJrnl->pServer->eMode!=SQLITE_HCT_FOLLOWER ){
    hctJournalSetDbError(db, SQLITE_ERROR, "not a FOLLOWER mode db");
    return SQLITE_ERROR;  
  }

  assert( pJrnl->iJrnlCid==0 );
  assert( pJrnl->iJrnlSnapshot==0 );
  assert( pJrnl->nJrnlData==0 );
  assert( pJrnl->pJrnlData==0 );
  assert( pJrnl->bInJrnlCommit==0 );

  pJrnl->pJrnlData = aData;
  pJrnl->nJrnlData = nData;
  pJrnl->iJrnlCid = iCid;
  pJrnl->iJrnlSnapshot = iSnapshot;

  pJrnl->bInJrnlCommit = 1;
  rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
  pJrnl->bInJrnlCommit = 0;

  pJrnl->pJrnlData = 0;
  pJrnl->nJrnlData = 0;
  pJrnl->iJrnlCid = 0;
  pJrnl->iJrnlSnapshot = 0;

  return rc;
}

/*
** Commit a leader transaction.
*/
int sqlite3_hct_journal_leader_commit(
  sqlite3 *db,                    /* Commit transaction for this db handle */
  const unsigned char *aData,     /* Data to write to "query" column */
  int nData,                      /* Size of aData[] in bytes */
  sqlite3_int64 *piCid,           /* OUT: CID of committed transaction */
  sqlite3_int64 *piSnapshot       /* OUT: Min. snapshot to recreate */
){
  HctJournal *pJrnl = sqlite3HctJrnlFind(db);
  int rc = SQLITE_OK;

  /* Check the db really is in LEADER mode */
  if( pJrnl->pServer->eMode!=SQLITE_HCT_LEADER ){
    hctJournalSetDbError(db, SQLITE_ERROR, "not a LEADER mode db");
    return SQLITE_ERROR;  
  }

  assert( pJrnl->iJrnlCid==0 );
  assert( pJrnl->iJrnlSnapshot==0 );
  assert( pJrnl->nJrnlData==0 );
  assert( pJrnl->pJrnlData==0 );

  pJrnl->pJrnlData = aData;
  pJrnl->nJrnlData = nData;

  pJrnl->bInJrnlCommit = 1;
  rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
  pJrnl->bInJrnlCommit = 0;

  *piCid = pJrnl->iJrnlCid;
  *piSnapshot = pJrnl->iJrnlSnapshot;

  pJrnl->pJrnlData = 0;
  pJrnl->nJrnlData = 0;
  pJrnl->iJrnlCid = 0;
  pJrnl->iJrnlSnapshot = 0;

  return rc;
}

int sqlite3HctJrnlCommitOk(HctJournal *pJrnl){
  if( pJrnl->pServer->eMode!=SQLITE_HCT_NORMAL && pJrnl->bInJrnlCommit==0 ){
    return 0;
  }
  return 1;
}

u64 sqlite3HctJrnlFollowerModeCid(HctJournal *pJrnl){
  assert( (pJrnl->iJrnlCid!=0)==(pJrnl->pServer->eMode==SQLITE_HCT_FOLLOWER) );
  return pJrnl->iJrnlCid;
}

void sqlite3HctJrnlSetRoot(HctJournal *pJrnl, Schema *pSchema){
  Table *pTab = (Table*)sqlite3HashFind(&pSchema->tblHash, "hct_journal");
  if( pTab ){
    HctAtomicStore(&pJrnl->pServer->iJrnlRoot, (i64)pTab->tnum);
  }
}

int sqlite3HctJrnlMode(HctJournal *pJrnl){
  return HctAtomicLoad(&pJrnl->pServer->eMode);
}

int sqlite3HctJrnlRecoveryMode(sqlite3 *db, HctJournal *pJrnl, int *peMode){
  int rc = SQLITE_OK;
  int eMode = SQLITE_HCT_NORMAL;
  if( pJrnl->pServer->iJrnlRoot ){
    const char *zSql = "SELECT length(logptr), max(cid) FROM hct_journal";
    i64 sz = 0;
    rc = hctDbExecForInt(db, zSql, &sz);
    if( sz==SIZEOF_FOLLOWER_LOGPTR ){
      eMode = SQLITE_HCT_FOLLOWER;
    }else{
      eMode = SQLITE_HCT_LEADER;
    }
  }

  *peMode = eMode;
  return rc;
}

/*
** Parameter aInt points to a sorted list of nInt 64-bit integer values.
** Return true if iVal appears in this list, or false otherwise.
*/
static int hctListContains(int nInt, i64 *aInt, i64 iVal){
  int i1 = 0;
  int i2 = nInt;
  while( i2>i1 ){
    int iTest = (i1 + i2) / 2;
    if( aInt[iTest]==iVal ){
      return 1;
    }
    if( aInt[iTest]<iVal ){
      i1 = iTest+1;
    }else{
      i2 = iTest;
    }
  }
  return 0;
}

int sqlite3HctJrnlFindLogs(
  sqlite3 *db,
  HctJournal *pJrnl, 
  int nDel, i64 *aDel,
  void *pCtx,
  int (*xLog)(void*, i64, int, const u8*),
  int (*xMap)(void*, i64, i64)
){
  int rc = SQLITE_OK;
  if( pJrnl->pServer->iJrnlRoot ){
    const char *zSql = 
      "SELECT cid, logptr FROM hct_journal WHERE cid> ( "
      "  SELECT max(cid)-? FROM hct_journal"
      ") ORDER BY cid ASC";
    sqlite3_stmt *pSql = 0;

    rc = sqlite3_prepare_v2(db, zSql, -1, &pSql, 0);
    if( rc==SQLITE_OK ){
      i64 iPrev = -1;
      sqlite3_bind_int(pSql, 1, HCT_MAX_LEADING_WRITE);
      while( SQLITE_ROW==sqlite3_step(pSql) ){
        i64 iCid = sqlite3_column_int64(pSql, 0);
        int nPtr = sqlite3_column_bytes(pSql, 1);
        const u8 *aPtr = (const u8*)sqlite3_column_blob(pSql, 1);

        if( nPtr>=sizeof(i64) ){
          i64 iVal;
          memcpy(&iVal, aPtr, sizeof(i64));
          if( hctListContains(nDel, aDel, iVal) ){
            continue;
          }
        }

        if( nPtr==SIZEOF_FOLLOWER_LOGPTR && iPrev>=0 && iPrev!=iCid-1 ){
          rc = xLog(pCtx, iCid, nPtr, aPtr);
        }else{
          iPrev = iCid;
        }

        if( rc==SQLITE_OK ){
          i64 iTid = 0;
          if( aPtr ) memcpy(&iTid, aPtr, sizeof(iTid));
          rc = xMap(pCtx, iCid, iTid);
        }
      }
      hctJrnlFinalize(&rc, pSql);
    }
  }

  return rc;
}

void **sqlite3HctJrnlLogPtrPtr(HctJournal *pJrnl){
  sqlite3_mutex_enter(pJrnl->pServer->pLogPtrMutex);
  return &pJrnl->pServer->pLogPtr;
}
void sqlite3HctJrnlLogPtrPtrRelease(HctJournal *pJrnl){
  sqlite3_mutex_leave(pJrnl->pServer->pLogPtrMutex);
}

/*
** This is called as part of stage 1 recovery if the journal mode before
** the restart was LEADER. It writes a zero-entry to the hct_journal table
** for each CID value in the aEntry[] array.
**
** Return SQLITE_OK if successful, or an SQLite error code otherwise.
*/
int sqlite3HctJrnlZeroEntries(HctJournal *pJrnl, int nEntry, i64 *aEntry){
  int ii;
  int rc = SQLITE_OK;
  for(ii=0; rc==SQLITE_OK && ii<nEntry; ii++){
    rc = hctJrnlWriteRecord(pJrnl, 1, aEntry[ii], 0, 0, 0, 0, 0);
  }

  return rc;
}

