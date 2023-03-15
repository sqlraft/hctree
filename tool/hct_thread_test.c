
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include <assert.h>
#include <sys/types.h> 
#include <sys/stat.h> 
#include <sys/time.h> 
#include <sys/resource.h> 
#include <fcntl.h>
#include <errno.h>

#include "sqlite3.h"

#include <valgrind/callgrind.h>


typedef sqlite3_int64 i64;
typedef unsigned int u32;

#define HST_DATABASE_NAME "hct_thread.db"

typedef struct Error Error;
typedef struct Thread Thread;
typedef struct Threadset Threadset;

struct Error {
  int rc;
  int iLine;
  char *zErr;
};

struct Thread {
  int iTid;                       /* Thread number within test */
  void* pArg;                     /* Pointer argument passed by caller */

  pthread_t tid;                  /* Thread id */
  char *(*xProc)(int,void*);      /* Thread main proc */
  char *zRes;                     /* Value returned by xProc */
  Thread *pNext;                  /* Next in this list of threads */
};

struct Threadset {
  int iNextTid;                   /* Next iTid value to allocate */
  Thread *pThread;                /* Linked list of threads */
};

/*
** State of a simple PRNG used for the per-connection and per-pager
** pseudo-random number generators.
*/
typedef struct FastPrng FastPrng;
struct FastPrng {
  u32 x, y;
};

/*
** Generate N bytes of pseudo-randomness using a FastPrng
*/
static void httFastRandomness(FastPrng *pPrng, int N, void *P){
  unsigned char *pOut = (unsigned char*)P;
  while( N-->0 ){
    pPrng->x = ((pPrng->x)>>1) ^ ((1+~((pPrng->x)&1)) & 0xd0000001);
    pPrng->y = (pPrng->y)*1103515245 + 12345;
    *(pOut++) = (pPrng->x ^ pPrng->y) & 0xff;
  }
}

static void frandomFunc(sqlite3_context *ctx, int nArg, sqlite3_value **aArg){
  FastPrng *p = (FastPrng*)sqlite3_user_data(ctx);
  i64 ret;
  httFastRandomness(p, sizeof(ret), &ret);
  sqlite3_result_int64(ctx, ret);
}

static void frandomBlobFunc(
  sqlite3_context *ctx, 
  int nArg, 
  sqlite3_value **aArg
){
  FastPrng *p = (FastPrng*)sqlite3_user_data(ctx);
  int nBlob = sqlite3_value_int(aArg[0]);
  unsigned char *aBlob = 0;

  aBlob = (unsigned char*)sqlite3_malloc(nBlob);
  httFastRandomness(p, nBlob, aBlob);
  sqlite3_result_blob(ctx, aBlob, nBlob, SQLITE_TRANSIENT);
  sqlite3_free(aBlob);
}



static sqlite3 *htt_sqlite3_open(const char *zDb){
  sqlite3 *db = 0;
  int flags = SQLITE_OPEN_CREATE|SQLITE_OPEN_READWRITE
             |SQLITE_OPEN_NOMUTEX|SQLITE_OPEN_URI;
  int rc = sqlite3_open_v2(zDb, &db, flags, 0);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "error in sqlite3_open: (%d) %s\n", rc, sqlite3_errmsg(db));
    exit(1);
  }else{
    FastPrng *p = (FastPrng*)sqlite3_malloc(sizeof(FastPrng));
    sqlite3_randomness(sizeof(FastPrng), (void*)p);
    p->x |= 1;
    sqlite3_create_function(
        db, "frandom", 0, SQLITE_ANY, (void*)p, frandomFunc, 0, 0
    );
    sqlite3_create_function(
        db, "frandomblob", 1, SQLITE_ANY, (void*)p, frandomBlobFunc, 0, 0
    );
  }
  return db;
}

static void htt_sqlite3_exec(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "error in sqlite3_exec: (%d) %s\n", rc, zErr);
    exit(1);
  }
}

#define SEL(e) ((e)->iLine = ((e)->rc ? (e)->iLine : __LINE__))


static void htt_sqlite3_exec_printf(
  Error *pErr,
  sqlite3 *db, 
  const char *zFmt, ...
){
  if( pErr->rc==SQLITE_OK ){
    int rc;
    char *zSql = 0;
    va_list ap;
    va_start(ap, zFmt);
    zSql = sqlite3_vmprintf(zFmt, ap);
    va_end(ap);

    rc = sqlite3_exec(db, zSql, 0, 0, 0);
    if( rc!=SQLITE_OK ){
      pErr->rc = rc;
      pErr->zErr = sqlite3_mprintf("sqlite3_exec: %s", sqlite3_errmsg(db));
    }
    sqlite3_free(zSql);
  }
}

static sqlite3_stmt *htt_sqlite3_prepare_printf(
  Error *pErr, 
  sqlite3 *db, 
  const char *zFmt,
  ...
){
  sqlite3_stmt *pRet = 0;
  if( pErr->rc==SQLITE_OK ){
    int rc;
    char *zSql = 0;
    va_list ap;
    va_start(ap, zFmt);
    zSql = sqlite3_vmprintf(zFmt, ap);
    va_end(ap);

    rc = sqlite3_prepare_v2(db, zSql, -1, &pRet, 0);
    if( rc!=SQLITE_OK ){
      pErr->rc = rc;
      pErr->zErr = sqlite3_mprintf("sqlite3_prepare: %s", sqlite3_errmsg(db));
    }
    sqlite3_free(zSql);
  }
  return pRet;
}


static sqlite3_stmt *htt_sqlite3_prepare(
  Error *pErr, 
  sqlite3 *db, 
  const char *zSql
){
  sqlite3_stmt *pRet = 0;
  if( pErr->rc==SQLITE_OK ){
    int rc = sqlite3_prepare_v2(db, zSql, -1, &pRet, 0);
    if( rc!=SQLITE_OK ){
      pErr->rc = rc;
      pErr->zErr = sqlite3_mprintf("sqlite3_prepare: %s", sqlite3_errmsg(db));
    }
  }
  return pRet;
}
#define htt_sqlite3_prepare(pErr, db, zSql) ( \
  SEL(pErr),                                  \
  htt_sqlite3_prepare(pErr, db, zSql)         \
)

static void htt_free_err(Error *pErr){
  sqlite3_free(pErr->zErr);
  memset(pErr, 0, sizeof(Error));
}

static void htt_print_and_free_err(Error *pErr){
  if( pErr->rc!=SQLITE_OK ){
    printf("Error: line %d: (rc=%d) %s\n", pErr->iLine, pErr->rc, pErr->zErr);
  }
  htt_free_err(pErr);
}

static void htt_sqlite3_reset(Error *pErr, sqlite3_stmt *pStmt){
  if( pErr->rc==SQLITE_OK ){
    int rc = sqlite3_reset(pStmt);
    if( rc!=SQLITE_OK ){
      pErr->rc = rc;
      pErr->zErr = sqlite3_mprintf(
          "sqlite3_reset: %s\n", sqlite3_errmsg(sqlite3_db_handle(pStmt))
      );
    }
  }
}
#define htt_sqlite3_reset(pErr, pStmt) ( \
  SEL(pErr),                             \
  htt_sqlite3_reset(pErr, pStmt)         \
)

static i64 htt_sqlite3_exec_i64(
  Error *pErr, 
  sqlite3 *db, 
  const char *zSql
){
  i64 iRet = 0;
  sqlite3_stmt *p = htt_sqlite3_prepare(pErr, db, zSql);
  if( p && SQLITE_ROW==sqlite3_step(p) ){
    iRet = sqlite3_column_int64(p, 0);
  }
  htt_sqlite3_reset(pErr, p);
  sqlite3_finalize(p);
  return iRet;
}
#define htt_sqlite3_exec_i64(pErr, db, zSql) ( \
  SEL(pErr),                                   \
  htt_sqlite3_exec_i64(pErr, db, zSql)         \
)

static void htt_sqlite3_exec_debug(Error *pErr, sqlite3 *db, const char *zSql){
  sqlite3_stmt *p = htt_sqlite3_prepare(pErr, db, zSql);
  while( p && SQLITE_ROW==sqlite3_step(p) ){
    int nCol = sqlite3_data_count(p);
    int iCol;
    for(iCol=0; iCol<nCol; iCol++){
      const char *z = (const char*)sqlite3_column_text(p, iCol);
      printf("%s%s", z ? z : "(null)", iCol==nCol-1 ? "\n" : "|");
    }
  }
  htt_sqlite3_reset(pErr, p);
  sqlite3_finalize(p);
}
#define htt_sqlite3_exec_debug(pErr, db, zSql) ( \
  SEL(pErr),                                     \
  htt_sqlite3_exec_debug(pErr, db, zSql)         \
)


static void system_error(Error *pErr, int iSys){
  pErr->rc = iSys;
  pErr->zErr = (char *)sqlite3_malloc(512);
  strerror_r(iSys, pErr->zErr, 512);
  pErr->zErr[511] = '\0';
}

static void *launch_thread_main(void *pArg){
  Thread *p = (Thread *)pArg;
  p->zRes = p->xProc(p->iTid, p->pArg);
  return 0;
}

static void htt_launch_thread(
  Error *pErr,                    /* IN/OUT: Error code */
  Threadset *pThreads,            /* Thread set */
  char *(*xProc)(int,void*),      /* Proc to run */
  void *pArg                      /* Argument passed to thread proc */
){
  if( pErr->rc==SQLITE_OK ){
    int iTid = pThreads->iNextTid++;
    Thread *p;
    int rc;

    p = (Thread *)sqlite3_malloc(sizeof(Thread));
    memset(p, 0, sizeof(Thread));
    p->iTid = iTid;
    p->pArg = pArg;
    p->xProc = xProc;

    rc = pthread_create(&p->tid, NULL, launch_thread_main, (void *)p);
    if( rc!=0 ){
      system_error(pErr, rc);
      sqlite3_free(p);
    }else{
      p->pNext = pThreads->pThread;
      pThreads->pThread = p;
    }
  }
}

static sqlite3_int64 htt_current_time(){
  struct timeval sNow;
  gettimeofday(&sNow, 0);
  return (sqlite3_int64)sNow.tv_sec*1000 + sNow.tv_usec/1000;
}


static void htt_join_threads(
  Error *pErr,                    /* IN/OUT: Error code */
  Threadset *pThreads             /* Thread set */
){
  Thread *p;
  Thread *pNext;
  for(p=pThreads->pThread; p; p=pNext){
    void *ret = 0;
    int rc = SQLITE_OK;
    pNext = p->pNext;

    rc = pthread_join(p->tid, &ret);

    if( rc!=0 ){
      if( pErr->rc==SQLITE_OK ) system_error(pErr, rc);
    }else{
      printf("Thread %d says: %s\n", 
          p->iTid, (p->zRes==0 ? "..." : p->zRes)
      );
      fflush(stdout);
    }
    sqlite3_free(p->zRes);
    sqlite3_free(p);
  }
  pThreads->pThread = 0;
}

static void htt_sqlite3_close(sqlite3 *db){
  int rc = sqlite3_close(db);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "error in sqlite3_close: (%d)\n", rc);
    assert( 0 );
    exit(1);
  }
}


///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

typedef struct Testcase Testcase;
struct Testcase {
  int nSecond;
  int nMinInsert;
  int nBlob;
  int nIdx;
  int nRow;
  int nUpdate;
  int nThread;
  int nTryBeforeUnevict;
  int bSeparate;                  /* Separate database per thread */
  int bSeptab;                    /* Separate table per thread */
  int nSleep;
  int nScan;
  int szScan;
  int bOverflow;     
  int bTrust;     
};

typedef struct TestCtx TestCtx;
struct TestCtx {
  Testcase *pTst;
  int nTotalTrans;                /* Total number of transactions attempted */
  int nBusyTrans;                 /* Number of SQLITE_BUSY errors */
  u32 *aVal;
};

/*
** Globals used by this test program.
*/
static struct TestGlobal {
  sqlite3_int64 iTimeToStop;
} g;

typedef struct TestUpdate TestUpdate;
struct TestUpdate {
  int iRow;
  u32 iVal;
};


/*
** Implementation of SQL function:
**
**   updateblob(BLOB, IDX, IVAL, OLDVAL)
**
** This function treats the input blob as an array of 32-bit unsigned 
** integers. In machine byte order. It returns a copy of BLOB with entry
** IDX set to IVAL.
*/
static void updateBlobFunc(
  sqlite3_context *pCtx, 
  int nArg, 
  sqlite3_value **apArg
){
  int nBlob = 0;
  u32 *aBlob = 0;
  u32 *aCopy= 0;
  int iIdx = 0;
  u32 iVal = 0;
  u32 iOldVal = 0;

  nBlob = sqlite3_value_bytes(apArg[0]);
  aBlob = (u32*)sqlite3_value_blob(apArg[0]);

  iIdx = sqlite3_value_int(apArg[1]);
  iVal = (u32)(sqlite3_value_int64(apArg[2]) & 0xFFFFFFFF);
  iOldVal = (u32)(sqlite3_value_int64(apArg[3]) & 0xFFFFFFFF);

#if 0
  /* This fails if the same row is updated twice by the transaction... */
  if( aBlob[iIdx]!=iOldVal ){
    char *zErr = sqlite3_mprintf(
        "updateblob mismatch - iIdx=%d iVal=%lld iOldVal=%lld", 
        iIdx, (i64)iVal, (i64)iOldVal
    );
    sqlite3_result_error(pCtx, zErr, -1);
    sqlite3_free(zErr);
    return;
  }
#endif

  aCopy = sqlite3_malloc(nBlob);
  memcpy(aCopy, aBlob, nBlob);
  aCopy[iIdx] = iVal;

  sqlite3_result_blob(pCtx, aCopy, nBlob, SQLITE_TRANSIENT);
  sqlite3_free(aCopy);
}

static char *testGetStats(Error *pErr, sqlite3 *db){
  sqlite3_stmt *p = 0;
  char *zRet = 0;

  p = htt_sqlite3_prepare(pErr, db, 
      "SELECT subsys||'/'||stat, val FROM hctstats"
  );
  while( SQLITE_ROW==sqlite3_step(p) ){
    const char *zStat = (const char*)sqlite3_column_text(p, 0);
    i64 iVal = sqlite3_column_int64(p, 1);
    zRet = sqlite3_mprintf("%z\n        % -24s    %lld", zRet, zStat, iVal);
  }
  htt_sqlite3_reset(pErr, p);
  sqlite3_finalize(p);

  return zRet;
}

static char *test_thread(int iTid, void *pArg){
  TestCtx *pCtx = (TestCtx*)pArg;
  Testcase *pTst = pCtx->pTst;
  sqlite3 *db = 0;
  sqlite3_stmt *pBegin = 0;
  sqlite3_stmt *pCommit = 0;
  sqlite3_stmt *pRollback = 0;
  sqlite3_stmt *pWrite = 0;
  sqlite3_stmt *pSelect = 0;
  sqlite3_stmt *pScan = 0;

  const i64 nInterval = 2000;
  i64 iStartTime = 0;              /* Start of first loop */
  i64 iEndTime = 0;                /* Start of first loop */
  i64 iIntervalTime = 0;           /* Start of current interval */
  int iIntervalWrite = 0;          /* nWrite @ start of interval */
  int nWrite = 0;                  /* Total number of writes */
  int nBusy = 0;                   /* Total number of SQLITE_BUSY errors */
  char *zRet = 0;
  i64 nCasFail = 0;

  FastPrng prng = {0,0};
  Error err = {0,0,0};
  int nError = 0;
  char *zStat = 0;

  TestUpdate *aUpdate = 0;
  char *zFile = sqlite3_mprintf(
      "%s%d", HST_DATABASE_NAME, pTst->bSeparate ? iTid : 0
  );
  char *zTbl = sqlite3_mprintf("tbl%d", pTst->bSeptab ? iTid : 0);

  aUpdate = sqlite3_malloc(sizeof(TestUpdate) * pTst->nUpdate);

  sqlite3_randomness(sizeof(FastPrng), (void*)&prng);
  db = htt_sqlite3_open(zFile);
  sqlite3_create_function(db,"updateblob",4,SQLITE_UTF8,0,updateBlobFunc,0 ,0);

  htt_sqlite3_exec(db, "PRAGMA journal_mode = wal");
  htt_sqlite3_exec(db, "PRAGMA wal_autocheckpoint = 10000");
  htt_sqlite3_exec(db, "PRAGMA mmap_size = 1000000000");
  htt_sqlite3_exec(db, "PRAGMA locking_mode = exclusive");
  htt_sqlite3_exec(db, "PRAGMA synchronous = off");
  htt_sqlite3_exec_printf(&err,
      db, "PRAGMA hct_try_before_unevict = %d", pTst->nTryBeforeUnevict
  );
  pBegin = htt_sqlite3_prepare(&err, db, "BEGIN CONCURRENT");
  pCommit = htt_sqlite3_prepare(&err, db, "COMMIT");
  pRollback = htt_sqlite3_prepare(&err, db, "ROLLBACK");
  if( pTst->bOverflow==0 ){
    pWrite = htt_sqlite3_prepare_printf(&err, db, "UPDATE "
        "%s SET b=updateblob(b,?,?,?), c=hex(frandomblob(32)) WHERE a = ?", zTbl
    );
  }else{
    pWrite = htt_sqlite3_prepare_printf(&err, db, "UPDATE "
        "%s SET padding=frandomblob(9000), "
             "b=updateblob(b,?,?,?), c=hex(frandomblob(32)) WHERE a = ?", zTbl
    );
  }
  pScan = htt_sqlite3_prepare_printf(&err, db, 
      "SELECT a,b,c FROM %s WHERE substr(c, 1, 16)>=hex(frandomblob(8))"
      " ORDER BY substr(c, 1, 16)", zTbl
  );

  if( err.rc ) goto test_out;
  sqlite3_bind_int(pWrite, 1, iTid);

  iStartTime = iIntervalTime = htt_current_time();
  while( err.rc==SQLITE_OK ){
    i64 iNow = htt_current_time();
    if( iNow>=(iIntervalTime + nInterval) ){
      i64 nIntervalWrite = nWrite - iIntervalWrite;
      printf("t%d: %d transactions at %d/second\n", iTid, (int)nIntervalWrite, 
          (int)((nIntervalWrite * 1000) / (iNow - iIntervalTime))
      );
      fflush(stdout);
      iIntervalTime = iNow;
      iIntervalWrite = nWrite;
    }

    if( iNow>=g.iTimeToStop && nWrite>pTst->nMinInsert ){
      break;
    }else{
      int ii = 0;
      sqlite3_step(pBegin);
      htt_sqlite3_reset(&err, pBegin);
      httFastRandomness(&prng, sizeof(TestUpdate)*pTst->nUpdate, aUpdate);

      for(ii=0; ii<pTst->nScan; ii++){
        int nn = 0;
        while( sqlite3_step(pScan)==SQLITE_ROW ){
          if( ++nn>=pTst->szScan ) break;
        }
        htt_sqlite3_reset(&err, pScan);
      }

      for(ii=0; ii<pTst->nUpdate; ii++){
        aUpdate[ii].iRow = 1+((aUpdate[ii].iRow & 0x7FFFFFFF) % pTst->nRow);
        aUpdate[ii].iVal = pCtx->aVal[ aUpdate[ii].iRow ]+1;
        sqlite3_bind_int64(pWrite, 2, aUpdate[ii].iVal);
        sqlite3_bind_int64(pWrite, 3, pCtx->aVal[ aUpdate[ii].iRow ]);
        sqlite3_bind_int(pWrite, 4, aUpdate[ii].iRow);
        sqlite3_step(pWrite);
        htt_sqlite3_reset(&err, pWrite);
      }

      sqlite3_step(pCommit);
      htt_sqlite3_reset(&err, pCommit);
      if( (err.rc&0xFF)==SQLITE_BUSY ){
        htt_free_err(&err);
        nBusy++;
        sqlite3_step(pRollback);
        htt_sqlite3_reset(&err, pRollback);
      }else{
        for(ii=0; ii<pTst->nUpdate; ii++){
          pCtx->aVal[ aUpdate[ii].iRow ] = aUpdate[ii].iVal;
        }
      }
      nWrite++;
    }
  }

  iEndTime = htt_current_time();
  if( iEndTime<=iStartTime ) iEndTime = iStartTime + 1;
  zStat = testGetStats(&err, db);

  /* Check that no updates made by this thread have been lost. */
  nError = 0;
  if( pTst->bTrust==0 ){
    pSelect = htt_sqlite3_prepare_printf(&err, db, "SELECT a, b FROM %s", zTbl);
    if( pSelect ){
      while( SQLITE_ROW==sqlite3_step(pSelect) ){
        int iRow = sqlite3_column_int(pSelect, 0);
        u32 *aBlob = (u32*)sqlite3_column_blob(pSelect, 1);
        if( aBlob[iTid]!=pCtx->aVal[iRow] ) nError++;
      }
      sqlite3_finalize(pSelect);
    }
  }

  /* Find the number of CAS collisions */
  nCasFail = htt_sqlite3_exec_i64(&err, db, "PRAGMA hct_ncasfail");

  zRet = sqlite3_mprintf("%d transactions "
      "(%d busy, %d cas-fail) at %d/second (%d lost updates)%s", 
      nWrite, nBusy, (int)nCasFail,
      (int)(((i64)nWrite * 1000) / (iEndTime - iStartTime)), nError, zStat
  );
  sqlite3_free(zStat);

  pCtx->nTotalTrans = nWrite;
  pCtx->nBusyTrans = nBusy;

  sqlite3_free(aUpdate);
  sqlite3_free(zTbl);

  sqlite3_finalize(pBegin);
  sqlite3_finalize(pCommit);
  sqlite3_finalize(pRollback);
  sqlite3_finalize(pWrite);
  sqlite3_finalize(pScan);

  htt_sqlite3_close(db);

 test_out:
  htt_print_and_free_err(&err);
  return zRet;
}

static void test_build_db(Error *pErr, Testcase *pTst, int iDb, TestCtx *aCtx){
  int ii;
  sqlite3_stmt *pIns = 0;
  sqlite3 *db = 0;
  char *zFile = sqlite3_mprintf("file:%s%d?hctree=1", HST_DATABASE_NAME, iDb);
  const int nInsertPerTrans = 10000;
  char *zRm = 0;

  int nTbl = pTst->bSeptab ? pTst->nThread : 1;
  int iTab = 0;

  /* Check if the database is already populated */
  db = htt_sqlite3_open(zFile);
  for(iTab=0; iTab<nTbl; iTab++){
    i64 nObj, nRow;
    int bOk = 0;
    char *zSql = sqlite3_mprintf(
        "SELECT count(*) FROM sqlite_schema WHERE tbl_name='tbl%d'", iTab
    );
    nObj = htt_sqlite3_exec_i64(pErr, db, zSql);
    sqlite3_free(zSql);
    if( nObj!=pTst->nIdx+1 ) break;

    zSql = sqlite3_mprintf("SELECT count(*) FROM tbl%d", iTab);
    nRow = htt_sqlite3_exec_i64(pErr, db, zSql);
    sqlite3_free(zSql);
    if( nRow!=pTst->nRow ) break;
  }

  if( iTab==nTbl ){
    printf("reusing database %d.\n", iDb); 
    fflush(stdout);

    /* The database already exists. Populate the aCtx[x].aVal arrays. */
    for(iTab=0; iTab<nTbl; iTab++){
      sqlite3_stmt *p = htt_sqlite3_prepare_printf(
          pErr, db, "SELECT a, b FROM tbl%d", iTab
      );
      while( sqlite3_step(p)==SQLITE_ROW ){
        int a = sqlite3_column_int(p, 0);
        const u32 *b = (u32*)sqlite3_column_blob(p, 1);
        int iTid;
        for(iTid=0; iTid<pTst->nThread; iTid++){
          if( pTst->bSeparate==1 && iDb!=iTid ) continue;
          if( pTst->bSeptab==1 && iTab!=iTid ) continue;
          aCtx[iTid].aVal[a] = b[iTid];
        }
      }
      htt_sqlite3_reset(pErr, p);
      sqlite3_finalize(p);
    }
    htt_sqlite3_close(db);
    return;
  }
  htt_sqlite3_close(db);

  zRm = sqlite3_mprintf(
      "rm -rf %s%d; rm -rf %s%d-data; rm -rf %s%d-pagemap", 
      HST_DATABASE_NAME, iDb,
      HST_DATABASE_NAME, iDb,
      HST_DATABASE_NAME, iDb
  );
  system(zRm);
  sqlite3_free(zRm);

  db = htt_sqlite3_open(zFile);
  printf("building initial database %d.", iDb); 
  fflush(stdout);
  for(iTab=0; iTab<nTbl; iTab++){
    htt_sqlite3_exec_printf(pErr, db,
      " CREATE TABLE tbl%d("
      "   a INTEGER PRIMARY KEY,"
      "   %s"
      "   b BLOB,"
      "   c CHAR(64)"
      ")", iTab, 
      (pTst->bOverflow ? "   padding BLOB DEFAULT (frandomblob(9000))," : "")
    );
    if( pTst->bOverflow && pTst->nIdx==1 ){
      htt_sqlite3_exec_printf(pErr, db, 
          "CREATE INDEX tbl%d_i1 ON tbl%d(padding, c)", iTab, iTab
      );
    }else{
      for(ii=0; ii<pTst->nIdx; ii++){
        htt_sqlite3_exec_printf(pErr, db, 
            "CREATE INDEX tbl%d_i%d ON tbl%d(substr(c, %d, 16));", 
            iTab, ii+1, iTab, ii+1
        );
      }
    }

    pIns = htt_sqlite3_prepare_printf(pErr, db, 
        "INSERT INTO tbl%d(a,b,c) "
        "VALUES(NULL, zeroblob(?), hex(frandomblob(32)))", iTab
    );
    sqlite3_bind_int(pIns, 1, pTst->nBlob);
    htt_sqlite3_exec(db, "BEGIN");
    for(ii=0; ii<pTst->nRow; ii++){
      if( pTst->nRow>20 && ((ii+1) % (pTst->nRow / 20))==0 ){
        printf(".");
        fflush(stdout);
      }

      sqlite3_step(pIns);
      htt_sqlite3_reset(pErr, pIns);
    }
    htt_sqlite3_exec(db, "COMMIT");
    sqlite3_finalize(pIns);
  }
  printf("\n");
  fflush(stdout);
  htt_sqlite3_close(db);
}

static void runtest(Testcase *pTst){
  char *zFile = 0;
  sqlite3 *db = 0;
  sqlite3_stmt *pIC = 0;
  int ii;
  int iDb;

  int nTrans;
  int nBusy;

  Error err;
  Threadset threadset;
  TestCtx *aCtx = 0;

  if( pTst->nThread>(pTst->nBlob/sizeof(u32)) ){
    printf("ERROR: nThread > (nBlob/4). Skipping test...\n");
    return;
  }else if( pTst->nIdx==0 && pTst->nScan>0 ){
    printf("ERROR: nIdx==0 && nScan>0. Skipping test...\n");
    return;
  }else if( pTst->bOverflow && pTst->nScan>0 ){
    printf("ERROR: bOverflow==1 && nScan>0. Skipping test...\n");
    return;
  }else if( pTst->bOverflow && pTst->nIdx>1 ){
    printf("ERROR: bOverflow==1 && nIdx>1. Skipping test...\n");
    return;
  }

  memset(&err, 0, sizeof(err));
  memset(&threadset, 0, sizeof(threadset));

  aCtx = sqlite3_malloc(sizeof(TestCtx)*(pTst->nThread+1));
  memset(aCtx, 0, sizeof(TestCtx)*pTst->nThread);
  for(iDb=0; iDb<pTst->nThread; iDb++){
    int nByte = sizeof(u32) * (pTst->nRow+1);
    aCtx[iDb].pTst = pTst;
    aCtx[iDb].aVal = (u32*)sqlite3_malloc(nByte);
    memset(aCtx[iDb].aVal, 0, nByte);
  }

  if( pTst->bTrust==0 ){
    for(iDb=0; iDb<pTst->nThread && (iDb==0 || pTst->bSeparate); iDb++){
      test_build_db(&err, pTst, iDb, aCtx);
    }
  }

  if( pTst->nSleep ){
    printf("sleeping for %d seconds\n", pTst->nSleep);
    fflush(stdout);
    sleep(pTst->nSleep);
  }

  /* Set the "time-to-stop" global */
  g.iTimeToStop = htt_current_time() + (i64)pTst->nSecond * 1000;

  CALLGRIND_START_INSTRUMENTATION;
  printf("launching %d threads\n", pTst->nThread);
  fflush(stdout);
  for(ii=0; ii<pTst->nThread; ii++){
    htt_launch_thread(&err, &threadset, test_thread, (void*)&aCtx[ii]);
  }
  htt_join_threads(&err, &threadset);
  CALLGRIND_STOP_INSTRUMENTATION;

  for(iDb=0; iDb<pTst->nThread; iDb++){
    sqlite3_free(aCtx[iDb].aVal);
    aCtx[iDb].aVal = 0;
  }

  nTrans = 0;
  nBusy = 0;
  for(ii=0; ii<pTst->nThread; ii++){
    nTrans += aCtx[ii].nTotalTrans;
    nBusy += aCtx[ii].nBusyTrans;
  }

  zFile = sqlite3_mprintf("%s0", HST_DATABASE_NAME);
  db = htt_sqlite3_open(zFile);

  if( pTst->nSecond ){
    char *zThread = 0;
    sqlite3_stmt *p = htt_sqlite3_prepare(&err, db, "PRAGMA hct_ncasfail");
    if( SQLITE_ROW==sqlite3_step(p) || pTst->nThread>1 ){
      zThread = sqlite3_mprintf("%d", pTst->nThread);
    }else{
      zThread = sqlite3_mprintf("'stock'");
    }
    htt_sqlite3_reset(&err, p);
    sqlite3_finalize(p);

    printf("Total transactions: %d (%d/second) (%d/cpu-second)\n", nTrans,
        nTrans / pTst->nSecond,
        nTrans / (pTst->nSecond*pTst->nThread)
    );

    printf("INSERT INTO tests(name, nthread, nsecond, ntrans, nbusy) VALUES(");
    printf("'nUpdate=%d nScan=%d%s', %s, %d, %d, %d);\n",
        pTst->nUpdate, pTst->nScan, 
        pTst->bSeparate ? " (sep)" : "",
        zThread, pTst->nSecond,
        nTrans, nBusy
    );
    sqlite3_free(zThread);
  }


  if( pTst->bSeparate==0 && pTst->bTrust==0 ){
    htt_sqlite3_exec(db, "PRAGMA hct_quiescent_integrity_check=1");
    pIC = htt_sqlite3_prepare(&err, db, "PRAGMA integrity_check");
    if( pIC ){
      while( sqlite3_step(pIC)==SQLITE_ROW ){
        printf("Integrity check: %s\n", sqlite3_column_text(pIC, 0));
      }
      sqlite3_finalize(pIC);
    }
    htt_sqlite3_exec_debug(&err, db, "");   /* to avoid a compiler warning */
  }

  htt_sqlite3_close(db);
  htt_print_and_free_err(&err);
}

static void usage(const char *zPrg){
  fprintf(stderr, "Usage: %s...\n", zPrg);
  exit(-1);
}

static void log_callback(void *pArg, int iErrCode, const char *zMsg){
  printf("(%d) %s\n", iErrCode, zMsg);
  fflush(stdout);
}

int main(int argc, char **argv){
  int iArg;
  Testcase tst;

#if 0
  struct rlimit rlim;
  getrlimit(RLIMIT_FSIZE, &rlim);
  rlim.rlim_cur = 1 * 1024*1024*1024 * 16;
  setrlimit(RLIMIT_FSIZE, &rlim);
#endif

  memset(&tst, 0, sizeof(Testcase));
  tst.nBlob = 200;
  tst.nRow = 1000000;
  tst.nUpdate = 1;
  tst.nSecond = 10;
  tst.nSleep = 1;
  tst.nTryBeforeUnevict = 100;

  tst.nScan = 0;
  tst.szScan = 10;

  sqlite3_config(SQLITE_CONFIG_LOG, log_callback, 0);
  sqlite3_config(SQLITE_CONFIG_MEMSTATUS, (int)0);

  for(iArg=1; iArg<argc; iArg++){
    const char *zArg = argv[iArg];
    if( zArg[0]=='+' ){
      tst.nThread = strtol(&zArg[1], 0, 0);
      runtest(&tst);
    }else{
      int nArg = strlen(zArg);
      int *pnVal = 0;
      if( nArg>3 && nArg<=6 && memcmp("-nscan", zArg, nArg)==0 ){
        pnVal = &tst.nScan;
      }else
      if( nArg>2 && nArg<=7 && memcmp("-szscan", zArg, nArg)==0 ){
        pnVal = &tst.szScan;
      }else
      if( nArg>2 && nArg<=11 && memcmp("-nmininsert", zArg, nArg)==0 ){
        pnVal = &tst.nMinInsert;
      }else
      if( nArg>4 && nArg<=9 && memcmp("-separate", zArg, nArg)==0 ){
        pnVal = &tst.bSeparate;
      }else
      if( nArg>4 && nArg<=7 && memcmp("-septab", zArg, nArg)==0 ){
        pnVal = &tst.bSeptab;
      }else
      if( nArg>3 && nArg<=7 && memcmp("-nsleep", zArg, nArg)==0 ){
        pnVal = &tst.nSleep;
      }else
      if( nArg>2 && nArg<=6 && memcmp("-nblob", zArg, nArg)==0 ){
        pnVal = &tst.nBlob;
      }else
      if( nArg>2 && nArg<=5 && memcmp("-nidx", zArg, nArg)==0 ){
        pnVal = &tst.nIdx;
      }else
      if( nArg>2 && nArg<=5 && memcmp("-nrow", zArg, nArg)==0 ){
        pnVal = &tst.nRow;
      }else
      if( nArg>3 && nArg<=8 && memcmp("-nsecond", zArg, nArg)==0 ){
        pnVal = &tst.nSecond;
      }else
      if( nArg>2 && nArg<=8 && memcmp("-ntrybeforeunevict", zArg, nArg)==0 ){
        pnVal = &tst.nTryBeforeUnevict;
      }else
      if( nArg>2 && nArg<=8 && memcmp("-nupdate", zArg, nArg)==0 ){
        pnVal = &tst.nUpdate;
      }else
      if( nArg>1 && nArg<=9 && memcmp("-overflow", zArg, nArg)==0 ){
        pnVal = &tst.bOverflow;
      }else 
      if( nArg>1 && nArg<=9 && memcmp("-trustdbs", zArg, nArg)==0 ){
        pnVal = &tst.bTrust;
      }else{
        usage(argv[0]);
      }

      iArg++;
      if( iArg==argc ) usage(argv[0]);
      *pnVal = strtol(argv[iArg], 0, 0);
    }
  }

  return 0;
}


