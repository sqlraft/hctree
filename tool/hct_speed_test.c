
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


typedef sqlite3_int64 i64;
typedef unsigned int u32;

#define HST_DATABASE_NAME "hct_speed.db"

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
  int nTrans;                     /* Number of transactions run by thread */

  pthread_t tid;                  /* Thread id */
  char *(*xProc)(int,void*,int*); /* Thread main proc */
  char *zRes;                     /* Value returned by xProc */
  Thread *pNext;                  /* Next in this list of threads */
};

struct Threadset {
  int iMaxTid;                    /* Largest iTid value allocated so far */
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
static void hstFastRandomness(FastPrng *pPrng, int N, void *P){
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
  hstFastRandomness(p, sizeof(ret), &ret);
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
  hstFastRandomness(p, nBlob, aBlob);
  sqlite3_result_blob(ctx, aBlob, nBlob, SQLITE_TRANSIENT);
  sqlite3_free(aBlob);
}



static sqlite3 *hst_sqlite3_open(const char *zDb){
  sqlite3 *db = 0;
  int flags = SQLITE_OPEN_CREATE|SQLITE_OPEN_READWRITE|SQLITE_OPEN_NOMUTEX;
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

static void hst_sqlite3_exec(sqlite3 *db, const char *zSql){
  char *zErr = 0;
  int rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "error in sqlite3_exec: (%d) %s\n", rc, zErr);
    exit(1);
  }
}

#define SEL(e) ((e)->iLine = ((e)->rc ? (e)->iLine : __LINE__))


static void hst_sqlite3_exec_printf(
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
      pErr->zErr = sqlite3_mprintf("sqlite3_prepare: %s", sqlite3_errmsg(db));
    }
  }
}

static sqlite3_stmt *hst_sqlite3_prepare(
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
#define hst_sqlite3_prepare(pErr, db, zSql) ( \
  SEL(pErr),                                  \
  hst_sqlite3_prepare(pErr, db, zSql)         \
)

static void hst_free_err(Error *pErr){
  sqlite3_free(pErr->zErr);
  memset(pErr, 0, sizeof(Error));
}

static void hst_print_and_free_err(Error *pErr){
  if( pErr->rc!=SQLITE_OK ){
    printf("Error: line %d: (rc=%d) %s\n", pErr->iLine, pErr->rc, pErr->zErr);
  }
  hst_free_err(pErr);
}

static void hst_sqlite3_reset(Error *pErr, sqlite3_stmt *pStmt){
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
#define hst_sqlite3_reset(pErr, pStmt) ( \
  SEL(pErr),                             \
  hst_sqlite3_reset(pErr, pStmt)         \
)

static i64 hst_sqlite3_exec_i64(Error *pErr, sqlite3 *db, const char *zSql){
  i64 iRet = 0;
  sqlite3_stmt *p = hst_sqlite3_prepare(pErr, db, zSql);
  if( p && SQLITE_ROW==sqlite3_step(p) ){
    iRet = sqlite3_column_int64(p, 0);
  }
  hst_sqlite3_reset(pErr, p);
  sqlite3_finalize(p);
  return iRet;
}
#define hst_sqlite3_exec_i64(pErr, db, zSql) ( \
  SEL(pErr),                                   \
  hst_sqlite3_exec_i64(pErr, db, zSql)         \
)

static void hst_sqlite3_exec_debug(Error *pErr, sqlite3 *db, const char *zSql){
  sqlite3_stmt *p = hst_sqlite3_prepare(pErr, db, zSql);
  while( p && SQLITE_ROW==sqlite3_step(p) ){
    int nCol = sqlite3_data_count(p);
    int iCol;
    for(iCol=0; iCol<nCol; iCol++){
      const char *z = (const char*)sqlite3_column_text(p, iCol);
      printf("%s%s", z ? z : "(null)", iCol==nCol-1 ? "\n" : "|");
    }
  }
  hst_sqlite3_reset(pErr, p);
  sqlite3_finalize(p);
}
#define hst_sqlite3_exec_debug(pErr, db, zSql) ( \
  SEL(pErr),                                     \
  hst_sqlite3_exec_debug(pErr, db, zSql)         \
)


static void system_error(Error *pErr, int iSys){
  pErr->rc = iSys;
  pErr->zErr = (char *)sqlite3_malloc(512);
  strerror_r(iSys, pErr->zErr, 512);
  pErr->zErr[511] = '\0';
}

static void *launch_thread_main(void *pArg){
  Thread *p = (Thread *)pArg;
  p->zRes = p->xProc(p->iTid, p->pArg, &p->nTrans);
  return 0;
}

static void hst_launch_thread(
  Error *pErr,                    /* IN/OUT: Error code */
  Threadset *pThreads,            /* Thread set */
  char *(*xProc)(int,void*,int*), /* Proc to run */
  void *pArg                      /* Argument passed to thread proc */
){
  if( pErr->rc==SQLITE_OK ){
    int iTid = ++pThreads->iMaxTid;
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

static sqlite3_int64 hst_current_time(){
  struct timeval sNow;
  gettimeofday(&sNow, 0);
  return (sqlite3_int64)sNow.tv_sec*1000 + sNow.tv_usec/1000;
}


static void hst_join_threads(
  Error *pErr,                    /* IN/OUT: Error code */
  Threadset *pThreads,            /* Thread set */
  int *pnTrans
){
  Thread *p;
  Thread *pNext;
  int nTrans = 0;
  for(p=pThreads->pThread; p; p=pNext){
    void *ret = 0;
    int rc = SQLITE_OK;
    pNext = p->pNext;

    rc = pthread_join(p->tid, &ret);

    if( rc!=0 ){
      if( pErr->rc==SQLITE_OK ) system_error(pErr, rc);
    }else{
      printf("Thread %d says: [%d] %s\n", 
          p->iTid, p->nTrans, (p->zRes==0 ? "..." : p->zRes)
      );
      fflush(stdout);
      nTrans += p->nTrans;
    }
    sqlite3_free(p->zRes);
    sqlite3_free(p);
  }
  *pnTrans = nTrans;
  pThreads->pThread = 0;
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
  int bSeparate;
  int nSleep;
  int nScan;
  int szScan;
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
**   updateblob(BLOB, IDX, IVAL)
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

  nBlob = sqlite3_value_bytes(apArg[0]);
  aBlob = (u32*)sqlite3_value_blob(apArg[0]);

  iIdx = sqlite3_value_int(apArg[1]);
  iVal = (u32)(sqlite3_value_int64(apArg[2]) & 0xFFFFFFFF);

  aCopy = sqlite3_malloc(nBlob);
  memcpy(aCopy, aBlob, nBlob);
  aCopy[iIdx] = iVal;

  sqlite3_result_blob(pCtx, aCopy, nBlob, SQLITE_TRANSIENT);
  sqlite3_free(aCopy);
}

static char *test_thread(int iTid, void *pArg, int *pnTrans){
  Testcase *pTst = (Testcase*)pArg;
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

  TestUpdate *aUpdate = 0;
  u32 *aVal = 0;
  char *zFile = sqlite3_mprintf(
      "%s%d", HST_DATABASE_NAME, pTst->bSeparate ? iTid-1 : 0
  );

  aUpdate = sqlite3_malloc(sizeof(TestUpdate) * pTst->nUpdate);
  aVal = sqlite3_malloc(sizeof(u32) * (pTst->nRow+1));
  memset(aVal, 0, (pTst->nRow+1) * sizeof(u32));

  sqlite3_randomness(sizeof(FastPrng), (void*)&prng);
  db = hst_sqlite3_open(zFile);
  sqlite3_create_function(db,"updateblob",3,SQLITE_UTF8,0,updateBlobFunc,0 ,0);

  hst_sqlite3_exec(db, "PRAGMA journal_mode = wal");
  hst_sqlite3_exec(db, "PRAGMA wal_autocheckpoint = 10000");
  hst_sqlite3_exec(db, "PRAGMA mmap_size = 1000000000");
  hst_sqlite3_exec(db, "PRAGMA locking_mode = exclusive");
  hst_sqlite3_exec_printf(&err,
      db, "PRAGMA hct_try_before_unevict = %d", pTst->nTryBeforeUnevict
  );
  pBegin = hst_sqlite3_prepare(&err, db, "BEGIN");
  pCommit = hst_sqlite3_prepare(&err, db, "COMMIT");
  pRollback = hst_sqlite3_prepare(&err, db, "ROLLBACK");
  pWrite = hst_sqlite3_prepare(&err, db, 
    "UPDATE tbl SET b=updateblob(b, ?, ?), c=hex(frandomblob(32)) WHERE a = ?"
  );
  pScan = hst_sqlite3_prepare(&err, db, 
      "SELECT * FROM tbl WHERE substr(c, 0, 16)>=hex(frandomblob(8))"
      " ORDER BY substr(c, 0, 16)"
  );

  if( err.rc ) goto test_out;
  sqlite3_bind_int(pWrite, 1, iTid);

  iStartTime = iIntervalTime = hst_current_time();
  while( err.rc==SQLITE_OK ){
    i64 iNow = hst_current_time();
    if( iNow>=(iIntervalTime + nInterval) ){
      i64 nIntervalWrite = nWrite - iIntervalWrite;
      printf("t%d: %d transactions at %d/second\n", iTid, (int)nIntervalWrite, 
          (int)((nIntervalWrite * 1000) / (iNow - iIntervalTime))
      );
      iIntervalTime = iNow;
      iIntervalWrite = nWrite;
    }

    if( iNow>=g.iTimeToStop && nWrite>pTst->nMinInsert ){
      break;
    }else{
      int ii = 0;
      sqlite3_step(pBegin);
      hst_sqlite3_reset(&err, pBegin);
      hstFastRandomness(&prng, sizeof(TestUpdate)*pTst->nUpdate, aUpdate);

      for(ii=0; ii<pTst->nScan; ii++){
        int nn = 0;
        while( sqlite3_step(pScan)==SQLITE_ROW ){
          if( ++nn>=pTst->szScan ) break;
        }
        hst_sqlite3_reset(&err, pScan);
      }

      for(ii=0; ii<pTst->nUpdate; ii++){
        aUpdate[ii].iRow = 1+((aUpdate[ii].iRow & 0x7FFFFFFF) % pTst->nRow);
        sqlite3_bind_int64(pWrite, 2, aUpdate[ii].iVal);
        sqlite3_bind_int(pWrite, 3, aUpdate[ii].iRow);
        sqlite3_step(pWrite);
        hst_sqlite3_reset(&err, pWrite);
      }

#if 0
      if( nWrite==4 ){
        hst_sqlite3_exec_debug(&err, db, "SELECT * FROM hctvalid");
      };
#endif

      sqlite3_step(pCommit);
      hst_sqlite3_reset(&err, pCommit);
      if( err.rc==SQLITE_BUSY ){
        hst_free_err(&err);
        nBusy++;
        sqlite3_step(pRollback);
        hst_sqlite3_reset(&err, pRollback);
      }else{
        for(ii=0; ii<pTst->nUpdate; ii++){
          aVal[ aUpdate[ii].iRow ] = aUpdate[ii].iVal;
        }
      }
      nWrite++;
    }
  }

  iEndTime = hst_current_time();
  if( iEndTime<=iStartTime ) iEndTime = iStartTime + 1;

  /* Check that no updates made by this thread have been lost. */
  nError = 0;
  pSelect = hst_sqlite3_prepare(&err, db, "SELECT a, b FROM tbl");
  if( pSelect ){
    while( SQLITE_ROW==sqlite3_step(pSelect) ){
      int iRow = sqlite3_column_int(pSelect, 0);
      u32 *aBlob = (u32*)sqlite3_column_blob(pSelect, 1);
      if( aBlob[iTid]!=aVal[iRow] ) nError++;
    }
    sqlite3_finalize(pSelect);
  }

  /* Find the number of CAS collisions */
  nCasFail = hst_sqlite3_exec_i64(&err, db, "PRAGMA hct_ncasfail");

  zRet = sqlite3_mprintf(
      "%d transactions (%d busy, %d cas-fail) at %d/second (%d lost updates)", 
      nWrite, nBusy, (int)nCasFail,
      (int)(((i64)nWrite * 1000) / (iEndTime - iStartTime)), nError
  );
  *pnTrans = nWrite;

  sqlite3_free(aUpdate);
  sqlite3_free(aVal);

  sqlite3_finalize(pBegin);
  sqlite3_finalize(pCommit);
  sqlite3_finalize(pRollback);
  sqlite3_finalize(pWrite);
  sqlite3_finalize(pScan);

  sqlite3_close(db);

 test_out:
  hst_print_and_free_err(&err);
  return zRet;
}

static void test_build_db(Error *pErr, Testcase *pTst, int iDb){
  int ii;
  sqlite3_stmt *pIns = 0;
  sqlite3 *db = 0;
  char *zFile = sqlite3_mprintf("%s%d", HST_DATABASE_NAME, iDb);

  db = hst_sqlite3_open(zFile);
  hst_sqlite3_exec(db,
      " CREATE TABLE tbl("
      "   a INTEGER PRIMARY KEY,"
      "   b BLOB,"
      "   c CHAR(64)"
      ")"
  );

  for(ii=0; ii<pTst->nIdx; ii++){
    char *zSql = sqlite3_mprintf(
        "CREATE INDEX tbl_i%d ON tbl(substr(c, %d, 16));", ii, ii
    );
    hst_sqlite3_exec(db, zSql);
    sqlite3_free(zSql);
  }

  printf("building initial database %d.", iDb); 
  fflush(stdout);
  pIns = hst_sqlite3_prepare(pErr, db, 
      "INSERT INTO tbl VALUES(NULL, zeroblob(?), hex(frandomblob(32)))"
  );
  sqlite3_bind_int(pIns, 1, pTst->nBlob);
  hst_sqlite3_exec(db, "BEGIN");
  for(ii=0; ii<pTst->nRow; ii++){
    if( pTst->nRow>20 && ((ii+1) % (pTst->nRow / 20))==0 ){
      printf(".");
      fflush(stdout);
    }

    sqlite3_step(pIns);
    hst_sqlite3_reset(pErr, pIns);
  }
  hst_sqlite3_exec(db, "COMMIT");
  sqlite3_finalize(pIns);
  printf("\n");
  sqlite3_close(db);
}

static void runtest(Testcase *pTst){
  sqlite3 *db = 0;
  sqlite3_stmt *pIC = 0;
  int ii;
  int iDb;
  int nTrans;

  Error err;
  Threadset threadset;

  memset(&err, 0, sizeof(err));
  memset(&threadset, 0, sizeof(threadset));

  for(ii=0; ii<32; ii++){
    char *zRm = sqlite3_mprintf("rm -rf %s%d", HST_DATABASE_NAME, ii);
    system(zRm);
    sqlite3_free(zRm);
  }

  for(iDb=0; iDb<pTst->nThread && (iDb==0 || pTst->bSeparate); iDb++){
    test_build_db(&err, pTst, iDb);
  }

  if( pTst->nSleep ){
    printf("sleeping for %d seconds\n", pTst->nSleep);
    sleep(pTst->nSleep);
  }

  /* Set the "time-to-stop" global */
  g.iTimeToStop = hst_current_time() + (i64)pTst->nSecond * 1000;

  printf("launching %d threads\n", pTst->nThread);
  for(ii=0; ii<pTst->nThread; ii++){
    hst_launch_thread(&err, &threadset, test_thread, (void*)pTst);
  }
  hst_join_threads(&err, &threadset, &nTrans);

  if( pTst->nSecond ){
    printf("Total transactions: %d (%d/second) (%d/cpu-second)\n", nTrans,
        nTrans / pTst->nSecond,
        nTrans / (pTst->nSecond*pTst->nThread)
    );
  }

  if( pTst->bSeparate==0 ){
    char *zFile = sqlite3_mprintf("%s0", HST_DATABASE_NAME);
    db = hst_sqlite3_open(zFile);
    pIC = hst_sqlite3_prepare(&err, db, "PRAGMA integrity_check");
    if( pIC ){
      while( sqlite3_step(pIC)==SQLITE_ROW ){
        printf("Integrity check: %s\n", sqlite3_column_text(pIC, 0));
      }
      sqlite3_finalize(pIC);
    }
    hst_sqlite3_exec_debug(&err, db, "");   /* to avoid a compiler warning */
  }

  hst_print_and_free_err(&err);
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

  struct rlimit rlim;
  getrlimit(RLIMIT_FSIZE, &rlim);
  rlim.rlim_cur = 1 * 1024*1024*1024;
  setrlimit(RLIMIT_FSIZE, &rlim);

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
      if( nArg>2 && nArg<=9 && memcmp("-separate", zArg, nArg)==0 ){
        pnVal = &tst.bSeparate;
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


