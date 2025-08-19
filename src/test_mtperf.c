/*
** 2022 November 16
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
**
** To create a new thread-test object:
**
**     sqlite_thread_test CMD-NAME DBPATH
**
** Which then supports the following commands:
**
**     $cmd configure OPTION ?VALUE?
**     $cmd thread NAME SQL-SCRIPT 
**     $cmd run
**     $cmd result ARRAY-NAME
**     $cmd destroy
**
** Configuration options:
**
**   -ntransaction INTEGER
**     Number of transactions that each thread should attempt when [run]
**     is called. If this is set to 0, the -nsecond option governs how long
**     each thread is run for by [run]. Default 0.
**
**   -nsecond INTEGER
**     If the -ntransaction option is not set to 0, this option is ignored.
**     Or, if -ntransaction is 0, then the number of seconds that [run] runs 
**     the configured test for. Default 10.
**
**   -sqlconfig SQL
**     SQL script run by each thread immediately after it opens its 
**     connection.
**
**   -nwalpage INTEGER
**     This is useful only when testing with legacy databases, including
**     wal2 legacy databases. It has two effects:
**
**       * Causes each thread to evaluate a "PRAGMA journal_size_limit"
**         command with a parameter (in bytes) that corresponds to -nwalpage
**         database pages. This causes wal2 mode databases to swap wal
**         files after the current wal contains -nwalpage frames.
**
**       * Causes a checkpoint to be run if the wal-hook is invoked with
**         a parameter of more than -nwalpage. Checkpoint is run outside
**         of the commit mutex, assuming one is used.
*/

/**************************************************************************
**
** To create a new migration object:
**
**     sqlite_migrate CMD-NAME SOURCE DEST NJOB
**
** Then, to copy an object from 
**
**     $cmd copy SOURCE-ROOT CREATE-TABLE INSERT-STMT
**
** Finally, to run the migration:
**
**     $cmd run
**
** Then to clean up:
**
**     $cmd destroy
*/

#include <tcl.h>
#include <sqlite3.h>
#include <sqlite3hct.h>
#include "sqliteInt.h"

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

#define TT_DEFAULT_NSECOND  10
#define TT_DEFAULT_NWALPAGE 4096

typedef struct TT_Step TT_Step;
typedef struct TT_Thread TT_Thread;
typedef struct TT_Test TT_Test;

struct TT_Step {
  int eType;
  sqlite3_stmt *pStmt;
};

/* 
** Values for TT_Step.eType 
*/
#define STEP_SQL          1
#define STEP_MUTEX_COMMIT 2

/*
** State of a simple PRNG used for the per-connection and per-pager
** pseudo-random number generators.
*/
typedef struct MTFastPrng MTFastPrng;
struct MTFastPrng {
  unsigned int x, y;
};

struct TT_Thread {
  TT_Test *pTest;
  Tcl_Obj *pName;                 /* Name of thread */

  MTFastPrng prng;

  int bDoCheckpoint;
  int bCheckpointer;

  int nStep;
  TT_Step *aStep;
  sqlite3 *db;
  sqlite3 *fdb;

  /* Result variables - number of successful transactions and number of
  ** SQLITE_BUSY transactions */
  int nTransOk;
  int nTransBusy;
  Tcl_Obj *pErr;                  /* If an error has occurred */

  pthread_t tid;                  /* Thread id from pthread_create() */
};


/*
** nJrnlTruncate:
*/
struct TT_Test {
  Tcl_Obj *pDatabase;             /* Path (or URI) to test database */
  Tcl_Interp *interp;
  int nTrans;                     /* At least this many transactions */
  int nSecond;                    /* Number of seconds to run for */
  int nThread;                    /* Number of entries in aThread[] */
  int nWalPage;                   /* Target size of wal files in pages */
  int nJrnlTruncate;              /* Truncate journal to this much */
  Tcl_Obj *pSqlConfig;            /* SQL script to configure db handles */
  TT_Thread *aThread;
};

/*
** Generate N bytes of pseudo-randomness using a MTFastPrng
*/
static void ttFastRandomness(MTFastPrng *pPrng, int N, void *P){
  unsigned char *pOut = (unsigned char*)P;
  while( N-->0 ){
    pPrng->x = ((pPrng->x)>>1) ^ ((1+~((pPrng->x)&1)) & 0xd0000001);
    pPrng->y = (pPrng->y)*1103515245 + 12345;
    *(pOut++) = (pPrng->x ^ pPrng->y) & 0xff;
  }
}

static void frandomFunc(sqlite3_context *ctx, int nArg, sqlite3_value **aArg){
  MTFastPrng *p = (MTFastPrng*)sqlite3_user_data(ctx);
  sqlite3_int64 ret;
  ttFastRandomness(p, sizeof(ret), &ret);
  sqlite3_result_int64(ctx, ret);
}

static void frandomIdFunc(sqlite3_context *ctx, int nArg, sqlite3_value **aArg){
  MTFastPrng *p = (MTFastPrng*)sqlite3_user_data(ctx);
  int ret;
  ttFastRandomness(p, sizeof(ret), &ret);
  ret = (ret & 0x7FFFFFFF) % sqlite3_value_int(aArg[0]);
  sqlite3_result_int(ctx, ret);
}

static void frandomBlobFunc(
  sqlite3_context *ctx, 
  int nArg, 
  sqlite3_value **aArg
){
  MTFastPrng *p = (MTFastPrng*)sqlite3_user_data(ctx);
  int nBlob = sqlite3_value_int(aArg[0]);
  unsigned char *aBlob = 0;

  aBlob = (unsigned char*)sqlite3_malloc(nBlob);
  ttFastRandomness(p, nBlob, aBlob);
  sqlite3_result_blob(ctx, aBlob, nBlob, SQLITE_TRANSIENT);
  sqlite3_free(aBlob);
}

static void ttInstallRandomFunctions(TT_Thread *pThread){
  sqlite3_create_function(pThread->db, "frandomid", 
      1, SQLITE_ANY, (void*)&pThread->prng, frandomIdFunc, 0, 0
  );
  sqlite3_create_function(pThread->db, "frandom", 
      0, SQLITE_ANY, (void*)&pThread->prng, frandomFunc, 0, 0
  );
  sqlite3_create_function(pThread->db, "frandomblob", 
      1, SQLITE_ANY, (void*)&pThread->prng, frandomBlobFunc, 0, 0
  );
}

static int ttWalHook(void *pCtx, sqlite3 *db, const char *z, int nPg){
  TT_Thread *pThread = (TT_Thread*)pCtx;
  TT_Test *pTest = pThread->pTest;
  if( pThread==&pTest->aThread[0] || nPg>(pTest->nWalPage*2) ){
    pThread->bDoCheckpoint = 1;
  }
  return SQLITE_OK;
}

/*
** Add a new thread to test object p.
*/
static int ttAddThread(TT_Test *p, Tcl_Obj *pName, Tcl_Obj *pSql){
  int nNew = p->nThread+1;
  TT_Thread *pNew = 0;
  const char *zSql = Tcl_GetString(pSql);
  char *zPragma = 0;
  int rc = SQLITE_OK;

  /* Extend the aThread[] array. */
  p->aThread = (TT_Thread*)ckrealloc(p->aThread, nNew*sizeof(TT_Thread));
  pNew = &p->aThread[p->nThread];
  p->nThread = nNew;
  memset(pNew, 0, sizeof(TT_Thread));

  /* Populate TT_Thread.pTest and TT_Thread.pName. */
  sqlite3_randomness(sizeof(MTFastPrng), (void*)&pNew->prng);
  pNew->prng.x |= 1;
  pNew->pTest = p;
  pNew->pName = pName;
  Tcl_IncrRefCount(pNew->pName);

  /* Open the database handle for this thread */
  rc = sqlite3_open_v2(Tcl_GetString(p->pDatabase), &pNew->db, 
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
      SQLITE_OPEN_URI |SQLITE_OPEN_NOMUTEX, "unix-excl"
  );
  if( rc!=SQLITE_OK ){
    Tcl_ResetResult(p->interp);
    Tcl_AppendResult(
        p->interp, "error opening db: ", sqlite3_errmsg(pNew->db), (char*)0
    );
    return TCL_ERROR;
  }
  ttInstallRandomFunctions(pNew);

  if( p->pSqlConfig ){
    rc = sqlite3_exec(pNew->db, Tcl_GetString(p->pSqlConfig), 0, 0, 0);
    if( rc!=SQLITE_OK ){
      Tcl_ResetResult(p->interp);
      Tcl_AppendResult(
          p->interp, "error in sqlconfig: ", sqlite3_errmsg(pNew->db), (char*)0
      );
      return TCL_ERROR;
    }
  }
  zPragma = sqlite3_mprintf(
      "PRAGMA journal_size_limit = %lld", (sqlite3_int64)p->nWalPage*4096
  );
  sqlite3_exec(pNew->db, zPragma, 0, 0, 0);
  sqlite3_free(zPragma);

  while( zSql ){
    int eType = STEP_SQL;
    sqlite3_stmt *pStmt = 0;
    int nByte = (pNew->nStep+1)*sizeof(TT_Step);

    /* Skip past any whitespace */
    while( zSql[0]==' ' || zSql[0]=='\n' || zSql[0]=='\r' || zSql[0]=='\t' ){
      zSql++;
    }

    if( sqlite3_strnicmp(".mutexcommit", zSql, 12)==0 ){
      zSql += 12;
      rc = sqlite3_prepare_v2(pNew->db, "COMMIT", -1, &pStmt, &zSql);
      eType = STEP_MUTEX_COMMIT;
    }else{
      rc = sqlite3_prepare_v2(pNew->db, zSql, -1, &pStmt, &zSql);
    }

    if( rc!=SQLITE_OK ){
      Tcl_ResetResult(p->interp);
      Tcl_AppendResult(
          p->interp, "error in sql: ", sqlite3_errmsg(pNew->db), (char*)0
      );
      return TCL_ERROR;
    }
    if( pStmt==0 ) break;

    /* Extend the aStep[] array */
    pNew->aStep = (TT_Step*)ckrealloc(pNew->aStep, nByte);
    pNew->aStep[pNew->nStep].pStmt = pStmt;
    pNew->aStep[pNew->nStep].eType = eType;
    pNew->nStep++;
  }

  return TCL_OK;
}

/*
** Current time in ms.
*/
static sqlite3_int64 tt_current_time(){
  struct timeval sNow;
  gettimeofday(&sNow, 0);
  return (sqlite3_int64)sNow.tv_sec*1000 + sNow.tv_usec/1000;
}

static void *ttThreadMain(void *pArg){
  TT_Thread *pThread = (TT_Thread*)pArg;
  TT_Test *p = pThread->pTest;
  sqlite3_int64 iStop = tt_current_time() + (p->nSecond * 1000);
  int nTrans = p->nTrans;

  while( 1 ){
    int rc = SQLITE_OK;
    int ii;
    if( nTrans==0 && tt_current_time()>=iStop ) break;
    if( nTrans>0 && (pThread->nTransOk+pThread->nTransBusy)>=nTrans ) break;

    for(ii=0; ii<pThread->nStep; ii++){
      TT_Step *pStep = &pThread->aStep[ii];

      if( pStep->eType==STEP_MUTEX_COMMIT ){
        pThread->bDoCheckpoint = 0;
        sqlite3_mutex_enter( sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_APP3) );
      }

      while( sqlite3_step(pStep->pStmt)==SQLITE_ROW );
      rc = sqlite3_reset(pStep->pStmt);
      assert( (rc&0xFF)==rc );

      if( pStep->eType==STEP_MUTEX_COMMIT ){
        sqlite3_mutex_leave( sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_APP3) );

        if( rc==SQLITE_OK ){
          if( pThread->bDoCheckpoint ){
            sqlite3_exec(pThread->db, "PRAGMA wal_checkpoint", 0, 0, 0);
          }
        }
      }

      if( rc!=SQLITE_OK ) break;
    }

    if( rc==SQLITE_OK ){
      pThread->nTransOk++;
    }
    else if( rc==SQLITE_BUSY ){
      pThread->nTransBusy++;
      if( sqlite3_get_autocommit(pThread->db)==0 ){
        sqlite3_exec(pThread->db, "ROLLBACK", 0, 0, 0);
      }
    }else{
      pThread->pErr = Tcl_ObjPrintf(
          "error in step %d: %s", ii, sqlite3_errmsg(pThread->db)
      );
      break;
    }
  }

  return 0;
}

static int ttRunTest(TT_Test *p){
  int ii = 0;

  for(ii=0; ii<p->nThread; ii++){
    TT_Thread *pThread = &p->aThread[ii];
    sqlite3_wal_hook(pThread->db, ttWalHook, (void*)pThread);
    ttInstallRandomFunctions(pThread);
    pthread_create(&pThread->tid, NULL, ttThreadMain, (void*)pThread);
  }

  for(ii=0; ii<p->nThread; ii++){
    void *ret = 0;
    TT_Thread *pThread = &p->aThread[ii];
    pthread_join(pThread->tid, &ret);
  }

  for(ii=0; ii<p->nThread; ii++){
    TT_Thread *pThread = &p->aThread[ii];
    if( pThread->pErr ){
      Tcl_ResetResult(p->interp);
      Tcl_AppendResult(
          p->interp, "error in thread \"", Tcl_GetString(pThread->pName), 
          "\" - ", Tcl_GetString(pThread->pErr), (char*)0
      );
      return TCL_ERROR;
    }
  }

  return TCL_OK;
}

static int ttTestResult(TT_Test *p){
  Tcl_Interp *interp = p->interp;
  int ii = 0;
  Tcl_Obj *pRet = Tcl_NewObj();

  for(ii=0; ii<p->nThread; ii++){
    TT_Thread *pThread = &p->aThread[ii];
    Tcl_Obj *pKey = 0;

    pKey = Tcl_ObjPrintf("%s,ok", Tcl_GetString(pThread->pName));
    Tcl_ListObjAppendElement(interp, pRet, pKey);
    Tcl_ListObjAppendElement(interp, pRet, Tcl_NewIntObj(pThread->nTransOk));
    pKey = Tcl_ObjPrintf("%s,busy", Tcl_GetString(pThread->pName));
    Tcl_ListObjAppendElement(interp, pRet, pKey);
    Tcl_ListObjAppendElement(interp, pRet, Tcl_NewIntObj(pThread->nTransBusy));
  }

  Tcl_SetObjResult(interp, pRet);
  return TCL_OK;
}

static int ttConfigure(TT_Test *p, Tcl_Obj *pOpt, Tcl_Obj *pVal){
  const char *azOpt[] = {
    "-nsecond", "-ntransaction", "-sqlconfig", "-nwalpage",
    "-jtruncate", 0
  };
  Tcl_Interp *interp = p->interp;
  int iOpt = 0;
  int rc = TCL_OK;

  rc = Tcl_GetIndexFromObj(interp, pOpt, azOpt, "option", 0, &iOpt);
  if( rc!=TCL_OK ) return rc;

  switch( iOpt ){
    case 0: assert( 0==strcmp(azOpt[iOpt], "-nsecond") ); {
      if( pVal ){
        int nSec = 0;
        rc = Tcl_GetIntFromObj(interp, pVal, &nSec);
        if( rc!=TCL_OK ) return TCL_ERROR;
        p->nSecond = nSec;
      }
      Tcl_SetObjResult(interp, Tcl_NewIntObj(p->nSecond));
      break;
    }
    case 1: assert( 0==strcmp(azOpt[iOpt], "-ntransaction") ); {
      if( pVal ){
        int nTrans = 0;
        rc = Tcl_GetIntFromObj(interp, pVal, &nTrans);
        if( rc!=TCL_OK ) return TCL_ERROR;
        p->nTrans = nTrans;
      }
      Tcl_SetObjResult(interp, Tcl_NewIntObj(p->nTrans));
      break;
    }
    case 2: assert( 0==strcmp(azOpt[iOpt], "-sqlconfig") ); {
      if( pVal ){
        if( p->pSqlConfig ) Tcl_DecrRefCount(p->pSqlConfig);
        p->pSqlConfig = Tcl_DuplicateObj(pVal);
        Tcl_IncrRefCount(p->pSqlConfig);
      }
      if( p->pSqlConfig ){
        Tcl_SetObjResult(interp, p->pSqlConfig);
      }else{
        Tcl_ResetResult(interp);
      }
      break;
    }
    case 3: assert( 0==strcmp(azOpt[iOpt], "-nwalpage") ); {
      if( pVal ){
        int nWalPage = 0;
        rc = Tcl_GetIntFromObj(interp, pVal, &nWalPage);
        if( rc!=TCL_OK ) return TCL_ERROR;
        p->nWalPage = nWalPage;
      }
      Tcl_SetObjResult(interp, Tcl_NewIntObj(p->nWalPage));
      break;
    }
    case 4: assert( 0==strcmp(azOpt[iOpt], "-jtruncate") ); {
      if( pVal ){
        int nJrnlTruncate = 0;
        rc = Tcl_GetIntFromObj(interp, pVal, &nJrnlTruncate);
        if( rc!=TCL_OK ) return TCL_ERROR;
        p->nJrnlTruncate = nJrnlTruncate;
      }
      Tcl_SetObjResult(interp, Tcl_NewIntObj(p->nJrnlTruncate));
      break;
      
      break;
    }
    default:
      assert( !"cannot happen" );
  }

  return rc;
}

/*
** Implementation of the test object command.
**
**   $cmd configure OPTION ?VALUE?
**   $cmd thread NAME SQL-SCRIPT 
**   $cmd run
**   $cmd result ARRAY-NAME
**   $cmd destroy
*/
static int tt_cmd(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  TT_Test *p = (TT_Test*)clientData;
  const char *azCmd[] = {
    "configure", "thread", "run", "result", "destroy", 0
  };
  int iCmd = 0;
  int rc = TCL_OK;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUB-COMMAND ?ARGS?");
    return TCL_ERROR;
  }

  rc = Tcl_GetIndexFromObj(interp, objv[1], azCmd, "sub-command", 0, &iCmd);
  if( rc!=TCL_OK ) return rc;

  switch( iCmd ){
    case 0: assert( 0==strcmp(azCmd[iCmd], "configure") ); {
      if( objc!=3 && objc!=4 ){
        Tcl_WrongNumArgs(interp, 1, objv, "configure OPTION ?VALUE?");
        return TCL_ERROR;
      }else{
        rc = ttConfigure(p, objv[2], objc==4 ? objv[3] : 0);
        if( rc ) return rc;
      }
      break;
    }

    case 1: assert( 0==strcmp(azCmd[iCmd], "thread") ); {
      if( objc!=4 ){
        Tcl_WrongNumArgs(interp, 2, objv, "NAME SQL");
        return TCL_ERROR;
      }else{
        rc = ttAddThread(p, objv[2], objv[3]);
        if( rc ) return rc;
      }
      break;
    }

    case 2: assert( 0==strcmp(azCmd[iCmd], "run") ); {
      if( objc!=2 ){
        Tcl_WrongNumArgs(interp, 2, objv, NULL);
        return TCL_ERROR;
      }else{
        rc = ttRunTest(p);
        if( rc ) return rc;
      }
      break;
    }

    case 3: assert( 0==strcmp(azCmd[iCmd], "result") ); {
      if( objc!=2 ){
        Tcl_WrongNumArgs(interp, 2, objv, NULL);
        return TCL_ERROR;
      }else{
        rc = ttTestResult(p);
        if( rc ) return rc;
      }
      break;
    }

    case 4: assert( 0==strcmp(azCmd[iCmd], "destroy") ); {
      if( objc!=2 ){
        Tcl_WrongNumArgs(interp, 1, objv, "");
        return TCL_ERROR;
      }else{
        Tcl_DeleteCommand(interp, Tcl_GetStringFromObj(objv[0], 0));
      }
      break;
    }
  }

  return TCL_OK;
}

static void tt_del(ClientData clientData){
  TT_Test *p = (TT_Test*)clientData;
  if( p ){
    int ii = 0;
    for(ii=0; ii<p->nThread; ii++){
      TT_Thread *pThread = &p->aThread[ii];
      int iStep = 0;
      for(iStep=0; iStep<pThread->nStep; iStep++){
        sqlite3_finalize(pThread->aStep[iStep].pStmt);
      }
      ckfree(pThread->aStep);
      if( pThread->pName ) Tcl_DecrRefCount(pThread->pName);
      if( pThread->pErr ) Tcl_DecrRefCount(pThread->pErr);
      sqlite3_close(pThread->db);
      sqlite3_close(pThread->fdb);
    }
    if( p->pDatabase ) Tcl_DecrRefCount(p->pDatabase);
    if( p->pSqlConfig ) Tcl_DecrRefCount(p->pSqlConfig);
    ckfree(p->aThread);
    ckfree(p);
  }
}

/*
** tclcmd: sqlite_thread_test OBJ-NAME DBPATH
*/
static int sqlite_thread_test(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  TT_Test *p = 0;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "OBJ-NAME DBPATH");
    return TCL_ERROR;
  }

  p = ckalloc(sizeof(TT_Test));
  memset(p, 0, sizeof(TT_Test));
  p->nSecond = TT_DEFAULT_NSECOND;
  p->nWalPage = TT_DEFAULT_NWALPAGE;
  p->pDatabase = objv[2];
  p->interp = interp;
  Tcl_IncrRefCount(p->pDatabase);

  Tcl_CreateObjCommand(interp, Tcl_GetString(objv[1]), tt_cmd, (void*)p,tt_del);
  Tcl_SetObjResult(interp, objv[1]);
  return TCL_OK;
}

/*
** tclcmd: sqlite_thread_test_config
**
** Set the SQLite global configuration for best multi-threaded performance.
*/
static int sqlite_thread_test_config(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  int rc;

  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }

#ifdef SQLITE_TEST
  Tcl_SetObjResult(interp, Tcl_NewStringObj(
        "Do not run multi-threaded performance tests with testfixture!", -1
  ));
  return TCL_ERROR;
#endif

  rc = sqlite3_config(SQLITE_CONFIG_MEMSTATUS, (int)0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
  }

  if( rc!=SQLITE_OK ){
    Tcl_SetObjResult(interp, Tcl_NewStringObj("error configuring sqlite", -1));
    return TCL_ERROR;
  }

  return TCL_OK;
}

typedef struct Migration Migration;
typedef struct MigrationJob MigrationJob;
typedef struct MigrationInsert MigrationInsert;

struct MigrationJob {
  sqlite3 *db;
  int rc;
  pthread_t tid;
  Migration *pMigration;
};

struct MigrationInsert {
  char *zCreate1;
  char *zCreate2;
  char *zInsert;
  char *zTableName;
  sqlite3_int64 srcRoot;
};

/*
** mutex:
**   Mutex used to protect nInsertStarted/nInsertDone within the [run]
**   sub-command.
*/
struct Migration {
  sqlite3_mutex *mutex;
  int nInsertStarted;
  int nInsertDone;

  int nInsert;
  MigrationInsert *aInsert;

  int nJob;
  MigrationJob aJob[1];
};

static void migration_del(ClientData clientData){
  Migration *p = (Migration*)clientData;
  int ii;
  sqlite3_mutex_free(p->mutex);
  for(ii=0; ii<p->nJob; ii++){
    sqlite3_close(p->aJob[ii].db);
  }
  for(ii=0; ii<p->nInsert; ii++){
    ckfree(p->aInsert[ii].zCreate1);
    ckfree(p->aInsert[ii].zCreate2);
    ckfree(p->aInsert[ii].zTableName);
    ckfree(p->aInsert[ii].zInsert);
  }
  ckfree(p->aInsert);
  ckfree(p);
}

static int rootPageOfTable(sqlite3 *db, const char *zName, int *piRoot){
  const char *zSql = "SELECT rootpage FROM sqlite_schema WHERE name=?";
  sqlite3_stmt *pStmt = 0;
  int rc = SQLITE_OK;

  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;

  *piRoot = 0;
  sqlite3_bind_text(pStmt, 1, zName, -1, SQLITE_STATIC);
  if( SQLITE_ROW==sqlite3_step(pStmt) ){
    *piRoot = sqlite3_column_int(pStmt, 0);
  }
  return sqlite3_finalize(pStmt);
}

static void *migration_main(void *pArg){
  MigrationJob *pJob = (MigrationJob*)pArg;
  Migration *p = pJob->pMigration;

  while( pJob->rc==SQLITE_OK ){
    MigrationInsert *pInsert = 0;
    sqlite3_mutex_enter(p->mutex);
    if( p->nInsertStarted<p->nInsert ){
      pInsert = &p->aInsert[p->nInsertStarted];
      p->nInsertStarted++;
    }
    sqlite3_mutex_leave(p->mutex);

    if( pInsert==0 ) break;

    sqlite3_exec(pJob->db, "PRAGMA hct_create_table_no_cookie = 1", 0, 0, 0);

    /* Create the imposter table in the source database. */
    sqlite3_test_control(
        SQLITE_TESTCTRL_IMPOSTER, pJob->db, "src", 1, (int)pInsert->srcRoot 
    );
    pJob->rc = sqlite3_exec(pJob->db, pInsert->zCreate2, 0, 0, 0);
      assert( pJob->rc==SQLITE_OK );
    sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, pJob->db, "src", 0, (int)0);
    sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, pJob->db, "main", 0, (int)0);

    if( pJob->rc==SQLITE_OK ){
      pJob->rc = sqlite3_exec(pJob->db, "BEGIN CONCURRENT", 0, 0, 0);
    }
    if( pJob->rc==SQLITE_OK ){
      pJob->rc = sqlite3_exec(pJob->db, pInsert->zCreate1, 0, 0, 0);
      assert( pJob->rc==SQLITE_OK );
    }

    if( pJob->rc==SQLITE_OK ){
      int iRoot = 0;
      pJob->rc = rootPageOfTable(pJob->db, pInsert->zTableName, &iRoot);
      if( pJob->rc==SQLITE_OK ){
        assert( iRoot!=0 );
        sqlite3_test_control(
            SQLITE_TESTCTRL_IMPOSTER, pJob->db, "main", 1, iRoot
        );
        pJob->rc = sqlite3_exec(pJob->db, pInsert->zCreate2, 0, 0, 0);
        sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, pJob->db, "main", 0, 0);
      }
    }

    if( pJob->rc==SQLITE_OK ){
      pJob->db->mDbFlags |= DBFLAG_Vacuum;
      pJob->rc = sqlite3_exec(pJob->db, pInsert->zInsert, 0, 0, 0);
      pJob->db->mDbFlags &= ~(u32)(DBFLAG_Vacuum);
    }
    if( pJob->rc==SQLITE_OK ){
      pJob->rc = sqlite3_exec(pJob->db, "COMMIT", 0, 0, 0);
    }

    sqlite3_mutex_enter(p->mutex);
    p->nInsertDone++;
    printf("(%d/%d)\r", p->nInsertDone, p->nInsert);
    fflush(stdout);
    sqlite3_mutex_leave(p->mutex);
  }

  return 0;
}

static char *migration_strdup(const char *zIn){
  char *zRet = 0;
  if( zIn ){
    int nIn = strlen(zIn);
    zRet = ckalloc(nIn+1);
    memcpy(zRet, zIn, nIn+1);
  }
  return zRet;
}

/*
** tclcmd: $cmd SUB-COMMAND ARGS...
*/
static int migration_cmd(
  ClientData clientData,          /* Migration object */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  Migration *p = (Migration*)clientData;
  const char *azSub[] = {
    "copy", "run", "destroy", "stats", 0
  };
  int iSub;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUB-COMMAND");
    return TCL_ERROR;
  }
  if( Tcl_GetIndexFromObj(interp, objv[1], azSub, "SUB-COMMAND", 0, &iSub) ){
    return TCL_ERROR;
  }

  switch( iSub ){
    case 0: assert( 0==strcmp(azSub[iSub], "copy") ); {
      const char *zCreate1 = 0;
      const char *zCreate2 = 0;
      const char *zInsert = 0;
      const char *zName = 0;
      sqlite3_int64 pgno = 0;

      /* $cmd copy SOURCE-ROOT NAME INSERT CREATE-TABLE-1 CREATE-TABLE-2 */
      if( objc!=7 ){
        Tcl_WrongNumArgs(interp, 2, objv, "ROOT NAME INSERT CT1 CT2");
        return TCL_ERROR;
      }
      if( Tcl_GetWideIntFromObj(interp, objv[2], &pgno) ) return TCL_ERROR;
      zName = Tcl_GetString(objv[3]);
      zInsert = Tcl_GetString(objv[4]);
      zCreate1 = Tcl_GetString(objv[5]);
      zCreate2 = Tcl_GetString(objv[6]);

      if( 0==(p->nInsert & (p->nInsert-1)) ){
        int nNew = (p->nInsert==0 ? 1 : p->nInsert*2);
        MigrationInsert *aNew = 0;

        aNew = ckrealloc(p->aInsert, sizeof(MigrationInsert) * nNew);
        memset(&aNew[p->nInsert], 0, sizeof(MigrationInsert)*(nNew-p->nInsert));
        p->aInsert = aNew;
      }

      p->aInsert[p->nInsert].zTableName = migration_strdup(zName);
      p->aInsert[p->nInsert].zCreate1 = migration_strdup(zCreate1);
      p->aInsert[p->nInsert].zCreate2 = migration_strdup(zCreate2);
      p->aInsert[p->nInsert].zInsert = migration_strdup(zInsert);
      p->aInsert[p->nInsert].srcRoot = pgno;
      p->nInsert++;
      break;
    }

    case 1: assert( 0==strcmp(azSub[iSub], "run") ); {
      int rc = TCL_OK;
      int ii;
      for(ii=0; ii<p->nJob; ii++){
        MigrationJob *pJob = &p->aJob[ii];
        pthread_create(&pJob->tid, NULL, migration_main, (void*)pJob);
      }

      for(ii=0; ii<p->nJob; ii++){
        void *ret = 0;
        MigrationJob *pJob = &p->aJob[ii];
        pthread_join(pJob->tid, &ret);
        if( rc==TCL_OK && pJob->rc!=SQLITE_OK ){
          char zErrcode[24];
          sprintf(zErrcode, "(%d) ", sqlite3_extended_errcode(pJob->db));
          Tcl_AppendResult(interp, "SQL error in job: ", 
              zErrcode, sqlite3_errmsg(pJob->db), (char*)0
          );
          rc = TCL_ERROR;
        }
      }

      return rc;
    }

    case 2: assert( 0==strcmp(azSub[iSub], "destroy") ); {
      if( objc!=2 ){
        Tcl_WrongNumArgs(interp, 1, objv, "");
        return TCL_ERROR;
      }else{
        Tcl_DeleteCommand(interp, Tcl_GetStringFromObj(objv[0], 0));
      }
      break;
    }
    case 3: assert( 0==strcmp(azSub[iSub], "stats") ); {
      int rc = TCL_OK;
      int ii;
      Tcl_Obj *pRes = 0;
      if( objc!=2 ){
        Tcl_WrongNumArgs(interp, 1, objv, "");
        return TCL_ERROR;
      }
      pRes = Tcl_NewObj();
      for(ii=0; ii<p->nJob; ii++){
        sqlite3 *db = p->aJob[ii].db;
        sqlite3_stmt *pStmt = 0;

        rc = sqlite3_prepare_v2(db, "SELECT * FROM hctstats", -1, &pStmt, 0);
        if( rc!=SQLITE_OK ){
          Tcl_AppendResult(interp, "SQL error: ", sqlite3_errmsg(db), (char*)0);
          return TCL_ERROR;
        }
        while( SQLITE_ROW==sqlite3_step(pStmt) ){
          const char *zSubsys = 0;
          const char *zStat = 0;
          const char *zVal = 0;
          char *zKey = 0;
          assert( sqlite3_data_count(pStmt)==3 );

          zSubsys = (const char*)sqlite3_column_text(pStmt, 0);
          zStat = (const char*)sqlite3_column_text(pStmt, 1);
          zVal = (const char*)sqlite3_column_text(pStmt, 2);
          
          zKey = sqlite3_mprintf("%d.%s.%s", ii, zSubsys, zStat);
          Tcl_ListObjAppendElement(interp, pRes, Tcl_NewStringObj(zKey, -1));
          sqlite3_free(zKey);
          Tcl_ListObjAppendElement(interp, pRes, Tcl_NewStringObj(zVal, -1));
        }
      }

      Tcl_SetObjResult(interp, pRes);
      break;
    }
  }

  return TCL_OK;
}

/*
** tclcmd: sqlite_migrate CMD-NAME SOURCE DEST NJOB
*/
static int sqlite_migrate(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  Migration *pNew = 0;
  int nByte = 0;
  int ii = 0;
  int rc = SQLITE_OK;

  /* Command arguments */
  const char *zCmd = 0;
  const char *zSrc = 0;
  const char *zDest = 0;
  int nJob = 0;

  char *zAttach = 0;

  if( objc!=5 ){
    Tcl_WrongNumArgs(interp, 1, objv, "CMD-NAME SOURCE DEST NJOB");
    return TCL_ERROR;
  }

  /* Extract the command line arguments into stack variables. */
  zCmd = Tcl_GetString(objv[1]);
  zSrc = Tcl_GetString(objv[2]);
  zDest = Tcl_GetString(objv[3]);
  if( TCL_OK!=Tcl_GetIntFromObj(interp, objv[4], &nJob) ){
    return TCL_ERROR;
  }
  if( nJob<1 ){
    Tcl_AppendResult(interp, "NJOB must be greater than 0", (char*)0);
    return TCL_ERROR;
  }

  /* Allocate the new migration handle */
  nByte = sizeof(Migration) + nJob * sizeof(MigrationJob);
  pNew = (Migration*)ckalloc(nByte);
  memset(pNew, 0, nByte);
  pNew->mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);

  zAttach = sqlite3_mprintf("ATTACH %Q AS src;", zSrc);

  /* Open a database connection for each job. And attach both source and
  ** destination databases to the handle. */
  pNew->nJob = nJob;
  for(ii=0; ii<nJob; ii++){
    MigrationJob *pJob = &pNew->aJob[ii];
    pJob->pMigration = pNew;
    rc = sqlite3_open(zDest, &pJob->db);
    if( rc!=SQLITE_OK ){
      Tcl_AppendResult(interp, "error in sqlite3_open()", (char*)0);
      return TCL_ERROR;
    }
    sqlite3_busy_timeout(pJob->db, 1000);
    rc = sqlite3_exec(pJob->db, zAttach, 0, 0, 0);
    if( rc!=SQLITE_OK ){
      Tcl_AppendResult(interp, 
          "error attaching databases: ", sqlite3_errmsg(pJob->db), (char*)0
      );
      return TCL_ERROR;
    }
  }
  sqlite3_free(zAttach);

  /* Create the new command. */
  Tcl_CreateObjCommand(interp, zCmd, migration_cmd, (void*)pNew, migration_del);
  Tcl_SetObjResult(interp, objv[1]);
  return TCL_OK;
}

/*
** Decode a pointer to an sqlite3 object.
*/
int getSqlite3Ptr(Tcl_Interp *interp, Tcl_Obj *pObj, sqlite3 **ppDb){
  Tcl_CmdInfo cmdInfo;
  if( Tcl_GetCommandInfo(interp, Tcl_GetString(pObj), &cmdInfo) ){
    sqlite3 **p = (sqlite3 **)cmdInfo.objClientData;
    *ppDb = *p;
  }else{
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** tclcmd: sqlite_imposter DB DBNAME ONOFF TNUM
*/
static int sqlite_imposter(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  sqlite3 *db = 0;
  const char *zDbname = 0;
  int bOnoff = 0;
  int tnum = 0;
  int rc = SQLITE_OK;

  if( objc!=5 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB DBNAME ONOFF TNUM");
    return TCL_ERROR;
  }
  zDbname = Tcl_GetString(objv[2]);
  if( getSqlite3Ptr(interp, objv[1], &db) 
   || Tcl_GetBooleanFromObj(interp, objv[3], &bOnoff) 
   || Tcl_GetIntFromObj(interp, objv[4], &tnum) 
  ){
    return TCL_ERROR;
  }

  rc = sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, db, zDbname, bOnoff,tnum);
  if( rc!=SQLITE_OK ){
    Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** tclcmd: fallocate FILE SIZE
*/
static int test_fallocate(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  const char *zFile = 0;
  Tcl_WideInt sz = 0;
  int fd = -1;

  struct stat sBuf;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "FILE SIZE");
    return TCL_ERROR;
  }
  zFile = Tcl_GetString(objv[1]);
  if( Tcl_GetWideIntFromObj(interp, objv[2], &sz) ){
    return TCL_ERROR;
  }

  fd = open(zFile, O_CREAT|O_RDWR, 0755);
  if( fd<0 ){
    Tcl_SetObjResult(interp, Tcl_NewStringObj("error in open()", -1));
    return TCL_ERROR;
  }

  memset(&sBuf, 0, sizeof(sBuf));
  fstat(fd, &sBuf);
  if( sBuf.st_size<sz ){
    int rc = ftruncate(fd, (off_t)sz);
    sqlite3_int64 iPg = 0;

    if( rc!=0 ){
      Tcl_SetObjResult(interp, Tcl_NewStringObj("error in ftruncate()", -1));
      return TCL_ERROR;
    }

    for(iPg=(sBuf.st_size+4095)/4096; iPg<(sz+4095)/4096; iPg++){
      lseek(fd, (iPg*4096), SEEK_SET);
      rc = write(fd, "", 1);
      if( rc!=1 ){
        Tcl_SetObjResult(interp, Tcl_NewStringObj("error in write()", -1));
        return TCL_ERROR;
      }
    }

#if 0
    rc = posix_fallocate(fd, 0, (off_t)sz);
    if( rc!=0 ){
      Tcl_SetObjResult(interp, Tcl_NewStringObj("error in fallocate()", -1));
      return TCL_ERROR;
    }
#endif
  }

  return TCL_OK;
}

int sqlite3_dbdata_init(sqlite3*, char**, const sqlite3_api_routines*);

/*
** tclcmd: sqlite3_dbdata_init DB
*/
static int test_dbdata_init(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  sqlite3 *db = 0;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  if( getSqlite3Ptr(interp, objv[1], &db) ){
    return TCL_ERROR;
  }

  sqlite3_dbdata_init(db, 0, 0);
  return TCL_OK;
}

/*
** tclcmd: sqlite3_extended_errcode DB
*/
static int test_extended_errcode(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  sqlite3 *db = 0;
  int rc = 0;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  if( getSqlite3Ptr(interp, objv[1], &db) ){
    return TCL_ERROR;
  }

  rc = sqlite3_extended_errcode(db);
  Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));

  return TCL_OK;
}

/*
** Callback registered by test_log_to_stdout().
*/
static void testLogCallback(void *unused, int err, const char *zMsg){
  printf("log: (%d) %s\n", err, zMsg);
  fflush(stdout);
}

/*
** tclcmd: sqlite_log_to_stdout
*/
static int test_log_to_stdout(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  int rc = 0;

  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }

  rc = sqlite3_config(SQLITE_CONFIG_LOG, testLogCallback, (void*)0);
  Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));

  return TCL_OK;
}

/*
** Register commands with the TCL interpreter.
*/
int SqliteThreadTest_Init(Tcl_Interp *interp){
  struct Cmd {
    Tcl_ObjCmdProc *xProc;
    const char *zName;
  } aCmd[] = {
    { sqlite_thread_test, "sqlite_thread_test" },
    { sqlite_thread_test_config, "sqlite_thread_test_config" },
    { sqlite_migrate, "sqlite_migrate" },
    { sqlite_imposter, "sqlite_imposter" },
    { test_fallocate, "fallocate" },
    { test_dbdata_init, "sqlite3_dbdata_init" },
    { test_extended_errcode, "sqlite3_extended_errcode" },
    { test_log_to_stdout, "sqlite_log_to_stdout" }
  };
  int ii;
  for(ii=0; ii<sizeof(aCmd)/sizeof(aCmd[0]); ii++){
    Tcl_CmdInfo cmd;
    if( Tcl_GetCommandInfo(interp, aCmd[ii].zName, &cmd)==0 ){
      Tcl_CreateObjCommand(interp, aCmd[ii].zName, aCmd[ii].xProc, 0, 0);
    }
  }

  return TCL_OK;
}


