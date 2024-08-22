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
**
**   -follower PATH
**
**   -truncate INTEGER
*/

/**************************************************************************
**
** To create a new migration object:
**
**     sqlite_migrate CMD-NAME SOURCE DEST NJOB
**
** Then, to add imposter table definitions:
**
**     $cmd imposter SQL SOURCE-ROOT DEST-ROOT
**
** To add INSERT statements:
**
**     $cmd insert INSERT
**
** Finally, to run the migration:
**
**     $cmd run
**
** Then:
**
**     $cmd destroy
*/

#include <tcl.h>
#include <sqlite3.h>
#include <sqlite3hct.h>

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
typedef struct FastPrng FastPrng;
struct FastPrng {
  unsigned int x, y;
};

struct TT_Thread {
  TT_Test *pTest;
  Tcl_Obj *pName;                 /* Name of thread */

  FastPrng *pPrng;
  int bDoCheckpoint;
  int bCheckpointer;
  int iTruncate;

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
  Tcl_Obj *pFollower;             /* Path to follower database */
  TT_Thread *aThread;
};

/*
** Generate N bytes of pseudo-randomness using a FastPrng
*/
static void ttFastRandomness(FastPrng *pPrng, int N, void *P){
  unsigned char *pOut = (unsigned char*)P;
  while( N-->0 ){
    pPrng->x = ((pPrng->x)>>1) ^ ((1+~((pPrng->x)&1)) & 0xd0000001);
    pPrng->y = (pPrng->y)*1103515245 + 12345;
    *(pOut++) = (pPrng->x ^ pPrng->y) & 0xff;
  }
}

static void frandomFunc(sqlite3_context *ctx, int nArg, sqlite3_value **aArg){
  FastPrng *p = (FastPrng*)sqlite3_user_data(ctx);
  sqlite3_int64 ret;
  ttFastRandomness(p, sizeof(ret), &ret);
  sqlite3_result_int64(ctx, ret);
}

static void frandomIdFunc(sqlite3_context *ctx, int nArg, sqlite3_value **aArg){
  FastPrng *p = (FastPrng*)sqlite3_user_data(ctx);
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
  FastPrng *p = (FastPrng*)sqlite3_user_data(ctx);
  int nBlob = sqlite3_value_int(aArg[0]);
  unsigned char *aBlob = 0;

  aBlob = (unsigned char*)sqlite3_malloc(nBlob);
  ttFastRandomness(p, nBlob, aBlob);
  sqlite3_result_blob(ctx, aBlob, nBlob, SQLITE_TRANSIENT);
  sqlite3_free(aBlob);
}

static void ttInstallRandomFunctions(TT_Thread *pThread){
  sqlite3_create_function(pThread->db, "frandomid", 
      1, SQLITE_ANY, (void*)pThread->pPrng, frandomIdFunc, 0, 0
  );
  sqlite3_create_function(pThread->db, "frandom", 
      0, SQLITE_ANY, (void*)pThread->pPrng, frandomFunc, 0, 0
  );
  sqlite3_create_function(pThread->db, "frandomblob", 
      1, SQLITE_ANY, (void*)pThread->pPrng, frandomBlobFunc, 0, 0
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
  pNew->pPrng = (FastPrng*)ckalloc(sizeof(FastPrng));
  sqlite3_randomness(sizeof(FastPrng), (void*)pNew->pPrng);
  pNew->pPrng->x |= 1;
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

    if( rc==SQLITE_OK && pThread->iTruncate ){
      sqlite3_hct_journal_truncate(pThread->fdb, pThread->iTruncate);
      sqlite3_hct_journal_truncate(pThread->db, pThread->iTruncate);
      pThread->iTruncate = 0;
    }
  }

  return 0;
}

static int ttValidationHook(
  void *pCopyOfArg,
  sqlite3_int64 iCid,
  const char *zSchema,
  const void *pData, int nData,
  sqlite3_int64 iSchemaCid
){
  TT_Thread *pThread = (TT_Thread*)pCopyOfArg;
  sqlite3 *fdb = pThread->fdb;
  int rc = SQLITE_OK;
  int nJrnlTruncate = pThread->pTest->nJrnlTruncate;

  rc = sqlite3_hct_journal_write(fdb, iCid, zSchema, pData, nData, iSchemaCid);
  if( rc!=SQLITE_OK ){
    sqlite3_hct_journal_write(fdb, iCid, "", 0, 0, 0);
  }

  if( nJrnlTruncate && (iCid % nJrnlTruncate)==0 ){
    pThread->iTruncate = (iCid - nJrnlTruncate);
  }

  return rc;
}

static int ttRunTest(TT_Test *p){
  int ii = 0;

  /* If one is configured, open a follower database handle for each thread. 
  ** And configure a custom-validation hook on the main db.  */
  if( p->pFollower ){
    const char *zPath = Tcl_GetString(p->pFollower);
    for(ii=0; ii<p->nThread; ii++){
      TT_Thread *pThread = &p->aThread[ii];
      int rc = sqlite3_open_v2(zPath, &pThread->fdb, 
          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
          SQLITE_OPEN_URI |SQLITE_OPEN_NOMUTEX, 0
      );
      if( rc!=SQLITE_OK ){
        Tcl_ResetResult(p->interp);
        Tcl_AppendResult(p->interp, 
            "error opening follower db \"", zPath, "\"", (char*)0
        );
        return TCL_ERROR;
      }
      sqlite3_hct_journal_hook(pThread->db, pThread, ttValidationHook);
    }
  }

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
    "-nsecond", "-ntransaction", "-sqlconfig", "-nwalpage", "-follower", 
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
    case 4: assert( 0==strcmp(azOpt[iOpt], "-follower") ); {
      if( pVal ){
        if( p->pFollower ){
          Tcl_DecrRefCount(p->pFollower);
          p->pFollower = 0;
        }
        if( Tcl_GetCharLength(pVal)>0 ){
          p->pFollower = Tcl_DuplicateObj(pVal);
          Tcl_IncrRefCount(p->pFollower);
        }
      }
      if( p->pFollower ){
        Tcl_SetObjResult(interp, p->pFollower);
      }else{
        Tcl_ResetResult(interp);
      }
      break;
    }
    case 5: assert( 0==strcmp(azOpt[iOpt], "-jtruncate") ); {
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
      ckfree(pThread->pPrng);
    }
    if( p->pDatabase ) Tcl_DecrRefCount(p->pDatabase);
    if( p->pSqlConfig ) Tcl_DecrRefCount(p->pSqlConfig);
    if( p->pFollower ) Tcl_DecrRefCount(p->pFollower);
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

struct MigrationJob {
  sqlite3 *db;
  int rc;
  pthread_t tid;
  Migration *pMigration;
};

/*
** mutex:
**   Mutex used to protect nInsertDone within the [run] sub-command.
*/
struct Migration {
  sqlite3_mutex *mutex;
  int nInsertDone;

  int nInsert;
  char **azInsert;

  int nJob;
  MigrationJob aJob[1];
};

static void migration_del(ClientData clientData){
  Migration *p = (Migration*)clientData;
  int ii;
  for(ii=0; ii<p->nJob; ii++){
    sqlite3_close(p->aJob[ii].db);
  }
  for(ii=0; ii<p->nInsert; ii++){
    ckfree(p->azInsert[ii]);
  }
  ckfree(p->azInsert);
  ckfree(p);
}

static void *migration_main(void *pArg){
  MigrationJob *pJob = (MigrationJob*)pArg;
  Migration *p = pJob->pMigration;

  sqlite3_hct_migrate_mode(pJob->db, 1);

  while( pJob->rc==SQLITE_OK ){
    const char *zInsert = 0;
    sqlite3_mutex_enter(p->mutex);
    if( p->nInsertDone<p->nInsert ){
      zInsert = p->azInsert[p->nInsertDone];
      p->nInsertDone++;
    }
    sqlite3_mutex_leave(p->mutex);

    if( zInsert==0 ) break;
printf("SQL (%d): %s\n", (int)(pJob-p->aJob), zInsert);
    pJob->rc = sqlite3_exec(pJob->db, "BEGIN CONCURRENT", 0, 0, 0);
    if( pJob->rc==SQLITE_OK ){
      pJob->rc = sqlite3_exec(pJob->db, zInsert, 0, 0, 0);
    }
    if( pJob->rc==SQLITE_OK ){
      pJob->rc = sqlite3_exec(pJob->db, "COMMIT", 0, 0, 0);
    }
  }

  sqlite3_hct_migrate_mode(pJob->db, 0);

  return 0;
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
    "insert", "imposter", "run", "destroy", "stats", 0
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
    case 0: assert( 0==strcmp(azSub[iSub], "insert") ); {
      int nByte = 0;
      const char *zInsert = 0;
      char *zCopy = 0;
      if( objc!=3 ){
        Tcl_WrongNumArgs(interp, 2, objv, "SQL");
        return TCL_ERROR;
      }
      zInsert = Tcl_GetString(objv[2]);
      nByte = strlen(zInsert) + 1;

      if( 0==(p->nInsert & (p->nInsert-1)) ){
        int nNew = (p->nInsert==0 ? 16 : p->nInsert*2);
        char **azNew = ckrealloc(p->azInsert, sizeof(char*)*nNew);
        memset(&azNew[p->nInsert], 0, sizeof(char*) * (nNew - p->nInsert));
        p->azInsert = azNew;
      }

      zCopy = ckalloc(nByte);
      memcpy(zCopy, zInsert, nByte);
      p->azInsert[p->nInsert] = zCopy;
      p->nInsert++;
      break;
    }

    case 1: assert( 0==strcmp(azSub[iSub], "imposter") ); {
      const char *zSql = 0;
      sqlite3_int64 iSrc = 0;
      sqlite3_int64 iDest = 0;
      int ii = 0;

      if( objc!=5 ){
        Tcl_WrongNumArgs(interp, 2, objv, "SQL SOURCE-ROOT DEST-ROOT");
        return TCL_ERROR;
      }

      zSql = Tcl_GetString(objv[2]);
      if( Tcl_GetWideIntFromObj(interp, objv[3], &iSrc)
       || Tcl_GetWideIntFromObj(interp, objv[4], &iDest)
      ){
        return TCL_ERROR;
      }

      for(ii=0; ii<p->nJob; ii++){
        int rc = SQLITE_OK;
        sqlite3 *db = p->aJob[ii].db;
        sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, db, "src", 1, (int)iSrc);
        rc = sqlite3_exec(db, zSql, 0, 0, 0);
        sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, db, "src", 0, 0);

        if( rc==SQLITE_OK ){
          sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER,db,"main",1,(int)iDest);
          rc = sqlite3_exec(db, zSql, 0, 0, 0);
          sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, db, "main", 0, 0);
        }

        if( rc!=SQLITE_OK ){
          Tcl_AppendResult(interp, 
              "error installing imposters: ", sqlite3_errmsg(db), (char*)0
          );
          return TCL_ERROR;
        }
      }

      break;
    }

    case 2: assert( 0==strcmp(azSub[iSub], "run") ); {
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
          rc = TCL_ERROR;
          Tcl_AppendResult(interp, 
              "SQL error in job: ", sqlite3_errmsg(pJob->db), (char*)0
          );
        }
      }

      return rc;
    }

    case 3: assert( 0==strcmp(azSub[iSub], "destroy") ); {
      if( objc!=2 ){
        Tcl_WrongNumArgs(interp, 1, objv, "");
        return TCL_ERROR;
      }else{
        Tcl_DeleteCommand(interp, Tcl_GetStringFromObj(objv[0], 0));
      }
      break;
    }
    case 4: assert( 0==strcmp(azSub[iSub], "stats") ); {
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
** Register commands with the TCL interpreter.
*/
int SqliteThreadTest_Init(Tcl_Interp *interp){
  struct Cmd {
    Tcl_ObjCmdProc *xProc;
    const char *zName;
  } aCmd[] = {
    { sqlite_thread_test, "sqlite_thread_test" },
    { sqlite_thread_test_config, "sqlite_thread_test_config" },
    { sqlite_migrate, "sqlite_migrate" }
  };
  int ii;
  for(ii=0; ii<sizeof(aCmd)/sizeof(aCmd[0]); ii++){
    Tcl_CreateObjCommand(interp, aCmd[ii].zName, aCmd[ii].xProc, 0, 0);
  }
  return TCL_OK;
}


