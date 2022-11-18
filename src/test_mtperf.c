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
*/

#include <tcl.h>
#include <sqlite3.h>

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

#define TT_DEFAULT_NSECOND 10

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

  FastPrng prng;

  int nStep;
  TT_Step *aStep;
  sqlite3 *db;

  /* Result variables - number of successful transactions and number of
  ** SQLITE_BUSY transactions */
  int nTransOk;
  int nTransBusy;
  Tcl_Obj *pErr;                  /* If an error has occurred */

  pthread_t tid;                  /* Thread id from pthread_create() */
};

struct TT_Test {
  Tcl_Obj *pDatabase;             /* Path (or URI) to test database */
  Tcl_Interp *interp;
  int nTrans;                     /* At least this many transactions */
  int nSecond;                    /* Number of seconds to run for */
  int nThread;                    /* Number of entries in aThread[] */
  Tcl_Obj *pSqlConfig;            /* SQL script to configure db handles */
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

/*
** Add a new thread to test object p.
*/
static int ttAddThread(TT_Test *p, Tcl_Obj *pName, Tcl_Obj *pSql){
  int nNew = p->nThread+1;
  TT_Thread *pNew = 0;
  const char *zSql = Tcl_GetString(pSql);
  int rc = SQLITE_OK;

  /* Extend the aThread[] array. */
  p->aThread = (TT_Thread*)ckrealloc(p->aThread, nNew*sizeof(TT_Thread));
  pNew = &p->aThread[p->nThread];
  p->nThread = nNew;
  memset(pNew, 0, sizeof(TT_Thread));

  /* Populate TT_Thread.pTest and TT_Thread.pName. */
  sqlite3_randomness(sizeof(FastPrng), (void*)&pNew->prng);
  pNew->prng.x |= 1;
  pNew->pTest = p;
  pNew->pName = pName;
  Tcl_IncrRefCount(pNew->pName);

  /* Open the database handle for this thread */
  rc = sqlite3_open_v2(Tcl_GetString(p->pDatabase), &pNew->db, 
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
      SQLITE_OPEN_URI |SQLITE_OPEN_NOMUTEX, 0
  );
  if( rc!=SQLITE_OK ){
    Tcl_ResetResult(p->interp);
    Tcl_AppendResult(
        p->interp, "error opening db: ", sqlite3_errmsg(pNew->db), (char*)0
    );
    return TCL_ERROR;
  }

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

  sqlite3_create_function(pNew->db, "frandomid", 
      1, SQLITE_ANY, (void*)&pNew->prng, frandomIdFunc, 0, 0
  );
  sqlite3_create_function(pNew->db, "frandom", 
      0, SQLITE_ANY, (void*)&pNew->prng, frandomFunc, 0, 0
  );
  sqlite3_create_function(pNew->db, "frandomblob", 
      1, SQLITE_ANY, (void*)&pNew->prng, frandomBlobFunc, 0, 0
  );

  while( zSql ){
    sqlite3_stmt *pStmt = 0;
    int nByte = (pNew->nStep+1)*sizeof(TT_Step);
    rc = sqlite3_prepare_v2(pNew->db, zSql, -1, &pStmt, &zSql);
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
    pNew->aStep[pNew->nStep].eType = STEP_SQL;
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
    int rc;
    int ii;
    if( nTrans==0 && tt_current_time()>=iStop ) break;
    if( nTrans>0 && (pThread->nTransOk+pThread->nTransBusy)>=nTrans ) break;

    for(ii=0; ii<pThread->nStep; ii++){
      TT_Step *pStep = &pThread->aStep[ii];
      while( sqlite3_step(pStep->pStmt)==SQLITE_ROW );
      rc = sqlite3_reset(pStep->pStmt);
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


/*
** Implementation of the test object command.
**
**   $cmd configure OPTION ?VALUE?
**   $cmd thread NAME SQL-SCRIPT 
**   $cmd run
**   $cmd result ARRAY-NAME
**   $cmd destroy
**
** Configure options are:
**
**   -nsecond
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
        const char *azOpt[] = {
          "-nsecond", "-ntransaction", "-sqlconfig", 0
        };
        int iOpt = 0;
        rc = Tcl_GetIndexFromObj(interp, objv[2], azOpt, "option", 0, &iOpt);
        if( rc!=TCL_OK ) return rc;

        switch( iOpt ){
          case 0: assert( 0==strcmp(azOpt[iOpt], "-nsecond") ); {
            if( objc==4 ){
              int nSec = 0;
              rc = Tcl_GetIntFromObj(interp, objv[3], &nSec);
              if( rc!=TCL_OK ) return TCL_ERROR;
              p->nSecond = nSec;
            }
            Tcl_SetObjResult(interp, Tcl_NewIntObj(p->nSecond));
            break;
          }
          case 1: assert( 0==strcmp(azOpt[iOpt], "-ntransaction") ); {
            if( objc==4 ){
              int nTrans = 0;
              rc = Tcl_GetIntFromObj(interp, objv[3], &nTrans);
              if( rc!=TCL_OK ) return TCL_ERROR;
              p->nTrans = nTrans;
            }
            Tcl_SetObjResult(interp, Tcl_NewIntObj(p->nTrans));
            break;
          }
          case 2: assert( 0==strcmp(azOpt[iOpt], "-sqlconfig") ); {
            if( objc==4 ){
              if( p->pSqlConfig ) Tcl_DecrRefCount(p->pSqlConfig);
              p->pSqlConfig = objv[3];
              Tcl_IncrRefCount(p->pSqlConfig);
            }
            if( p->pSqlConfig ){
              Tcl_SetObjResult(interp, p->pSqlConfig);
            }else{
              Tcl_ResetResult(interp);
            }
            break;
          }
          default:
            assert( !"cannot happen" );
        }
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
    }
    if( p->pDatabase ) Tcl_DecrRefCount(p->pDatabase);
    if( p->pSqlConfig ) Tcl_DecrRefCount(p->pSqlConfig);
    ckfree(p->aThread);
    ckfree(p);
  }
}

/*
** tclcmd: sqlite_thread_test OBJ-NAME DBPATH
**
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
  p->pDatabase = objv[2];
  p->interp = interp;
  Tcl_IncrRefCount(p->pDatabase);

  Tcl_CreateObjCommand(interp, Tcl_GetString(objv[1]), tt_cmd, (void*)p,tt_del);
  Tcl_SetObjResult(interp, objv[1]);
  return TCL_OK;
}

/*
** Register commands with the TCL interpreter.
*/
int SqliteThreadTest_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "sqlite_thread_test", sqlite_thread_test, 0, 0);
  return TCL_OK;
}


