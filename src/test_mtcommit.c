/*
** 2022 December 29
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
** OVERVIEW:
**
** This file contains an extension to help test code for multi-threaded
** hctree commit and rollback operations. It allows the test script to
** break a commit into 4 stages:
**
**   1. Allocating the TID value.
**   2. Writing data to the database.
**   3. Allocating the CID value.
**   4. Validation (and possible rollback).
**
** Assuming [db] is a database handle where the "main" database is an
** hct database with an open write transaction.
**
**   db eval {
**     BEGIN CONCURRENT;
**       ...sql statements to write db...
**   }
**   set T [sqlite_hct_mtcommit <name> db]
**   $T step            #; allocates TID value
**   $T step            #; writes data to db file
**   $T step            #; allocates CID value
**   $T step            #; Validates (and possibly rolls back) transaction.
**   $T destroy
**
** If an error occurs at any [step], a Tcl exception is thrown and the
** open transaction ROLLBACK'd. Otherwise, the transaction is committed
** by the 4th [step] call.
**
** This works by spawning an external thread for each object created by
** a [sqlite_hct_mtcommit] command. There are hooks in the library to
** invoke test functions after each of the steps in the COMMIT procedure
** enumerated above. When the hook is invoked, the external thread is
** blocked until the test script thread next calls [step].
**
** A database handle may be configured with the callback function using:
**
**   sqlite3 *db = ...;
**   void(*xCb)(void *, int) = ...;
**   void *pCtx = ...;
**   sqlite3_test_control(SQLITE_TESTCTRL_HCT_MTCOMMIT, db, xCb, pCtx);
**
** For a successful commit, the callback is invoked 3 times. Each time
** the first parameter is a copy of the pCtx value passed to
** sqlite3_test_control(). The integer parameter is 0, 1 or 2, to indicate
** which of the following three reasons has caused the callback to be invoked:
**
**   0: After TID has been allocated.
**   1: After data has been written to database file.
**   2: After CID has been allocated.
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

int getDbPointer(Tcl_Interp *interp, const char *zA, sqlite3 **ppDb);

/*
** One object of this type is created by each [sqlite_hct_mtcommit] command.
**
** iState:
**   (iState==0):
*/
typedef struct TestHctMtCommit TestHctMtCommit;
struct TestHctMtCommit {
  pthread_t t;
  pthread_mutex_t m;
  pthread_cond_t c;

  int iState;
  int iRequestedState;
  sqlite3 *db;
  int rc;
  char *zMsg;
};

static void mtCommitCb(void *pCtx, int ii){
  TestHctMtCommit *p = (TestHctMtCommit*)pCtx;
  p->iState = ii;
  pthread_cond_broadcast(&p->c);
  while( p->iState==p->iRequestedState ){
    pthread_cond_wait(&p->c, &p->m);
  }
}

static void *hct_mtcommit_thread(void *pCtx){
  TestHctMtCommit *p = (TestHctMtCommit*)pCtx;
  sqlite3 *db = p->db;

  pthread_mutex_lock(&p->m);
  sqlite3_test_control(SQLITE_TESTCTRL_HCT_MTCOMMIT, db, mtCommitCb, (void*)p);

  assert( p->iState==-2 && p->iRequestedState==-1 );
  mtCommitCb(pCtx, -1);
  assert( p->iState==-1 && p->iRequestedState>=0 );
  p->rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
  if( p->rc!=SQLITE_OK ){
    p->zMsg = sqlite3_mprintf("error (%d): %s", p->rc, sqlite3_errmsg(db));
  }
  sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
  p->iState = 3;
  pthread_cond_broadcast(&p->c);

  sqlite3_test_control(SQLITE_TESTCTRL_HCT_MTCOMMIT, db, 0, (void*)0);
  pthread_mutex_unlock(&p->m);
  return 0;
}

static void mtcommit_del(ClientData clientData){
  TestHctMtCommit *p = (TestHctMtCommit*)clientData;
  void *dummy = 0;

  p->iRequestedState = 10;
  pthread_cond_broadcast(&p->c);
  pthread_mutex_unlock(&p->m);
  pthread_join(p->t, &dummy);
  pthread_mutex_destroy(&p->m);
  pthread_cond_destroy(&p->c);

  sqlite3_free(p->zMsg);
  ckfree(p);
}

static int mtcommit_cmd(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  TestHctMtCommit *p = (TestHctMtCommit*)clientData;
  const char *azCmd[] = {
    "step", "destroy", 0
  };
  int iCmd = 0;
  int rc = TCL_OK;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUB-COMMAND");
    return TCL_ERROR;
  }
  rc = Tcl_GetIndexFromObj(interp, objv[1], azCmd, "sub-command", 0, &iCmd);
  if( rc!=TCL_OK ) return rc;

  switch( iCmd ){
    case 0: assert( 0==strcmp(azCmd[iCmd], "step") ); {
      if( p->iState<3 ){
        p->iRequestedState = p->iState+1;
        pthread_cond_broadcast(&p->c);
        while( p->iState<p->iRequestedState ){
          pthread_cond_wait(&p->c, &p->m);
        }
      }
      Tcl_ResetResult(interp);
      if( p->iState>=3 && p->rc!=SQLITE_OK ){
        Tcl_SetObjResult(interp, Tcl_NewStringObj(p->zMsg, -1));
        return TCL_ERROR;
      }
      Tcl_SetObjResult(interp, Tcl_NewIntObj(p->iState>=3));
      break;
    }
    case 1: assert( 0==strcmp(azCmd[iCmd], "destroy") ); {
      Tcl_DeleteCommand(interp, Tcl_GetStringFromObj(objv[0], 0));
      break;
    }
  }

  return TCL_OK;
}

/*
** tclcmd: sqlite_hct_mtcommit NAME DBHANDLE
**
*/
static int sqlite_hct_mtcommit(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  TestHctMtCommit *p = 0;
  sqlite3 *db = 0;
  const char *zCmd = 0;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "NAME DBHANDLE");
    return TCL_ERROR;
  }
  zCmd = Tcl_GetString(objv[1]);
  if( getDbPointer(interp, Tcl_GetString(objv[2]), &db) ) return TCL_ERROR;

  p = ckalloc(sizeof(TestHctMtCommit));
  memset(p, 0, sizeof(TestHctMtCommit));

  p->db = db;
  p->iState = -2;
  p->iRequestedState = -1;
  pthread_mutex_init(&p->m, 0);
  pthread_cond_init(&p->c, 0);

  pthread_mutex_lock(&p->m);
  pthread_create(&p->t, 0, hct_mtcommit_thread, (void*)p);
  while( p->iState<p->iRequestedState ){
    pthread_cond_wait(&p->c, &p->m);
  }

  Tcl_CreateObjCommand(interp, zCmd, mtcommit_cmd, (void*)p, mtcommit_del);
  Tcl_SetObjResult(interp, objv[1]);
  return TCL_OK;
}

/*
** Register commands with the TCL interpreter.
*/
int SqliteMtCommit_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "sqlite_hct_mtcommit", sqlite_hct_mtcommit, 0, 0);
  return TCL_OK;
}


