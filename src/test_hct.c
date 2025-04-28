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
#include <assert.h>
#include <sqlite3.h>
#include <sqlite3hct.h>
#include <string.h>

int getDbPointer(Tcl_Interp *interp, const char *zA, sqlite3 **ppDb);
const char *sqlite3ErrName(int);

/*
** tclcmd: sqlite3_hct_journal_init DB
*/
static int test_hct_journal_init(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  sqlite3 *db = 0;
  int rc = TCL_OK;
  int src = SQLITE_OK;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }

  rc = getDbPointer(interp, Tcl_GetString(objv[1]), &db);
  if( rc!=TCL_OK ) return rc;

  src = sqlite3_hct_journal_init(db);
  Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3ErrName(src), -1));
  return TCL_OK;
}

/*
** tclcmd: sqlite3_hct_journal_mode DB
*/
static int test_hct_journal_mode(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  sqlite3 *db = 0;
  int rc = TCL_OK;
  int eRet;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  rc = getDbPointer(interp, Tcl_GetString(objv[1]), &db);
  if( rc!=TCL_OK ) return rc;

  eRet = sqlite3_hct_journal_mode(db);
  switch( eRet ){
    case SQLITE_HCT_NORMAL:
      Tcl_SetObjResult(interp, Tcl_NewStringObj("NORMAL", -1));
      break;
    case SQLITE_HCT_FOLLOWER:
      Tcl_SetObjResult(interp, Tcl_NewStringObj("FOLLOWER", -1));
      break;
    case SQLITE_HCT_LEADER:
      Tcl_SetObjResult(interp, Tcl_NewStringObj("LEADER", -1));
      break;
    default:
      Tcl_SetObjResult(interp, Tcl_NewIntObj(eRet));
      break;
  }

  return TCL_OK;
}

/*
** tclcmd: sqlite3_hct_journal_setmode DB mode
*/
static int test_hct_journal_setmode(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  const char *azArg[] = { "NORMAL", "FOLLOWER", "LEADER", 0 };
  sqlite3 *db = 0;
  int rc = TCL_OK;
  int eVal;

  assert( SQLITE_HCT_NORMAL==0 );
  assert( SQLITE_HCT_FOLLOWER==1 );
  assert( SQLITE_HCT_LEADER==2 );

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  rc = getDbPointer(interp, Tcl_GetString(objv[1]), &db);
  if( rc!=TCL_OK ) return rc;
  rc = Tcl_GetIndexFromObj(interp, objv[2], azArg, "ARGUMENT", 0, &eVal);
  if( rc!=TCL_OK ) return rc;

  rc = sqlite3_hct_journal_setmode(db, eVal);
  Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3ErrName(rc), -1));

  return TCL_OK;
}

/*
** tclcmd: sqlite3_hct_journal_snapshot DB
*/
static int test_hct_journal_snapshot(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  sqlite3 *db = 0;
  int rc = TCL_OK;
  sqlite3_int64 iCid = 0;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  rc = getDbPointer(interp, Tcl_GetString(objv[1]), &db);
  if( rc!=TCL_OK ) return rc;

  rc = sqlite3_hct_journal_snapshot(db, &iCid);
  if( rc==SQLITE_OK ){
    Tcl_SetObjResult(interp, Tcl_NewWideIntObj(iCid));
  }else{
    const char *zErr = sqlite3_errmsg(db);
    Tcl_ResetResult(interp);
    Tcl_AppendResult(interp, sqlite3ErrName(rc), " ", zErr, (char*)0);
    rc = TCL_ERROR;
  }
  return rc;
}

/*
** tclcmd: sqlite3_hct_journal_leader_commit DB DATA
*/
static int test_hct_journal_leader_commit(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  sqlite3 *db = 0;
  int rc = TCL_OK;
  const char *zSql = 0;
  int nSql = 0;
  sqlite3_int64 iCid = 0;
  sqlite3_int64 iSnapshot = 0;
  Tcl_Obj *pRet = 0;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB SQL");
    return TCL_ERROR;
  }
  rc = getDbPointer(interp, Tcl_GetString(objv[1]), &db);
  if( rc!=TCL_OK ) return rc;
  zSql = Tcl_GetStringFromObj(objv[2], &nSql);

  rc = sqlite3_hct_journal_leader_commit(
      db, (const unsigned char*)zSql, nSql, &iCid, &iSnapshot
  );

  pRet = Tcl_NewObj();
  Tcl_ListObjAppendElement(interp,pRet,Tcl_NewStringObj(sqlite3ErrName(rc),-1));
  Tcl_ListObjAppendElement(interp, pRet, Tcl_NewWideIntObj(iCid));
  Tcl_ListObjAppendElement(interp, pRet, Tcl_NewWideIntObj(iSnapshot));
  Tcl_SetObjResult(interp, pRet);

  return TCL_OK;
}

/*
** tclcmd: sqlite3_hct_journal_follower_commit DB DATA CID SNAPSHOT
*/
static int test_hct_journal_follower_commit(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  sqlite3 *db = 0;
  int rc = TCL_OK;
  const char *zSql = 0;
  int nSql = 0;
  sqlite3_int64 iCid = 0;
  sqlite3_int64 iSnapshot = 0;
  Tcl_Obj *pRet = 0;

  if( objc!=5 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB SQL CID SNAPSHOT");
    return TCL_ERROR;
  }
  rc = getDbPointer(interp, Tcl_GetString(objv[1]), &db);
  if( rc!=TCL_OK ) return rc;
  zSql = Tcl_GetStringFromObj(objv[2], &nSql);
  if( Tcl_GetWideIntFromObj(interp, objv[3], &iCid) ) return TCL_ERROR;
  if( Tcl_GetWideIntFromObj(interp, objv[4], &iSnapshot) ) return TCL_ERROR;

  rc = sqlite3_hct_journal_follower_commit(
      db, (const unsigned char*)zSql, nSql, iCid, iSnapshot
  );

  pRet = Tcl_NewStringObj(sqlite3ErrName(rc),-1);
  Tcl_SetObjResult(interp, pRet);

  return TCL_OK;
}

/*
** Register commands with the TCL interpreter.
*/
int SqliteHctTest_Init(Tcl_Interp *interp){
  struct TestCmd {
    const char *zName;
    Tcl_ObjCmdProc *x;
  } aCmd[] = {
    { "sqlite3_hct_journal_init",            test_hct_journal_init },
    { "sqlite3_hct_journal_mode",            test_hct_journal_mode },
    { "sqlite3_hct_journal_setmode",         test_hct_journal_setmode },
    { "sqlite3_hct_journal_snapshot",        test_hct_journal_snapshot },
    { "sqlite3_hct_journal_leader_commit",   test_hct_journal_leader_commit },
    { "sqlite3_hct_journal_follower_commit", test_hct_journal_follower_commit },
  };
  int ii = 0;

  for(ii=0; ii<sizeof(aCmd)/sizeof(aCmd[0]); ii++){
    Tcl_CreateObjCommand(interp, aCmd[ii].zName, aCmd[ii].x, 0, 0);
  }

  return TCL_OK;
}


