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

typedef struct ValidationHook ValidationHook;
struct ValidationHook {
  Tcl_Interp *interp;
  Tcl_Obj *pScript;
};

static void testValidationHookDestroy(void *pCtx){
  ValidationHook *pHook = (ValidationHook*)pCtx;
  Tcl_DecrRefCount(pHook->pScript);
  ckfree(pHook);
}
static void testJournalHookDummy(
  sqlite3_context *pCtx, 
  int nArg, 
  sqlite3_value **apArg
){
}

static int testValidationHook(
  void *pArg,
  sqlite3_int64 iCid,
  const char *zSchema,
  const void *pData, int nData,
  sqlite3_int64 iSchemaCid
){
  ValidationHook *pHook = (ValidationHook*)pArg;
  Tcl_Obj *pEval = 0;
  Tcl_Interp *p = pHook->interp;
  int rc = TCL_OK;
  int res = 0;

  pEval = Tcl_DuplicateObj(pHook->pScript);
  Tcl_IncrRefCount(pEval);
  if( Tcl_ListObjAppendElement(p, pEval, Tcl_NewWideIntObj(iCid))
   || Tcl_ListObjAppendElement(p, pEval, Tcl_NewStringObj(zSchema, -1))
   || Tcl_ListObjAppendElement(p, pEval, Tcl_NewByteArrayObj(pData, nData))
   || Tcl_ListObjAppendElement(p, pEval, Tcl_NewWideIntObj(iSchemaCid))
  ){
    rc = TCL_ERROR;
  }

  if( rc==TCL_OK ){
    rc = Tcl_EvalObjEx(p, pEval, TCL_EVAL_DIRECT);
  }
  if( rc==TCL_OK ){
    Tcl_Obj *pObj = Tcl_GetObjResult(p);
    Tcl_GetIntFromObj(p, pObj, &res);
  }

  Tcl_DecrRefCount(pEval);
  assert( rc==TCL_OK );
  return res;
}

/*
** tclcmd: sqlite3_hct_journal_hook DB SCRIPT
*/
static int test_hct_journal_hook(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  ValidationHook *pNew = 0;
  sqlite3 *db = 0;
  int rc = TCL_OK;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB SCRIPT");
    return TCL_ERROR;
  }
  rc = getDbPointer(interp, Tcl_GetString(objv[1]), &db);
  if( rc!=TCL_OK ) return rc;

  pNew = (ValidationHook*)ckalloc(sizeof(ValidationHook));
  memset(pNew, 0, sizeof(ValidationHook));
  pNew->interp = interp;
  pNew->pScript = Tcl_DuplicateObj(objv[2]);
  Tcl_IncrRefCount(pNew->pScript);

  sqlite3_hct_journal_hook(db, (void*)pNew, testValidationHook);
  sqlite3_create_function_v2(
      db, "_hct_journal_hook_dummy", 0, SQLITE_UTF8, (void*)pNew,
      testJournalHookDummy, 0, 0, testValidationHookDestroy
  );

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
    case SQLITE_HCT_JOURNAL_MODE_FOLLOWER:
      Tcl_SetObjResult(interp, Tcl_NewStringObj("FOLLOWER", -1));
      break;
    case SQLITE_HCT_JOURNAL_MODE_LEADER:
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
  const char *azArg[] = { "FOLLOWER", "LEADER", 0 };
  sqlite3 *db = 0;
  int rc = TCL_OK;
  int eVal;

  assert( SQLITE_HCT_JOURNAL_MODE_FOLLOWER==0 );
  assert( SQLITE_HCT_JOURNAL_MODE_LEADER==1 );

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
** tclcmd: sqlite3_hct_journal_write DB MINCID
*/
static int test_hct_journal_truncate(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  sqlite3 *db = 0;
  sqlite3_int64 iMinCid = 0;
  int rc = TCL_OK;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB MINCID");
    return TCL_ERROR;
  }
  rc = getDbPointer(interp, Tcl_GetString(objv[1]), &db);
  if( rc!=TCL_OK ) return rc;
  rc = Tcl_GetWideIntFromObj(interp, objv[2], &iMinCid);
  if( rc!=TCL_OK ) return rc;

  rc = sqlite3_hct_journal_truncate(db, iMinCid);
  Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3ErrName(rc), -1));
  return TCL_OK;
}

/*
** tclcmd: sqlite3_hct_journal_write DB CID SCHEMA DATA SCHEMA_VERSION
*/
static int test_hct_journal_write(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  sqlite3_int64 iCid = 0;
  sqlite3_int64 iSchemaCid = 0;
  const char *zSchema = 0;
  const unsigned char *aData = 0;
  int nData = 0;
  int rc;
  sqlite3 *db = 0;

  if( objc!=6 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB CID SCHEMA DATA SCHEMACID");
    return TCL_ERROR;
  }
  rc = getDbPointer(interp, Tcl_GetString(objv[1]), &db);
  if( rc!=TCL_OK ) return rc;
  rc = Tcl_GetWideIntFromObj(interp, objv[2], &iCid);
  if( rc!=TCL_OK ) return rc;
  zSchema = Tcl_GetString(objv[3]);
  aData = Tcl_GetByteArrayFromObj(objv[4], &nData);
  rc = Tcl_GetWideIntFromObj(interp, objv[5], &iSchemaCid);
  if( rc!=TCL_OK ) return rc;

  rc = sqlite3_hct_journal_write(db, iCid, zSchema, aData, nData, iSchemaCid);

  Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3ErrName(rc), -1));
  return TCL_OK;
}

/*
** tclcmd: sqlite3_hct_journal_rollback DB MINCID
*/
static int test_hct_journal_rollback(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  sqlite3 *db = 0;
  int rc = TCL_OK;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB MINCID");
    return TCL_ERROR;
  }
  rc = getDbPointer(interp, Tcl_GetString(objv[1]), &db);
  if( rc!=TCL_OK ) return rc;

  rc = sqlite3_hct_journal_rollback(db, SQLITE_HCT_ROLLBACK_MAXIMUM);
  Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3ErrName(rc), -1));
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
    { "sqlite3_hct_journal_hook", test_hct_journal_hook },
    { "sqlite3_hct_journal_mode",            test_hct_journal_mode },
    { "sqlite3_hct_journal_setmode",         test_hct_journal_setmode },
    { "sqlite3_hct_journal_write",           test_hct_journal_write },
    { "sqlite3_hct_journal_snapshot",        test_hct_journal_snapshot },
    { "sqlite3_hct_journal_truncate",        test_hct_journal_truncate },
    { "sqlite3_hct_journal_rollback",        test_hct_journal_rollback },
  };
  int ii = 0;

  for(ii=0; ii<sizeof(aCmd)/sizeof(aCmd[0]); ii++){
    Tcl_CreateObjCommand(interp, aCmd[ii].zName, aCmd[ii].x, 0, 0);
  }

  return TCL_OK;
}


