/*
** 2020-09-22
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
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include "hctInt.h"
#include <assert.h>
#include <string.h>

#define LARGEST_INT64  (0xffffffff|(((i64)0x7fffffff)<<32))
#define SMALLEST_INT64 (((i64)-1) - LARGEST_INT64)

/* Forward declaration of subclasses of virtual table objects */
typedef struct hct_vtab hct_vtab;
typedef struct hct_cursor hct_cursor;

/* Primitive types */
typedef unsigned char u8;
typedef unsigned int u32;
typedef sqlite3_uint64 u64;

/* An open connection to an HCT table */
struct hct_vtab {
  sqlite3_vtab base;          /* Base class - must be first */
  sqlite3 *db;
  HctTree *pTree;
  u32 iRoot;                  /* Current root page */

  char *zTable;               /* Name of this table */
  char *zDb;                  /* Name of this db ("main", "temp" etc.) */
};


/* 
** hct_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
struct hct_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  i64 iMin;
  i64 iMax;
  int bDesc;                 /* DESC order scan */
  HctTreeCsr *pCsr;
};

/*
** This method is the destructor for hct_cursor objects.
*/
static int hctVtabDisconnect(sqlite3_vtab *pVtab){
  hct_vtab *p = (hct_vtab*)pVtab;
  if( p ){
    sqlite3HctTreeFree(p->pTree);
    sqlite3_free(p);
  }
  return SQLITE_OK;
}

static int hctVtabDestroy(sqlite3_vtab *pVtab){
  return hctVtabDisconnect(pVtab);
}

/*
** The argv[] array contains the following:
**
**   argv[0]   -> module name  ("hct")
**   argv[1]   -> database name
**   argv[2]   -> table name
**   argv[...] -> "column name" and other module argument fields.
*/
static int hctVtabInit(
  int bCreate,
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  hct_vtab *pNew;
  int rc = SQLITE_OK;
  const char *zDb = argv[1];
  const char *zTable = argv[2];
  int nByte;
  int nDb = strlen(zDb);
  int nTable = strlen(zTable);

  nByte = sizeof(hct_vtab) + nDb + nTable + 2;
  pNew = (hct_vtab*)sqlite3_malloc(nByte);
  if( pNew==0 ){
    rc = SQLITE_NOMEM;
  }else{
    memset(pNew, 0, nByte);
    pNew->db = db;
    pNew->zTable = (char*)&pNew[1];
    memcpy(pNew->zTable, zTable, nTable);
    pNew->zDb = &pNew->zTable[nTable+1];
    pNew->iRoot = 1;
    memcpy(pNew->zDb, zDb, nDb);
    rc = sqlite3HctTreeNew(&pNew->pTree);
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3_declare_vtab(db, "CREATE TABLE abc(k INTEGER, v BLOB)");
  }

  if( rc!=SQLITE_OK ){
    hctVtabDisconnect((sqlite3_vtab*)pNew);
    pNew = 0;
  }
  *ppVtab = (sqlite3_vtab*)pNew;
  return rc;
}

/*
** The hctVtabConnect() method is invoked to create a new
** hctVtab_vtab that describes the virtual table.
*/
static int hctVtabConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  return hctVtabInit(0, db, pAux, argc, argv, ppVtab, pzErr);
}

static int hctVtabCreate(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  return hctVtabInit(1, db, pAux, argc, argv, ppVtab, pzErr);
}

/*
** Destructor for a hct_cursor.
*/
static int hctVtabClose(sqlite3_vtab_cursor *cur){
  hct_cursor *pCsr = (hct_cursor*)cur;
  if( pCsr ){
    sqlite3HctTreeCsrClose(pCsr->pCsr);
    sqlite3_free(pCsr);
  }
  return SQLITE_OK;
}

/*
** Constructor for a new hct_cursor object.
*/
static int hctVtabOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  hct_vtab *pTab = (hct_vtab*)pVtab;
  hct_cursor *pNew;
  int rc = SQLITE_OK;

  pNew = sqlite3_malloc(sizeof(hct_cursor));
  if( pNew==0 ){
    rc = SQLITE_NOMEM;
  }else{
    memset(pNew, 0, sizeof(hct_cursor));
    rc = sqlite3HctTreeCsrOpen(pTab->pTree, pTab->iRoot, &pNew->pCsr);
  }

  if( rc!=SQLITE_OK ){
    hctVtabClose((sqlite3_vtab_cursor*)pNew);
    pNew = 0;
  }
  *ppCursor = (sqlite3_vtab_cursor*)pNew;
  return rc;
}


/*
** Advance a hct_cursor to its next row of output.
*/
static int hctVtabNext(sqlite3_vtab_cursor *cur){
  hct_cursor *pCsr = (hct_cursor*)cur;
  if( pCsr->bDesc ){
    return sqlite3HctTreeCsrPrev(pCsr->pCsr);
  }
  return sqlite3HctTreeCsrNext(pCsr->pCsr);
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int hctVtabEof(sqlite3_vtab_cursor *cur){
  hct_cursor *pCsr = (hct_cursor*)cur;
  int res;
  res = sqlite3HctTreeCsrEof(pCsr->pCsr);
  if( res==0 ){
    i64 iKey;
    sqlite3HctTreeCsrKey(pCsr->pCsr, &iKey);
    if( iKey<pCsr->iMin || iKey>pCsr->iMax ) res = 1;
  }
  return res;
}

/*
** Rowids are not supported by the underlying virtual table.  So always
** return 0 for the rowid.
*/
static int hctVtabRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  hct_cursor *pCsr = (hct_cursor*)cur;
  return sqlite3HctTreeCsrKey(pCsr->pCsr, pRowid);
}

/*
** Return values of columns for the row at which the hct_cursor
** is currently pointing.
*/
static int hctVtabColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int iCol                    /* Which column to return */
){
  hct_cursor *pCsr = (hct_cursor*)cur;
  assert( iCol==0 || iCol==1 );
  if( iCol==0 ){
    i64 iKey = 0;
    sqlite3HctTreeCsrKey(pCsr->pCsr, &iKey);
    sqlite3_result_int64(ctx, iKey);
  }else{
    int nData = 0;
    const u8 *aData = 0;
    sqlite3HctTreeCsrData(pCsr->pCsr, &nData, &aData);
    sqlite3_result_blob(ctx, aData, nData, SQLITE_TRANSIENT);
  }
  return SQLITE_OK;
}

/* Move to the first row to return.
*/
static int hctVtabFilter(
  sqlite3_vtab_cursor *cur, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  int rc = SQLITE_OK;
  hct_cursor *pCsr = (hct_cursor*)cur;
  int iArg = 0;

  pCsr->bDesc = (0!=(idxNum & 0x20));
  pCsr->iMin = SMALLEST_INT64;
  pCsr->iMax = LARGEST_INT64;

  if( idxNum & 0x01 ){
    i64 iVal = sqlite3_value_int64(argv[iArg++]);
    pCsr->iMin = pCsr->iMax = iVal;
  }
  if( idxNum & 0x02 ){
    i64 iVal = sqlite3_value_int64(argv[iArg++]);
    if( iVal>=pCsr->iMin ) pCsr->iMin = iVal + (iVal==LARGEST_INT64?0:1);
  }
  if( idxNum & 0x04 ){
    i64 iVal = sqlite3_value_int64(argv[iArg++]);
    if( iVal<=pCsr->iMax ) pCsr->iMax = iVal - (iVal==SMALLEST_INT64?0:1);
  }
  if( idxNum & 0x08 ){
    i64 iVal = sqlite3_value_int64(argv[iArg++]);
    if( iVal>pCsr->iMin ) pCsr->iMin = iVal;
  }
  if( idxNum & 0x10 ){
    i64 iVal = sqlite3_value_int64(argv[iArg++]);
    if( iVal<pCsr->iMax ) pCsr->iMax = iVal;
  }

  if( pCsr->bDesc ){
    if( pCsr->iMax!=LARGEST_INT64 ){
      int res;
      rc = sqlite3HctTreeCsrSeek(pCsr->pCsr, 0, pCsr->iMax, &res);
      if( rc==SQLITE_OK && res>0 ){
        rc = sqlite3HctTreeCsrPrev(pCsr->pCsr);
      }
    }else{
      rc = sqlite3HctTreeCsrLast(pCsr->pCsr);
    }
  }else{
    if( pCsr->iMin!=SMALLEST_INT64 ){
      int res;
      rc = sqlite3HctTreeCsrSeek(pCsr->pCsr, 0, pCsr->iMax, &res);
      if( rc==SQLITE_OK && res<0 && !sqlite3HctTreeCsrEof(pCsr->pCsr) ){
        rc = sqlite3HctTreeCsrNext(pCsr->pCsr);
      }
    }else{
      rc = sqlite3HctTreeCsrFirst(pCsr->pCsr);
    }
  }

  return rc;
}

/*
** Only comparisons against the key are allowed.  The idxNum defines
** which comparisons are available:
**
**   0x01  ==
**   0x02   >
**   0x04   <
**   0x08  >=
**   0x10  <=
**
** Also, to indicate a DESC order scan, the 0x20 bit may be set.
*/
static int hctVtabBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;
  int iEq = -1;
  int iGt = -1;
  int iLt = -1;
  int iGe = -1;
  int iLe = -1;

  for(i=0; i<pIdxInfo->nConstraint; i++){
    struct sqlite3_index_constraint *pCons = &pIdxInfo->aConstraint[i];
    if( pCons->usable && pCons->iColumn==0 ){
      switch( pCons->op ){
        case SQLITE_INDEX_CONSTRAINT_EQ:
          iEq = i;
          break;
        case SQLITE_INDEX_CONSTRAINT_GT:
          iGt = i;
          break;
        case SQLITE_INDEX_CONSTRAINT_LT:
          iLt = i;
          break;
        case SQLITE_INDEX_CONSTRAINT_GE:
          iGe = i;
          break;
        case SQLITE_INDEX_CONSTRAINT_LE:
          iLe = i;
          break;
      }
    }
  }

  i = 1;
  pIdxInfo->idxNum = 0; 
  if( iEq>=0 ){
    pIdxInfo->aConstraintUsage[iEq].omit = 1;
    pIdxInfo->aConstraintUsage[iEq].argvIndex = i++;
    pIdxInfo->idxNum |= 0x01;
  }
  if( iGt>=0 ){
    pIdxInfo->aConstraintUsage[iGt].omit = 1;
    pIdxInfo->aConstraintUsage[iGt].argvIndex = i++;
    pIdxInfo->idxNum |= 0x02;
  }
  if( iLt>=0 ){
    pIdxInfo->aConstraintUsage[iLt].omit = 1;
    pIdxInfo->aConstraintUsage[iLt].argvIndex = i++;
    pIdxInfo->idxNum |= 0x04;
  }
  if( iGe>=0 ){
    pIdxInfo->aConstraintUsage[iGe].omit = 1;
    pIdxInfo->aConstraintUsage[iGe].argvIndex = i++;
    pIdxInfo->idxNum |= 0x08;
  }
  if( iLe>=0 ){
    pIdxInfo->aConstraintUsage[iLe].omit = 1;
    pIdxInfo->aConstraintUsage[iLe].argvIndex = i++;
    pIdxInfo->idxNum |= 0x10;
  }

  if( pIdxInfo->nOrderBy==1 && pIdxInfo->aOrderBy[0].iColumn==0 ){
    pIdxInfo->orderByConsumed = 1;
    pIdxInfo->idxNum |= (pIdxInfo->aOrderBy[0].desc ? 0x20 : 0x00);
  }

  return SQLITE_OK;
}

/*
** A delete specifies a single argument - the rowid of the row to remove.
** 
** Update and insert operations pass:
**
**   1. The "old" rowid, or NULL.
**   2. The "new" rowid.
**   3. Values for each column.
*/
int hctVtabUpdate(
  sqlite3_vtab *pVTab,
  int argc,
  sqlite3_value **argv,
  sqlite_int64 *pRowid
){
  hct_vtab *pTab = (hct_vtab*)pVTab;
  i64 iKey = 0;
  int nData = 0;
  const u8 *aData = 0;
  int rc = SQLITE_OK;
  HctTreeCsr *pCsr = 0;

  if( argc==4 && sqlite3_value_type(argv[2])==SQLITE_TEXT ){
    const char *z = (const char*)sqlite3_value_text(argv[2]);
    if( sqlite3_stricmp(z, "root")==0 ){
      u32 iRoot = sqlite3_value_int64(argv[3]);
      if( iRoot==0 ){
        return SQLITE_ERROR;
      }
      pTab->iRoot = iRoot;
      return SQLITE_OK;
    }
  }

  rc = sqlite3HctTreeCsrOpen(pTab->pTree, pTab->iRoot, &pCsr);
  if( rc==SQLITE_OK){
    assert( argc==4 || argc==1 );
    if( sqlite3_value_type(argv[0])!=SQLITE_NULL ){
      int res;
      iKey = sqlite3_value_int64(argv[0]);
      sqlite3HctTreeCsrSeek(pCsr, 0, iKey, &res);
      if( res==0 ){
        rc = sqlite3HctTreeDelete(pCsr);
      }
    }

    if( rc==SQLITE_OK && argc>1 ){
      iKey = sqlite3_value_int64(argv[2]);
      aData = sqlite3_value_blob(argv[3]);
      nData = sqlite3_value_bytes(argv[3]);
      rc = sqlite3HctTreeInsert(pCsr, 0, iKey, nData, aData);
    }
    sqlite3HctTreeCsrClose(pCsr);
  }

  return rc;
}      

/* Begin a transaction
*/
static int hctVtabBegin(sqlite3_vtab *pVtab){
  hct_vtab *pTab = (hct_vtab*)pVtab;
  return sqlite3HctTreeBegin(pTab->pTree, 1);
}

/* Phase 1 of a transaction commit.
*/
static int hctVtabSync(sqlite3_vtab *pVtab){
  return SQLITE_OK;
}

/* Commit a transaction
*/
static int hctVtabCommit(sqlite3_vtab *pVtab){
  hct_vtab *pTab = (hct_vtab*)pVtab;
  return sqlite3HctTreeRelease(pTab->pTree, 0);
}

/* Rollback a transaction
*/
static int hctVtabRollback(sqlite3_vtab *pVtab){
  hct_vtab *pTab = (hct_vtab*)pVtab;
  return sqlite3HctTreeRollbackTo(pTab->pTree, 0);
}

/* Open a savepoint.
*/
static int hctVtabSavepoint(sqlite3_vtab *pVtab, int iSave){
  hct_vtab *pTab = (hct_vtab*)pVtab;
  return sqlite3HctTreeBegin(pTab->pTree, iSave+2);
}

/* Release a savepoint.
*/
static int hctVtabRelease(sqlite3_vtab *pVtab, int iSave){
  hct_vtab *pTab = (hct_vtab*)pVtab;
  return sqlite3HctTreeRelease(pTab->pTree, iSave+1);
}

/* Rollback to a savepoint.
*/
static int hctVtabRollbackTo(sqlite3_vtab *pVtab, int iSave){
  hct_vtab *pTab = (hct_vtab*)pVtab;
  return sqlite3HctTreeRollbackTo(pTab->pTree, iSave+2);
}

/*
** This following structure defines all the methods for the 
** generate_hctVtab virtual table.
*/
static sqlite3_module hctVtabModule = {
  2,                          /* iVersion */
  hctVtabCreate,              /* xCreate */
  hctVtabConnect,             /* xConnect */
  hctVtabBestIndex,           /* xBestIndex */
  hctVtabDisconnect,          /* xDisconnect */
  hctVtabDestroy,             /* xDestroy */
  hctVtabOpen,                /* xOpen - open a cursor */
  hctVtabClose,               /* xClose - close a cursor */
  hctVtabFilter,              /* xFilter - configure scan constraints */
  hctVtabNext,                /* xNext - advance a cursor */
  hctVtabEof,                 /* xEof - check for end of scan */
  hctVtabColumn,              /* xColumn - read data */
  hctVtabRowid,               /* xRowid - read data */
  hctVtabUpdate,              /* xUpdate */
  hctVtabBegin,               /* xBegin */
  hctVtabSync,                /* xSync */
  hctVtabCommit,              /* xCommit */
  hctVtabRollback,            /* xRollback */
  0,                          /* xFindMethod */
  0,                          /* xRename */
  hctVtabSavepoint,
  hctVtabRelease,
  hctVtabRollbackTo
};


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_hct_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  rc = sqlite3_create_module(db, "hct", &hctVtabModule, 0);
  return rc;
}
