/*
** 2022 September 28
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

typedef struct hctstats_vtab hctstats_vtab;
typedef struct hctstats_cursor hctstats_cursor;
struct hctstats_vtab {
  sqlite3_vtab base;              /* Base class - must be first */
  sqlite3 *db;
};
struct hctstats_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  int iSubsys;
  int iStat;

  i64 iRowid;
  const char *zStat;         /* Value for "stat" column. NULL for EOF. */
  i64 iVal;                  /* Value for "val" column. */
};

typedef struct HctStatsSubsys HctStatsSubsys;
struct HctStatsSubsys {
  const char *zSubsys;
  i64 (*xStat)(sqlite3*, int iStat, const char **pzStat);
};

static HctStatsSubsys aHctStatGlobal[] = {
  { "file", sqlite3HctFileStats },
  { "db", sqlite3HctDbStats },
  { "tmap", sqlite3HctTMapStats },
  { "pman", sqlite3HctPManStats }
};

#define HCTSTATS_SCHEMA "CREATE TABLE x(subsys, stat, val)"

/*
** xConnect() callback for hctstats table.
*/
static int hctstatsConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  hctstats_vtab *pNew = 0;
  int rc = SQLITE_OK;

  *ppVtab = 0;
  rc = sqlite3_declare_vtab(db, HCTSTATS_SCHEMA);

  if( rc==SQLITE_OK ){
    pNew = sqlite3MallocZero( sizeof(*pNew) );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ) return SQLITE_NOMEM;
    pNew->db = db;
  }
  return rc;
}

/*
** xBestIndex() callback for hctstats table.
*/
static int hctstatsBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  pIdxInfo->estimatedCost = (double)10000;
  pIdxInfo->estimatedRows = 10000;
  return SQLITE_OK;
}

/*
** xDisconnect() callback for hctstats table. Free the vtab handle.
*/
static int hctstatsDisconnect(sqlite3_vtab *pVtab){
  hctstats_vtab *p = (hctstats_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** xOpen() callback for hctstats table. Free the vtab handle.
*/
static int hctstatsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  hctstats_cursor *pCur;
  pCur = sqlite3MallocZero(sizeof(*pCur));
  if( pCur==0 ) return SQLITE_NOMEM;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** xClose() callback for hctstats table. Free the vtab handle.
*/
static int hctstatsClose(sqlite3_vtab_cursor *cur){
  hctstats_cursor *pCur = (hctstats_cursor*)cur;
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int hctstatsNext(sqlite3_vtab_cursor *cur){
  hctstats_cursor *pCsr = (hctstats_cursor*)cur;
  hctstats_vtab *pTab = (hctstats_vtab*)(pCsr->base.pVtab);

  pCsr->zStat = 0;
  pCsr->iStat++;

  while( pCsr->zStat==0 && pCsr->iSubsys<ArraySize(aHctStatGlobal) ){
    HctStatsSubsys *p = &aHctStatGlobal[pCsr->iSubsys];
    pCsr->iVal = p->xStat(pTab->db, pCsr->iStat, &pCsr->zStat);
    if( pCsr->zStat==0 ){
      pCsr->iStat = 0;
      pCsr->iSubsys++;
    }
  }

  return SQLITE_OK;
}

static int hctstatsFilter(
  sqlite3_vtab_cursor *cur, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  hctstats_cursor *pCsr = (hctstats_cursor*)cur;

  if( sqlite3HctDbFind(((hctstats_vtab*)cur->pVtab)->db, 0)==0 ){
    /* Main database is not an hctree db */
    return SQLITE_OK;
  }

  pCsr->iStat = -1;
  pCsr->iSubsys = 0;
  pCsr->iRowid = 0;
  return hctstatsNext(cur);
}

static int hctstatsEof(sqlite3_vtab_cursor *cur){
  hctstats_cursor *pCsr = (hctstats_cursor*)cur;
  return (pCsr->zStat==0);
}

static int hctstatsColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  hctstats_cursor *pCsr = (hctstats_cursor*)cur;

  assert( i==0 || i==1 || i==2 );
  switch( i ){
    case 0: {
      HctStatsSubsys *p = &aHctStatGlobal[pCsr->iSubsys];
      sqlite3_result_text(ctx, p->zSubsys, -1, SQLITE_STATIC);
      break;
    }

    case 1:
      sqlite3_result_text(ctx, pCsr->zStat, -1, SQLITE_STATIC);
      break;

    default:
      assert( i==2 );
      sqlite3_result_int64(ctx, pCsr->iVal);
      break;
  }
  return SQLITE_OK;
}

static int hctstatsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  hctstats_cursor *pCsr = (hctstats_cursor*)cur;
  *pRowid = pCsr->iRowid;
  return SQLITE_OK;
}


/*
** Register the hct_stats virtual table module with the supplied 
** SQLite database handle.
*/
int sqlite3HctStatsInit(sqlite3 *db){
  static sqlite3_module hctstatsModule = {
    /* iVersion    */ 0,
    /* xCreate     */ 0,
    /* xConnect    */ hctstatsConnect,
    /* xBestIndex  */ hctstatsBestIndex,
    /* xDisconnect */ hctstatsDisconnect,
    /* xDestroy    */ 0,
    /* xOpen       */ hctstatsOpen,
    /* xClose      */ hctstatsClose,
    /* xFilter     */ hctstatsFilter,
    /* xNext       */ hctstatsNext,
    /* xEof        */ hctstatsEof,
    /* xColumn     */ hctstatsColumn,
    /* xRowid      */ hctstatsRowid,
    /* xUpdate     */ 0,
    /* xBegin      */ 0,
    /* xSync       */ 0,
    /* xCommit     */ 0,
    /* xRollback   */ 0,
    /* xFindMethod */ 0,
    /* xRename     */ 0,
    /* xSavepoint  */ 0,
    /* xRelease    */ 0,
    /* xRollbackTo */ 0,
    /* xShadowName */ 0
  };

  return sqlite3_create_module(db, "hctstats", &hctstatsModule, 0);
}


