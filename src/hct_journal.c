/*
** 2020 October 13
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

#define HCT_JOURNAL_SCHEMA                  \
"CREATE TABLE sqlite_hct_journal("          \
    "cid INTEGER PRIMARY KEY,"              \
    "schema TEXT,"                          \
    "data BLOB,"                            \
    "schema_version BLOB,"                  \
    "hash BLOB,"                            \
    "tid INTEGER,"                          \
    "validcid INTEGER"                      \
");"


#define HCT_BASELINE_SCHEMA                 \
"CREATE TABLE sqlite_hct_baseline("         \
    "cid INTEGER,"                          \
    "schema_version BLOB,"                  \
    "hash BLOB"                             \
");"

/*
** Initialize the main database for replication.
*/
int sqlite3_hct_journal_init(sqlite3 *db){
  const char *zTest1 = "PRAGMA hct_ndbfile";
  const char *zTest2 = "SELECT 1 WHERE (SELECT count(*) FROM sqlite_schema)=0";
  sqlite3_stmt *pTest = 0;
  int rc = SQLITE_OK;

  /* Test that there is not already an open transaction on this database. */
  if( sqlite3_get_autocommit(db)==0 ){
    sqlite3ErrorWithMsg(db, SQLITE_ERROR, "open transaction on database");
    return SQLITE_ERROR;
  }

  /* Test that the main db really is an hct database. Leave rc set to 
  ** something other than SQLITE_OK and an error message in the database
  ** handle if it is not. */
  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(db, zTest1, -1, &pTest, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_step(pTest);
    sqlite3_finalize(pTest);
    if( rc==SQLITE_DONE ){
      sqlite3ErrorWithMsg(db, SQLITE_ERROR, "not an hct database");
    }else if( rc==SQLITE_ROW ){
      rc = SQLITE_OK;
    }
  }

  /* Open a transaction on the db */
  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(db, "BEGIN", 0, 0, 0);
  }

  /* Test that the main db really is empty */
  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(db, zTest2, -1, &pTest, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_step(pTest);
    sqlite3_finalize(pTest);
    if( rc==SQLITE_DONE ){
      sqlite3ErrorWithMsg(db, SQLITE_ERROR, "not an empty database");
      rc = SQLITE_ERROR;
    }else if( rc==SQLITE_ROW ){
      rc = SQLITE_OK;
    }
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(db, 
        "PRAGMA writable_schema = 1;"
        HCT_JOURNAL_SCHEMA ";"
        HCT_BASELINE_SCHEMA ";"
        "INSERT INTO sqlite_hct_baseline VALUES(0, zeroblob(16), zeroblob(16));"
        "PRAGMA writable_schema = 0;"
        ,0 ,0 ,0
    );
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
  }
  if( rc!=SQLITE_OK ){
    char *zErr = sqlite3_mprintf("%s", sqlite3_errmsg(db));
    sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
    sqlite3ErrorWithMsg(db, rc, "%s", zErr);
    sqlite3_free(zErr);
  }else{
    sqlite3HctDetectJournals(db);
  }

  return rc;
}




