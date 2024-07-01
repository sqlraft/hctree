/*
** 2023 May 16
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



#ifndef SQLITE3HCT_H
#define SQLITE3HCT_H

/*
** Make sure we can call this stuff from C++.
*/
#ifdef __cplusplus
extern "C" {
#endif

#define SQLITE_HCT_JOURNAL_HASHSIZE 16

/*
** Initialize the main database for replication.
*/
int sqlite3_hct_journal_init(sqlite3 *db);

/*
** Write a transaction into the database.
*/
int sqlite3_hct_journal_write(
  sqlite3 *db,                    /* Write to "main" db of this handle */
  sqlite3_int64 iCid,
  const char *zSchema,
  const void *pData, int nData,
  sqlite3_int64 iSchemaCid
);

int sqlite3_hct_journal_truncate(sqlite3 *db, sqlite3_int64 iMinCid);

/* 
** Candidate values for second arg to sqlite3_hct_journal_setmode() 
*/
#define SQLITE_HCT_JOURNAL_MODE_FOLLOWER 0
#define SQLITE_HCT_JOURNAL_MODE_LEADER   1

/*
** Query the LEADER/FOLLOWER setting of the db passed as the only argument.
*/
int sqlite3_hct_journal_mode(sqlite3 *db);

/*
** Set the LEADER/FOLLOWER setting of the db passed as the first argument.
** Return SQLITE_OK if successful. Otherwise, return an SQLite error code
** and leave an English language error message (accessible using
** sqlite3_errmsg()) in the database handle.
*/
int sqlite3_hct_journal_setmode(sqlite3 *db, int eMode);

/*
** Rollback transactions that follow the first hole in the journal.
*/
int sqlite3_hct_journal_rollback(sqlite3 *db, sqlite3_int64 iCid);

/* 
** Special values that may be passed as second argument to
** sqlite3_hct_journal_rollback().
*/
#define SQLITE_HCT_ROLLBACK_MAXIMUM   0
#define SQLITE_HCT_ROLLBACK_PRESERVE -1

/*
** Set output variable (*piCid) to the CID of the newest available 
** database snapshot. Return SQLITE_OK if successful, or an SQLite
** error code if something goes wrong.
*/
int sqlite3_hct_journal_snapshot(sqlite3 *db, sqlite3_int64 *piCid);

/*
** Register a custom validation callback with the database handle.
*/
int sqlite3_hct_journal_hook(
  sqlite3 *db,
  void *pArg,
  int(*xValidate)(
    void *pCopyOfArg,
    sqlite3_int64 iCid,
    const char *zSchema,
    const void *pData, int nData,
    sqlite3_int64 iSchemaCid
  )
);

/*
** Both arguments are assumed to point to SQLITE_HCT_JOURNAL_HASHSIZE
** byte buffers. This function updates the hash stored in buffer pHash
** based on the contents of buffer pData.
*/
void sqlite3_hct_journal_hash(void *pHash, const void *pData);

/*
** It is assumed that buffer pHash points to a buffer
** SQLITE_HCT_JOURNAL_HASHSIZE bytes in size. This function populates this
** buffer with a hash based on the remaining arguments.
*/
void sqlite3_hct_journal_hashentry(
  void *pHash,              /* OUT: Hash of other arguments */
  sqlite3_int64 iCid,
  const char *zSchema,
  const void *pData, int nData,
  sqlite3_int64 iSchemaCid
);

#ifdef __cplusplus
}
#endif
#endif /* SQLITE3HCT_H */
