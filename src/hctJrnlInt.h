/*
** 2023 January 6
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
*/

/*
** In follower mode, it is not possible to call sqlite3_hct_journal_write()
** for the transaction with CID (N + HCT_MAX_LEADING_WRITE) until all 
** transactions with CID values of N or less have been committed.
*/
#define HCT_MAX_LEADING_WRITE (8*1024)

typedef struct HctJournal HctJournal;

int sqlite3HctJournalServerNew(void **pJrnlPtr);
void sqlite3HctJournalServerFree(void *pJrnlPtr);

int sqlite3HctJournalNew(
  HctTree *pTree, 
  HctDatabase *pDb, 
  HctJournal **pp
);

/*
** If schema pSchema contains the special tables sqlite_hct_journal and
** sqlite_hct_baseline, allocate a new HctJournal object, set (*pp)
** to point to it and return SQLITE_OK. Or, if neither table can be
** found, set (*pp) to NULL and return SQLITE_OK.
**
** If only one of the required tables is found (SQLITE_CORRUPT), or if an
** OOM error occurs (SQLITE_NOMEM), return an SQLite error code. The final
** value of (*pp) is NULL in this case.
*/
int sqlite3HctJournalNewIf(Schema*, HctTree*, HctDatabase*, HctJournal **pp);

void sqlite3HctJournalClose(HctJournal*);


int sqlite3HctJrnlLog(HctJournal *pJrnl, u64 iCid, u64 iSnap, u64 iTid);

/*
** This is called as part of stage 1 recovery (the bit after the upper layer
** has loaded the database schema). The recovery mutex is held, so the client
** has exclusive access to the database on disk.
*/
int sqlite3HctJrnlRecovery(HctJournal *pJrnl, HctDatabase *pDb);

int sqlite3HctJrnlSavePhysical(sqlite3 *db, HctJournal *pJrnl, 
  int (*xSave)(void*, i64 iPhys), void *pSave
);

/*
** Register the hct_journal_entry() SQL user-function with the database
** handle. For decoding the "data" column of the sqlite_hct_journal table.
*/
int sqlite3HctJrnlInit(sqlite3 *db);

/*
** Return non-zero if (1) argument pJrnl is not NULL, and either (2a) argument 
** iTable is the logical root page of either the journal or baseline table 
** represented by pJrnl, or (2b) the connection is in follower mode.
**
** Before returning, set output variable (*pbNosnap) to non-zero if condition
** (2a) was true. To indicate that the table does not use snapshots - all
** committed rows are visible.
*/
int sqlite3HctJournalIsReadonly(HctJournal *pJrnl, u64 iTable, int *pbNosnap);

int sqlite3HctJrnlRollbackEntry(HctJournal *pJrnl, i64 iTid);

int sqlite3HctJrnlWriteEmpty(HctJournal *Jrnl, u64 iCid, u64 iTid, sqlite3 *db);

u64 sqlite3HctJrnlWriteTid(HctJournal *pJrnl, u64 *piCid);

u64 sqlite3HctJournalSnapshot(HctJournal *pJrnl);

void sqlite3HctJournalFixSchema(HctJournal *pJrnl, sqlite3*, void *pSchema);

void sqlite3HctJournalSchemaVersion(HctJournal *pJrnl, u32 *pSchemaVersion);

void sqlite3HctJrnlInvokeHook(HctJournal *pJrnl, sqlite3 *db);
