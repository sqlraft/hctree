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

int sqlite3HctJournalNew(HctDatabase *pDb, HctJournal **pp);

void sqlite3HctJournalClose(HctJournal*);


int sqlite3HctJrnlLog(HctJournal *pJrnl, u64 iCid, u64 iSnap, u64 iTid, int rc);

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
** Return true if iTable is the root of the hct_journal table and the
** system is currently in FOLLOWER or LEADER mode.
*/
int sqlite3HctJournalIsNosnap(HctJournal *pJrnl, i64 iTable);

int sqlite3HctJrnlRollbackEntry(HctJournal *pJrnl, i64 iTid);

u64 sqlite3HctJrnlSnapshot(HctJournal *pJrnl);

void sqlite3HctJournalSchemaVersion(HctJournal *pJrnl, u32 *pSchemaVersion);

int sqlite3HctJrnlCommitOk(HctJournal *pJrnl);

u64 sqlite3HctJrnlFollowerModeCid(HctJournal *pJrnl);

void sqlite3HctJrnlSetRoot(HctJournal *pJrnl, Schema *pSchema);

/*
** Return true if it is guaranteed that the transaction with TID value iTid
** will never need to be rolled back as part of journal recovery.
*/
int sqlite3HctJrnlIsSafe(HctJournal *pJrnl, i64 iTid);

