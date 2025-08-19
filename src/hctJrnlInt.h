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


int sqlite3HctJrnlLog(
    HctJournal *, u64 iCid, u64 iSnap, int nPtr, const u8 *aPtr, int rc
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

int sqlite3HctJrnlRollbackEntry(HctJournal *pJrnl, i64 iCid);

u64 sqlite3HctJrnlSnapshot(HctJournal *pJrnl);

int sqlite3HctJrnlCommitOk(HctJournal *pJrnl);

int sqlite3HctJrnlFollowerModeCid(HctJournal *pJrnl, u64 *piCid);

void sqlite3HctJrnlSetRoot(HctJournal *pJrnl, Schema *pSchema);

/*
** Return one of SQLITE_HCT_NORMAL, SQLITE_HCT_FOLLOWER or SQLITE_HCT_LEADER
** to indicate the mode that the journal is in.
*/
int sqlite3HctJrnlMode(HctJournal *pJrnl);

int sqlite3HctJrnlFindLogs(
    sqlite3 *db, 
    HctJournal*, 
    void*, 
    int(*)(void*,i64,int,const u8*),
    int (*xMap)(void*, i64, i64)
);

void **sqlite3HctJrnlLogPtrPtr(HctJournal *pJrnl);
void sqlite3HctJrnlLogPtrPtrRelease(HctJournal *pJrnl);

int sqlite3HctJrnlZeroEntries(HctJournal *pJrnl, int nEntry, i64 *aEntry);

int sqlite3HctJrnlRecoveryMode(sqlite3 *db, HctJournal *pJrnl, int *peMode);

/*
** True if currently inside a call to sqlite3_hct_journal_local_commit()
*/
int sqlite3HctJrnlIsLocalCommit(HctJournal *pJrnl);

