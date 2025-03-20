/*
** 2025 March 19
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

typedef struct HctLog HctLog;


/*
** Open a new log for writing transactions to file pFile.
*/
typedef struct HctJournal HctJournal;
int sqlite3HctLogNew(HctFile *pFile, HctJournal *pJrnl, HctLog **ppLog);

/*
** Close existing log.
*/
void sqlite3HctLogClose(HctLog *pLog);

/*
** Write a transaction to the log.
*/
int sqlite3HctLogBegin(HctLog *pLog);
int sqlite3HctLogRecord(HctLog *pLog, i64 iRoot, i64 nData, const u8 *aData);
int sqlite3HctLogFinish(HctLog *pLog, u64 iTid);

/*
** This function is used to obtain a 16-byte blob value that describes
** the current live transaction. This is stored in the hct_journal table
** in follower mode. The blob contains:
**
**     + 8-byte TID value,
**     + 4-byte value identifying the log file on disk,
**     + 4-byte offset within the log file on disk.
**
** Before returning, output variable (*pnRef) is set to the size of the
** blob in bytes. (*paRef) is set to point to a buffer belonging to the
** HctLog object containing the blob. The buffer is valid until the next
** call to sqlite3HctLogPointer() or sqlite3HctLogClose() on the same
** HctLog object.
*/
int sqlite3HctLogPointer(HctLog *pLog, int *pnRef, const u8 **paRef);


/*
** Mark the live transaction as finished. So that it is not rolled back
** if there is a crash and recovery.
*/
int sqlite3HctLogZero(HctLog *pLog);


