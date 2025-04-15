/*
** 2021 February 24
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
** This header file describes the transaction map implementation. It
** serves two tasks:
**
**   * Provides the transaction map itself, a mapping from 56-bit TID values
**     to a combination of a CID value (also 56 bits) and some flags.
**
**   * Provides the read-lock system required by readers to ensure that old
**     database pages and other resources are not reused before they
**     are guaranteed to be finished with them.
*/

/*
*/

/* #define HCT_TMAP_PAGESIZE 1024 */

#define HCT_TMAP_PGSZBITS 10
#define HCT_TMAP_PAGESIZE (1 << HCT_TMAP_PGSZBITS)

#define HCT_TMAP_ENTRYSLOT(iEntry) \
  (((iEntry) >> 3) + (((iEntry) & 0x07) << (HCT_TMAP_PGSZBITS-3)))

// #define HCT_TMAP_ENTRYSLOT(iEntry) (iEntry)

/*
** Transaction state - stored in the MSB of the 8-byte transaction map entry.
*/
#define HCT_TMAP_WRITING     (((u64)0x00) << 56)
#define HCT_TMAP_VALIDATING  (((u64)0x01) << 56)
#define HCT_TMAP_ROLLBACK    (((u64)0x02) << 56)
#define HCT_TMAP_COMMITTED   (((u64)0x03) << 56)

#define HCT_TMAP_STATE_MASK  (((u64)0x07) << 56)
#define HCT_TMAP_CID_MASK   ~(((u64)0xFF) << 56)

/*
** There is a single object of this type for each distinct database 
** opened within the process. All connections to said database have 
** a pointer to the same HctTMapServer object.
*/
typedef struct HctTMapServer HctTMapServer;

/*
** Each separate database connection holds a handle of this type for
** the lifetime of the connection. Obtained and later released using
** functions:
**
**     sqlite3HctTMapServerNew()
**     sqlite3HctTMapServerFree()
*/
typedef struct HctTMapClient HctTMapClient;

/*
*/
typedef struct HctTMap HctTMap;

/*
** A transaction-map object.
*/
struct HctTMap {
  /* Transaction map */
  u64 iFirstTid;                  /* TID corresponding to aaMap[0][0] */
  int nMap;                       /* Number of mapping pages in aaMap[] */
  u64 **aaMap;                    /* Array of u64[HCT_TMAP_PAGESIZE] arrays */
};

/*
** Create or delete a tmap server object.
*/
int sqlite3HctTMapServerNew(u64 iFirstTid, u64 iLastTid, HctTMapServer **pp);
void sqlite3HctTMapServerFree(HctTMapServer *p);

/*
** Connect/disconnect a tmap client object.
*/
int sqlite3HctTMapClientNew(HctTMapServer*, HctConfig*, HctTMapClient**);
void sqlite3HctTMapClientFree(HctTMapClient *pClient);

/*
** Obtain, update or release a reference to a transaction map object.
*/
int sqlite3HctTMapBegin(HctTMapClient *p, u64 iSnapshot, HctTMap **ppMap);
int sqlite3HctTMapUpdate(HctTMapClient *p, HctTMap **ppMap);
int sqlite3HctTMapEnd(HctTMapClient *p, u64 iCID);

/*
** Return a TID value for which:
**
**    1. the transactions associated with it and all smaller TID values
**       have been finalized (marked as committed or rolled back), and
**
**    2. the transactions associated with it and all smaller TID values
**       are included in the snapshots accessed by all current and future
**       readers.
**
** All physical and logical pages freed by transactions with TIDs equal to 
** or smaller than the returned value may now be reused without disturbing
** current or future readers.
*/
u64 sqlite3HctTMapSafeTID(HctTMapClient*);

/*
** Called when a writer obtains the TID value for its transaction. To
** check that the transaction-map is large enough to accomodate it.
*/
int sqlite3HctTMapNewTID(HctTMapClient *p, u64 iTid, HctTMap **ppMap);

/*
** Return TID value T for all transactions with tid values less than or
** equal to T were finished (marked as committed or rolled back), last
** time sqlite3HctTMapBegin() was called.
*/
u64 sqlite3HctTMapCommitedTID(HctTMapClient*);

i64 sqlite3HctTMapStats(sqlite3 *db, int iStat, const char **pzStat);

void sqlite3HctTMapScan(HctTMapClient*);


/* 
** The following API is used when switching a replication-enabled database
** to FOLLOWER or LEADER mode. In that case, a new HctTMap object must be
** created during recovery to reflect the contents of the hct_journal table.
*/
int sqlite3HctTMapRecoverySet(HctTMapClient*, u64 iTid, u64 iCid);
void sqlite3HctTMapRecoveryFinish(HctTMapClient*, int rc);

int sqlite3HctTMapServerSet(HctTMapServer *pServer, u64 iTid, u64 iCid);

void **sqlite3HctTMapLogPtrPtr(HctTMapClient *p);
