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

#define HCT_TMAP_PAGESIZE 1024

/*
** Transaction state - stored in the MSB of the 8-byte transaction map entry.
*/
#define HCT_TMAP_WRITING     (((u64)0x01) << 56)
#define HCT_TMAP_VALIDATING  (((u64)0x02) << 56)
#define HCT_TMAP_ROLLBACK    (((u64)0x03) << 56)
#define HCT_TMAP_COMMITTED   (((u64)0x04) << 56)

#define HCT_TMAP_STATE_MASK  (((u64)0x07) << 56)
#define HCT_TMAP_CID_MASK   ~(((u64)0x07) << 56)

/*
** There is a single object of this type for each distinct database 
** opened within the process. All connections to said database have 
** a pointer to the same HctTMapServer object.
*/
typedef struct HctTMapServer HctTMapServer;

/*
*/
typedef struct HctTMapClient HctTMapClient;

/*
*/
typedef struct HctTMap HctTMap;

/*
**
*/
struct HctTMap {
  /* Snapshot locking values */
  u64 iMinCid;                    /* This + all smaller CIDs fully committed */
  u64 iMinTid;                    /* This + all smaller TIDs fully committed */

  /* Transaction map */
  u64 iFirstTid;                  /* TID corresponding to aaMap[0][0] */
  int nMap;                       /* Number of mapping pages in aaMap[] */
  u64 **aaMap;                    /* Array of u64[HCTDB_TMAP_SIZE] arrays */
};

/*
** Create or delete a tmap server object.
*/
int sqlite3HctTMapServerNew(u64 iFirstTid, HctTMapServer **pp);
void sqlite3HctTMapServerFree(HctTMapServer *p);

/*
** Connect/disconnect a tmap client object.
*/
int sqlite3HctTMapClientNew(HctTMapServer *p, HctTMapClient **ppClient);
void sqlite3HctTMapClientFree(HctTMapClient *pClient);

/*
** Obtain, update or release a reference to a transaction map object.
*/
int sqlite3HctTMapBegin(HctTMapClient *p, HctTMap **ppMap);
int sqlite3HctTMapUpdate(HctTMapClient *p, HctTMap **ppMap);
int sqlite3HctTMapEnd(HctTMapClient *p, u64 iCID);

/*
** Return a TID value for which:
**
**    * the transactions associated with it and all smaller TID values
**      have been completely committed, and
**
**    * the transactions associated with it and all smaller TID values
**      are included in the snapshots accessed by all current and future
**      readers.
**
** All physical and logical pages freed by transactions with TIDs equal to 
** or smaller than the returned value may now be reused without disturbing
** current or future readers.
*/
u64 sqlite3HctTMapActiveTID(HctTMapServer*);

int sqlite3HctTMapNewTID(HctTMapClient *p, u64 iCid, u64 iTid, HctTMap **ppMap);



