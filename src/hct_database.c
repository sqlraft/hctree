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
#include "vdbeInt.h"
#include <string.h>
#include <assert.h>

int hct_extra_logging = 0;

int hct_extra_write_logging = 0;

static void hctExtraLogging(
  const char *zFunc, 
  int iLine, 
  void *pHctDb,
  char *zMsg
){
  sqlite3_log(SQLITE_NOTICE, "%s:%d: %p: %s", zFunc, iLine, pHctDb, zMsg);
  sqlite3_free(zMsg);
}

static void hctExtraWriteLogging(const char *zFunc, int iLine, char *zMsg){
  sqlite3_log(SQLITE_NOTICE, "%s:%d: %s", zFunc, iLine, zMsg);
  sqlite3_free(zMsg);
}

#define HCT_EXTRA_LOGGING(pDb, x)                                \
  if( pDb->pConfig->bHctExtraLogging ) {                         \
    hctExtraLogging(__func__, __LINE__, pDb, sqlite3_mprintf x); \
  }

#define HCT_EXTRA_WR_LOGGING(x) if( hct_extra_write_logging ) { hctExtraWriteLogging(__func__, __LINE__, sqlite3_mprintf x); }

typedef struct HctDatabase HctDatabase;
typedef struct HctDbIndexEntry HctDbIndexEntry;
typedef struct HctDbIndexLeaf HctDbIndexLeaf;
typedef struct HctDbIndexNode HctDbIndexNode;
typedef struct HctDbIndexNodeEntry HctDbIndexNodeEntry;
typedef struct HctDbIndexNodeHdr HctDbIndexNodeHdr;
typedef struct HctDbIntkeyEntry HctDbIntkeyEntry;
typedef struct HctDbIntkeyLeaf HctDbIntkeyLeaf;
typedef struct HctDbIntkeyNodeEntry HctDbIntkeyNodeEntry;
typedef struct HctDbIntkeyNode HctDbIntkeyNode;
typedef struct HctDbKey HctDbKey;
typedef struct HctDbLeaf HctDbLeaf;
typedef struct HctDbLeafHdr HctDbLeafHdr;
typedef struct HctDbWriter HctDbWriter;
typedef struct HctDbPageHdr HctDbPageHdr;
typedef struct HctDbHistoryFan HctDbHistoryFan;
typedef struct HctDbRangeCsr HctDbRangeCsr;

typedef struct HctCsrIntkeyOp HctCsrIntkeyOp;
typedef struct HctCsrIndexOp HctCsrIndexOp;

typedef struct HctDbPageArray HctDbPageArray;

struct HctCsrIntkeyOp {
  HctCsrIntkeyOp *pNextOp;
  i64 iFirst;
  i64 iLast;

  u32 iLogical;
  u32 iPhysical;
};

struct HctCsrIndexOp {
  HctCsrIndexOp *pNextOp;
  u8 *pFirst;
  int nFirst;
  u8 *pLast;
  int nLast;

  u32 iLogical;
  u32 iPhysical;
};

struct CsrIntkey {
  HctCsrIntkeyOp *pOpList;
  HctCsrIntkeyOp *pCurrentOp;
};
struct CsrIndex {
  HctCsrIndexOp *pOpList;
  HctCsrIndexOp *pCurrentOp;
};

struct HctDbKey {
  i64 iKey;                       /* Integer key value */
  UnpackedRecord *pKey;           /* Index key value */
  HctBuffer buf;                  /* Buffer for pKey data (if required) */
};

/*
** eRange:
**   Set to one of the HCT_RANGE_* constants defined below.
**
*/
struct HctDbRangeCsr {
  HctDbKey lowkey;
  HctDbKey highkey;
  u64 iRangeTid;                  /* The range TID that was followed here */

  int eRange;                     /* HCT_RANGE_* constant */
  int iCell;
  HctFilePage pg;
};

#define HCT_RANGE_FOLLOW 0        /* Follow range-pointers only */
#define HCT_RANGE_MERGE  1        /* Merge in data + follow range-pointers */
#define HCT_RANGE_FAN    2        /* HctDbRangeCsr.pg is a fan page */

/*
** iRoot:
**   Logical root page of tree structure that this cursor is open on.
**
** pKeyInfo:
**   NULL for cursors open on intkey trees, otherwise points to the 
**   KeyInfo used to compare keys in the open index tree. For cursors
**   opened by the user, this is set when the cursor is opened within
**   sqlite3HctDbCsrOpen() and never modified.
**
** pRec:
**   UnpackedRecord structure suitable for use with pKeyInfo. This is
**   allocated the first time it is required and then retained for
**   the lifetime of the HctDbCsr structure.
**
** eDir:
**   One of BTREE_DIR_NONE, BTREE_DIR_FORWARD or BTREE_DIR_REVERSE.
**
** pIntkeyOps:
*/ 
struct HctDbCsr {
  HctDatabase *pDb;               /* Database that owns this cursor */
  u32 iRoot;                      /* Root page cursor is opened on */
  KeyInfo *pKeyInfo;
  UnpackedRecord *pRec;
  int eDir;                       /* Direction cursor will step after Seek() */
  int bNosnap;                    /* The "no-snapshot" flag */
  int bNoscan;                    /* The "no-scan" flag */

  u8 *aRecord;                    /* Record in allocated memory */
  int nRecord;                    /* Size of aRecord[] in bytes */
  HctBuffer rec;                  /* Buffer used to manage aRecord[] */

  struct CsrIntkey intkey;
  struct CsrIndex index;
  HctDbCsr *pNextScanner;

  int iCell;                      /* Current cell within page */
  HctFilePage pg;                 /* Current leaf page */

  int nRange;
  int nRangeAlloc;
  HctDbRangeCsr *aRange;
};

#define HCTDB_MAX_DIRTY (HCTDB_MAX_PAGEARRAY-2)
// #define HCTDB_MAX_DIRTY (HCTDB_STATIC_PAGEARRAY-2)
#define HCTDB_MAX_PAGEARRAY 2048
#define HCTDB_STATIC_PAGEARRAY (8+2)

#define HCTDB_APPEND_MODE_THRESHOLD 5


#define LARGEST_TID  ((((u64)1)<<56)-1)
#define HCT_TID_ROLLBACK_OVERRIDE (((u64)0x01) << 56)


struct HctDbPageArray {
  int nPg;
  HctFilePage *aPg;
  HctFilePage aStatic[HCTDB_STATIC_PAGEARRAY];
  HctFilePage *aDyn;
  int nDyn;
};

typedef struct HctDbOverflow HctDbOverflow;
typedef struct HctDbOverflowArray HctDbOverflowArray;

struct HctDbOverflow {
  u32 pgno;
  int nOvfl;
};

struct HctDbOverflowArray {
  int nEntry;
  int nAlloc;
  HctDbOverflow *aOvfl;
};

typedef struct HctDbFPKey HctDbFPKey;
struct HctDbFPKey {
  i64 iKey;
  u8 *aKey;
  HctBuffer buf;
};

/*
**
** iHeight:
**   The height of the list that this writer is writing to. 0 for leaves,
**   1 for the parents of leaves, etc.
**
** aWritePg/nWritePg:
**
** nWriteKey:
**   Number of hctDbInsert() calls since last flush - i.e. how many have to
**   be retried if we hit a CAS failure and have to redo this write operation.
**
** iWriteFpKey/aWriteFpKey:
**   These two variables store the fence-post key for the peer page of
**   the rightmost page in the aWritePg[] array - aWritePg[nWritePg-1].
**   For intkey tables, iWriteFpKey is the 64-bit integer key value. For
**   index tables, aWriteFpKey points to a buffer containing the FP key,
**   and iWriteFpKey its size in bytes. The buffer is allocated with
**   sqlite3_malloc().
**
**   If there is no peer page and writing to an intkey list, iWriteFpKey 
**   is set to LARGEST_INT64. If writing to an index list, aWriteFpKey is
**   set to NULL and iWriteFpKey to 0.
**
** discardpg:
**   Pages to the right of writepg[0] that will be removed from the list
**   if the CAS instruction for this write succeeds.
**
** bAppend:
**   True if the writer is in append mode.
**
** bDoCleanup:
**   True if hctDbInsert() has been called since the most recent
**   hctDbWriterCleanup().
*/
struct HctDbWriter {
  int iHeight;                    /* Height to write at (0==leaves) */
  HctDbPageArray writepg;
  int nWriteKey;                  /* Number of new keys in writepg array */

  int bAppend;                    /* Writer is in "append" mode */
  HctDbFPKey fp;                  /* Fence-Post key. */

  HctDbCsr writecsr;              /* Used to find target page while writing */
  HctDbPageArray discardpg;
  HctFilePage fanpg;

  int bDoCleanup;
  int nEvictLocked;
  u32 iEvictLockedPgno;

  HctDbOverflowArray delOvfl;     /* Overflow chains to free on write */
  HctDbOverflowArray insOvfl;     /* Overflow chains to free on don't-write */

  int nOverflow;

  int nMigrateKey;
};

/*
** This is used by the rebalance operation implemented by hctDbBalance().
** The first step of that operation is to assemble an array of these
** structures - one for each cell that will be distributed between the
** output pages.
**
** nByte:
**   Total bytes of space required by cell on new page. This includes
**   the header entry and the data stored in the cell area.
**
** aEntry:
**   Pointer to buffer containing cell entry. Or NULL to indicate that
**   the HctDbCellSz structure corresponds to a new cell being written 
**   (that is not on any input page).
**
** aCell:
**   Only valid if (aEntry!=0). Pointer to buffer containing leaf-page
**   portion of cell.
*/
typedef struct HctDbCellSz HctDbCellSz;
struct HctDbCellSz {
  int nByte;                      /* Size of cell in bytes */
  u8 *aEntry;                     /* Buffer containing cell entry */
  u8 *aCell;                      /* Buffer containing cell body */
};

typedef struct HctBalance HctBalance;
struct HctBalance {
  u8 *aPg[3];
  int nSzAlloc;                   /* Allocated size of aSz[] array */
  HctDbCellSz *aSz;               /* aSz[] array */
};

/*
** Given the database page-size as an argument, the maximum number of cells
** that may fit on any page with variable sized entries (an index leaf or node,
** or intkey leaf page).
*/
#define MAX_CELLS_PER_PAGE(pgsz) ((pgsz) / 8)

/*
** This structure, an instance of which is part of each HctDatabase object,
** holds counters collected for the hctstats structure.
*/
typedef struct HctDatabaseStats HctDatabaseStats;
struct HctDatabaseStats {
  i64 nBalanceIntkey;
  i64 nBalanceIndex;
  i64 nBalanceSingle;
  i64 nTMapLookup;
  i64 nUpdateInPlace;
  i64 nInternalRetry;
};

/*
** pScannerList:
**   Linked list of cursors used by the current transaction. If this turns
**   out to be a write transaction, this list is used to detect read/write
**   conflicts.
**
** iJrnlWriteCid:
**   This value is set within calls to sqlite3_hct_journal_write(). The CID
**   of the journal entry being written to the db.
**
** iLocalMinTid:
**   This is set whenever a read-transaction is ongoing. The client may
**   assume that the transactions associated with this and all smaller TID
**   values have been fully committed or rolled back. No pointers associated
**   with such values need be followed.
*/
struct HctDatabase {
  HctFile *pFile;
  HctConfig *pConfig;
  i64 nCasFail;                   /* Number cas-collisions so far */
  int pgsz;                       /* Page size in bytes */

  u8 *aTmp;                       /* Temp buffer pgsz bytes in size */
  HctBalance *pBalance;           /* Space for hctDbBalance() */

  HctDbCsr *pScannerList;

  u64 iJrnlWriteCid;

  HctTMap *pTmap;                 /* Transaction map (non-NULL if trans open) */
  u64 iSnapshotId;                /* Snapshot id for reading */
  u64 iReqSnapshotId;             /* Required snapshot to replicate trans. */
  u64 iLocalMinTid;
  HctDbWriter pa;
  HctDbCsr rbackcsr;              /* Used to find old values during rollback */
  u64 iTid;                       /* Transaction id for writing */
  u64 nWriteCount;                /* Write-count at start of commit */

  int eMode;                      /* HCT_MODE_XXX constant */
  int bConcurrent;                /* Collect validation information */

  int (*xSavePhysical)(void*, i64);
  void *pSavePhysical;

  HctDatabaseStats stats;
};

/*
** Values for HctDatabase.eMode.
*/
#define HCT_MODE_NORMAL    0
#define HCT_MODE_ROLLBACK  1
#define HCT_MODE_VALIDATE  3


/* 
** 8-byte database page header. Described in fileformat.wiki.
*/
struct HctDbPageHdr {
  u8 hdrFlags;
  u8 nHeight;                     /* 0 for leaves, 1 for parents etc. */
  u16 nEntry;
  u32 iPeerPg;
};

/*
** Page types. These are the values that may appear in the page-type
** field of a page header.
*/
#define HCT_PAGETYPE_INTKEY      0x01
#define HCT_PAGETYPE_INDEX       0x03
#define HCT_PAGETYPE_OVERFLOW    0x05
#define HCT_PAGETYPE_HISTORY     0x06

#define HCT_PAGETYPE_MASK     0x07

/*
** Page types may be ORed with the following:
*/
#define HCT_PAGETYPE_LEFTMOST 0x80

#define hctPagetype(p)   (((HctDbPageHdr*)(p))->hdrFlags&HCT_PAGETYPE_MASK)
#define hctIsLeftmost(p) (((HctDbPageHdr*)(p))->hdrFlags&HCT_PAGETYPE_LEFTMOST)
#define hctPageheight(p)   (((HctDbPageHdr*)(p))->nHeight)
#define hctPagenentry(p)   (((HctDbPageHdr*)(p))->nEntry)
#define hctPagePeer(p)   (((HctDbPageHdr*)(p))->iPeerPg)

#define hctDbIsConcurrent(pDb) (pDb->bConcurrent)

/*
** 16-byte leaf page header. Used by both index and intkey leaf pages.
** Described in fileformat.wiki.
*/
struct HctDbLeafHdr {
  u16 nFreeGap;                   /* Size of free-space region, in bytes */
  u16 nFreeBytes;                 /* Total free bytes on page */
  u32 unused;
};

struct HctDbLeaf {
  HctDbPageHdr pg;
  HctDbLeafHdr hdr;
};


struct HctDbIntkeyEntry {
  u32 nSize;                      /* 0: Total size of data (local+overflow) */
  u16 iOff;                       /* 4: Offset of record within this page */
  u8 flags;                       /* 6: Flags (see below) */
  u8 unused;                      /* 7: */
  i64 iKey;                       /* 8: Integer key value */
};

struct HctDbIndexEntry {
  u32 nSize;                      /* 0: Total size of data (local+overflow) */
  u16 iOff;                       /* 4: Offset of record within this page */
  u8 flags;                       /* 6: Flags (see below) */
  u8 unused;                      /* 7: */
};

struct HctDbIndexNodeEntry {
  u32 nSize;
  u16 iOff;
  u8 flags;
  u8 unused;
  u32 iChildPg;
};

struct HctDbIntkeyNodeEntry {
  i64 iKey;                       /* Value of FP key on page iChild */
  u32 iChildPg;                   /* Child page */
  u32 unused;
};

struct HctDbIntkeyNode {
  HctDbPageHdr pg;
  HctDbIntkeyNodeEntry aEntry[0];
};

struct HctDbIntkeyLeaf {
  HctDbPageHdr pg;
  HctDbLeafHdr hdr;
  HctDbIntkeyEntry aEntry[0];
};

struct HctDbIndexLeaf {
  HctDbPageHdr pg;
  HctDbLeafHdr hdr;
  HctDbIndexEntry aEntry[0];
};

struct HctDbIndexNodeHdr {
  u16 nFreeGap;                   /* Size of free-space region, in bytes */
  u16 nFreeBytes;                 /* Total free bytes on page */
};

struct HctDbIndexNode {
  HctDbPageHdr pg;
  HctDbIndexNodeHdr hdr;
  HctDbIndexNodeEntry aEntry[0];
};

/*
** History fanout page.
**
** iSplit0:
**   The index of a key in page aPgOld1[0]. This key is the first that
**   should be considered in aPgOld1[0]. Implying that no key equal to
**   or greater than this from pgOld0 should be considered.
*/
struct HctDbHistoryFan {
  HctDbPageHdr pg;

  u64 iRangeTid0;
  u64 iFollowTid0;
  u32 pgOld0;

  int iSplit0;

  u64 iRangeTid1;
  u32 aPgOld1[0];
};

/*
** Structure for reading/writing cells from and to pages.
*/
typedef struct HctDbCell HctDbCell;
struct HctDbCell {
  u64 iTid;
  u64 iRangeTid;
  u32 iRangeOld;
  u32 iOvfl;
  const u8 *aPayload;
};

#if 1
__attribute__ ((noinline)) 
static void hctMemcpy(void *a, const void *b, size_t c){
  if( c ) memcpy(a, b, c);
}
#else
# define hctMemcpy memcpy
#endif



/*
** Flags for HctDbIntkeyEntry.flags
*/
#define HCTDB_HAS_TID      0x01         /* 8 bytes */
#define HCTDB_HAS_OVFL     0x04         /* 4 bytes */
#define HCTDB_HAS_RANGETID 0x08         /* 8 bytes */
#define HCTDB_HAS_RANGEOLD 0x10         /* 4 bytes */

#define HCTDB_MAX_EXTRA_CELL_DATA (8+4+8+4)

void sqlite3HctBufferGrow(HctBuffer *pBuf, int nSize){
  if( nSize>pBuf->nAlloc ){
    pBuf->aBuf = sqlite3HctRealloc(pBuf->aBuf, nSize);
    pBuf->nAlloc = nSize;
  }
}

void sqlite3HctBufferFree(HctBuffer *pBuf){
  sqlite3_free(pBuf->aBuf);
  memset(pBuf, 0, sizeof(HctBuffer));
}

static void hctBufferSet(HctBuffer *pBuf, const u8 *aData, int nData){
  sqlite3HctBufferGrow(pBuf, nData);
  hctMemcpy(pBuf->aBuf, aData, nData);
}


#if 1 || defined(SQLITE_DEBUG)
static int hctSqliteBusy(const char *zFile, int iLine){
  sqlite3_log(SQLITE_WARNING, "HCT_SQLITE_BUSY at %s:%d", zFile, iLine);
  return SQLITE_BUSY_SNAPSHOT;
}
# define HCT_SQLITE_BUSY hctSqliteBusy(__FILE__, __LINE__)
#else
# define HCT_SQLITE_BUSY SQLITE_BUSY_SNAPSHOT
#endif /* SQLITE_DEBUG */

static u64 hctDbTMapLookup(HctDatabase *pDb, u64 iTid, u64 *peState){
  u64 iVal = 0;
  HctTMap *pTmap = pDb->pTmap;
  if( iTid==LARGEST_TID ){
    *peState = HCT_TMAP_ROLLBACK;
  }else if( iTid<pTmap->iFirstTid ){
    *peState = HCT_TMAP_COMMITTED;
  }else{
    int iMap = (iTid - pTmap->iFirstTid) / HCT_TMAP_PAGESIZE;

    if( iMap>=pTmap->nMap ){
      HctTMapClient *pTMapClient = sqlite3HctFileTMapClient(pDb->pFile);
      sqlite3HctTMapUpdate(pTMapClient, &pDb->pTmap);
      assert( iTid<(pDb->pTmap->nMap*HCT_TMAP_PAGESIZE)+pDb->pTmap->iFirstTid );
      return hctDbTMapLookup(pDb, iTid, peState);
    }

    {
      int iOff = (iTid - pTmap->iFirstTid) % HCT_TMAP_PAGESIZE;
      iOff = HCT_TMAP_ENTRYSLOT(iOff);
      iVal = AtomicLoad(&pTmap->aaMap[iMap][iOff]);
      pDb->stats.nTMapLookup++;
    }

    *peState = (iVal & HCT_TMAP_STATE_MASK);
  }
  return (iVal & HCT_TMAP_CID_MASK);
}


#if 0
static void print_out_tmap(HctDatabase *pDb, int nLimit){
  int ii;

  for(ii=0; ii<nLimit; ii++){
    u64 eState;
    u64 iTid = pDb->pTmap->iFirstTid + ii;
    u64 iCid = hctDbTMapLookup(pDb, iTid, &eState);

    printf("tid=%d -> (%s, %d)\n", (int)iTid, 
      eState==HCT_TMAP_WRITING ? "WRITING" :
      eState==HCT_TMAP_VALIDATING ? "VALIDATING" :
      eState==HCT_TMAP_ROLLBACK ? "ROLLBACK" :
      eState==HCT_TMAP_COMMITTED ? "COMMITTED" : "???",
      (int)iCid
    );
  }
}
#endif

static void hctDbPageArrayReset(HctDbPageArray *pArray){
  sqlite3_free(pArray->aDyn);
  pArray->nPg = 0;
  pArray->aPg = pArray->aStatic;
  pArray->aDyn = 0;
  pArray->nDyn = 0;
}

static void hctDbPageArrayGrow(HctDbPageArray *pArray){
  assert( pArray->aDyn==0 );
  pArray->aDyn = sqlite3HctMalloc(sizeof(HctFilePage) * HCTDB_MAX_PAGEARRAY);
  pArray->nDyn = HCTDB_MAX_PAGEARRAY;
  pArray->aPg = pArray->aDyn;
  hctMemcpy(pArray->aPg, pArray->aStatic, 
      sizeof(HctFilePage)*HCTDB_STATIC_PAGEARRAY
  );
}

/*
** Grow the dynamic arrays used by the writer, if necessary
*/
static void hctDbWriterGrow(HctDbWriter *pWriter){
  if( pWriter->writepg.aDyn==0 ){
    if( pWriter->writepg.nPg>=(HCTDB_STATIC_PAGEARRAY-2) 
     || pWriter->discardpg.nPg>=(HCTDB_STATIC_PAGEARRAY-2) 
    ){
      hctDbPageArrayGrow(&pWriter->writepg);
      hctDbPageArrayGrow(&pWriter->discardpg);
    }
  }
}

HctDatabase *sqlite3HctDbOpen(
  int *pRc,
  const char *zFile, 
  HctConfig *pConfig
){
  int rc = *pRc;
  HctDatabase *pNew = 0;

  pNew = (HctDatabase*)sqlite3HctMalloc(sizeof(*pNew));
  pNew->pFile = sqlite3HctFileOpen(&rc, zFile, pConfig);
  pNew->pConfig = pConfig;
  assert( (pNew->pFile==0)==(rc!=SQLITE_OK) );
  if( pNew->pFile ){
    pNew->pgsz = sqlite3HctFilePgsz(pNew->pFile);
  }

  if( rc!=SQLITE_OK ){
    sqlite3HctDbClose(pNew);
    pNew = 0;
  }
  *pRc = rc;
  return pNew;
}

int sqlite3HctDbPagesize(HctDatabase *pDb){
  return pDb->pgsz;
}


void sqlite3HctDbClose(HctDatabase *p){
  if( p ){
    assert( p->rbackcsr.pKeyInfo==0 );
    assert( p->rbackcsr.pRec==0 );
    sqlite3_free(p->aTmp);
    sqlite3HctFileClose(p->pFile);
    p->pFile = 0;
    sqlite3_free(p->pBalance);
    sqlite3_free(p);
  }
}

HctFile *sqlite3HctDbFile(HctDatabase *pDb){
  return pDb->pFile;
}

int sqlite3HctDbRootNew(HctDatabase *p, u32 *piRoot){
  return sqlite3HctFileRootPgno(p->pFile, piRoot);
}

static void hctDbRootPageInit(
  int bIndex,                     /* True for an index, false for intkey */
  int nHeight,                    /* Initial height */
  u32 iChildPg,                   /* Child page number */
  u8 *aPage,                      /* Buffer to initialize */
  int szPage                      /* Size of aPage[] in bytes */
){
  HctDbPageHdr *pPg = (HctDbPageHdr*)aPage;
  memset(aPage, 0, szPage);
  if( bIndex ){
    pPg->hdrFlags = HCT_PAGETYPE_INDEX | HCT_PAGETYPE_LEFTMOST;
  }else{
    pPg->hdrFlags = HCT_PAGETYPE_INTKEY | HCT_PAGETYPE_LEFTMOST;
  }
  if( nHeight>0 ){
    pPg->nHeight = nHeight;
    pPg->nEntry = 1;
    if( bIndex ){
      HctDbIndexNode *pNode = (HctDbIndexNode*)pPg;
      pNode->aEntry[0].iChildPg = iChildPg;
      pNode->hdr.nFreeBytes = 
        szPage - sizeof(HctDbIndexNode) - sizeof(HctDbIndexNodeEntry);
      pNode->hdr.nFreeGap = pNode->hdr.nFreeBytes;
    }else{
      HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)pPg;
      pNode->aEntry[0].iKey = SMALLEST_INT64;
      pNode->aEntry[0].iChildPg = iChildPg;
    }
  }else{
    HctDbLeaf *pLeaf = (HctDbLeaf*)pPg;
    pLeaf->hdr.nFreeBytes = szPage - sizeof(HctDbLeaf);
    pLeaf->hdr.nFreeGap = pLeaf->hdr.nFreeBytes;
  }
}

void sqlite3HctDbRootPageInit(
  int bIndex,                     /* True for an index, false for intkey */
  u8 *aPage,                      /* Buffer to initialize */
  int szPage                      /* Size of aPage[] in bytes */
){
  hctDbRootPageInit(bIndex, 0, 0, aPage, szPage);
}


/*
** Open a read transaction, if one is not already open.
*/
int sqlite3HctDbStartRead(HctDatabase *pDb, HctJournal *pJrnl){
  int rc = SQLITE_OK;

  assert( (pDb->iSnapshotId==0)==(pDb->pTmap==0) );
  assert( pDb->iSnapshotId!=0 || hctDbIsConcurrent(pDb)==0 );
  if( pDb->iSnapshotId==0 && SQLITE_OK==(rc=sqlite3HctFileNewDb(pDb->pFile)) ){
    if( pDb->aTmp==0 ){
      pDb->pgsz = sqlite3HctFilePgsz(pDb->pFile);
      pDb->aTmp = (u8*)sqlite3HctMallocRc(&rc, pDb->pgsz);
    }
    if( rc==SQLITE_OK ){
      u64 iSnapshot = 0;
      HctTMapClient *pTMapClient = sqlite3HctFileTMapClient(pDb->pFile);

      iSnapshot = sqlite3HctJrnlSnapshot(pJrnl);
      rc = sqlite3HctTMapBegin(pTMapClient, iSnapshot, &pDb->pTmap);
      assert( rc==SQLITE_OK );  /* todo */

      if( iSnapshot==0 ){
        iSnapshot = sqlite3HctFileGetSnapshotid(pDb->pFile);
      }
      pDb->iSnapshotId = iSnapshot;
      pDb->iLocalMinTid = sqlite3HctTMapCommitedTID(pTMapClient);
      assert( pDb->iSnapshotId>0 );

      assert( pDb->iReqSnapshotId==0 );
      pDb->iReqSnapshotId = 1;
      if( pDb->iSnapshotId>(HCT_MAX_LEADING_WRITE/2) ){
        pDb->iReqSnapshotId = pDb->iSnapshotId - (HCT_MAX_LEADING_WRITE/2);
      }

      HCT_EXTRA_WR_LOGGING((
            "%p: starting read with snapshot=%lld, localmintid=%lld", 
            pDb, pDb->iSnapshotId, pDb->iLocalMinTid
      ));
    }
  }

  return rc;
}

static u64 hctGetU64(const u8 *a){
  u64 ret;
  hctMemcpy(&ret, a, sizeof(u64));
  return ret;
}
static u32 hctGetU32(const u8 *a){
  u32 ret;
  hctMemcpy(&ret, a, sizeof(u32));
  return ret;
}

#if 0
static void hctPutU32(u8 *a, u32 val){
  hctMemcpy(a, &val, sizeof(u32));
}
#endif

/*
** Return true if TID iTid maps to a commit-id visible to the current
** client. Or false otherwise.
*/
static int hctDbTidIsVisible(HctDatabase *pDb, u64 iTid, int bNosnap){

  // if( (iTid & HCT_TID_MASK)<=pDb->iLocalMinTid ) return 1;

  while( 1 ){
    u64 eState = 0;
    u64 iCid = hctDbTMapLookup(pDb, (iTid & HCT_TID_MASK), &eState);
    if( iTid & HCT_TID_ROLLBACK_OVERRIDE ){
      eState = HCT_TMAP_COMMITTED;
    }
    if( eState==HCT_TMAP_WRITING || eState==HCT_TMAP_ROLLBACK ){
      return 0;
    }
    if( eState==HCT_TMAP_COMMITTED ){
      if( bNosnap==0 && iCid>pDb->iSnapshotId ){
        return 0;
      }
      pDb->iReqSnapshotId = MAX(pDb->iReqSnapshotId, iCid);
      return 1;
    }
    assert( eState==HCT_TMAP_VALIDATING );
    if( iCid>pDb->iSnapshotId || iTid==pDb->iTid ){
      return 0;
    }
  }

  assert( 0 );
  return 0;
}

/*
** This is called when writing keys to the database as part of committing
** a transaction. One of the writes will clobber a key associated with
** transaction-id iTid. This function returns true if this represents
** a write/write conflict and the transaction should be rolled back, or
** false if the write should proceed.
*/
static int hctDbTidIsConflict(HctDatabase *pDb, u64 iTid){
  if( iTid==pDb->iTid /* || iTid<=pDb->iLocalMinTid */ || iTid==LARGEST_TID ){
    return 0;
  }else{
    u64 eState = 0;
    u64 iCid = hctDbTMapLookup(pDb, iTid & HCT_TID_MASK, &eState);

    /* This should only be called while writing or validating. */
    assert( pDb->iTid );
    if( iTid & HCT_TID_ROLLBACK_OVERRIDE ){
      eState = HCT_TMAP_COMMITTED;
    }

    pDb->iReqSnapshotId = MAX(pDb->iReqSnapshotId, iCid);
    if( eState==HCT_TMAP_COMMITTED && iCid<=pDb->iSnapshotId ) return 0;
    /* if( iCid==pDb->iJrnlWriteCid ) return 0; */
    return 1;

    if( eState==HCT_TMAP_WRITING || eState==HCT_TMAP_VALIDATING ) return 1;

    /* It's tempting to return 0 here - how can a key that has been rolled
    ** back be a conflict? The problem is that the previous version of the
    ** key - the one before this rolled back version - may be a write/write
    ** conflict. Ideally, this code would check that and return accordingly. */
    if( eState==HCT_TMAP_ROLLBACK ) return 1;

    assert( eState==HCT_TMAP_COMMITTED );
    return (iCid > pDb->iSnapshotId);
  }
}


static int hctDbOffset(int iOff, int flags){
  static const int aVal[] = {
    0+0+0+0+0, 0+0+0+0+8, 0+0+0+0+0, 0+0+0+0+8,
    0+0+4+0+0, 0+0+4+0+8, 0+0+4+0+0, 0+0+4+0+8,
    0+8+0+0+0, 0+8+0+0+8, 0+8+0+0+0, 0+8+0+0+8,
    0+8+4+0+0, 0+8+4+0+8, 0+8+4+0+0, 0+8+4+0+8,

    4+0+0+0+0, 4+0+0+0+8, 4+0+0+0+0, 4+0+0+0+8,
    4+0+4+0+0, 4+0+4+0+8, 4+0+4+0+0, 4+0+4+0+8,
    4+8+0+0+0, 4+8+0+0+8, 4+8+0+0+0, 4+8+0+0+8,
    4+8+4+0+0, 4+8+4+0+8, 4+8+4+0+0, 4+8+4+0+8,
  };

  assert( HCTDB_HAS_RANGEOLD==0x10 );  /* +4 */
  assert( HCTDB_HAS_RANGETID==0x08 );  /* +8 */
  assert( HCTDB_HAS_OVFL==0x04 );      /* +4 */
  assert( HCTDB_HAS_TID==0x01 );       /* +8 */

  assert( aVal[ flags & 0x1F ]==(
      ((flags & HCTDB_HAS_TID) ? 8 : 0)
    + ((flags & HCTDB_HAS_RANGETID) ? 8 : 0)
    + ((flags & HCTDB_HAS_RANGEOLD) ? 4 : 0)
    + ((flags & HCTDB_HAS_OVFL) ? 4 : 0)
  ));

  return iOff + aVal[ flags&0x1F ];
}


/*
** Wrapper around sqlite3HctFilePageGetPhysical() that also invokes the
** xSavePhysical callback, if one is configured.
*/
static int hctDbGetPhysical(HctDatabase *pDb, u32 iPg, HctFilePage *pPg){
  int rc = sqlite3HctFilePageGetPhysical(pDb->pFile, iPg, pPg);
  if( rc==SQLITE_OK && pDb->xSavePhysical ){
    rc = pDb->xSavePhysical(pDb->pSavePhysical, (i64)iPg);
  }
  return rc;
}

/*
** Load the meta-data record from the database and store it in buffer aBuf
** (size nBuf bytes). The meta-data record is stored with rowid=0 int the
** intkey table with root-page=2.
*/
int sqlite3HctDbGetMeta(HctDatabase *pDb, u8 *aBuf, int nBuf){
  HctFilePage pg;
  int rc;

  assert( pDb->iSnapshotId );
  memset(aBuf, 0, nBuf);
  rc = sqlite3HctFilePageGet(pDb->pFile, 2, &pg);
  while( rc==SQLITE_OK ){
    HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)pg.aOld;
    int iOff;
    u8 flags;

    if( pLeaf->pg.nEntry==0 ){
      break;
    }

    assert( pLeaf->pg.nEntry==1 );
    assert( pLeaf->aEntry[0].iKey==0 );
    assert( pLeaf->aEntry[0].nSize==nBuf );
    iOff = pLeaf->aEntry[0].iOff;
    flags = pLeaf->aEntry[0].flags;

    assert( flags==HCTDB_HAS_TID 
         || flags==(HCTDB_HAS_RANGEOLD|HCTDB_HAS_RANGETID|HCTDB_HAS_TID) 
    );
    if( (flags & HCTDB_HAS_RANGEOLD)
     && 0==hctDbTidIsVisible(pDb, hctGetU64(&pg.aOld[iOff]), 0) 
    ){
      u32 iOld = hctGetU32(&pg.aOld[iOff+8+8]);
      if( iOld==0 ) break;
      sqlite3HctFilePageRelease(&pg);
      rc = hctDbGetPhysical(pDb, iOld, &pg);
    }else{
      iOff = hctDbOffset(iOff, pLeaf->aEntry[0].flags );
      hctMemcpy(aBuf, &pg.aOld[iOff], nBuf);
      sqlite3HctFilePageRelease(&pg);
      break;
    }
  }

  return rc;
}

void sqlite3HctDbTransIsConcurrent(HctDatabase *pDb, int bConcurrent){
  pDb->bConcurrent = bConcurrent;
}

static int hctDbValidateMeta(HctDatabase *pDb){
  int rc = SQLITE_OK;
  HctFilePage pg;

  assert( pDb->iSnapshotId>0 );
  rc = sqlite3HctFilePageGet(pDb->pFile, 2, &pg);
  if( rc==SQLITE_OK ){
    HctDbIntkeyEntry *p = &((HctDbIntkeyLeaf*)pg.aOld)->aEntry[0];
    if( p->flags & HCTDB_HAS_TID ){
      u64 iTid = hctGetU64(&pg.aOld[p->iOff]);
      if( hctDbTidIsConflict(pDb, iTid) ) rc = HCT_SQLITE_BUSY;
    }
    sqlite3HctFilePageRelease(&pg);
  }

  return rc;
}

int sqlite3HctDbRootInit(HctDatabase *p, int bIndex, u32 iRoot){
  HctFilePage pg;
  int rc = SQLITE_OK;

  rc = sqlite3HctFileRootNew(p->pFile, iRoot, &pg);
  if( rc==SQLITE_OK ){
    sqlite3HctDbRootPageInit(bIndex, pg.aNew, p->pgsz);
    rc = sqlite3HctFilePageRelease(&pg);
  }
  return rc;
}

int sqlite3HctDbDirectClear(HctDatabase *pDb, u32 iRoot){
  int rc = sqlite3HctFileTreeClear(pDb->pFile, iRoot);
  if( rc==SQLITE_OK ){
    HctFilePage pg;
    rc = sqlite3HctFilePageGet(pDb->pFile, iRoot, &pg);
    if( rc==SQLITE_OK ){
      int bIndex = hctPagetype(pg.aOld)==HCT_PAGETYPE_INDEX;
      sqlite3HctDbRootPageInit(bIndex, pg.aOld, pDb->pgsz);
    }
  }
  return rc;
}

/*
** Return true if the cursor currently points to the last entry in its
** table. Return false if the cursor is invalid or points to some other
** entry. 
*/
int sqlite3HctDbCsrIsLast(HctDbCsr *pCsr){
  if( pCsr->pg.aOld 
   && ((HctDbPageHdr*)pCsr->pg.aOld)->iPeerPg==0
   && ((HctDbPageHdr*)pCsr->pg.aOld)->nEntry==pCsr->iCell+1
  ){
    return 1;
  }
  return 0;
}


static i64 hctDbIntkeyFPKey(const void *aPg){
  if( ((HctDbPageHdr*)aPg)->nHeight==0 ){
    return ((HctDbIntkeyLeaf*)aPg)->aEntry[0].iKey;
  }
  return ((HctDbIntkeyNode*)aPg)->aEntry[0].iKey;
}


static i64 hctDbGetIntkey(const u8 *aTarget, int iCell){
  assert( hctPagetype(aTarget)==HCT_PAGETYPE_INTKEY );
  assert( iCell>=0 && iCell<((HctDbIntkeyLeaf*)aTarget)->pg.nEntry );

  if( hctPageheight(aTarget)==0 ){
    return ((HctDbIntkeyLeaf*)aTarget)->aEntry[iCell].iKey;
  }
  return ((HctDbIntkeyNode*)aTarget)->aEntry[iCell].iKey;
}

#if 0
static i64 hctDbGetIntkeyFromPhys(
  int *pRc, 
  HctDatabase *pDb, 
  u32 iPhys, 
  int iCell
){
  i64 iRet = 0;
  int rc = *pRc;
  if( rc==SQLITE_OK ){
    HctFilePage pg;
    rc = sqlite3HctFilePageGetPhysical(pDb->pFile, iPhys, &pg);
    if( rc==SQLITE_OK ){
      iRet = hctDbGetIntkey(pg.aOld, iCell);
      sqlite3HctFilePageRelease(&pg);
    }
  }
  *pRc = rc;
  return iRet;
}
#endif


/*
** Buffer aPg contains an intkey leaf page.
**
** This function searches the leaf page for key iKey. If found, it returns
** the index of the matching key within the page and sets output variable
** (*pbExact) to 1. If there is no match for key iKey, this function returns
** the index of the smallest key on the page that is larger than iKey, or 
** (nEntry) if all keys on the page are smaller than iKey. (*pbExact) is 
** set to 0 before returning in this case.
*/
static int hctDbIntkeyLeafSearch(
  const u8 *aPg,
  i64 iKey,
  int *pbExact
){
  const HctDbIntkeyLeaf *pLeaf = (const HctDbIntkeyLeaf*)aPg;
  int i1 = 0;
  int i2 = pLeaf->pg.nEntry;

  assert( hctPagetype(aPg)==HCT_PAGETYPE_INTKEY );
  assert( pLeaf->pg.nHeight==0 );
  while( i2>i1 ){
    int iTest = (i1+i2)/2;
    i64 iPgKey = pLeaf->aEntry[iTest].iKey;
    if( iPgKey==iKey ){
      *pbExact = 1;
      return iTest;
    }else if( iPgKey<iKey ){
      i1 = iTest+1;
    }else{
      i2 = iTest;
    }
  }
  assert( i1==i2 );

  assert( i2>=0 );
  assert( i2==pLeaf->pg.nEntry || iKey<pLeaf->aEntry[i2].iKey );
  assert( i2==0 || iKey>pLeaf->aEntry[i2-1].iKey );

  *pbExact = 0;
  return i2;
}

static int hctDbIntkeyLocalsize(int pgsz, int nSize){
  const int nMax = (
      pgsz - 
      sizeof(HctDbIntkeyLeaf) - 
      sizeof(HctDbIntkeyEntry) - 
      (HCTDB_MAX_EXTRA_CELL_DATA - sizeof(u32))
  );

  int nLocal;
  if( nSize<nMax ){
    nLocal = nSize;
  }else{
    int nMin = pgsz / 8;
    nLocal = nSize % (pgsz-sizeof(HctDbPageHdr));
    if( nLocal<nMin || nLocal> (nMax-sizeof(u32)) ){
      nLocal = nMin;
    }
  }

  return nLocal;
}

static int hctDbIndexLocalsize(int pgsz, int nSize){
  int nLocal;
  int nMax = pgsz/4;
  if( nSize<nMax ){
    nLocal = nSize;
  }else{
    int nMin = 64;
    nLocal = nSize % (pgsz-sizeof(HctDbPageHdr));
    if( nLocal<nMin || nLocal>nMax ){
      nLocal = nMin;
    }
  }
  return nLocal;
}

static int hctDbLocalsize(const u8 *aPg, int pgsz, int nSize){
  if( hctPagetype(aPg)==HCT_PAGETYPE_INTKEY ){
    return hctDbIntkeyLocalsize(pgsz, nSize);
  }
  return hctDbIndexLocalsize(pgsz, nSize);
}

static int hctDbIntkeyEntrySize(HctDbIntkeyEntry *pEntry, int pgsz){
  int sz = hctDbIntkeyLocalsize(pgsz, pEntry->nSize)
    + hctDbOffset(0, pEntry->flags);
  return sz;
}

static int hctDbIndexEntrySize(HctDbIndexEntry *pEntry, int pgsz){
  int sz = hctDbIndexLocalsize(pgsz, pEntry->nSize)
    + hctDbOffset(0, pEntry->flags);
  return sz;
}

static int hctDbIndexNodeEntrySize(HctDbIndexNodeEntry *pEntry, int pgsz){
  return hctDbIndexLocalsize(pgsz, pEntry->nSize)
    + ((pEntry->flags & HCTDB_HAS_OVFL) ? 4 : 0);
}

/*
** The pointer passed as the first argument is a pointer to a buffer
** containing a page that uses variable sized records. That is, an
** intkey leaf page, or an index leaf or node page. This function
** returns the number of bytes of record-area space consumed by
** entry iEntry on the page.
*/
static int hctDbPageRecordSize(void *aPg, int pgsz, int iEntry){
  int eType = hctPagetype(aPg);
  if( eType==HCT_PAGETYPE_INTKEY ){
    assert( hctPageheight(aPg)==0 );
    return hctDbIntkeyEntrySize(&((HctDbIntkeyLeaf*)aPg)->aEntry[iEntry], pgsz);
  }else if( hctPageheight(aPg)==0 ){
    return hctDbIndexEntrySize(&((HctDbIndexLeaf*)aPg)->aEntry[iEntry], pgsz);
  }
  return hctDbIndexNodeEntrySize(&((HctDbIndexNode*)aPg)->aEntry[iEntry], pgsz);
}
static int hctDbPageEntrySize(void *aPg){
  int eType = hctPagetype(aPg);
  if( eType==HCT_PAGETYPE_INTKEY ){
    assert( hctPageheight(aPg)==0 );
    return sizeof(HctDbIntkeyEntry);
  }else if( hctPageheight(aPg)==0 ){
    return sizeof(HctDbIndexEntry);
  }
  return sizeof(HctDbIndexNodeEntry);
}

/*
** The buffer passed as the first argument contains a page that is 
** guaranteed to be either an intkey leaf, or an index leaf or node.
** This function returns a pointer to HctDbIndexEntry structure
** associated with page entry iEntry.
*/
static HctDbIndexEntry *hctDbEntryEntry(const void *aPg, int iEntry){
  int iOff;

  assert( (hctPagetype(aPg)==HCT_PAGETYPE_INTKEY && hctPageheight(aPg)==0)
       || (hctPagetype(aPg)==HCT_PAGETYPE_INDEX)
  );

  if( hctPagetype(aPg)==HCT_PAGETYPE_INTKEY ){
    iOff = sizeof(HctDbIntkeyLeaf) + iEntry*sizeof(HctDbIntkeyEntry);
  }else if( hctPageheight(aPg)==0 ){
    iOff = sizeof(HctDbIndexLeaf) + iEntry*sizeof(HctDbIndexEntry);
  }else{
    iOff = sizeof(HctDbIndexNode) + iEntry*sizeof(HctDbIndexNodeEntry);
  }

  return (HctDbIndexEntry*)&((u8*)aPg)[iOff];
}

/*
** Argument aPg[] is a buffer containing either an index tree page, or an 
** intkey leaf page. This function locates the record associated with
** cell iCell on the page, and populates output variables *pnData and
** *paData with the size and a pointer to a buffer containing the record,
** respectively.
**
** If the record in cell iCell does not overflow the page, (*paData) is
** set to point into the body of the page itself. If the record does
** overflow the page, then buffer pBuf is used to store the record and
** (*paData) is set to point to the buffer's allocation. In this case
** it is the responsibility of the caller to eventually release the buffer.
**
** SQLITE_OK is returned if successful, or an SQLite error code otherwise.
*/
static int hctDbLoadRecord(
  HctDatabase *pDb,
  HctBuffer *pBuf,
  const u8 *aPg,
  int iCell,
  int *pnData,
  const u8 **paData
){
  int rc = SQLITE_OK;
  HctDbIndexEntry *p = hctDbEntryEntry(aPg, iCell);

  *pnData = p->nSize;
  if( paData ){
    if( p->flags & HCTDB_HAS_OVFL ){
      sqlite3HctBufferGrow(pBuf, p->nSize);
      *paData = pBuf->aBuf;
      if( rc==SQLITE_OK ){
        u32 pgOvfl;
        int nLocal = hctDbLocalsize(aPg, pDb->pgsz, p->nSize);

        int iOff = hctDbOffset(p->iOff, p->flags);
        hctMemcpy(pBuf->aBuf, &aPg[iOff], nLocal);
        pgOvfl = hctGetU32(&aPg[iOff-sizeof(u32)]);
        iOff = nLocal;

        while( rc==SQLITE_OK && iOff<p->nSize ){
          HctFilePage ovfl;
          rc = hctDbGetPhysical(pDb, pgOvfl, &ovfl);
          if( rc==SQLITE_OK ){
            int nCopy = MIN(pDb->pgsz-8, p->nSize-iOff);
            hctMemcpy(&pBuf->aBuf[iOff],&ovfl.aOld[sizeof(HctDbPageHdr)],nCopy);
            iOff += nCopy;
            pgOvfl = ((HctDbPageHdr*)ovfl.aOld)->iPeerPg;
            sqlite3HctFilePageRelease(&ovfl);
          }
        }
      }
    }else{
      int iOff = hctDbOffset(p->iOff, p->flags);
      *paData = &aPg[iOff];
    }
  }

  return rc;
}

/*
** Buffer aPg[] contains either an index page or an intkey leaf (i.e. a page
** that contains variable length records). This function loads the record
** associated with cell iCell on the page, and populates output object
** pFP with the results.
**
** SQLITE_OK is returned if successful, or an SQLite error code otherwise.
*/
static int hctDbLoadRecordFP(
  HctDatabase *pDb,               /* Database handle */
  const u8 *aPg,                  /* Page to load record from */
  int iCell,                      /* Cell to load */
  HctDbFPKey *pFP                 /* Populate this structure with record */
){
  const u8 *aKey = 0;
  int nKey = 0;
  int rc = SQLITE_OK;

  rc = hctDbLoadRecord(pDb, &pFP->buf, aPg, iCell, &nKey, &aKey);
  if( rc==SQLITE_OK ){
    if( aKey!=pFP->buf.aBuf ){
      sqlite3HctBufferGrow(&pFP->buf, nKey);
      hctMemcpy(pFP->buf.aBuf, aKey, nKey);
    }
    pFP->iKey = nKey;
    pFP->aKey = pFP->buf.aBuf;
  }

  return rc;
}

/*
** Buffer aPg[] contains a history fan page.
**
** This page searches the page, returning the index of the entry that
** points to the page with the largest key that is less than or equal
** to parameter pKey/iKey.
*/
static int hctDbFanSearch(
  int *pRc,
  HctDatabase *pDb,
  const u8 *aPg,
  UnpackedRecord *pKey,
  i64 iKey
){
  HctDbHistoryFan *pFan = (HctDbHistoryFan*)aPg;
  int rc = *pRc;
  int i1 = 0;
  int i2 = pFan->pg.nEntry-1;
  HctBuffer buf = {0, 0, 0};

  assert( hctPagetype(aPg)==HCT_PAGETYPE_HISTORY );

  while( rc==SQLITE_OK && i2>i1 ){
    HctFilePage pg;
    int iTest = (i1+i2)/2;

    rc = hctDbGetPhysical(pDb, pFan->aPgOld1[iTest], &pg);
    while( rc==SQLITE_OK && hctPagetype(pg.aOld)==HCT_PAGETYPE_HISTORY ){
      HctDbHistoryFan *pFan = (HctDbHistoryFan*)pg.aOld;
      rc = hctDbGetPhysical(pDb, pFan->pgOld0, &pg);
    }
    if( rc==SQLITE_OK ){
      int iCell = (iTest==0 ? pFan->iSplit0 : 0);

      assert( pKey    || hctPagetype(pg.aOld)==HCT_PAGETYPE_INTKEY );
      assert( pKey==0 || hctPagetype(pg.aOld)==HCT_PAGETYPE_INDEX );

      if( pKey==0 ){
        i64 iPgKey = hctDbGetIntkey(pg.aOld, iCell);
        if( iPgKey==iKey ){
          i1 = i2 = iTest+1;
        }else if( iPgKey<iKey ){
          i1 = iTest+1;
        }else{
          i2 = iTest;
        }
      }else{
        const u8 *aRec = 0;
        int nRec = 0;
        int res = 0;

        rc = hctDbLoadRecord(pDb, &buf, pg.aOld, iCell, &nRec, &aRec);
        if( rc==SQLITE_OK ){
          assert( nRec>0 );
          res = sqlite3VdbeRecordCompare(nRec, aRec, pKey);
        }
        if( res==0 ){
          i1 = i2 = iTest+1;
        }else if( res<0 ){
          i1 = iTest+1;
        }else{
          i2 = iTest;
        }
      }

      sqlite3HctFilePageRelease(&pg);
    }
  }
  sqlite3HctBufferFree(&buf);
  assert( i1==i2 );

  *pRc = rc;
  return i2;
}


static void hctDbFreeUnpacked(UnpackedRecord *pRec){
  if( pRec ){
    sqlite3DbFree(pRec->pKeyInfo->db, pRec);
  }
}

static UnpackedRecord *hctDbAllocateUnpacked(int *pRc, KeyInfo *pKeyInfo){
  UnpackedRecord *pRet = 0;
  if( *pRc==SQLITE_OK ){
    pRet = sqlite3VdbeAllocUnpackedRecord(pKeyInfo);
    if( pRet==0 ) *pRc = SQLITE_NOMEM_BKPT;
  }
  return pRet;
}

void sqlite3HctDbRecordTrim(UnpackedRecord *pRec){
  if( pRec && pRec->pKeyInfo->nUniqField ){
    int ii;
    u16 nUniqField = pRec->pKeyInfo->nUniqField;
    for(ii=0; ii<nUniqField; ii++){
      if( pRec->aMem[ii].flags & MEM_Null ){
        return;
      }
    }
    pRec->nField = nUniqField;
  }
}


/*
** This function returns the current snapshot-id. It may only be called
** when a read transaction is active.
*/
i64 sqlite3HctDbSnapshotId(HctDatabase *pDb){
  assert( pDb->iSnapshotId>0 );
  return pDb->iSnapshotId;
}

u64 sqlite3HctDbReqSnapshot(HctDatabase *pDb){
  return pDb->iReqSnapshotId;
}

/*
** Load the key belonging to cell iCell on page aPg[] into structure (*pKey).
*/
static void hctDbGetKey(
  int *pRc, 
  HctDatabase *pDb,
  KeyInfo *pKeyInfo,
  int bDup,
  const u8 *aPg,
  int iCell,
  HctDbKey *pKey
){
  int rc = *pRc;

  if( rc==SQLITE_OK ){
    assert( iCell>=0 && iCell<hctPagenentry(aPg) );
    assert( (pKeyInfo==0)==(hctPagetype(aPg)==HCT_PAGETYPE_INTKEY) );
    assert( (pKeyInfo!=0)==(hctPagetype(aPg)==HCT_PAGETYPE_INDEX) );

    if( pKeyInfo==0 ){
      pKey->iKey = hctDbGetIntkey(aPg, iCell);
    }else{
      const u8 *aRec = 0;
      int nRec = 0;
      rc = hctDbLoadRecord(pDb, &pKey->buf, aPg, iCell, &nRec, &aRec);
      if( nRec==0 ){
        assert( pKey->pKey==0 );
      }else{
        if( aRec!=pKey->buf.aBuf && bDup && rc==SQLITE_OK ){
          hctBufferSet(&pKey->buf, aRec, nRec);
          aRec = pKey->buf.aBuf;
        }
        pKey->pKey = hctDbAllocateUnpacked(&rc, pKeyInfo);
        if( rc==SQLITE_OK ){
          sqlite3VdbeRecordUnpack(pKeyInfo, nRec, aRec, pKey->pKey);
        }
        if( rc==SQLITE_OK ){
          sqlite3HctDbRecordTrim(pKey->pKey);
        }
      }
    }
  }
  *pRc = rc;
}

/*
** Retrieve the key from iCell of physical page iPhys. iPhys may be an
** intkey or index leaf page. Populate structure (*pKey) with the key
** value before returning.
*/
static void hctDbGetKeyFromPage(
  int *pRc, 
  HctDatabase *pDb,
  KeyInfo *pKeyInfo,
  int bLogical,                   /* True for logical, false for physical */
  u32 iPg, 
  int iCell,
  HctDbKey *pKey
){
  int rc = *pRc;

  if( rc==SQLITE_OK ){
    HctFilePage pg;
    if( bLogical ){
      rc = sqlite3HctFilePageGet(pDb->pFile, iPg, &pg);
    }else{
      rc = hctDbGetPhysical(pDb, iPg, &pg);
      while( rc==SQLITE_OK && hctPagetype(pg.aOld)==HCT_PAGETYPE_HISTORY ){
        HctDbHistoryFan *pFan = (HctDbHistoryFan*)pg.aOld;
        rc = hctDbGetPhysical(pDb, pFan->pgOld0, &pg);
      }
    }
    if( rc==SQLITE_OK ){
      hctDbGetKey(&rc, pDb, pKeyInfo, 1, pg.aOld, iCell, pKey);
      sqlite3HctFilePageRelease(&pg);
    }
  }
  *pRc = rc;
}

/* static RecordCompare find_record_compare((UnpackedRecord*, RecordCompare); */
#define find_record_compare(pRec, xCompare) (                 \
    (xCompare) ? (xCompare) : sqlite3VdbeFindCompare(pRec)    \
)


static int hctDbIndexSearch(
  HctDatabase *pDb,
  const u8 *aPg, 
  RecordCompare xCompare,
  UnpackedRecord *pRec,
  int *piPos,
  int *pbExact
){
  int rc = SQLITE_OK;
  HctBuffer buf;
  int i1 = 0;
  int i2 = ((HctDbPageHdr*)aPg)->nEntry;

  if( pRec ) xCompare = find_record_compare(pRec, xCompare);
  memset(&buf, 0, sizeof(buf));

  *pbExact = 0;
  while( i2>i1 ){
    int iTest = (i1+i2)/2;
    int res;
    int nRec = 0;
    const u8 *aRec = 0;

    rc = hctDbLoadRecord(pDb, &buf, aPg, iTest, &nRec, &aRec);
    if( rc!=SQLITE_OK ) break;
    if( nRec==0 ){
      res = -1;
    }else{
      res = xCompare(nRec, aRec, pRec);
    }

    if( res==0 ){
      *pbExact = 1;
      i2 = iTest;
    }else if( res<0 ){
      i1 = iTest+1;
    }else{
      i2 = iTest;
    }
  }

  assert( i1==i2 && i2>=0 );
  sqlite3HctBufferFree(&buf);
  *piPos = i2;
  return rc;
}

static void hctDbDebugIndexSearch(
  const char *zFunc,
  int iLine,
  HctDatabase *pDb,
  i64 iPg,
  UnpackedRecord *pRec,
  int iRes,
  int bExact
){
  char *zKey = sqlite3HctDbUnpackedToText(pRec);

  hctExtraLogging(zFunc, iLine, pDb, sqlite3HctMprintf(
        "search of page %lld for (%z) lands on (cell=%d, exact=%d)", 
        iPg, zKey, iRes, bExact
  ));
}

#define HCT_EXTRA_LOGGING_INDEXSEARCH(a,b,c,d,e) \
  if( a->pConfig->bHctExtraLogging ) { \
    hctDbDebugIndexSearch(__func__, __LINE__, a,b,c,d,e); \
  }

/*
** The first argument is a pointer to an intkey internal node page.
**
** This function searches the node page for key iKey. If found, it returns
** the index of the matching key within the page and sets output variable
** (*pbExact) to 1. If there is no match for key iKey, this function returns
** the index of the smallest key on the page that is larger than iKey, or 
** (nEntry) if all keys on the page are smaller than iKey. (*pbExact) is 
** set to 0 before returning in this case.
*/
static int hctDbIntkeyNodeSearch(
  void *aPg,
  i64 iKey,
  int *pbExact
){
  HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)aPg;
  int i1 = 0;
  int i2 = pNode->pg.nEntry;

  assert( hctPagetype(pNode)==HCT_PAGETYPE_INTKEY && pNode->pg.nHeight>0 );
  while( i2>i1 ){
    int iTest = (i1+i2)/2;
    i64 iPgKey = pNode->aEntry[iTest].iKey;
    if( iPgKey==iKey ){
      *pbExact = 1;
      return iTest;
    }else if( iPgKey<iKey ){
      i1 = iTest+1;
    }else{
      i2 = iTest;
    }
  }
  assert( i1==i2 );

  assert( i2>=0 );
  assert( i2==pNode->pg.nEntry || iKey<pNode->aEntry[i2].iKey );
  assert( i2==0 || iKey>pNode->aEntry[i2-1].iKey );

  *pbExact = 0;
  return i2;
}

/*
** Set (*bGe) to true if (pRec >= (FP-key for aPg)).
*/
static int hctDbCompareFPKey(
  HctDatabase *pDb, 
  UnpackedRecord *pRec, 
  const u8 *aPg,
  int *pbGe
){
  const u8 *aFP = 0;
  int nFP = 0;
  int res;
  int rc;
  HctBuffer buf = {0,0,0};

  /* aPg[] may not be a leftmost page. If it was, then the record loaded
  ** below might be a special 0-byte record. Causing sqlite3VdbeRecordCompare()
  ** to malfunction. */
  assert( hctIsLeftmost(aPg)==0 );

  rc = hctDbLoadRecord(pDb, &buf, aPg, 0, &nFP, &aFP);
  if( rc==SQLITE_OK ){
    assert( nFP>0 );
    res = sqlite3VdbeRecordCompare(nFP, aFP, pRec);
    sqlite3HctBufferFree(&buf);
    *pbGe = (res<=0);
  }
  return rc;
}

static int hctDbCsrGoLeft(HctDbCsr*, int);

/*
** Seek the cursor within its tree. This only seeks within the tree, it does
** not follow any old-data pointers.
*/
int hctDbCsrSeek(
  HctDbCsr *pCsr,                 /* Cursor to seek */
  HctDbFPKey *pFP,
  int iHeight,                    /* Height to seek at (0==leaf, 1==parent) */
  RecordCompare xCompare,
  UnpackedRecord *pRec,           /* Key for index/without rowid tables */
  i64 iKey,                       /* Key for intkey tables */
  int *pbExact
){
  HctFile *pFile = pCsr->pDb->pFile;
  u32 iPg = pCsr->iRoot;
  int rc = SQLITE_OK;

  HctFilePage par;
  memset(&par, 0, sizeof(par));
  int iPar = 0;

  if( pRec ) xCompare = find_record_compare(pRec, xCompare);
  while( rc==SQLITE_OK ){
    if( iPg ) rc = sqlite3HctFilePageGet(pFile, iPg, &pCsr->pg);
    if( rc==SQLITE_OK ){
      HctDbPageHdr *pHdr = (HctDbPageHdr*)pCsr->pg.aOld;
      int i2 = pHdr->nEntry-1;
      int bExact;
      if( pHdr->nHeight==0 ){
        if( pRec ){
          rc = hctDbIndexSearch(
              pCsr->pDb, pCsr->pg.aOld, xCompare, pRec, &i2, &bExact
          );
          HCT_EXTRA_LOGGING_INDEXSEARCH(
              pCsr->pDb, pCsr->pg.iOldPg, pRec, i2, bExact
          );
        }else{
          i2 = hctDbIntkeyLeafSearch(pCsr->pg.aOld, iKey, &bExact);
        }
        if( bExact==0 ) i2--;
      }else{ 
        if( pRec ){
          HctDbIndexNode *pNode = (HctDbIndexNode*)pCsr->pg.aOld;
          rc = hctDbIndexSearch(
              pCsr->pDb, pCsr->pg.aOld, xCompare, pRec, &i2, &bExact
          );
          HCT_EXTRA_LOGGING_INDEXSEARCH(
              pCsr->pDb, pCsr->pg.iOldPg, pRec, i2, bExact
          );
          i2 -= !bExact;
          assert( i2>=0 );
          iPg = pNode->aEntry[i2].iChildPg;
          assert( iPg );
        }else{
          HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)pCsr->pg.aOld;
          i2 = hctDbIntkeyNodeSearch(pNode, iKey, &bExact);
          assert( i2==pHdr->nEntry || iKey<=pNode->aEntry[i2].iKey );
          assert( i2==pHdr->nEntry || bExact==(iKey==pNode->aEntry[i2].iKey) );
          assert( i2<pHdr->nEntry || bExact==0 );
          i2 -= !bExact;
          iPg = pNode->aEntry[i2].iChildPg;
          assert( iPg );
        }

        /* Avoid following a pointer to an EVICTED page */
        if( pHdr->nHeight!=iHeight ){
          while( sqlite3HctFilePageIsEvicted(pFile, iPg) ){
            i2--;
            if( i2<0 ){
              rc = hctDbCsrGoLeft(pCsr, 0);
              if( rc!=SQLITE_OK ) break;
              i2 = pCsr->iCell;
            }

            bExact = 0;
            if( pRec ){
              HctDbIndexNode *pNode = (HctDbIndexNode*)pCsr->pg.aOld;
              iPg = pNode->aEntry[i2].iChildPg;
            }else{
              HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)pCsr->pg.aOld;
              iPg = pNode->aEntry[i2].iChildPg;
            }
          }
        }
      }


      /* Test if it is necessary to skip to the peer node. */
      if( i2>=0 && i2==pHdr->nEntry-1 && pHdr->iPeerPg!=0 ){
        HctFilePage peer;
        rc = sqlite3HctFilePageGet(pFile, pHdr->iPeerPg, &peer);
        if( rc==SQLITE_OK ){
          int bGotoPeer;
          if( pRec ){
            rc = hctDbCompareFPKey(pCsr->pDb, pRec, peer.aOld, &bGotoPeer);
          }else{
            i64 iFP = hctDbIntkeyFPKey(peer.aOld);
            bGotoPeer = (iFP<=iKey);
          }
          if( bGotoPeer ){
            HCT_EXTRA_LOGGING(pCsr->pDb, (
                "cursor moving to peer (physical page %lld)", peer.iOldPg
            ));
            SWAP(HctFilePage, pCsr->pg, peer);
            sqlite3HctFilePageRelease(&peer);
            assert( pCsr->pg.aOld );
            iPg = 0;
            continue;
          }
          sqlite3HctFilePageRelease(&peer);
        }
      }

      if( pHdr->nHeight==iHeight ){
        pCsr->iCell = i2;
        if( pbExact ) *pbExact = bExact;

        /* If parameter pFP was not NULL and there is a parent page stored
        ** in variable par, try to load the FP key from that page. This
        ** is used when seeking a cursor for writing.  */
        if( pFP && par.aOld ){
          i64 iPeer = ((HctDbPageHdr*)pCsr->pg.aOld)->iPeerPg;
          if( pRec ){
            HctDbIndexNode *pPar = (HctDbIndexNode*)par.aOld;
            if( (iPar+1)<pPar->pg.nEntry 
             && pPar->aEntry[iPar+1].iChildPg==iPeer
            ){
              rc = hctDbLoadRecordFP(pCsr->pDb, par.aOld, iPar+1, pFP);
            }
          }else{
            HctDbIntkeyNode *pPar = (HctDbIntkeyNode*)par.aOld;
            if( (iPar+1)<pPar->pg.nEntry 
             && pPar->aEntry[iPar+1].iChildPg==iPeer
            ){
              pFP->iKey = pPar->aEntry[iPar+1].iKey;
            }
          }
        }

        break;
      }

      if( pFP && pHdr->nHeight==iHeight+1 ){
        par = pCsr->pg;
        iPar = i2;
        memset(&pCsr->pg, 0, sizeof(HctFilePage));
      }else{
        sqlite3HctFilePageRelease(&pCsr->pg);
      }
      assert( rc!=SQLITE_OK || iPg!=0 );
    }
  }

  if( pFP ) sqlite3HctFilePageRelease(&par);
  return rc;
}

void sqlite3HctDbCsrDir(HctDbCsr *pCsr, int eDir){
  pCsr->eDir = eDir;
}

static int hctDbCellOffset(const u8 *aPage, int iCell, u8 *pFlags){
  HctDbPageHdr *pHdr = (HctDbPageHdr*)aPage;
  int iRet;
  if( hctPagetype(pHdr)==HCT_PAGETYPE_INTKEY ){
    HctDbIntkeyEntry *pEntry = &((HctDbIntkeyLeaf*)pHdr)->aEntry[iCell];
    *pFlags = pEntry->flags;
    iRet = pEntry->iOff;
  }else if( hctPageheight(pHdr)>0 ){
    HctDbIndexNodeEntry *pEntry = &((HctDbIndexNode*)pHdr)->aEntry[iCell];
    *pFlags = pEntry->flags;
    iRet = pEntry->iOff;
  }else{
    HctDbIndexEntry *pEntry = &((HctDbIndexLeaf*)pHdr)->aEntry[iCell];
    *pFlags = pEntry->flags;
    iRet = pEntry->iOff;
  }
  return iRet;
}

/*
** Return a pointer to the current page accessed by the cursor. Before
** returning, also set output variable (*piCell) to the index of the
** current cell within the page.
*/
static const u8 *hctDbCsrPageAndCell(HctDbCsr *pCsr, int *piCell){
  const u8 *aPg = 0;
  int iCell = 0;
  if( pCsr->nRange ){
    aPg = pCsr->aRange[pCsr->nRange-1].pg.aOld;
    iCell = pCsr->aRange[pCsr->nRange-1].iCell;
  }else{
    aPg = pCsr->pg.aOld;
    iCell = pCsr->iCell;
  }

  *piCell = iCell;
  return aPg;
}

static u64 hctDbGetTid(const u8 *aPg, int iCell){
  u64 iTid = 0;
  u8 flags;
  int iOff = hctDbCellOffset(aPg, iCell, &flags);
  if( flags & HCTDB_HAS_TID ){
    iTid = hctGetU64(&aPg[iOff]);
  }
  return iTid;
}

static u64 hctDbCsrTid(HctDbCsr *pCsr){
  const u8 *aPg = 0;
  int iCell = 0;
  aPg = hctDbCsrPageAndCell(pCsr, &iCell);
  return hctDbGetTid(aPg, iCell);
}

/*
** If the cursor is open on an index tree, ensure that the UnpackedRecord
** structure is allocated. Return SQLITE_NOMEM if an OOM is encountered
** while attempting to allocate said structure, or SQLITE_OK otherwise.
*/
static int hctDbCsrAllocateUnpacked(HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  if( pCsr->pKeyInfo && pCsr->pRec==0 ){
    pCsr->pRec = sqlite3VdbeAllocUnpackedRecord(pCsr->pKeyInfo);
    if( pCsr->pRec==0 ){
      rc = SQLITE_NOMEM_BKPT;
    }
  }
  return rc;
}

static const u8 *hctDbCsrPageAndCellIdx(
  HctDbCsr *pCsr, 
  int iIdx,
  int *piCell
){
  const u8 *aPg = 0;
  int iCell = 0;

  if( iIdx<0 ){
    aPg = pCsr->pg.aOld;
    iCell = pCsr->iCell;
  }else{
    aPg = pCsr->aRange[iIdx].pg.aOld;
    iCell = pCsr->aRange[iIdx].iCell;
  }
  *piCell = iCell;
  return aPg;
}

static void hctDbFreeKeyContents(HctDbKey *pKey){
  hctDbFreeUnpacked(pKey->pKey);
  sqlite3HctBufferFree(&pKey->buf);
}

static void hctDbCsrAscendRange(HctDbCsr *pCsr){
  HctDbRangeCsr *pLast = &pCsr->aRange[--pCsr->nRange];
  assert( pCsr->nRange>=0 );
  hctDbFreeKeyContents(&pLast->highkey);
  hctDbFreeKeyContents(&pLast->lowkey);
  sqlite3HctFilePageRelease(&pLast->pg);
}

static void hctDbCsrReset(HctDbCsr *pCsr){
  sqlite3HctFilePageRelease(&pCsr->pg);
  pCsr->iCell = -1;
  while( pCsr->nRange>0 ){
    hctDbCsrAscendRange(pCsr);
  }
}

static void hctDbFreeCsr(HctDbCsr *pCsr){
  hctDbCsrReset(pCsr);
  while( pCsr->intkey.pOpList ){
    HctCsrIntkeyOp *pOp = pCsr->intkey.pOpList;
    pCsr->intkey.pOpList = pOp->pNextOp;
    sqlite3_free(pOp);
  }
  while( pCsr->index.pOpList ){
    HctCsrIndexOp *pOp = pCsr->index.pOpList;
    pCsr->index.pOpList = pOp->pNextOp;
    if( pOp->pLast!=pOp->pFirst ){
      sqlite3_free(pOp->pLast);
    }
    sqlite3_free(pOp->pFirst);
    sqlite3_free(pOp);
  }
  if( pCsr->pRec ) sqlite3DbFree(pCsr->pKeyInfo->db, pCsr->pRec);
  sqlite3KeyInfoUnref(pCsr->pKeyInfo);
  sqlite3HctBufferFree(&pCsr->rec);
  sqlite3_free(pCsr->aRange);
  pCsr->aRange = 0;
  pCsr->nRangeAlloc = 0;
  sqlite3_free(pCsr);
}

static void hctDbCsrCleanup(HctDbCsr *pCsr){
  hctDbCsrReset(pCsr);
  assert( pCsr->pKeyInfo || pCsr->pRec==0 );
  if( pCsr->pKeyInfo ){ 
    sqlite3DbFree(pCsr->pKeyInfo->db, pCsr->pRec);
    sqlite3KeyInfoUnref(pCsr->pKeyInfo);
    pCsr->pKeyInfo = 0;
    pCsr->pRec = 0;
  }
  sqlite3_free(pCsr->aRange);
  pCsr->aRange = 0;
  pCsr->nRangeAlloc = 0;
  sqlite3HctBufferFree(&pCsr->rec);
  pCsr->iRoot = 0;
}

static int hctDbCsrScanStart(HctDbCsr *pCsr, UnpackedRecord *pRec, i64 iKey){
  int rc = SQLITE_OK;

  if( hctDbIsConcurrent(pCsr->pDb) && pCsr->bNoscan==0 ){
    if( pCsr->pDb->iTid==0 ){
      if( pCsr->pKeyInfo==0 ){
        HctCsrIntkeyOp *pOp = 0;
        pOp = sqlite3MallocZero(sizeof(HctCsrIntkeyOp));
        if( pOp==0 ){
          rc = SQLITE_NOMEM_BKPT;
        }else{
          assert( pCsr->intkey.pCurrentOp==0 );
          pOp->iFirst = pOp->iLast = iKey;
          pCsr->intkey.pCurrentOp = pOp;
          pOp->iLogical = pCsr->pg.iPg;
          pOp->iPhysical = pCsr->pg.iOldPg;
        }
      }else{
        HctCsrIndexOp *pOp = 0;
        pOp = sqlite3MallocZero(sizeof(HctCsrIndexOp));
        if( pOp==0 ){
          rc = SQLITE_NOMEM_BKPT;
        }else{
          if( pRec ){
            rc = sqlite3HctSerializeRecord(pRec, &pOp->pFirst, &pOp->nFirst);
            pOp->pLast = pOp->pFirst;
            pOp->nLast = pOp->nFirst;
            pOp->iLogical = pCsr->pg.iPg;
            pOp->iPhysical = pCsr->pg.iOldPg;
          }
          assert( pCsr->index.pCurrentOp==0 );
          pCsr->index.pCurrentOp = pOp;
        }
      }
    }
  }

  return rc;
}

static int hctDbCsrScanFinish(HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  if( hctDbIsConcurrent(pCsr->pDb) && pCsr->bNoscan==0 ){
    if( pCsr->pKeyInfo==0 ){
      HctCsrIntkeyOp *pOp = pCsr->intkey.pCurrentOp;
      pCsr->intkey.pCurrentOp = 0;
      if( pOp ){
        HctCsrIntkeyOp *pPrev = pCsr->intkey.pOpList;
  
        if( pCsr->eDir!=BTREE_DIR_NONE ){
          i64 iVal = 0; 
          if( sqlite3HctDbCsrEof(pCsr) ){
            if( pCsr->eDir==BTREE_DIR_FORWARD ){
              iVal = LARGEST_INT64;
            }else{
              iVal = SMALLEST_INT64;
            }
            pOp->iLogical = pOp->iPhysical = 0;
          }else{
            sqlite3HctDbCsrKey(pCsr, &iVal);
            if( pCsr->pg.iPg!=pOp->iLogical ){
              pOp->iLogical = pOp->iPhysical = 0;
            }
          }
  
          if( iVal>=pOp->iFirst ){
            pOp->iLast = iVal;
          }else{
            pOp->iLast = pOp->iFirst;
            pOp->iFirst = iVal;
          }
        }
  
        if( pPrev && pOp->iLast<=pPrev->iLast && pOp->iFirst>=pPrev->iFirst ){
          pPrev->iLogical = pPrev->iPhysical = 0;
          sqlite3_free(pOp);
        }else{
          pOp->pNextOp = pPrev;
          pCsr->intkey.pOpList = pOp;
        }
      }
    }else{
      HctCsrIndexOp *pOp = pCsr->index.pCurrentOp;
      pCsr->index.pCurrentOp = 0;
      if( pOp ){
        if( pCsr->eDir!=BTREE_DIR_NONE ){
          int nKey = 0;
          u8 *aCopy = 0;
          if( !sqlite3HctDbCsrEof(pCsr) ){
            const u8 *aKey = 0;
            rc = sqlite3HctDbCsrData(pCsr, &nKey, &aKey);
            if( rc==SQLITE_OK ){
              aCopy = sqlite3_malloc(nKey);
              if( aCopy==0 ){
                rc = SQLITE_NOMEM_BKPT;
              }else{
                hctMemcpy(aCopy, aKey, nKey);
              }
            }
            if( pCsr->pg.iPg!=pOp->iLogical ){
              pOp->iLogical = pOp->iPhysical = 0;
            }
          }else{
            pOp->iLogical = pOp->iPhysical = 0;
          }
  
          if( pCsr->eDir==BTREE_DIR_FORWARD ){
            pOp->pLast = aCopy;
            pOp->nLast = nKey;
          }else{
            pOp->pFirst = aCopy;
            pOp->nFirst = nKey;
          }
        }
  
        pOp->pNextOp = pCsr->index.pOpList;
        pCsr->index.pOpList = pOp;
      }
    }
  }

  return rc;
}

static int hctDbCsrFirst(HctDbCsr *pCsr){
  int rc = SQLITE_OK;

  /* Starting at the root of the tree structure, follow the left-most 
  ** pointers to find the left-most node in the list of leaves. */
  u32 iPg = pCsr->iRoot;
  HctFile *pFile = pCsr->pDb->pFile;
  HctFilePage pg;
  while( 1 ){
    HctDbPageHdr *pPg;
    rc = sqlite3HctFilePageGet(pFile, iPg, &pg);
    if( rc!=SQLITE_OK ) break;
    pPg = (HctDbPageHdr*)pg.aOld;
    if( pPg->nHeight==0 ){
      break;
    }else if( hctPagetype(pPg)==HCT_PAGETYPE_INTKEY ){
      iPg = ((HctDbIntkeyNode*)pPg)->aEntry[0].iChildPg;
    }else{
      iPg = ((HctDbIndexNode*)pPg)->aEntry[0].iChildPg;
    }
    sqlite3HctFilePageRelease(&pg);
  }
  hctMemcpy(&pCsr->pg, &pg, sizeof(pg));
  if( ((HctDbPageHdr*)pCsr->pg.aOld)->nEntry>0 ){
    pCsr->iCell = 0;
  }else{
    pCsr->iCell = -1;
  }
  return rc;
}

static int hctDbCsrFirstValid(HctDbCsr *pCsr){
  int rc = SQLITE_OK;

  rc = hctDbCsrFirst(pCsr);

  /* Skip forward to the first visible entry, if any. */
  if( rc==SQLITE_OK ){
    pCsr->iCell = -1;
    rc = sqlite3HctDbCsrNext(pCsr);
  }

  return rc;
}

static int hctDbCellPut(
  u8 *aBuf, 
  HctDbCell *pCell,
  int nLocal
){
  int iOff = 0;
  if( pCell->iTid ){
    hctMemcpy(&aBuf[iOff], &pCell->iTid, sizeof(u64));
    iOff += sizeof(u64);
  }
  if( pCell->iRangeTid ){
    hctMemcpy(&aBuf[iOff], &pCell->iRangeTid, sizeof(u64));
    iOff += sizeof(u64);
  }
  if( pCell->iRangeOld ){
    hctMemcpy(&aBuf[iOff], &pCell->iRangeOld, sizeof(u32));
    iOff += sizeof(u32);
  }
  if( pCell->iOvfl ){
    hctMemcpy(&aBuf[iOff], &pCell->iOvfl, sizeof(u32));
    iOff += sizeof(u32);
  }
  hctMemcpy(&aBuf[iOff], pCell->aPayload, nLocal);
  return iOff+nLocal;
}

static void hctDbCellGet(
  HctDatabase *pDb, 
  const u8 *aBuf,
  int flags,
  HctDbCell *pCell
){
  int iOff = 0;
  memset(pCell, 0, sizeof(HctDbCell));

  if( flags & HCTDB_HAS_TID ){
    hctMemcpy(&pCell->iTid, &aBuf[iOff], sizeof(u64));
    iOff += sizeof(u64);
  }
  if( flags & HCTDB_HAS_RANGETID ){
    hctMemcpy(&pCell->iRangeTid, &aBuf[iOff], sizeof(u64));
    iOff += sizeof(u64);
  }
  if( flags & HCTDB_HAS_RANGEOLD ){
    hctMemcpy(&pCell->iRangeOld, &aBuf[iOff], sizeof(u32));
    iOff += sizeof(u32);
  }
  if( flags & HCTDB_HAS_OVFL ){
    hctMemcpy(&pCell->iOvfl, &aBuf[iOff], sizeof(u32));
    iOff += sizeof(u32);
  }

  pCell->aPayload = &aBuf[iOff];
}

static void hctDbCellGetByIdx(
  HctDatabase *pDb, 
  const u8 *aPg,
  int iIdx,
  HctDbCell *pCell
){
  HctDbIndexEntry *p = hctDbEntryEntry(aPg, iIdx);
  hctDbCellGet(pDb, &aPg[p->iOff], p->flags, pCell);
}

static u8 hctDbCellToFlags(HctDbCell *pCell){
  u8 flags = 0;
  if( pCell->iTid ) flags |= HCTDB_HAS_TID;
  if( pCell->iOvfl ) flags |= HCTDB_HAS_OVFL;
  if( pCell->iRangeTid ) flags |= HCTDB_HAS_RANGETID;
  if( pCell->iRangeOld ) flags |= HCTDB_HAS_RANGEOLD;
  return flags;
}

typedef struct HctRangePtr HctRangePtr;
struct HctRangePtr {
  u64 iRangeTid;
  u64 iFollowTid;
  u32 iOld;
};

/*
** This function is called when a reader encounters an old-range pointer
** with associated TID value iRangeTid. It returns true if the pointer
** should be followed, or false otherwise.
**
** If the data items on the linked page should be merged in to the cursor
** results, output parameter (*pbMerge) is set to true before returning.
** This happens if the transaction with TID iRangeTid is not visible to
** the reader. Or, if the only reason to follow the pointer is in order
** to follow other pointers on the indicated page, (*pbMerge) is set to
** false. This happens when iRangeTid is included in the transaction, but
** there exists one or more transactions with TID values smaller than 
** iRangeTid that are not.
*/
static int hctDbFollowRangeOld(
  HctDatabase *pDb, 
  HctRangePtr *pPtr, 
  int *pbMerge
){
  int bRet = 0;
  int bMerge = 0;
  u64 iRangeTidValue = (pPtr->iRangeTid & HCT_TID_MASK);

  if( pPtr->iOld==0 ){
    *pbMerge = 0;
    return 0;
  }

  /* HctDatabase.iTid is set when writing, validating or rolling back a
  ** transaction. When writing or validating, old-ranges created by this
  ** transaction should not be merge in, even if they are followed. But, when
  ** doing rollback, they must be merged in (to find the old data).  */

  i64 iDoNotMergeTid = (pDb->eMode==HCT_MODE_VALIDATE) ? 0 : pDb->iTid;
  assert( pDb->eMode!=HCT_MODE_ROLLBACK );

  if( iRangeTidValue>pDb->iLocalMinTid ){
    bRet = 1;
    if( iDoNotMergeTid!=iRangeTidValue ){
      bMerge = (0==hctDbTidIsVisible(pDb, pPtr->iRangeTid, 0));
    }
  }else if( (pPtr->iFollowTid & HCT_TID_MASK)>pDb->iLocalMinTid ){
    bRet = 1;
    assert( bMerge==0 );
  }

  HCT_EXTRA_LOGGING(pDb, (
        "%sfollowing, %smerging for history pointer "
        "(iRangeTid=%lld, iFollowTid=%lld, iOldPg=%lld)",
        (bRet ? "" : "not "), (bMerge ? "" : "not "),
        iRangeTidValue, (pPtr->iFollowTid & HCT_TID_MASK), pPtr->iOld
  ));

  /* TODO: Optimizations - this is only required when committing in LEADER
  ** mode. And can be omitted if hctDbTidIsVisible() was called above. */
  if( bMerge==0 ){
    u64 eDummy = 0;
    u64 iCid = hctDbTMapLookup(pDb, iRangeTidValue, &eDummy);
    pDb->iReqSnapshotId = MAX(pDb->iReqSnapshotId, iCid);
  }

  *pbMerge = bMerge;
  assert( bRet==0 || iRangeTidValue>0 );
  return bRet;
}

static int hctDbCsrExtendRange(HctDbCsr *pCsr){
  if( pCsr->nRange==pCsr->nRangeAlloc ){
    int nNew = pCsr->nRangeAlloc ? pCsr->nRangeAlloc*2 : 16;
    HctDbRangeCsr *aNew = 0;

    aNew = (HctDbRangeCsr*)sqlite3_realloc(
        pCsr->aRange, nNew*sizeof(HctDbRangeCsr)
    );
    if( aNew==0 ) return SQLITE_NOMEM_BKPT;
    pCsr->nRangeAlloc = nNew;
    pCsr->aRange = aNew;
  }

  memset(&pCsr->aRange[pCsr->nRange], 0, sizeof(HctDbRangeCsr));
  pCsr->nRange++;
  return SQLITE_OK;
}

static int hctDbCompareKey2(
  KeyInfo *pKeyInfo, 
  UnpackedRecord *pKey1,
  i64 iKey1,
  HctDbKey *p2
){
  int ret = 0;
  if( pKeyInfo ){
    int ii = 0;
    int n1, n2;

    n1 = pKey1->nField;
    n2 = p2->pKey->nField;

    for(ii=0; ret==0 && ii<MIN(n1, n2); ii++){
      CollSeq *pColl = pKeyInfo->aColl[ii];
      ret = sqlite3MemCompare(&pKey1->aMem[ii], &p2->pKey->aMem[ii], pColl);
      if( pKeyInfo->aSortFlags[ii] & KEYINFO_ORDER_DESC ) ret = -ret;
    }
    if( ret==0 ){
      assert( pKey1->default_rc>=-1 && pKey1->default_rc<=1 );
      assert( p2->pKey->default_rc>=-1 && p2->pKey->default_rc<=1 );

      if( n2<n1 ){
        ret = p2->pKey->default_rc;
      }else if( n1<n2 ){
        ret = (pKey1->default_rc * -1);
      }else{
        ret = p2->pKey->default_rc - pKey1->default_rc;
      }
    }
    if( ret==0 ){
      if( n1<n2 ){
        ret = -1;
      }else if( n1>n2 ){
        ret = +1;
      }
    }
  }else{
    if( iKey1<p2->iKey ){
      ret = -1;
    }else if( iKey1>p2->iKey ){
      ret = +1;
    }
  }
  return ret;
}

/*
** Compare the key values in p1 and p2, returning a value less than, equal
** to, or greater than zero if p1 is respectively less than, equal to or
** greater than p2. i.e.
**
**   res = (*p1) - (*p2)
*/
static int hctDbCompareKey(
  KeyInfo *pKeyInfo, 
  HctDbKey *p1, 
  HctDbKey *p2,
  int bNullIsBiggest
){
  if( pKeyInfo ){
    if( p1->pKey==0 ) return (bNullIsBiggest ? 1 : -1);
    if( p2->pKey==0 ) return (bNullIsBiggest ? -1 : 1);
  }
  return hctDbCompareKey2(pKeyInfo, p1->pKey, p1->iKey, p2);
}

static int hctDbCopyKey(HctDbKey *p1, HctDbKey *p2){
  if( p2->pKey ){
    int ii;
    int bNew = 0;
    if( p1->pKey==0 || p1->pKey->nField<p2->pKey->nField ){
      int rc = SQLITE_OK;
      hctDbFreeUnpacked(p1->pKey);
      p1->pKey = hctDbAllocateUnpacked(&rc, p2->pKey->pKeyInfo);
      if( rc!=SQLITE_OK ) return rc;
      bNew = 1;
      p1->pKey->default_rc = 0;
    }
    for(ii=0; ii<p2->pKey->nField; ii++){
      Mem *pFrom = &p2->pKey->aMem[ii];
      Mem *pTo = &p1->pKey->aMem[ii];
      if( bNew ) sqlite3VdbeMemInit(pTo, pFrom->db, 0);
      sqlite3VdbeMemShallowCopy(pTo, pFrom, MEM_Static);
    }
    p1->pKey->nField = p2->pKey->nField;
    p1->pKey->default_rc = p2->pKey->default_rc;
  }else{
    p1->iKey = p2->iKey;
  }
  return SQLITE_OK;
}

static void hctDbDecrementKey(HctDbKey *pKey){
  if( pKey->pKey ){
    /* TODO: Is this correct? Or should it be +1? Or...? */
    pKey->pKey->default_rc = +1;
  }else if( pKey->iKey!=SMALLEST_INT64 ){
    pKey->iKey--;
  }
}

static void hctDbIncrementKey(HctDbKey *pKey){
  if( pKey->pKey ){
    /* TODO: Is this correct? Or should it be +1? Or...? */
    pKey->pKey->default_rc = -1;
  }else if( pKey->iKey!=LARGEST_INT64 ){
    pKey->iKey++;
  }
}

static void hctDbCsrDescendRange(
  int *pRc, 
  HctDbCsr *pCsr,
  u64 iRangeTid,
  u32 iRangeOld,
  int bMerge
){
  int rc = *pRc;

  if( rc==SQLITE_OK ){
    rc = hctDbCsrExtendRange(pCsr);
  }

  if( rc==SQLITE_OK ){
    HctDbRangeCsr *pNew = &pCsr->aRange[pCsr->nRange-1];
    assert( bMerge==HCT_RANGE_FOLLOW || bMerge==HCT_RANGE_MERGE );

    pNew->eRange = bMerge;
    pNew->iRangeTid = iRangeTid;
    rc = hctDbGetPhysical(pCsr->pDb, iRangeOld, &pNew->pg);

    if( rc==SQLITE_OK ){
      int iPar = pCsr->nRange-2;
      int iPCell = 0;
      const u8 *aParent = hctDbCsrPageAndCellIdx(pCsr, iPar, &iPCell);
      const HctDbPageHdr *pPar = (HctDbPageHdr*)aParent;
      int bSeen = 0;

      /* Figure out the upper limit key for the scan of this page */ 
      if( hctPagetype(aParent)==HCT_PAGETYPE_HISTORY ){
        if( iPCell==0 && pPar->nEntry>1 ){
          const HctDbHistoryFan *pFan = (const HctDbHistoryFan*)aParent;
          hctDbGetKeyFromPage(&rc, pCsr->pDb, pCsr->pKeyInfo, 
              0, pFan->aPgOld1[0], pFan->iSplit0, &pNew->highkey
          );
          bSeen = 1;
        }
      }else{
        if( iPCell==(pPar->nEntry-1) ){
          if( pPar->iPeerPg ){
            hctDbGetKeyFromPage(&rc, pCsr->pDb, pCsr->pKeyInfo, 
                1, pPar->iPeerPg, 0, &pNew->highkey
            );
            bSeen = 1;
          }
        }else{
          hctDbGetKey(&rc,
              pCsr->pDb, pCsr->pKeyInfo, 0, aParent, iPCell+1, &pNew->highkey
          );
          bSeen = 1;
        }
      }

      if( bSeen==0 ){
        if( iPar>=0 ){
          hctDbCopyKey(&pNew->highkey, &pNew[-1].highkey);
        }else{
          pNew->highkey.iKey = LARGEST_INT64;
          assert( pNew->highkey.pKey==0 );
        }
      }else if( iPar>=0 ){
        /* The 'highkey' should be the minimum of pNew->highkey and the
        ** parent highkey.  highkey = MIN(highkey, parent.highkey); */
        HctDbKey *pPKey = &pNew[-1].highkey;
        if( hctDbCompareKey(pCsr->pKeyInfo, &pNew->highkey, pPKey, 1)>0 ){
          hctDbCopyKey(&pNew->highkey, pPKey);
        }
      }

      /* Figure the lower limit key for the scan of this page */
      pNew->lowkey.iKey = SMALLEST_INT64;
      if( hctPagetype(aParent)==HCT_PAGETYPE_HISTORY ){
        if( iPCell>0 ){
          const HctDbHistoryFan *pFan = (const HctDbHistoryFan*)aParent;
          hctDbGetKeyFromPage(&rc, pCsr->pDb, pCsr->pKeyInfo, 
              0, pFan->aPgOld1[0], pFan->iSplit0, &pNew->lowkey
          );
          // hctDbDecrementKey(&pNew->lowkey);
        }else{
          hctDbCopyKey(&pNew->lowkey, &pNew[-1].lowkey);
        }
      }else{
        HctDbCell pcell;
        hctDbGetKey(&rc, 
            pCsr->pDb, pCsr->pKeyInfo, 0, aParent, iPCell, &pNew->lowkey
        );
        hctDbCellGetByIdx(pCsr->pDb, aParent, iPCell, &pcell);
        if( hctDbTidIsVisible(pCsr->pDb, pcell.iTid, 0) ){
          hctDbIncrementKey(&pNew->lowkey);
        }
      }
      if( iPar>=0 ){
        /* The 'lowkey' should be the maximum of pNew->lowkey and the
        ** parent lowkey.  lowkey = MAX(lowkey, parent.lowkey); */
        HctDbKey *pPKey = &pNew[-1].lowkey;
        if( hctDbCompareKey(pCsr->pKeyInfo, &pNew->lowkey, pPKey, 0)<0 ){
          hctDbCopyKey(&pNew->lowkey, pPKey);
        }
      }

      if( rc==SQLITE_OK && hctPagetype(pNew->pg.aOld)==HCT_PAGETYPE_HISTORY){
        pNew->eRange = HCT_RANGE_FAN;
      }
    }
  }

  *pRc = rc;
}

static void hctDbGetRange(
  const u8 *aPg,
  int iCell,
  HctRangePtr *pPtr
){
  if( iCell<0 ){
    memset(pPtr, 0, sizeof(*pPtr));
  }else if( hctPagetype(aPg)==HCT_PAGETYPE_HISTORY ){
    HctDbHistoryFan *pFan = (HctDbHistoryFan*)aPg;
    if( iCell==0 ){
      pPtr->iRangeTid = pFan->iRangeTid0;
      pPtr->iFollowTid = pFan->iFollowTid0;
      pPtr->iOld = pFan->pgOld0;
    }else{
      pPtr->iFollowTid = pPtr->iRangeTid = pFan->iRangeTid1;
      pPtr->iOld = pFan->aPgOld1[iCell-1];
    }
  }else{
    HctDbCell cell;
    hctDbCellGetByIdx(0, aPg, iCell, &cell);
    pPtr->iFollowTid = pPtr->iRangeTid = cell.iRangeTid;
    pPtr->iOld = cell.iRangeOld;
  }

  assert( (pPtr->iFollowTid & HCT_TID_MASK)>=(pPtr->iRangeTid & HCT_TID_MASK) );
}

static void hctDbCsrGetRange(
  HctDbCsr *pCsr,
  HctRangePtr *pPtr
){
  const u8 *aPg = 0;
  int iCell = 0;
  aPg = hctDbCsrPageAndCell(pCsr, &iCell);
  assert( ((HctDbPageHdr*)aPg)->nEntry>iCell );
  assert( ((HctDbPageHdr*)aPg)->nHeight==0 );
  hctDbGetRange(aPg, iCell, pPtr);
}

/*
** Return true if the entry that the cursor currently points to is visible
** to the current transaction, or false otherwise.
*/
static int hctDbCurrentIsVisible(HctDbCsr *pCsr){
  int iCell = 0;
  HctDbIndexEntry *p;
  const u8 *aPg = hctDbCsrPageAndCell(pCsr, &iCell);
  u64 iTid = 0;

  if( pCsr->pKeyInfo ){
    p = &((HctDbIndexLeaf*)aPg)->aEntry[iCell];
  }else{
    p = (HctDbIndexEntry*)&((HctDbIntkeyLeaf*)aPg)->aEntry[iCell];
  }
  if( (p->flags & HCTDB_HAS_TID)==0 ) return 1;
  hctMemcpy(&iTid, &aPg[p->iOff], sizeof(u64));
  if( pCsr->pDb->iTid==iTid && pCsr->pDb->eMode==HCT_MODE_VALIDATE ) return 1;

  return hctDbTidIsVisible(pCsr->pDb, iTid, pCsr->bNosnap);
}

/*
** Search leaf page aPg[] for a specified key. 
**
** If the key is present in the page, set output variable (*piPos) to
** the index of the key in the page, and (*pbExact) to true.
**
** Or, if the key is not present in the page, set output variable (*piPos)
** to the index of the SMALLEST KEY THAT IS LARGER THAN IKEY/PKEY, and
** set (*pbExact) to false.
*/
static int hctDbLeafSearch(
  HctDatabase *pDb,
  const u8 *aPg,
  i64 iKey,
  UnpackedRecord *pKey,
  int *piPos,
  int *pbExact
){
  if( hctPagetype(aPg)==HCT_PAGETYPE_INDEX ){
    if( pKey==0 ){
      assert( 0 );
      *piPos = hctPagenentry(aPg);
      *piPos = 0;
      *pbExact = 0;
    }else{
      int rc = hctDbIndexSearch(pDb, aPg, 0, pKey, piPos, pbExact);
      if( rc ) return rc;
    }
  }else{
    *piPos = hctDbIntkeyLeafSearch(aPg, iKey, pbExact);
  }
  return SQLITE_OK;
}

static int hctDbCsrRollbackDescend(
  HctDbCsr *pCsr,                 /* Cursor to seek */
  UnpackedRecord *pRec,           /* Key for index/without rowid tables */
  i64 iKey,                       /* Key for intkey tables */
  int *pbExact
){
  HctDatabase *pDb = pCsr->pDb;
  int rc = SQLITE_OK;

  *pbExact = 0;

  assert( pDb->eMode==HCT_MODE_ROLLBACK );
  while( 1 ){
    HctRangePtr ptr;
    HctDbRangeCsr *p = 0;

    hctDbCsrGetRange(pCsr, &ptr);

    if( (ptr.iFollowTid & HCT_TID_MASK)<pDb->iTid ) break;

    rc = hctDbCsrExtendRange(pCsr);
    if( rc==SQLITE_OK ){
      p = &pCsr->aRange[pCsr->nRange-1];
      rc = hctDbGetPhysical(pDb, ptr.iOld, &p->pg);
    }
    if( rc==SQLITE_OK ){
      p->iRangeTid = ptr.iRangeTid & HCT_TID_MASK;
      if( hctPagetype(p->pg.aOld)==HCT_PAGETYPE_HISTORY ){
        p->eRange = HCT_RANGE_FAN;
        p->iCell = hctDbFanSearch(&rc, pDb, p->pg.aOld, pRec, iKey);
      }else{
        int bExact = 0;
        rc = hctDbLeafSearch(
            pDb, p->pg.aOld, iKey, pRec, &p->iCell, &bExact
        );
        if( bExact ){
          if( hctDbCsrTid(pCsr)!=pDb->iTid ){
            *pbExact = 1;
            break;
          }
        }
        if( !bExact ) p->iCell--;
        if( p->iCell<0 ) break;
      }
    }
  }

  return rc;
}


static int hctDbCheckForOverlap(
  KeyInfo *pKeyInfo, 
  HctDbKey *pLow, 
  HctDbKey *pHigh
){
  if( pKeyInfo ){
    if( pLow->pKey==0 || pHigh->pKey==0 ) return 0;
    return hctDbCompareKey2(pKeyInfo, pLow->pKey, 0, pHigh)>=0;
  }
  return (pLow->iKey>=pHigh->iKey);
}

static int hctDbKeyIsNull(KeyInfo *pKeyInfo, HctDbKey *pKey){
  return (pKeyInfo && pKey->pKey==0);
}


static void hctExtraLoggingNewRangeCsr(
  const char *zFunc, 
  int iLine,
  HctDbCsr *pCsr,
  int bCreate
){
  char *zLow = 0;
  char *zHigh = 0;
  char *zMsg = 0;
  HctDbRangeCsr *p = &pCsr->aRange[pCsr->nRange-1];

  if( pCsr->pKeyInfo ){
    if( p->lowkey.pKey ) zLow = sqlite3HctDbUnpackedToText(p->lowkey.pKey);
    if( p->highkey.pKey ) zHigh = sqlite3HctDbUnpackedToText(p->highkey.pKey);
  }else{
    zLow = sqlite3_mprintf("[%lld]", p->lowkey.iKey);
    zHigh = sqlite3_mprintf("[%lld]", p->highkey.iKey);
  }

  if( bCreate ){
    zMsg = sqlite3_mprintf(
        "%p: created range cursor %d lowkey=%s highkey=%s (pg=%lld cell=%d)"
        " eRange=%s"
        , pCsr->pDb, pCsr->nRange - 1, zLow, zHigh, p->pg.iOldPg, p->iCell,
        p->eRange==HCT_RANGE_FAN ? "FAN" :
        p->eRange==HCT_RANGE_MERGE ? "MERGE" :
        p->eRange==HCT_RANGE_FOLLOW ? "FOLLOW" : "???"
    );
  }else{
    zMsg = sqlite3_mprintf(
        "%p: not creating range cursor %d - (%s)>=(%s)",
        pCsr->pDb, pCsr->nRange - 1, zLow, zHigh
    );
  }
  sqlite3_free(zLow);
  sqlite3_free(zHigh);
  hctExtraLogging(zFunc, iLine, pCsr->pDb, zMsg);
}

#define HCT_EXTRA_LOGGING_NEWRANGECSR(pCsr)                  \
  if( pCsr->pDb->pConfig->bHctExtraLogging ){                \
    hctExtraLoggingNewRangeCsr(__func__, __LINE__, pCsr, 1); \
  }

#define HCT_EXTRA_LOGGING_NO_NEWRANGECSR(pCsr)               \
  if( pCsr->pDb->pConfig->bHctExtraLogging ){                \
    hctExtraLoggingNewRangeCsr(__func__, __LINE__, pCsr, 0); \
  }

static int hctDbCompareCellKey(
  int *pRc,
  HctDatabase *pDb,
  const u8 *aPg1,
  int iCell1,
  HctDbKey *pKey2,
  int iDefaultRet
);
static int hctDbCsrPrev(HctDbCsr *pCsr);

static int hctDbCsrSeekAndDescend(
  HctDbCsr *pCsr,                 /* Cursor to seek */
  UnpackedRecord *pRec,           /* Key for index/without rowid tables */
  i64 iKey,                       /* Key for intkey tables */
  int bStopOnExact,               /* Stop on exact match, even if not visible */
  int *pbExact
){
  int rc = SQLITE_OK;
  int bExact = 0;
  HctDatabase *pDb = pCsr->pDb;

  /* This function is never called when writing to the database. Or while
  ** doing rollback. But it is called during transaction preparation (iTid==0),
  ** and validation (eMode==HCT_MODE_VALIDATE). */
  assert( pDb->eMode==HCT_MODE_VALIDATE || pDb->iTid==0 );

  rc = hctDbCsrSeek(pCsr, 0, 0, 0, pRec, iKey, &bExact);
  if( bExact && bStopOnExact ){
    *pbExact = 1;
    return rc;
  }

  while( rc==SQLITE_OK && (0==bExact || 0==hctDbCurrentIsVisible(pCsr)) ){
    HctRangePtr ptr;
    int bMerge = 0;

    /* Check if there is a range pointer that we should follow */
    hctDbCsrGetRange(pCsr, &ptr);
    if( hctDbFollowRangeOld(pDb, &ptr, &bMerge) ){
      hctDbCsrDescendRange(&rc, pCsr, ptr.iRangeTid, ptr.iOld, bMerge);
      if( rc==SQLITE_OK ){
        HctDbRangeCsr *p = &pCsr->aRange[pCsr->nRange-1];

        if( hctDbKeyIsNull(pCsr->pKeyInfo, &p->lowkey)==0
         && hctDbCompareKey2(pCsr->pKeyInfo, pRec, iKey, &p->lowkey)<0 
        ){
          p->iCell = -1;
          break;
        }

        if( p->eRange==HCT_RANGE_FAN ){
          p->iCell = hctDbFanSearch(&rc, pDb, p->pg.aOld, pRec, iKey);
          bExact = 0;
          HCT_EXTRA_LOGGING_NEWRANGECSR(pCsr);
        }else{
          rc = hctDbLeafSearch(pDb, p->pg.aOld, iKey, pRec, &p->iCell, &bExact);
          HCT_EXTRA_LOGGING_NEWRANGECSR(pCsr);
          if( rc!=SQLITE_OK ) break;
          if( bExact==0 ){
            p->iCell--;
          }else if( bStopOnExact ){
            *pbExact = 1;
            return SQLITE_OK;
          }
          if( p->iCell<0 ) break;
          if( p->eRange==HCT_RANGE_FOLLOW ) bExact = 0;
        }
      }
    }else{
      break;
    }
  }

  if( rc==SQLITE_OK && pCsr->nRange>0 ){
    HctDbRangeCsr *p = &pCsr->aRange[pCsr->nRange-1];
    if( p->iCell<0
     || p->eRange!=HCT_RANGE_MERGE 
     || hctDbCompareCellKey(&rc, pDb, p->pg.aOld, p->iCell, &p->lowkey,1)<0
     || hctDbCompareCellKey(&rc, pDb, p->pg.aOld, p->iCell, &p->highkey,-1)>0
    ){
      rc = hctDbCsrPrev(pCsr);
      bExact = 0;
    }
  }

  *pbExact = bExact;
  return rc;
}

static char *hctDbDebugKey(UnpackedRecord *pRec, i64 iKey){
  char *zKey = 0;
  if( pRec ){
    zKey = sqlite3HctDbUnpackedToText(pRec);
  }else{
    zKey = sqlite3HctMprintf("[%lld]", iKey);
  }
  return zKey;
}

static char *hctDbCsrDebugCsrPos(HctDbCsr *pCsr){
  char *zMsg = 0;
  if( pCsr->iCell<0 ){
    zMsg = sqlite3_mprintf("EOF");
  }else{
    char *zKey = 0;

    int bRange = 0;
    int eRange = 0;
    int iCell = -1;
    i64 iPg = -1;

    if( pCsr->nRange ){
      HctDbRangeCsr *p = &pCsr->aRange[pCsr->nRange-1];
      bRange = 1;
      eRange = p->eRange;
      iCell = p->iCell;
      iPg = p->pg.iOldPg;
      if( eRange==HCT_RANGE_FAN ){
        zKey = sqlite3HctMprintf("<fan-page>");
      }else if( p->iCell>=hctPagenentry(p->pg.aOld) ){
        zKey = sqlite3HctMprintf("<range-cursor eof>");
      }
    }else{
      iCell = pCsr->iCell;
      iPg = pCsr->pg.iOldPg;
    }

    if( zKey==0 ){
      if( pCsr->pKeyInfo ){
        int nData = 0;
        const u8 *aData = 0;
        sqlite3HctDbCsrData(pCsr, &nData, &aData);
        zKey = sqlite3HctDbRecordToText(0, aData, nData);
      }else{
        i64 iKey = 0;
        sqlite3HctDbCsrKey(pCsr, &iKey);
        zKey = sqlite3_mprintf("%lld", iKey);
      }
    }

    zMsg = sqlite3_mprintf("%scell %d, phys. page %lld, key (%s)",
        bRange ? "(range cursor) " : "", iCell, iPg, zKey
    );
    sqlite3_free(zKey);
  }

  return zMsg;
}


/*
** An integer is written into *pRes which is the result of
** comparing the key with the entry to which the cursor is 
** pointing.  The meaning of the integer written into
** *pRes is as follows:
**
**     *pRes<0      The cursor is left pointing at an entry that
**                  is smaller than iKey/pRec or if the table is empty
**                  and the cursor is therefore left point to nothing.
**
**     *pRes==0     The cursor is left pointing at an entry that
**                  exactly matches iKey/pRec.
**
**     *pRes>0      The cursor is left pointing at an entry that
**                  is larger than iKey/pRec.
*/
int sqlite3HctDbCsrSeek(
  HctDbCsr *pCsr,                 /* Cursor to seek */
  UnpackedRecord *pRec,           /* Key for index tables */
  i64 iKey,                       /* Key for intkey tables */
  int *pRes                       /* Result of seek (see above) */
){
  int rc = SQLITE_OK;
  int bExact;

  /* Should not be called while committing, validating or during rollback. */
  assert( pCsr->pDb->eMode==HCT_MODE_NORMAL );
  assert( pCsr->pDb->iTid==0 );

  rc = hctDbCsrScanFinish(pCsr);
  hctDbCsrReset(pCsr);

  if( rc==SQLITE_OK ){
    rc = hctDbCsrSeekAndDescend(pCsr, pRec, iKey, 0, &bExact);
    HCT_EXTRA_LOGGING(pCsr->pDb, ("SeekAndDescend for (%z) lands at (%z)",
        hctDbDebugKey(pRec, iKey), hctDbCsrDebugCsrPos(pCsr)
    ));
  }
  if( rc==SQLITE_OK ){
    rc = hctDbCsrScanStart(pCsr, pRec, iKey);
  }

  /* The main cursor now points to the largest entry less than or equal 
  ** to the supplied key (pRec or iKey). If the supplied key is smaller 
  ** than all entries in the table, then pCsr->iCell is set to -1. */
  if( rc==SQLITE_OK ){
    if( pCsr->iCell<0 ){
      /* The supplied key is smaller than all keys in the table. If the cursor
      ** is BTREE_DIR_REVERSE or NONE, then leave it as it is at EOF.
      ** Otherwise, if the cursor is BTREE_DIR_FORWARD, attempt to move 
      ** it to the first valid entry. */
      if( pCsr->eDir==BTREE_DIR_FORWARD ){
        rc = hctDbCsrFirstValid(pCsr);
        *pRes = sqlite3HctDbCsrEof(pCsr) ? -1 : +1;
      }else{
        *pRes = -1;
      }
    }else{
    
      if( pCsr->eDir==BTREE_DIR_NONE && bExact==0 ){
        *pRes = -1;
      }else if( 0==hctDbCurrentIsVisible(pCsr) ){
        switch( pCsr->eDir ){
          case BTREE_DIR_FORWARD:
            *pRes = 1;
            rc = sqlite3HctDbCsrNext(pCsr);
            *pRes = sqlite3HctDbCsrEof(pCsr) ? -1 : +1;
            break;
          case BTREE_DIR_REVERSE:
            rc = sqlite3HctDbCsrPrev(pCsr);
            /* Either the cursor is is now at EOF or it points to a key 
            ** smaller than iKey/pRec. Either way, set (*pRes) to -ve. */
            *pRes = -1;
            break;
          default: assert( pCsr->eDir==BTREE_DIR_NONE );
            hctDbCsrReset(pCsr);
            *pRes = -1;
            break;
        }
      }else{
        *pRes = (bExact ? 0 : -1); 
      }
    }
  }

  HCT_EXTRA_LOGGING(pCsr->pDb, ("Seek for (%z) finally lands at (%z)",
    hctDbDebugKey(pRec, iKey), hctDbCsrDebugCsrPos(pCsr)
  ));

  return rc;
}

void sqlite3HctDbSetSavePhysical(
  HctDatabase *pDb,
  int (*xSave)(void*, i64 iPhys),
  void *pSave
){
  pDb->xSavePhysical = xSave;
  pDb->pSavePhysical = pSave;
}

int sqlite3HctDbCsrRollbackSeek(
  HctDbCsr *pCsr,                 /* Cursor to seek */
  UnpackedRecord *pRec,           /* Key for index tables */
  i64 iKey,                       /* Key for intkey tables */
  int *pOp                        /* Required rollback op */
){
  HctDatabase *pDb = pCsr->pDb;
  int rc = SQLITE_OK;
  int bLeafHit = 0;

  hctDbCsrReset(pCsr);

  /* At this point pDb->bRollback is set and pDb->iTid is set to the TID
  ** of the transaction being rolled back. There are four possibilities:
  **
  **   1) The key was written by transaction pDb->iTid and there was no 
  **      previous entry. 
  **
  **   2) The key was written by transaction pDb->iTid and there is a
  **      previous entry to restore.
  **
  **   3) The key was deleted by transaction pDb->iTid.
  **
  **   4) None of the above. No rollback required.
  */

  rc = hctDbCsrSeek(pCsr, 0, 0, 0, pRec, iKey, &bLeafHit);
  if( rc==SQLITE_OK && bLeafHit ){
    u64 iLeafTid = hctDbCsrTid(pCsr);
    if( iLeafTid!=pDb->iTid ){
      /* Last thing that happened to this key was that it was written by
      ** a transaction other than the one being rolled back. So this
      ** is case (4) - set (*pOp) to 0 and return early.  */
      *pOp = 0;
      return SQLITE_OK;
    }

    /* Either case (1) or (2). If this is case (2), (*pOp) will be changed
    ** to +1 below.  */
    *pOp = -1;
  }

  if( rc==SQLITE_OK && pCsr->iCell>=0 ){
    int bHistHit = 0;
    rc = hctDbCsrRollbackDescend(pCsr, pRec, iKey, &bHistHit);
    assert( bHistHit==0 || pCsr->nRange>0 );
    if( bHistHit && pDb->iTid==pCsr->aRange[pCsr->nRange-1].iRangeTid ){
      /* Either case (3) or case (2). */
      *pOp = +1;
    }
  }

  return rc;
}

static void hctDbCsrInit(
  HctDatabase *pDb, 
  u32 iRoot, 
  KeyInfo *pKeyInfo,
  HctDbCsr *pCsr
){
  memset(pCsr, 0, sizeof(HctDbCsr));
  pCsr->pDb = pDb;
  pCsr->iRoot = iRoot;
  if( pKeyInfo ){
    pCsr->pKeyInfo = sqlite3KeyInfoRef(pKeyInfo);
  }
}



/*
** Return the size of the local part of a nData byte record stored on
** an intkey leaf page.
*/
#if 0
static int hctDbLocalSize(HctDatabase *pDb, int nData){
  int nOther = sizeof(HctDbIntkeyLeaf) + sizeof(HctDbIntkeyEntry) + 12;
  if( nData<=(pDb->pgsz-nOther) ){
    return nData;
  }
  assert( !"todo" );
  return 0;
}
#endif

#if 0
static i64 hctDbIntkeyGetKey(u8 *aPg, int ii){
  HctDbIntkeyLeaf *p = (HctDbIntkeyLeaf*)aPg;
  return p->aEntry[ii].iKey;
}
#endif



/*
** Return the maximum number of entries that fit on an intkey internal
** node if the database page size is as specified by the only parameter.
*/
static int hctDbMaxCellsPerIntkeyNode(int pgsz){
  return (pgsz - sizeof(HctDbIntkeyNode)) / sizeof(HctDbIntkeyNodeEntry);
}
static int hctDbMinCellsPerIntkeyNode(int pgsz){
  return (pgsz - sizeof(HctDbIntkeyNode)) / (3*sizeof(HctDbIntkeyNodeEntry));
}

static void hctDbIrrevocablyEvictPage(HctDatabase *pDb, HctDbWriter *p);

static int hctDbOverflowArrayFree(HctDatabase *pDb, HctDbOverflowArray *p){
  int ii = 0;
  int rc = SQLITE_OK;

  for(ii=0; rc==SQLITE_OK && ii<p->nEntry; ii++){
    u32 pgno = p->aOvfl[ii].pgno;
    int nRem = p->aOvfl[ii].nOvfl;
    while( 1 ){
      HctFilePage pg;
      sqlite3HctFileClearPhysInUse(pDb->pFile, pgno, 0);
      nRem--;
      if( nRem==0 ) break;
      rc = hctDbGetPhysical(pDb, pgno, &pg);
      assert( rc==SQLITE_OK );
      pgno = ((HctDbPageHdr*)pg.aOld)->iPeerPg;
      sqlite3HctFilePageRelease(&pg);
    }
  }

  return rc;
}

#ifdef SQLITE_DEBUG
/*
** Do some assert() statements to check that:
**
**   * the pages in discardpg[] are sorted according to key.
*/
static void assert_writer_is_ok(HctDatabase *pDb, HctDbWriter *p){
  int ii;
  HctBuffer buf = {0,0,0};
  UnpackedRecord *pRec = 0;

  for(ii=1; ii<p->discardpg.nPg; ii++){
    u8 *a1 = p->discardpg.aPg[ii-1].aOld;
    u8 *a2 = p->discardpg.aPg[ii].aOld;

    if( hctPagetype(a1)==HCT_PAGETYPE_INTKEY ){
      i64 i1 = hctDbIntkeyFPKey(a1);
      i64 i2 = hctDbIntkeyFPKey(a2);
      assert( i2>i1 );
    }else{
      int nData = 0;
      const u8 *aData = 0;
      int rc = hctDbLoadRecord(pDb, &buf, a1, 0, &nData, &aData);
      if( rc==SQLITE_OK && pRec==0 ){
        pRec = sqlite3VdbeAllocUnpackedRecord(p->writecsr.pKeyInfo);
        if( pRec==0 ){
          rc = SQLITE_NOMEM;
        }
      }
      if( rc==SQLITE_OK ){
        int bGe = 555;
        sqlite3VdbeRecordUnpack(p->writecsr.pKeyInfo, nData, aData, pRec);
        rc = hctDbCompareFPKey(pDb, pRec, a2, &bGe);
        assert( rc!=SQLITE_OK || bGe==0 );
      }
    }
  }

  sqlite3HctBufferFree(&buf);
  hctDbFreeUnpacked(pRec);
}
#else /* if !SQLITE_DEBUG */
# define assert_writer_is_ok(pDb, p)
#endif

/*
** Cleanup the writer object passed as the first argument.
*/
static void hctDbWriterCleanup(HctDatabase *pDb, HctDbWriter *p, int bRevert){

  if( p->bDoCleanup ){
    int ii;

    sqlite3HctFileDebugPrint(pDb->pFile, 
        "writer cleanup height=%d bRevert=%d\n", p->iHeight, bRevert
    );

    assert_writer_is_ok(pDb, p);

    sqlite3HctBufferFree(&p->fp.buf);
    memset(&p->fp, 0, sizeof(p->fp));

    /* sqlite3HctFilePageUnwrite(&p->fanpg); */
    sqlite3HctFilePageRelease(&p->fanpg);

    /* If not reverting, mark the overflow chains in p->delOvfl as free */
    if( bRevert==0 ){
      hctDbOverflowArrayFree(pDb, &p->delOvfl);
    }else{
      hctDbOverflowArrayFree(pDb, &p->insOvfl);
    }
    sqlite3_free(p->delOvfl.aOvfl);
    sqlite3_free(p->insOvfl.aOvfl);
    memset(&p->delOvfl, 0, sizeof(p->delOvfl));
    memset(&p->insOvfl, 0, sizeof(p->insOvfl));

    for(ii=0; ii<p->writepg.nPg; ii++){
      HctFilePage *pPg = &p->writepg.aPg[ii];
      if( bRevert ){
        if( pPg->aNew ){
          sqlite3HctFilePageUnwrite(pPg);
        }else if( ii>0 ){
          sqlite3HctFileClearInUse(pPg, 1);
        }
      }
      sqlite3HctFilePageRelease(pPg);
    }
    hctDbPageArrayReset(&p->writepg);

    for(ii=0; ii<p->discardpg.nPg; ii++){
      if( bRevert && pDb->pConfig->nTryBeforeUnevict>1 ){
        sqlite3HctFilePageUnevict(&p->discardpg.aPg[ii]);
      }
      sqlite3HctFilePageRelease(&p->discardpg.aPg[ii]);
    }

    hctDbPageArrayReset(&p->discardpg);
    p->fp.iKey = 0;
    p->fp.aKey = 0;

    if( p->iEvictLockedPgno ){
      assert( p->writecsr.iRoot );
      p->nEvictLocked++;
      if( p->nEvictLocked>=pDb->pConfig->nTryBeforeUnevict ){
        p->nEvictLocked = -1;
        hctDbIrrevocablyEvictPage(pDb, p);
        p->nEvictLocked = 0;
      }
    }else{
      p->nEvictLocked = 0;
    }
    p->iEvictLockedPgno = 0;
    p->bAppend = 0;

    /* Free/zero various buffers and caches */
    hctDbCsrCleanup(&p->writecsr);
    p->bDoCleanup = 0;
  }
}

static int hctDbInsert(
  HctDatabase *pDb,
  HctDbWriter *p,
  u32 iRoot,
  UnpackedRecord *pRec,           /* The key value for index tables */
  i64 iKey,                       /* For intkey tables, the key value */
  u32 iChildPg,                   /* For internal node ops, the child pgno */
  int bDel,                       /* True for a delete operation */
  int nData, const u8 *aData      /* Record/key to insert */
);

typedef struct HctDbWriterOrigin HctDbWriterOrigin;
struct HctDbWriterOrigin {
  u8 bDiscard;                    /* 1 for aDiscard[], 0 for aWritePg[] */
  i16 iPg;                        /* Index of page in array*/
};

static int hctdbWriterSortFPKeys(
  HctDatabase *pDb, 
  int eType,
  HctDbWriter *p, 
  HctDbWriterOrigin *aOrigin      /* Populate this array */
){
  int iDiscard = 0;
  int iWP = 1;
  int iOut = 0;
  int rc = SQLITE_OK;

  assert( eType==HCT_PAGETYPE_INDEX || eType==HCT_PAGETYPE_INTKEY );

  while( iDiscard<p->discardpg.nPg || iWP<p->writepg.nPg ){
    if( iDiscard>=p->discardpg.nPg ){
      aOrigin[iOut].bDiscard = 0;
      aOrigin[iOut].iPg = iWP++;
      iOut++;
    }
    else if( iWP>=p->writepg.nPg ){
      aOrigin[iOut].bDiscard = 1;
      aOrigin[iOut].iPg = iDiscard++;
      iOut++;
    }else{
      int bDiscard = 0;
      const u8 *aD = p->discardpg.aPg[iDiscard].aOld;
      const u8 *aW = p->writepg.aPg[iWP].aOld;

      if( eType==HCT_PAGETYPE_INTKEY ){
        i64 i1 = hctDbIntkeyFPKey(aD);
        i64 i2 = hctDbIntkeyFPKey(aW);
        bDiscard = (i1<=i2);
      }else{
        int nFP = 0;
        const u8 *aFP = 0;
        UnpackedRecord *pRec = p->writecsr.pRec;
        rc = hctDbLoadRecord(pDb, &p->writecsr.rec, aW, 0, &nFP, &aFP);
        if( rc!=SQLITE_OK ) break;
        sqlite3VdbeRecordUnpack(p->writecsr.pKeyInfo, nFP, aFP, pRec);
        rc = hctDbCompareFPKey(pDb, pRec, aD, &bDiscard);
        if( rc!=SQLITE_OK ) break;
      }

      aOrigin[iOut].bDiscard = bDiscard;
      if( bDiscard ){
        aOrigin[iOut].iPg = iDiscard++;
      }else{
        aOrigin[iOut].iPg = iWP++;
      }
      iOut++;
    }
  }

  return rc;
}

#if 0
/*
**
*/
static int hctDbTruncateRecord(
  HctBuffer *pBuf,                /* Buffer to use for storage space */
  KeyInfo *pKeyInfo,              /* Description of index */
  int *pnFP,                      /* IN/OUT: Size of record */
  const u8 **aFP                  /* IN/OUT: Pointer to record */
){
}
#endif

/*
** This is a wrapper around:
**
**   sqlite3HctFilePageEvict(pPg, 0);
**
** If the call fails with SQLITE_LOCKED because page pPg has been evicted,
** HctDbWriter.iEvictLockedPgno is set to the logical page number of pPg.
*/
static int hctDbFilePageEvict(HctDbWriter *p, HctFilePage *pPg){
  int rc = sqlite3HctFilePageEvict(pPg, 0);
  if( rc==SQLITE_LOCKED && sqlite3HctFilePageIsEvicted(pPg->pFile, pPg->iPg) ){
    p->iEvictLockedPgno = pPg->iPg;
  }
  return rc;
}

static int hctDbFilePageCommit(HctDbWriter *p, HctFilePage *pPg){
  int rc = sqlite3HctFilePageCommit(pPg);
  if( rc==SQLITE_LOCKED && sqlite3HctFilePageIsEvicted(pPg->pFile, pPg->iPg) ){
    p->iEvictLockedPgno = pPg->iPg;
  }
  return rc;
}

static int hctDbMigrateReinsertKeys(HctDatabase *pDb, HctDbWriter *p);

static int hctDbInsertFlushWrite(HctDatabase *pDb, HctDbWriter *p){
  int rc = SQLITE_OK;
  int ii;
  int eType = hctPagetype(p->writepg.aPg[0].aNew);
  HctFilePage root;
  int bUnevict = 0;

  memset(&root, 0, sizeof(root));

  rc = hctDbMigrateReinsertKeys(pDb, p);

#ifdef SQLITE_DEBUG
  for(ii=1; rc==SQLITE_OK && ii<p->writepg.nPg; ii++){
    u32 iPeer = ((HctDbPageHdr*)p->writepg.aPg[ii-1].aNew)->iPeerPg;
    assert( p->writepg.aPg[ii].iPg==iPeer );
  }
#endif

  /* Test if this is a split of a root page of the tree. */
  if( rc==SQLITE_OK 
   && p->writepg.nPg>1 
   && p->writepg.aPg[0].iPg==p->writecsr.iRoot 
  ){
    HctFilePage *pPg0 = &p->writepg.aPg[0];
    hctMemcpy(&root, pPg0, sizeof(HctFilePage));
    memset(pPg0, 0, sizeof(HctFilePage));
    rc = sqlite3HctFilePageNew(pDb->pFile, pPg0);
    if( rc==SQLITE_OK ){
      hctMemcpy(pPg0->aNew, root.aNew, pDb->pgsz);
      hctDbRootPageInit(eType==HCT_PAGETYPE_INDEX,
          hctPageheight(root.aNew)+1, pPg0->iPg, root.aNew, pDb->pgsz
      );
    }
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3HctFilePageRelease(&p->fanpg);
  }

  /* Loop through the set of pages to write out. They must be
  ** written in reverse order - so that page aWritePg[0] is written
  ** last. */
  assert( p->writepg.nPg>0 );
  for(ii=p->writepg.nPg-1; rc==SQLITE_OK && ii>=0; ii--){
    rc = hctDbFilePageCommit(p, &p->writepg.aPg[ii]);
  }

  /* If there is one, write the new root page to disk */
  if( rc==SQLITE_OK && root.iPg ){
    rc = hctDbFilePageCommit(p, &root);
    sqlite3HctFilePageRelease(&root);
  }

  if( rc!=SQLITE_OK ){
    bUnevict = 1;
  }

  /* If there is more than one page in the writepg array, or more than
  ** zero in the discardpg array, then the parent list must be updated.
  ** This block does that.  */
  if( (p->writepg.nPg>1 || p->discardpg.nPg>0) && rc==SQLITE_OK ){
    const u32 iRoot = p->writecsr.iRoot;
    const int nOrig = p->discardpg.nPg + p->writepg.nPg - 1;
    HctDbWriterOrigin aStatic[6];
    HctDbWriterOrigin *aDyn = 0;
    HctDbWriterOrigin *aOrig = aStatic;
    HctBuffer buf;
    HctDbWriter wr;
    int iOrig = 0;

    memset(&buf, 0, sizeof(buf));
    memset(&wr, 0, sizeof(wr));
    hctDbPageArrayReset(&wr.writepg);
    hctDbPageArrayReset(&wr.discardpg);

    if( nOrig>ArraySize(aStatic) ){
      int nByte = sizeof(HctDbWriterOrigin) * nOrig;
      aOrig = aDyn = (HctDbWriterOrigin*)sqlite3HctMallocRc(&rc, nByte);
    }

    if( rc==SQLITE_OK ){
      wr.iHeight = p->iHeight + 1;
      rc = hctDbCsrAllocateUnpacked(&p->writecsr);
    }

    if( rc==SQLITE_OK ){
      rc = hctdbWriterSortFPKeys(pDb, eType, p, aOrig);
    }

    if( rc==SQLITE_OK ){
      do {
        assert( rc==SQLITE_OK || rc==SQLITE_LOCKED );
        rc = SQLITE_OK;

        while( iOrig<nOrig && rc==SQLITE_OK ){
          HctDbWriterOrigin *pOrig = &aOrig[iOrig++];
          HctFilePage *pPg;
          i64 iKey = 0;
          const u8 *aFP = 0;
          int nFP = 0;
          UnpackedRecord *pRec = 0;
          int bDel = pOrig->bDiscard;

          pPg = &(bDel ? p->discardpg.aPg : p->writepg.aPg)[pOrig->iPg];
          if( eType==HCT_PAGETYPE_INTKEY ){
            iKey = hctDbIntkeyFPKey(pPg->aOld);
          }else{
            rc = hctDbLoadRecord(pDb, &buf, pPg->aOld, 0, &nFP, &aFP);
            if( rc!=SQLITE_OK ) break;
            pRec = p->writecsr.pRec;
            sqlite3VdbeRecordUnpack(p->writecsr.pKeyInfo, nFP, aFP, pRec);
            sqlite3HctDbRecordTrim(pRec);
          }

          rc = hctDbInsert(
              pDb, &wr, iRoot, pRec, iKey, pPg->iPg, bDel, nFP, aFP
          );
        }

        if( rc==SQLITE_OK ){
          rc = hctDbInsertFlushWrite(pDb, &wr);
        }
        if( rc==SQLITE_LOCKED ){
          assert( iOrig>=wr.nWriteKey );
          iOrig -= wr.nWriteKey;
          pDb->nCasFail++;
          pDb->stats.nInternalRetry++;
        }
        hctDbWriterCleanup(pDb, &wr, (rc!=SQLITE_OK));
        wr.nWriteKey = 0;

      }while( rc==SQLITE_LOCKED );
    }

    sqlite3HctBufferFree(&buf);
    sqlite3_free(aDyn);
  }

  if( rc==SQLITE_OK ){
    for(ii=0; ii<p->discardpg.nPg; ii++){
      sqlite3HctFileClearInUse(&p->discardpg.aPg[ii], 0);
    }
  }

  HCT_EXTRA_WR_LOGGING(("%p: flush done rc=%d", pDb, rc));

  /* Clean up the Writer object */
  hctDbWriterCleanup(pDb, p, bUnevict);
  return rc;
}

void sqlite3HctDbRollbackMode(HctDatabase *pDb, int eRollback){
  assert( eRollback==0 || pDb->eMode==HCT_MODE_NORMAL );
  pDb->pa.nWriteKey = 0;
  pDb->eMode = eRollback ? HCT_MODE_ROLLBACK : HCT_MODE_NORMAL;
  if( eRollback>1 ){
    memset(&pDb->pa, 0, sizeof(pDb->pa));
    hctDbPageArrayReset(&pDb->pa.writepg);
    hctDbPageArrayReset(&pDb->pa.discardpg);

    /* During recovery rollback the connection should read the latest 
    ** version of the db - no exceptions. Set these two to the largest 
    ** possible values to ensure that this happens.  */
    pDb->iSnapshotId = LARGEST_TID-1;
    pDb->iLocalMinTid = LARGEST_TID-1;
  }
}

i64 sqlite3HctDbNCasFail(HctDatabase *pDb){
  return pDb->nCasFail;
}

#if 0
static HctDbIntkeyEntry *hctDbIntkeyEntry(u8 *aPg, int iCell){
  return iCell<0 ? 0 : (&((HctDbIntkeyLeaf*)aPg)->aEntry[iCell]);
}
#endif

int sqlite3HctDbInsertFlush(HctDatabase *pDb, int *pnRetry){
  int rc = SQLITE_OK;
  if( pDb->pa.writepg.nPg ){
    rc = hctDbInsertFlushWrite(pDb, &pDb->pa);
    if( rc==SQLITE_LOCKED ){
      *pnRetry = pDb->pa.nWriteKey;
      rc = SQLITE_OK;
      pDb->nCasFail++;
      HCT_EXTRA_WR_LOGGING(("%p: retrying %d ops", pDb, *pnRetry));
    }else{
      *pnRetry = 0;
    }
#if 0
  {
    sqlite3HctFileDebugPrint(pDb->pFile, 
        "%p: %s sqlite3HctDbInsertFlush() -> %d (nRetry=%d)\n",
        pDb, (pDb->eMode==HCT_MODE_ROLLBACK ? "RB" : "  "), rc, *pnRetry
    );
    fflush(stdout);
  }
#endif
    pDb->pa.nWriteKey = 0;
  }
  return rc;
}

/*
** If pRec is not NULL, it contains an unpacked index key. Compare this key
** with the write-fp-key in pDb->pa.aWriteFpKey. Return true if pRec is greater
** than or equal to the write-fp-key.
**
** Or, if pRec is NULL, iKey is the key and it is compared to 
** pDb->iWriteFpKey.
*/
static int hctDbTestWriteFpKey(
  HctDbWriter *p, 
  RecordCompare xCompare,
  UnpackedRecord *pRec, 
  i64 iKey
){
  if( pRec ){
    int r;
    if( p->fp.aKey==0 ){
      r = 1;
    }else{
      assert( p->fp.iKey>0 );
      r = xCompare(p->fp.iKey, p->fp.aKey, pRec);
    }
    return (r <= 0);
  }
  return iKey>=p->fp.iKey;
}

static int hctDbSetWriteFpKey(HctDatabase *pDb, HctDbWriter *p){
  int rc = SQLITE_OK;
  HctDbPageHdr *pHdr = (HctDbPageHdr*)p->writepg.aPg[p->writepg.nPg-1].aNew;

  p->fp.aKey = 0;
  p->fp.iKey = 0;

  if( pHdr->iPeerPg==0 ){
    if( hctPagetype(pHdr)==HCT_PAGETYPE_INTKEY ){
      p->fp.iKey = LARGEST_INT64;
    }
  }else{
    HctFilePage pg;
    rc = sqlite3HctFilePageGet(pDb->pFile, pHdr->iPeerPg, &pg);
    if( rc==SQLITE_OK ){
      if( hctPagetype(pHdr)==HCT_PAGETYPE_INTKEY ){
        p->fp.iKey = hctDbIntkeyFPKey(pg.aOld);
      }else{
        rc = hctDbLoadRecordFP(pDb, pg.aOld, 0, &p->fp);
      }
      sqlite3HctFilePageRelease(&pg);
    }
  }

  return rc;
}

/*
** Buffer aTarget[] contains a page that contains variable length keys
** (i.e. an intkey leaf or an index leaf or node). This function returns
** the offset of the aEntry[] array in aTarget. Before doing so, it sets
** output variable (*pszEntry) to the sizeof(aEntry[0]).
*/
static int hctDbEntryArrayDim(const u8 *aTarget, int *pszEntry){
  int eType = hctPagetype(aTarget);
  int nHeight = hctPageheight(aTarget);
  int nRet;

  assert( eType==HCT_PAGETYPE_INTKEY || eType==HCT_PAGETYPE_INDEX );
  assert( eType==HCT_PAGETYPE_INDEX || nHeight==0 );
  if( eType==HCT_PAGETYPE_INTKEY ){
    *pszEntry = sizeof(HctDbIntkeyEntry);
    nRet = sizeof(HctDbIntkeyLeaf);
  }else if( nHeight==0 ){
    *pszEntry = sizeof(HctDbIndexEntry);
    nRet = sizeof(HctDbIndexLeaf);
  }else{
    *pszEntry = sizeof(HctDbIndexNodeEntry);
    nRet = sizeof(HctDbIndexNode);
  }

  return nRet;
}

static int hctIsVarRecords(const u8 *aTarget){
  int eType = hctPagetype(aTarget);
  int nHeight = hctPageheight(aTarget);
  return (nHeight==0 || eType==HCT_PAGETYPE_INDEX);
}

#ifdef SQLITE_DEBUG

static void print_out_page(const char *zCaption, const u8 *aData, int nData){
  HctDbPageHdr *pPg = (HctDbPageHdr*)aData;

  if( hctPagetype(pPg)==HCT_PAGETYPE_INTKEY && pPg->nHeight==0 ){
    HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)pPg;
    char *zPrint = 0;
    const char *zSep = "";
    int ii;

    for(ii=0; ii<pLeaf->pg.nEntry; ii++){
      HctDbIntkeyEntry *pEntry = &pLeaf->aEntry[ii];
      zPrint = sqlite3_mprintf("%z%s(k=%lld f=%.2x %d..%d)", zPrint, zSep, 
          pEntry->iKey, pEntry->flags,
          pEntry->iOff, pEntry->iOff+ hctDbIntkeyEntrySize(pEntry, nData)
      );
      zSep = ",";
    }

    printf("%s: nFreeGap=%d nFreeBytes=%d (intkey leaf)\n", zCaption,
      pLeaf->hdr.nFreeGap,
      pLeaf->hdr.nFreeBytes
    );
    printf("%s: %s\n", zCaption, zPrint);
    sqlite3_free(zPrint);
  }

  if( hctPagetype(pPg)==HCT_PAGETYPE_INDEX && pPg->nHeight==0 ){
    HctDbIndexLeaf *pLeaf = (HctDbIndexLeaf*)pPg;
    char *zPrint = 0;
    const char *zSep = "";
    int ii;

    for(ii=0; ii<pLeaf->pg.nEntry; ii++){
      HctDbIndexEntry *pEntry = &pLeaf->aEntry[ii];
      zPrint = sqlite3_mprintf("%z%s(%d..%d)", zPrint, zSep, 
          pEntry->iOff, pEntry->iOff + hctDbIndexEntrySize(pEntry, nData)
      );
      zSep = ",";
    }

    printf("%s: nFreeGap=%d nFreeBytes=%d (index leaf)\n", zCaption,
      pLeaf->hdr.nFreeGap,
      pLeaf->hdr.nFreeBytes
    );
    printf("%s: %s\n", zCaption, zPrint);
    fflush(stdout);
    sqlite3_free(zPrint);
  }


}

#define assert_or_print(E)                         \
  if( !(E) ){                                      \
    print_out_page("page", aData, nData);          \
    assert( E );                                   \
  }

typedef struct VarCellReader VarCellReader;
struct VarCellReader {
  const u8 *aData;
  int nData;
  int szEntry;
  int iEntry0;
};

static void hctVCRInit(VarCellReader *p, const u8 *aData, int nData){
  p->aData = aData;
  p->nData = nData;
  p->iEntry0 = hctDbEntryArrayDim(aData, &p->szEntry);
}

static int hctVCRFindCell(VarCellReader *p, int iCell, int *pnByte){
  HctDbIndexNodeEntry *pEntry;

  pEntry = (HctDbIndexNodeEntry*)&p->aData[p->iEntry0 + iCell*p->szEntry];
  *pnByte = hctDbLocalsize(p->aData, p->nData, pEntry->nSize)
    + hctDbOffset(0, pEntry->flags);

  return pEntry->iOff;
}

static void assert_page_is_ok(const u8 *aData, int nData){

  if( aData && hctIsVarRecords(aData) ){
    HctDbIndexNode *p = (HctDbIndexNode*)aData;
    VarCellReader vcr;
    int iEnd = nData;
    int iStart = 0;
    int nRecTotal = 0;
    int ii = 0;
    int nFreeExpect;

    hctVCRInit(&vcr, aData, nData);
    for(ii=0; ii<p->pg.nEntry; ii++){
      int sz = 0;
      int iOff = hctVCRFindCell(&vcr, ii, &sz);
      if( iOff ){
        assert_or_print( (iOff+sz)<=nData );
        iEnd = MIN(iEnd, iOff);
        nRecTotal += sz;
      }else{
        assert( sz==0 && ii==0 );
      }
    }

    iStart = vcr.iEntry0 + vcr.szEntry * p->pg.nEntry;
    nFreeExpect = nData - (iStart + nRecTotal);

    assert_or_print( p->hdr.nFreeGap==(iEnd - iStart) );
    assert_or_print( p->hdr.nFreeBytes==nFreeExpect);
  }

}
#else
# define assert_page_is_ok(x,y)
#endif

#ifdef SQLITE_DEBUG
static void assert_all_pages_ok(HctDatabase *pDb, HctDbWriter *p){
  int ii;
  return;
  for(ii=0; ii<p->writepg.nPg; ii++){
    u8 *aPg = p->writepg.aPg[ii].aNew;
    assert( aPg[0]!=0x00 );
    assert( hctIsVarRecords(aPg) );
    assert_page_is_ok(aPg, pDb->pgsz);
    assert( ii==p->writepg.nPg-1 
         || ((HctDbPageHdr*)aPg)->iPeerPg==p->writepg.aPg[ii+1].iPg
    );
  }
}
static void assert_all_pages_nonempty(HctDatabase *pDb, HctDbWriter *p){
  return;
  if( p->writepg.nPg>1 ){
    int ii;
    for(ii=0; ii<p->writepg.nPg; ii++){
      HctDbPageHdr *pPg = (HctDbPageHdr*)p->writepg.aPg[ii].aNew;
      assert( pPg->nEntry>0 );
    }
  }
}
#else
# define assert_all_pages_ok(x,y)
# define assert_all_pages_nonempty(x,y)
#endif


/*
** HOW INSERT/DELETE OPERATIONS WORK:
**
** 1. If the page array is not empty, flush it to disk if required. It 
**    should be flushed to disk if either:
**
**      a) the key being written (specified by iKey/pRec) is greater or
**         equal to the FP key to the right of the page array (stored
**         in HctDbWriter.iWriteFpKey/aWriteFpKey).
**
**      b) there are more than HCTDB_MAX_DIRTY pages in the array.
**
** 2. If the page array is empty, either because it was flushed to disk
**    in (1) or because it was empty when this function was called, seek
**    the write-cursor (HctDbWriter.writecsr) to the key being written.
**    The page the cursor seeks to becomes the first page of the page
**    array.
**
** 3. Locate within the page array the page into which the new key 
**    or delete-key should be inserted. There are three possible outcomes:
**
**      i)   the new key may just be written to the page.
**
**      ii)  the new key fits on the page, but leaves it underfull. In this
**           context, "underfull" means that the total amount of free space
**           on the page is less than or equal to (pgsz*2/3).
**
**      iii) the new key does not fit on the page.
**
** In cases (ii) or (iii), first ensure that that the page has two peers in
** the page array (unless there are fewer than three pages in the list, in
** which case the entire list should be loaded). Then redistribute the keys
** between the minimum number of pages, discarding or adding nodes as
** required.
*/

/*
** Insert nPg new pages at index iPg into the write-array of the HctDbWriter
** passed as the second argument and link them into the list.
*/
static int hctDbExtendWriteArray(
  HctDatabase *pDb,
  HctDbWriter *p,
  int iPg,
  int nPg
){
  int rc = SQLITE_OK;
  int ii;

  assert( iPg>0 );
  assert( (p->writepg.nPg+nPg)>0 );
  assert( p->writepg.nPg>0 );

  /* Add any new pages required */
  for(ii=iPg; rc==SQLITE_OK && ii<iPg+nPg; ii++){
    // assert( p->writepg.nPg<ArraySize(p->writepg.aPg) );
    assert( ii>0 );
    if( ii<p->writepg.nPg ){
      int nByte = sizeof(HctFilePage) * (p->writepg.nPg-ii);
      memmove(&p->writepg.aPg[ii+1], &p->writepg.aPg[ii], nByte);
    }
    p->writepg.nPg++;
    memset(&p->writepg.aPg[ii], 0, sizeof(HctFilePage));
    rc = sqlite3HctFilePageNew(pDb->pFile, &p->writepg.aPg[ii]);
    if( rc==SQLITE_OK ){
      HctDbPageHdr *pNew = (HctDbPageHdr*)p->writepg.aPg[ii].aNew;
      HctDbPageHdr *pPrev = (HctDbPageHdr*)p->writepg.aPg[ii-1].aNew;
      memset(pNew, 0, sizeof(HctDbPageHdr));
      pNew->hdrFlags = hctPagetype(pPrev);
      pNew->nHeight = pPrev->nHeight;
      pNew->iPeerPg = pPrev->iPeerPg;
      pPrev->iPeerPg = p->writepg.aPg[ii].iPg;
    }
  }

  /* Remove pages that are not required */
  for(ii=nPg; ii<0; ii++){
    HctDbPageHdr *pPrev = (HctDbPageHdr*)(p->writepg.aPg[iPg-1].aNew);
    HctDbPageHdr *pRem = (HctDbPageHdr*)(p->writepg.aPg[iPg].aNew);
    pPrev->iPeerPg = pRem->iPeerPg;
    assert( p->writepg.nPg>1 );
    p->writepg.nPg--;

    assert( iPg!=0 );
    assert( p->writepg.aPg[iPg].aOld==0 );
    sqlite3HctFilePageUnwrite(&p->writepg.aPg[iPg]);

    if( iPg!=p->writepg.nPg ){
      int nByte = sizeof(HctFilePage) * (p->writepg.nPg-iPg);
      assert( nByte>0 );
      memmove(&p->writepg.aPg[iPg], &p->writepg.aPg[iPg+1], nByte);
    }
  }

  return rc;
}

/*
** Cursor pCsr must point to an index b-tree, not an intkey b-tree, and
** it must be valid. This function loads the current record from the
** database and sets *ppRec to point to the decoded version. SQLITE_OK
** is returned if successful, or else an SQLite error code if something
** goes wrong.
*/
int sqlite3HctDbCsrLoadAndDecode(HctDbCsr *pCsr, UnpackedRecord **ppRec){
  int nData = 0;
  const u8 *aData = 0;
  const u8 *aPg = 0;
  int iCell = 0;
  int rc = SQLITE_OK;

  assert( pCsr->pg.aNew==0 );
  aPg = hctDbCsrPageAndCell(pCsr, &iCell);

  rc = hctDbLoadRecord(pCsr->pDb, &pCsr->rec, aPg, iCell, &nData, &aData);
  if( rc==SQLITE_OK ){
    rc = hctDbCsrAllocateUnpacked(pCsr);
  }

  if( rc==SQLITE_OK ){
    *ppRec = pCsr->pRec;
    sqlite3VdbeRecordUnpack(pCsr->pKeyInfo, nData, aData, pCsr->pRec);
    assert( pCsr->pRec->nField>0 );
  }

  return rc;
}

/*
**
*/
static int hctDbFindLhsPeer(
  HctDatabase *pDb,
  HctDbWriter *p,
  HctFilePage *pPg,
  HctFilePage *pOut
){
  HctDbCsr csr;
  u8 *aLeft = pPg->aNew ? pPg->aNew : pPg->aOld;
  int rc = SQLITE_OK;

  hctDbCsrInit(pDb, p->writecsr.iRoot, p->writecsr.pKeyInfo, &csr);
  if( hctPagetype(aLeft)==HCT_PAGETYPE_INTKEY ){
    i64 iKey = hctDbIntkeyFPKey(aLeft);
    assert( iKey!=SMALLEST_INT64 );
    rc = hctDbCsrSeek(&csr, 0, p->iHeight, 0, 0, iKey-1, 0);
  }else{
    UnpackedRecord *pRec = 0;
    HctBuffer buf;
    int nData = 0;
    const u8 *aData = 0;
    memset(&buf, 0, sizeof(buf));
    rc = hctDbLoadRecord(pDb, &buf, aLeft, 0, &nData, &aData);
    if( rc==SQLITE_OK ){
      rc = hctDbCsrAllocateUnpacked(&p->writecsr);
    }
    if( rc==SQLITE_OK ){
      pRec = p->writecsr.pRec;
      sqlite3VdbeRecordUnpack(p->writecsr.pKeyInfo, nData, aData, pRec);
      sqlite3HctDbRecordTrim(pRec);
      pRec->default_rc = 1;
      rc = hctDbCsrSeek(&csr, 0, p->iHeight, 0, pRec, 0, 0);
      pRec->default_rc = 0;

      assert( csr.pg.iPg!=pPg->iPg );
    }
    sqlite3HctBufferFree(&buf);
  }

  if( rc==SQLITE_OK 
   && ((HctDbPageHdr*)csr.pg.aOld)->iPeerPg==pPg->iPg
  ){
    *pOut = csr.pg;
  }else{
    memset(pOut, 0, sizeof(HctFilePage));
    rc = SQLITE_LOCKED_ERR(pPg->iPg, "peer");
  }
  hctDbCsrCleanup(&csr);

  return rc;
}

static void hctDbIrrevocablyEvictPage(HctDatabase *pDb, HctDbWriter *p){
  int rc = SQLITE_OK;
  u32 iLocked = p->iEvictLockedPgno;
  int bDone = 0;

  KeyInfo *pKeyInfo = sqlite3KeyInfoRef(p->writecsr.pKeyInfo);
  u32 iRoot = p->writecsr.iRoot;

  sqlite3HctFileDebugPrint(pDb->pFile,"BEGIN forced eviction of %d\n", iLocked);
  
  do {
    HctFilePage pg1;
    HctFilePage pg0;
    memset(&pg1, 0, sizeof(pg1));
    if( p->writecsr.iRoot==0 ){
      hctDbCsrInit(pDb, iRoot, pKeyInfo, &p->writecsr);
    }
    rc = sqlite3HctFilePageGet(pDb->pFile, iLocked, &pg1);
    while( rc==SQLITE_OK ){
      memset(&pg0, 0, sizeof(pg0));
      rc = hctDbFindLhsPeer(pDb, p, &pg1, &pg0);
      if( rc ) break;
      if( 0==sqlite3HctFilePageIsEvicted(pg0.pFile, pg0.iPg) ) break;
      sqlite3HctFilePageRelease(&pg1);
      pg1 = pg0;
      memset(&pg0, 0, sizeof(pg0));
    }

    if( rc==SQLITE_OK ){
      bDone = (pg1.iPg==iLocked);
      sqlite3HctFileDebugPrint(
          pDb->pFile, "forcing write of %d->%d\n", pg0.iPg, pg1.iPg
      ); 

      rc = sqlite3HctFilePageEvict(&pg1, 1);
      if( rc==SQLITE_OK ){
        rc = sqlite3HctFilePageWrite(&pg0);
      }
      if( rc==SQLITE_OK ){
        hctMemcpy(pg0.aNew, pg0.aOld, pDb->pgsz);
      }
      if( rc==SQLITE_OK ){
        p->writepg.aPg[0] = pg0;
        p->writepg.nPg = 1;
        rc = hctDbExtendWriteArray(pDb, p, 1, 1);
      }
      if( rc==SQLITE_OK ){
        hctMemcpy(p->writepg.aPg[1].aNew, pg1.aOld, pDb->pgsz);
        p->discardpg.aPg[0] = pg1;
        p->discardpg.nPg = 1;
      }

      p->bDoCleanup = 1;
      if( rc==SQLITE_OK ){
        rc = hctDbInsertFlushWrite(pDb, p);
      }else{
        hctDbWriterCleanup(pDb, p, 1);
      }

    }else{
      sqlite3HctFilePageRelease(&pg0);
      sqlite3HctFilePageRelease(&pg1);
    }
  }while( rc==SQLITE_OK && bDone==0 );

  sqlite3KeyInfoUnref(pKeyInfo);
  sqlite3HctFileDebugPrint(pDb->pFile,"END forced eviction of %d\n", iLocked);
}

/*
**
*/
static int hctDbLoadPeers(HctDatabase *pDb, HctDbWriter *p, int *piPg){
  int rc = SQLITE_OK;
  int iPg = *piPg;

  if( p->writepg.nPg<3 ){
    HctFilePage *pLeft = &p->writepg.aPg[0];

    if( p->writepg.nPg==1 && 0==hctIsLeftmost(pLeft->aNew) ){
      HctFilePage *pCopy = 0;
      assert( iPg==0 );

      /* First, evict the page currently in p->writepg.aPg[0]. If we 
      ** successfully evict the page here, then of course no other thread
      ** can - which guarantees that the seek operation below really does
      ** find the left-hand peer (assuming the db is not corrupt).  */
      rc = hctDbFilePageEvict(p, pLeft);

      /* Assuming the LOGICAL_EVICTED flag was successfully set, seek 
      ** cursor csr to the leaf page immediately to the left of pLeft. */
      if( rc==SQLITE_OK ){
        if( p->discardpg.nPg>0 ){
          int nMove = p->discardpg.nPg * sizeof(HctFilePage);
          memmove(&p->discardpg.aPg[1], &p->discardpg.aPg[0], nMove);
        }
        pCopy = &p->discardpg.aPg[0];
        p->discardpg.nPg++;
        *pCopy = *pLeft;
        rc = hctDbFindLhsPeer(pDb, p, pCopy, pLeft);
      }
      if( rc==SQLITE_OK ){
        assert( ((HctDbPageHdr*)pLeft->aOld)->iPeerPg==pCopy->iPg );
        rc = sqlite3HctFilePageWrite(pLeft);
      }
      
      if( rc==SQLITE_OK ){
        hctMemcpy(pLeft->aNew, pLeft->aOld, pDb->pgsz);
        rc = hctDbExtendWriteArray(pDb, p, 1, 1);
      }
      if( rc==SQLITE_OK ){
        hctMemcpy(p->writepg.aPg[1].aNew, pCopy->aNew, pDb->pgsz);
        sqlite3HctFilePageUnwrite(pCopy);
        *piPg = 1;
      }
    }

    if( rc==SQLITE_OK ){
      HctDbPageHdr *pHdr = (HctDbPageHdr*)p->writepg.aPg[p->writepg.nPg-1].aNew;
      if( pHdr->iPeerPg ){
        HctFilePage *pCopy = &p->discardpg.aPg[p->discardpg.nPg];

        rc = sqlite3HctFilePageGet(pDb->pFile, pHdr->iPeerPg, pCopy);
        if( rc==SQLITE_OK ){
          /* Evict the page immediately */
          rc = hctDbFilePageEvict(p, pCopy);
          if( rc!=SQLITE_OK ){
            sqlite3HctFilePageRelease(pCopy);
          }else{
            p->discardpg.nPg++;
          }
        }

        if( rc==SQLITE_OK ){
          rc = hctDbExtendWriteArray(pDb, p, p->writepg.nPg, 1);
        }
        if( rc==SQLITE_OK ){
          HctFilePage *pPg = &p->writepg.aPg[p->writepg.nPg-1];
          hctMemcpy(pPg->aNew, pCopy->aOld, pDb->pgsz);
          rc = hctDbSetWriteFpKey(pDb, p);
        }
      }
    }
  }

  return rc;
}

static int hctDbOverflowArrayAppend(HctDbOverflowArray *p, u32 ovfl, int nOvfl){
  assert( p->nAlloc>=p->nEntry );
  assert( ovfl>0 && nOvfl>0 );

  if( p->nAlloc==p->nEntry ){
    int nNew = p->nAlloc ? p->nAlloc*2 : 16;
    int nByte = nNew*sizeof(HctDbOverflow);
    HctDbOverflow *aNew = (HctDbOverflow*)sqlite3_realloc(p->aOvfl, nByte);

    if( aNew==0 ){
      return SQLITE_NOMEM_BKPT;
    }
    p->aOvfl = aNew;
    p->nAlloc = nNew;
  }

  p->aOvfl[p->nEntry].pgno = ovfl;
  p->aOvfl[p->nEntry].nOvfl = nOvfl;
  p->nEntry++;

  return SQLITE_OK;
}


/*
** Buffer aTarget[] must contain a page with variable sized records - an
** index leaf or node, or an intkey leaf. This function returns the offset
** of the record for entry iEntry, and populates output variable *pFlags
** with the entry flags.
*/
static int hctDbFindEntry(u8 *aTarget, int iEntry, u8 *pFlags, int *pnSize){
  int iRet;
  if( hctPagetype(aTarget)==HCT_PAGETYPE_INTKEY ){
    iRet = ((HctDbIntkeyLeaf*)aTarget)->aEntry[iEntry].iOff;
    *pFlags = ((HctDbIntkeyLeaf*)aTarget)->aEntry[iEntry].flags;
    *pnSize = ((HctDbIntkeyLeaf*)aTarget)->aEntry[iEntry].nSize;
  }else if( hctPageheight(aTarget)==0 ){
    iRet = ((HctDbIndexLeaf*)aTarget)->aEntry[iEntry].iOff;
    *pFlags = ((HctDbIndexLeaf*)aTarget)->aEntry[iEntry].flags;
    *pnSize = ((HctDbIndexLeaf*)aTarget)->aEntry[iEntry].nSize;
  }else{
    iRet = ((HctDbIndexNode*)aTarget)->aEntry[iEntry].iOff;
    *pFlags = ((HctDbIndexNode*)aTarget)->aEntry[iEntry].flags;
    *pnSize = ((HctDbIndexNode*)aTarget)->aEntry[iEntry].nSize;
  }
  return iRet;
}

static int hctDbRemoveOverflow(
  HctDatabase *pDb,
  HctDbWriter *p,
  u8 *aPage,
  int iCell
){
  int rc = SQLITE_OK;

  int nSize = 0;
  u8 flags = 0;
  int iOff = hctDbFindEntry(aPage, iCell, &flags, &nSize);
  if( flags & HCTDB_HAS_OVFL ){
    u32 ovfl = 0;
    int nOvfl = 0;
    const int nBytePerOvfl = pDb->pgsz - sizeof(HctDbPageHdr);
    int nLocal = hctDbLocalsize(aPage, pDb->pgsz, nSize);

    if( flags & HCTDB_HAS_TID ) iOff += 8;
    if( flags & HCTDB_HAS_RANGETID ) iOff += 8;
    if( flags & HCTDB_HAS_RANGEOLD ) iOff += 4;

    ovfl = hctGetU32(&aPage[iOff]);
    nOvfl = ((nSize - nLocal) + nBytePerOvfl - 1) / nBytePerOvfl;
    
    rc = hctDbOverflowArrayAppend(&p->delOvfl, ovfl, nOvfl);
  }

  return rc;
}

static void hctDbRemoveTids(
  HctDbIndexNodeEntry *p,
  u8 *aPg,
  u64 iSafeTid
){
  if( (p->flags & HCTDB_HAS_TID)==HCTDB_HAS_TID ){
    u64 iTid;
    memcpy(&iTid, &aPg[p->iOff], sizeof(u64));
    if( (iTid & HCT_TID_MASK)<=iSafeTid ){
      p->flags &= ~HCTDB_HAS_TID;
      p->iOff += sizeof(u64);
    }
  }
  if( (p->flags & (HCTDB_HAS_TID|HCTDB_HAS_RANGETID))==HCTDB_HAS_RANGETID ){
    u64 iTid;
    assert( p->flags & HCTDB_HAS_RANGEOLD );
    memcpy(&iTid, &aPg[p->iOff], sizeof(u64));
    if( (iTid & HCT_TID_MASK)<=iSafeTid ){
      p->flags &= ~(HCTDB_HAS_RANGETID|HCTDB_HAS_RANGEOLD);
      p->iOff += (sizeof(u64) + sizeof(u32));
    }
  }
}

/* 
** Populate the aSz[] array with the sizes and locations of each cell
**
** (bClobber && nNewCell==0)   ->   full-delete
** (bClobber)                  ->   clobber
** (bClobber==0)               ->   insert of new key
*/
static void hctDbBalanceGetCellSz(
  HctDatabase *pDb,
  HctDbWriter *pWriter,
  int iInsert,
  int bClobber,
  int nNewCell,                     /* Bytes stored on page for new cell */
  u8 *aPg,
  HctDbCellSz *aSz,
  int *pnSz                         /* OUT: number of entries in aSz[] */
){
  HctDbPageHdr *pPg = (HctDbPageHdr*)aPg;
  u64 iSafeTid = sqlite3HctFileSafeTID(pDb->pFile);
  int szEntry;
  int i0 = hctDbEntryArrayDim(aPg, &szEntry);
  int iCell = 0;                    /* Current cell of aPgCopy[ii] */
  int iSz = 0;                      /* Current populated size of aSz[] */
  int iIns = iInsert;

  for(iSz=0; iCell<pPg->nEntry || iCell==iIns; iSz++){
    HctDbCellSz *pSz = &aSz[iSz];

    assert( pPg->nEntry<pDb->pgsz );
    if( iCell==iIns ){
      assert( nNewCell>0 || bClobber );
      if( nNewCell ){
        pSz->nByte = szEntry + nNewCell;
        pSz->aEntry = 0;
        pSz->aCell = 0;
      }else{
        iSz--;
      }
      if( bClobber ){
        iCell++;
      }
      iIns = -1;
    }else{
      HctDbIndexNodeEntry *pE = (HctDbIndexNodeEntry*)&aPg[i0+iCell*szEntry];
      hctDbRemoveTids(pE, aPg, iSafeTid);

      pSz->nByte = szEntry + hctDbPageRecordSize(pPg, pDb->pgsz, iCell);
      pSz->aEntry = (u8*)pE;
      pSz->aCell = &aPg[pE->iOff];
      assert( pSz->nByte>0 );
      iCell++;
    }
  }
  if( pnSz ) *pnSz = iSz;
}

typedef struct HctDbInsertOp HctDbInsertOp;
struct HctDbInsertOp {
  u8 entryFlags;                  /* Flags for page entry added by this call */
  u8 *aEntry;                     /* Buffer containing formatted entry */
  int nEntry;                     /* Size of aEntry[] */
  int nEntrySize;                 /* Value for page header nSize field */

  int iPg;                        /* Index in HctDbWriter.writepg.aPg */
  int iInsert;                    /* Index in page to write to */

  i64 iIntkey;                    /* Key to insert (if intkey page)  */

  int eBalance;                   /* True if balance routine must be called */
  int bFullDel;                   /* True to skip insert */

  u32 iOldPg;
  const u8 *aOldPg;
};

/*
** Values for HctDbInsertOp.eBalance
*/
#define BALANCE_NONE 0
#define BALANCE_OPTIONAL  1
#define BALANCE_REQUIRED  2


static int hctDbBalanceAppend(
  HctDatabase *pDb, 
  HctDbWriter *p, 
  HctDbInsertOp *pOp
){
  int rc = hctDbExtendWriteArray(pDb, p, p->writepg.nPg, 1);
  if( rc==SQLITE_OK ){
    HctDbLeaf *pLeaf = (HctDbLeaf*)p->writepg.aPg[p->writepg.nPg-1].aNew;
    pLeaf->hdr.nFreeBytes = pDb->pgsz - sizeof(HctDbLeaf);
    pLeaf->hdr.nFreeGap = pLeaf->hdr.nFreeBytes;
    assert( p->iHeight==0 );
    assert_all_pages_ok(pDb, p);
    pOp->iPg = p->writepg.nPg-1;
    pOp->iInsert = 0;
  }
  return rc;
}

static HctBalance *hctDbBalanceSpace(int *pRc, HctDatabase *pDb){
  if( pDb->pBalance==0 ){
    HctBalance *p = 0;
    int nPg = ArraySize(p->aPg);
    int nSzAlloc = (nPg * 2 * MAX_CELLS_PER_PAGE(pDb->pgsz)) + 1;

    pDb->pBalance = p = (HctBalance*)sqlite3HctMallocRc(pRc, 
        sizeof(HctBalance) + 
        nPg * pDb->pgsz + 
        sizeof(HctDbCellSz) * nSzAlloc
    );
    if( p ){
      u8 *aCsr = (u8*)&p[1];
      int ii;
      for(ii=0; ii<nPg; ii++){
        p->aPg[ii] = aCsr;
        aCsr += pDb->pgsz;
      }
      p->aSz = (HctDbCellSz*)aCsr;
      p->nSzAlloc = nSzAlloc;
    }
  }
  return pDb->pBalance;
}

/*
** Rebalance routine for pages with variably-sized records - intkey leaves,
** index leaves and index nodes.
*/
static int hctDbBalance(
  HctDatabase *pDb,
  HctDbWriter *p,
  HctDbInsertOp *pOp,
  int bClobber
){
  int rc = SQLITE_OK;             /* Return code */
  int iPg = pOp->iPg;
  int iIns = pOp->iInsert;

  int iLeftPg;                    /* Index of leftmost page used in balance */
  int nIn = 1;                    /* Number of input peers for balance */
  int ii;                         /* Iterator used for various things */
  int nOut = 0;                   /* Number of output peers */
  int szEntry = 0;
  int iEntry0 = 0;
  HctDbCellSz *aSz = 0;
  int nSz = 0;
  u8 **aPgCopy = 0;

  int nRem;

  int aPgRem[5];
  int aPgFirst[6];

  /* Grab the temporary space used by balance operations.  */
  HctBalance *pBal = 0;
  pBal = hctDbBalanceSpace(&rc, pDb);
  if( pBal==0 ) return rc;

  /* Populate the aSz[] and aPgCopy[] arrays as if this were a single-page
  ** rebalance only. */
  aSz = &pBal->aSz[MAX_CELLS_PER_PAGE(pDb->pgsz) * 2];
  aPgCopy = pBal->aPg;
  hctMemcpy(aPgCopy[0], p->writepg.aPg[iPg].aNew, pDb->pgsz);
  hctDbBalanceGetCellSz(pDb, p, iIns, bClobber,pOp->nEntry,aPgCopy[0],aSz,&nSz);

  if( pOp->eBalance==BALANCE_OPTIONAL ){
    int nTotal = 0;
    for(ii=0; ii<nSz; ii++){
      nTotal += aSz[ii].nByte;
    }
    if( nTotal<=(pDb->pgsz - sizeof(HctDbIntkeyLeaf)) ){
      /* This is a single page balance */
      nIn = 1;
      nOut = 1;
      iLeftPg = iPg;
    }
  }

  if( nOut==0 ){
    HctDbPageHdr *pHdr = (HctDbPageHdr*)p->writepg.aPg[iPg].aNew;
    if( p->iHeight==0 
     && bClobber==0 && pOp->nEntry>0 
     && pHdr->iPeerPg==0 && pHdr->nEntry==iIns 
    ){
      p->bAppend = 1;
      rc = hctDbBalanceAppend(pDb, p, pOp);
      return rc;
    }

    /* If the HctDbWriter.writepg.aPg[] array still contains a single page, 
    ** load some peer pages into it. */
    assert( p->discardpg.nPg>=0 );
    rc = hctDbLoadPeers(pDb, p, &iPg);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    assert_all_pages_ok(pDb, p);

    /* Determine the subset of HctDbWriter.writepg.aPg[] pages that will be 
    ** rebalanced. Variable nIn is set to the number of input pages, and
    ** iLeftPg to the index of the leftmost of them.  */
    iLeftPg = iPg;
    if( iPg==0 ){
      nIn = MIN(p->writepg.nPg, 3);
    }else{
      if( iPg==p->writepg.nPg-1 ){
        nIn = MIN(p->writepg.nPg, 3);
        iLeftPg -= (nIn-1);
      }else{
        nIn = 3;
        iLeftPg--;
      }
      SWAP(u8*, aPgCopy[0], aPgCopy[iPg-iLeftPg]);
    }

    /* aPgCopy[iPg-iLeftPg] already contains a copy of page iPg at this 
    ** point. This loop takes copies of the other pages involved in the 
    ** balance operation. */ 
    for(ii=0; ii<nIn; ii++){
      if( ii==(iPg-iLeftPg) ) continue;
      hctMemcpy(aPgCopy[ii], p->writepg.aPg[iLeftPg+ii].aNew, pDb->pgsz);
    }

    for(ii=(iPg-iLeftPg)-1; ii>=0; ii--){
      int nCell = hctPagenentry(aPgCopy[ii]);
      aSz -= nCell;
      nSz += nCell;
      hctDbBalanceGetCellSz(pDb, p, -1, 0, 0, aPgCopy[ii], aSz, 0);
    }
    for(ii=(iPg-iLeftPg)+1; ii<nIn; ii++){
      hctDbBalanceGetCellSz(pDb, p, -1, 0, 0, aPgCopy[ii], &aSz[nSz], 0);
      nSz += hctPagenentry(aPgCopy[ii]);
    }
  }

  /* Update stats as required. */
  if( p->writecsr.pKeyInfo==0 ){
    pDb->stats.nBalanceIntkey++;
  }else{
    pDb->stats.nBalanceIndex++;
  }
  if( nIn==1 ){
    pDb->stats.nBalanceSingle++;
  }

  /* Figure out how many output pages will be required. This loop calculates
  ** a mapping heavily biased to the left. */
  aPgFirst[0] = 0;
  if( nOut==0 ){
    assert( sizeof(HctDbIntkeyLeaf)==sizeof(HctDbIndexLeaf) );
    nRem = pDb->pgsz - sizeof(HctDbIntkeyLeaf);
    nOut = 1;
    for(ii=0; ii<nSz; ii++){
      if( aSz[ii].nByte>nRem ){
        aPgRem[nOut-1] = nRem;
        aPgFirst[nOut] = ii;
        nOut++;
        nRem = pDb->pgsz - sizeof(HctDbIntkeyLeaf);
        assert( nOut<=ArraySize(aPgRem) );
      }
      nRem -= aSz[ii].nByte;
    }
    aPgRem[nOut-1] = nRem;
  }
  aPgFirst[nOut] = nSz;

  /* Adjust the packing calculated by the previous loop. */
  for(ii=nOut-1; ii>0; ii--){
    /* Try to shift cells from output page (ii-1) to output page (ii). Shift
    ** cells for as long as (a) there is more free space on page (ii) than on
    ** page (ii-1), and (b) there is enough free space on page (ii) to fit
    ** the last cell from page (ii-1).  */
    while( aPgRem[ii]>aPgRem[ii-1] ){       /* condition (a) */
      HctDbCellSz *pLast = &aSz[aPgFirst[ii]-1];
      if( pLast->nByte>aPgRem[ii] ) break;  /* condition (b) */
      aPgRem[ii] -= pLast->nByte;
      aPgRem[ii-1] += pLast->nByte;
      aPgFirst[ii] = (pLast - aSz);
    }
  }

  /* Allocate any required new pages and link them into the list. */
  rc = hctDbExtendWriteArray(pDb, p, iLeftPg+1, nOut-nIn);

  /* Populate the output pages */
  iEntry0 = hctDbEntryArrayDim(aPgCopy[0], &szEntry);
  for(ii=0; ii<nOut; ii++){
    int iIdx = ii+iLeftPg;
    u8 *aTarget = p->writepg.aPg[iIdx].aNew;
    HctDbIndexLeaf *pLeaf = (HctDbIndexLeaf*)aTarget;
    int iOff = pDb->pgsz;         /* Start of data area in aTarget[] */
    int iLast = (ii==(nOut-1) ? nSz : aPgFirst[ii+1]);
    int nNewEntry = 0;            /* Number of entries on this output page */
    int i2;

    for(i2=0; i2<(iLast - aPgFirst[ii]); i2++){
      HctDbCellSz *pSz = &aSz[aPgFirst[ii] + i2];
      if( pSz->aEntry ){
        u8 *aETo = &aTarget[iEntry0 + nNewEntry*szEntry];
        int nCopy = pSz->nByte - szEntry;
        hctMemcpy(aETo, pSz->aEntry, szEntry);
        iOff -= nCopy;
        ((HctDbIndexEntry*)aETo)->iOff = iOff;
        hctMemcpy(&aTarget[iOff], pSz->aCell, nCopy);
        nNewEntry++;
      }else{
        pOp->iPg = iIdx;
        pOp->iInsert = i2;
      }
    }

    pLeaf->pg.nEntry = nNewEntry;
    pLeaf->hdr.nFreeBytes = iOff - (iEntry0 + nNewEntry*szEntry);
    pLeaf->hdr.nFreeGap = iOff - (iEntry0 + nNewEntry*szEntry);
  }

  return rc;
}


static int hctDbBalanceIntkeyNode(
  HctDatabase *pDb,
  HctDbWriter *p,
  int iPg,
  int iInsert,                    /* Index in iPg for new key, if any */
  i64 iKey,                       /* Integer key value */
  u32 iChildPg                    /* The child pgno */
){
  int nMax = hctDbMaxCellsPerIntkeyNode(pDb->pgsz);
  int rc = SQLITE_OK;
  int nIn;                        /* Number of input pages */
  int nOut;                       /* Number of output pages */
  int iLeftPg;                    /* Index of left-most page in balance */
  int ii;                         /* Iterator variable */
  int nTotal = 0;                 /* Total number of keys for balance */
  u8 *aPgCopy[3];
  u8 *pFree = 0;

  assert( p->writepg.aPg[p->writepg.nPg-1].aNew );
  rc = hctDbLoadPeers(pDb, p, &iPg);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  iLeftPg = iPg;
  if( iPg==0 ){
    nIn = MIN(p->writepg.nPg, 3);
  }else if( iPg==p->writepg.nPg-1 ){
    nIn = MIN(p->writepg.nPg, 3);
    iLeftPg -= (nIn-1);
  }else{
    nIn = MIN(p->writepg.nPg, 3);
    iLeftPg--;
    assert( iLeftPg+nIn<=p->writepg.nPg );
  }

  /* Take a copy of each input page. Make the buffer used to store each
  ** copy larger than required by the size of one entry. Then, there is
  ** a new entry to add in stack variables (iKey/iChildPg), add it to the
  ** copy of its page. This is to make the loop that populates the output
  ** pages below easier to write. A real candidate for optimization, this. */
  pFree = (u8*)sqlite3Malloc(nIn*(pDb->pgsz+sizeof(HctDbIntkeyNodeEntry)));
  if( pFree==0 ) return SQLITE_NOMEM;
  for(ii=0; ii<nIn; ii++){
    aPgCopy[ii] = &pFree[(pDb->pgsz + sizeof(HctDbIntkeyNodeEntry)) * ii];
    hctMemcpy(aPgCopy[ii], p->writepg.aPg[iLeftPg+ii].aNew, pDb->pgsz);
  }
  if( iInsert>=0 ){
    HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)aPgCopy[iPg-iLeftPg];
    if( iInsert<pNode->pg.nEntry ){
      int nByte = sizeof(HctDbIntkeyNodeEntry) * (pNode->pg.nEntry-iInsert);
      memmove(&pNode->aEntry[iInsert+1], &pNode->aEntry[iInsert], nByte);
    }
    pNode->pg.nEntry++;
    pNode->aEntry[iInsert].iKey = iKey;
    pNode->aEntry[iInsert].iChildPg = iChildPg;
  }

  /* Figure out how many entries there are, in total */
  for(ii=0; ii<nIn; ii++){
    HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)aPgCopy[ii];
    nTotal += pNode->pg.nEntry;
  }

  /* Figure out how many output pages are required */
  nOut = (nTotal + (nMax-1)) / nMax;
  rc = hctDbExtendWriteArray(pDb, p, iLeftPg+1, nOut-nIn);
  assert( rc==SQLITE_OK );  /* todo */

  /* Populate the output pages */
  if( rc==SQLITE_OK ){
    int nRem = nTotal;
    int iIn = 0;
    int iInEntry = 0;

    for(ii=0; ii<nOut; ii++){
      HctDbIntkeyNodeEntry *pEntry;
      int nCell = nRem / (nOut-ii); /* Cells for this output page */
      HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)p->writepg.aPg[ii+iLeftPg].aNew;
      for(pEntry=pNode->aEntry; pEntry<&pNode->aEntry[nCell]; pEntry++){
        HctDbIntkeyNode *pIn = (HctDbIntkeyNode*)aPgCopy[iIn];
        *pEntry = pIn->aEntry[iInEntry++];
        if( iInEntry>=pIn->pg.nEntry ){
          iInEntry = 0;
          iIn++;
        }
      }
      pNode->pg.nEntry = nCell;
      nRem -= nCell;
    }
  }

  sqlite3_free(pFree);
  return rc;
}

/*
** This function handles the second part of an insert or delete operation
** on an internal intkey node key. The implementation is separate from the
** usual insert/delete routine because internal intkey nodes use fixed size
** records. The other three types of pages found in lists - intkey leaves,
** index leaves and index nodes - all use variable sized entries.
*/
static int hctDbInsertIntkeyNode(
  HctDatabase *pDb,
  HctDbWriter *p,
  int iPg,
  int iInsert,
  i64 iKey,                       /* Integer key value */
  u32 iChildPg,                   /* The child pgno */
  int bClobber,                   /* True to clobber entry iInsert */
  int bDel                        /* True for a delete operation */
){
  int nMax = hctDbMaxCellsPerIntkeyNode(pDb->pgsz);
  int nMin = hctDbMinCellsPerIntkeyNode(pDb->pgsz);
  HctDbIntkeyNode *pNode;
  int rc = SQLITE_OK;

  /* If bDel is set, then bClobber must also be set. */
  assert( bDel==0 || bClobber );

  pNode = (HctDbIntkeyNode*)p->writepg.aPg[iPg].aNew;
  if( (pNode->pg.nEntry>=nMax && bClobber==0 && bDel==0 ) ){
    /* Need to do a balance operation to make room for the new entry */
    rc = hctDbBalanceIntkeyNode(pDb, p, iPg, iInsert, iKey, iChildPg);
  }else if( bDel ){
    assert( iInsert<pNode->pg.nEntry );
    if( iInsert==0 ){
      rc = hctDbLoadPeers(pDb, p, &iPg);
      pNode = (HctDbIntkeyNode*)p->writepg.aPg[iPg].aNew;
    }
    if( rc==SQLITE_OK ){
      if( iInsert<(pNode->pg.nEntry-1) ){
        int nByte = sizeof(HctDbIntkeyNodeEntry) * (pNode->pg.nEntry-1-iInsert);
        memmove(&pNode->aEntry[iInsert], &pNode->aEntry[iInsert+1], nByte);
      }
      pNode->pg.nEntry--;
      if( iInsert==0 || pNode->pg.nEntry<nMin ){
        rc = hctDbBalanceIntkeyNode(pDb, p, iPg, -1, 0, 0);
      }
    }
  }else{
    if( bClobber==0 ){
      if( iInsert<pNode->pg.nEntry ){
        int nByte = sizeof(HctDbIntkeyNodeEntry) * (pNode->pg.nEntry-iInsert);
        memmove(&pNode->aEntry[iInsert+1], &pNode->aEntry[iInsert], nByte);
      }
      pNode->pg.nEntry++;
    }
    pNode->aEntry[iInsert].iKey = iKey;
    pNode->aEntry[iInsert].iChildPg = iChildPg;
    pNode->aEntry[iInsert].unused = 0;
  }

  return rc;
}


/*
** The buffer passed as the first
*/
static int hctDbFreegap(void *aPg){
  assert( 
      (hctPagetype(aPg)==HCT_PAGETYPE_INTKEY && hctPageheight(aPg)==0)
   || (hctPagetype(aPg)==HCT_PAGETYPE_INDEX)
  );
  return ((HctDbIndexNode*)aPg)->hdr.nFreeGap;
}

static int hctDbFreebytes(void *aPg){
  assert( 
      (hctPagetype(aPg)==HCT_PAGETYPE_INTKEY && hctPageheight(aPg)==0)
   || (hctPagetype(aPg)==HCT_PAGETYPE_INDEX)
  );
  return ((HctDbIndexNode*)aPg)->hdr.nFreeBytes;
}

static int hctDbInsertOverflow(
  HctDatabase *pDb, 
  HctDbWriter *pWriter,
  u8 *aTarget, 
  int nData, 
  const u8 *aData, 
  int *pnWrite,
  u32 *ppgOvfl
){
  int rc = SQLITE_OK;
  int nLocal = hctDbLocalsize(aTarget, pDb->pgsz, nData);

  if( nLocal==nData ){
    *pnWrite = nData;
    *ppgOvfl = 0;
  }else{
    const int sz = (pDb->pgsz - sizeof(HctDbPageHdr));
    int nRem;
    int nCopy;
    u32 iPg = 0;
    int nOvfl = 0;

    nRem = nData;
    nCopy = (nRem-nLocal) % sz;
    if( nCopy==0 ) nCopy = sz;
    while( rc==SQLITE_OK && nRem>nLocal ){
      HctFilePage pg;
      nOvfl++;
      rc = sqlite3HctFilePageNewPhysical(pDb->pFile, &pg);
      if( rc==SQLITE_OK ){
        HctDbPageHdr *pPg = (HctDbPageHdr*)pg.aNew;
        memset(pPg, 0, sizeof(HctDbPageHdr));
        pPg->iPeerPg = iPg;
        pPg->nEntry = nCopy;
        hctMemcpy(&pPg[1], &aData[nRem-nCopy], nCopy);
        iPg = pg.iNewPg;
        sqlite3HctFilePageRelease(&pg);
      }
      nRem -= nCopy;
      nCopy = sz;
    }

    *ppgOvfl = iPg;
    *pnWrite = nLocal;

    if( rc==SQLITE_OK && pWriter ){
      rc = hctDbOverflowArrayAppend(&pWriter->insOvfl, iPg, nOvfl);
    }
  }

  return rc;
}

static void hctDbRemoveCell(
  HctDatabase *pDb, 
  HctDbWriter *pWriter, 
  u8 *aTarget, 
  int iRem
){
  HctDbIndexNode *p = (HctDbIndexNode*)aTarget;
  const int eType = hctPagetype(aTarget);
  const int nHeight = hctPageheight(aTarget);
  const int pgsz = pDb->pgsz;

  int szEntry = 0;                /* Size of each entry in aEntry[] array */
  int iArrayOff = 0;              /* Offset of aEntry array in aTarget */
  int iData = 0;                  /* Offset of cell in aTarget[] */
  int nData = 0;                  /* Local size of cell to remove */

  /* Populate stack variables szEntry, iArrayOff, iData and nData. */
  assert( eType==HCT_PAGETYPE_INTKEY || eType==HCT_PAGETYPE_INDEX );
  assert( eType==HCT_PAGETYPE_INDEX || nHeight==0 );
  if( eType==HCT_PAGETYPE_INTKEY ){
    HctDbIntkeyEntry *pEntry = &((HctDbIntkeyLeaf*)aTarget)->aEntry[iRem]; 
    iData = pEntry->iOff;
    nData = hctDbIntkeyEntrySize(pEntry, pgsz);
    szEntry = sizeof(*pEntry);
    iArrayOff = sizeof(HctDbIntkeyLeaf);
  }else if( nHeight==0 ){
    HctDbIndexEntry *pEntry = &((HctDbIndexLeaf*)aTarget)->aEntry[iRem]; 
    iData = pEntry->iOff;
    nData = hctDbIndexEntrySize(pEntry, pgsz);
    szEntry = sizeof(*pEntry);
    iArrayOff = sizeof(HctDbIndexLeaf);
  }else{
    HctDbIndexNodeEntry *pEntry = &((HctDbIndexNode*)aTarget)->aEntry[iRem]; 
    iData = pEntry->iOff;
    nData = hctDbIndexNodeEntrySize(pEntry, pgsz);
    szEntry = sizeof(*pEntry);
    iArrayOff = sizeof(HctDbIndexNode);
  }

  /* Remove the aEntry[] array entry */
  if( iRem<p->pg.nEntry-1 ){
    u8 *aTo = &aTarget[iArrayOff + iRem*szEntry];
    memmove(aTo, &aTo[szEntry], (p->pg.nEntry-iRem-1) * szEntry);
  }
  p->pg.nEntry--;
  p->hdr.nFreeBytes += szEntry;
  p->hdr.nFreeGap += szEntry;

  /* Remove the cell from the data area */
  if( iData==(iArrayOff + szEntry*p->pg.nEntry + p->hdr.nFreeGap) ){
    int ii;
    int iFirst = pDb->pgsz;
    p->hdr.nFreeGap += nData;
    for(ii=0; ii<p->pg.nEntry; ii++){
      int iOff = ((HctDbIndexEntry*)&aTarget[iArrayOff + szEntry*ii])->iOff;
      if( iOff && iOff<iFirst ) iFirst = iOff;
    }
    p->hdr.nFreeGap = iFirst - (iArrayOff + szEntry*p->pg.nEntry);
  }
  p->hdr.nFreeBytes += nData;

}

/*
** Buffer aTarget must contain the image of a page that uses variable 
** length records - an intkey leaf, or an index leaf or node. This
** function does part of the job of inserting a new record into the
** page. 
** 
** Buffer aEntry[], size nEntry bytes, contains the sequence of bytes that
** will be stored in the data area of the page (i.e. any serialized
** tids, the old page number if any, any overflow page number and the
** portion of the database record that will be stored on the main
** page. Parameter iIns specifies the index within the page at which
** the new entry will be inserted.
*/
static void hctDbInsertEntry(
  HctDatabase *pDb,
  u8 *aTarget,
  int iIns,
  const u8 *aEntry,
  int nEntry
){
  HctDbIndexNode *p = (HctDbIndexNode*)aTarget;
  int szEntry = 0;                /* Size of each entry in aEntry[] array */
  int iEntry0 = 0;                /* Offset of aEntry array in aTarget */
  int iOff = 0;                   /* Offset of new cell data in aTarget */
  u8 *aFrom = 0;

  iEntry0 = hctDbEntryArrayDim(aTarget, &szEntry);

  /* This might fail if the db is corrupt */
  assert( p->hdr.nFreeGap>=(nEntry + szEntry) );

  /* Insert the new zeroed entry into the aEntry[] array */
  aFrom = &aTarget[iEntry0 + szEntry*iIns];
  if( iIns<p->pg.nEntry ){
    memmove(&aFrom[szEntry], aFrom, (p->pg.nEntry-iIns) * szEntry);
  }
  memset(aFrom, 0, szEntry);
  p->hdr.nFreeBytes -= szEntry;
  p->hdr.nFreeGap -= szEntry;
  p->pg.nEntry++;

  /* Insert the cell into the data area */ 
  iOff = iEntry0 + p->pg.nEntry*szEntry + p->hdr.nFreeGap - nEntry;
  hctMemcpy(&aTarget[iOff], aEntry, nEntry);
  p->hdr.nFreeBytes -= nEntry;
  p->hdr.nFreeGap -= nEntry;

  /* Set the aEntry[].iOff field */
  ((HctDbIndexEntry*)aFrom)->iOff = iOff;
}


static int hctDbMigrateReinsertKeys(HctDatabase *pDb, HctDbWriter *p){
  int rc = SQLITE_OK;
  if( p->nMigrateKey>0 ){
    assert( p->iHeight==0 );

    /* Append a page to the write-array */
    rc = hctDbExtendWriteArray(pDb, p, p->writepg.nPg, 1);


    if( rc==SQLITE_OK ){
      int ii = 0;
      HctDbInsertOp op;
      HctDbLeaf *pOld = (HctDbLeaf*)p->writepg.aPg[0].aOld;
      HctDbLeaf *pNew = (HctDbLeaf*)p->writepg.aPg[p->writepg.nPg-1].aNew;

      /* TODO: Might this not be a part of ExtendWriteArray() ? */
      pNew->hdr.nFreeBytes = pDb->pgsz - sizeof(HctDbLeaf);
      pNew->hdr.nFreeGap = pNew->hdr.nFreeBytes;

      /* Loop through the last nMigrateKey on the old page, copying them
      ** to the new page. */
      for(ii=0; ii<p->nMigrateKey; ii++){
        int iOld = (pOld->pg.nEntry - p->nMigrateKey) + ii;
        HctDbIndexEntry *pOldE = 0;
        HctDbIndexEntry *pNewE = 0;
        int nEntry = 0;

        pOldE = hctDbEntryEntry(pOld, iOld);
        nEntry = hctDbPageRecordSize(pOld, pDb->pgsz, iOld);
        hctDbInsertEntry(pDb, (u8*)pNew, ii, &((u8*)pOld)[pOldE->iOff], nEntry);

        pNewE = hctDbEntryEntry(pNew, ii);
        pNewE->nSize = pOldE->nSize;
        pNewE->flags = pOldE->flags;
        if( hctPagetype(pOld)==HCT_PAGETYPE_INTKEY ){
          ((HctDbIntkeyEntry*)pNewE)->iKey = ((HctDbIntkeyEntry*)pOldE)->iKey;
        }
      }

      memset(&op, 0, sizeof(op));
      op.iPg = p->writepg.nPg-1;
      op.iInsert = -1;
      op.eBalance = BALANCE_OPTIONAL;
      rc = hctDbBalance(pDb, p, &op, 0);
    }
  }

  return rc;
}

/*
** Parameter aTarget points to a buffer containing an intkey or index
** internal node. Return the child-page number for entry iInsert on
** that page.
*/
u32 hctDbGetChildPage(u8 *aTarget, int iInsert){
  const int eType = hctPagetype(aTarget);
  u32 iChildPg;
  if( eType==HCT_PAGETYPE_INTKEY ){
    iChildPg = ((HctDbIntkeyNode*)aTarget)->aEntry[iInsert].iChildPg;
  }else{
    assert( eType==HCT_PAGETYPE_INDEX );
    iChildPg = ((HctDbIndexNode*)aTarget)->aEntry[iInsert].iChildPg;
  }
  return iChildPg;
}

static void hctDbClobberEntry(
  HctDatabase *pDb,
  u8 *aTarget,
  HctDbInsertOp *pOp
){
  HctDbIndexEntry *pEntry;        /* Entry being clobbered */
  int nOld = hctDbPageRecordSize(aTarget, pDb->pgsz, pOp->iInsert);

  pEntry = hctDbEntryEntry(aTarget, pOp->iInsert);
  pEntry->nSize = pOp->nEntrySize;
  pEntry->flags = pOp->entryFlags;

  memcpy(&aTarget[pEntry->iOff], pOp->aEntry, pOp->nEntry);
  ((HctDbIndexNode*)aTarget)->hdr.nFreeBytes += (nOld - pOp->nEntry);

  pDb->stats.nUpdateInPlace++;
}

static int hctDbFindOldPage(
  HctDatabase *pDb,
  HctDbWriter *p,
  UnpackedRecord *pKey,
  i64 iKey,
  u32 *piOld,
  const u8 **paOld
){
  HctFilePage *pPg = 0;
  int rc = SQLITE_OK;
  int iTest;

  for(iTest=p->discardpg.nPg-1; iTest>=0; iTest--){
    pPg = &p->discardpg.aPg[iTest];
    if( pKey ){
      int bGe = 0;
      rc = hctDbCompareFPKey(pDb, pKey, pPg->aOld, &bGe);
      if( bGe || rc!=SQLITE_OK ) break;
    }else{
      i64 iFP = hctDbIntkeyFPKey(pPg->aOld);
      if( iKey>=iFP ) break;
    }
    pPg = 0;
  }

  if( pPg==0 ){
    pPg = &p->writepg.aPg[0];
  }
  assert( pPg->iOldPg!=0 );
  *piOld = pPg->iOldPg;
  *paOld = pPg->aOld;

  return rc;
}

static u64 hctDbGetRangeTidByIdx(HctDatabase *pDb, u8 *aTarget, int iIdx){
  HctDbCell cell;
  hctDbCellGetByIdx(pDb, aTarget, iIdx, &cell);
  return cell.iRangeTid;
}

static u32 hctDbMakeFollowPtr(
  int *pRc, 
  HctDatabase *pDb, 
  u64 iFollowTid, 
  u32 iPg
){
  int rc = *pRc;
  HctFilePage pg;
  u32 iRet = 0;

  memset(&pg, 0, sizeof(pg));
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFilePageNewPhysical(pDb->pFile, &pg);
    iRet = pg.iNewPg;
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFileClearPhysInUse(pDb->pFile, iRet, 0);
  }
  if( rc==SQLITE_OK ){
    HctDbHistoryFan *pFan = (HctDbHistoryFan*)pg.aNew;
    memset(pFan, 0, sizeof(*pFan));
    pFan->pg.hdrFlags = HCT_PAGETYPE_HISTORY;
    pFan->pg.nEntry = 1;
    pFan->iRangeTid0 = pDb->iTid;
    pFan->iFollowTid0 = iFollowTid;
    pFan->pgOld0 = iPg;
    rc = sqlite3HctFilePageRelease(&pg);
  }else{
    sqlite3HctFilePageUnwrite(&pg);
    sqlite3HctFilePageRelease(&pg);
  }

  *pRc = rc;
  return iRet;
}

static int hctDbDelete(
  HctDatabase *pDb,
  HctDbWriter *p,
  UnpackedRecord *pRec,
  HctDbInsertOp *pOp
){
  u64 iTidOr = (pDb->eMode==HCT_MODE_ROLLBACK ? HCT_TID_ROLLBACK_OVERRIDE : 0);
  u64 iSafeTid = sqlite3HctFileSafeTID(pDb->pFile);
  u64 iTidValue = pDb->iTid | iTidOr;
  u64 iDelRangeTid = 0;
  int rc = SQLITE_OK;
  u8 *aNull = 0;
  int prevFlags = 0;
  int nLocalSz = 0;
  u8 *aTarget = p->writepg.aPg[pOp->iPg].aNew;
  int bLeftmost = (hctIsLeftmost(aTarget) && pOp->iInsert==0);

  HctDbCell prev;           /* Previous cell on page */

  assert( pOp->bFullDel==0 );

  if( pOp->iInsert==0 && !bLeftmost ){
    /* If deleting the first key on the first page, set the eBalance flag (as 
    ** deleting a FP key means the parent list must be adjusted) and load peer 
    ** pages into memory.  */
    pOp->eBalance = BALANCE_REQUIRED;
    if( pOp->iPg==0 ){
      rc = hctDbLoadPeers(pDb, p, &pOp->iPg);
      if( rc!=SQLITE_OK ) return rc;
      aTarget = p->writepg.aPg[pOp->iPg].aNew;
    }
  }
  assert_page_is_ok(aTarget, pDb->pgsz);

  /* Deal with the case where the cell we are about to remove (cell iInsert)
  ** has a range-tid greater than that of the current transaction (iTid) */
  iDelRangeTid = hctDbGetRangeTidByIdx(pDb, aTarget, pOp->iInsert);
  if( (iDelRangeTid & HCT_TID_MASK)>pDb->iTid ){
    iTidValue = iDelRangeTid;
    pOp->iOldPg = hctDbMakeFollowPtr(&rc, pDb, iDelRangeTid, pOp->iOldPg);
    sqlite3HctFilePageRelease(&p->fanpg);
  }

  if( bLeftmost ){

    memset(&prev, 0, sizeof(prev));
    prev.iTid = LARGEST_TID;
    prevFlags |= HCTDB_HAS_TID;

    assert( pOp->iPg==0 );
    prev.iTid = LARGEST_TID;
    prevFlags |= HCTDB_HAS_TID;
    pOp->nEntrySize = 0;
    nLocalSz = hctDbLocalsize(aTarget, pDb->pgsz, pOp->nEntrySize);

  }else{
    HctDbIndexEntry *pPrev = 0;

    /* Remove the cell being deleted from the target page. This must be done
    ** after hctDbLoadPeers() is called (if it is called). */
    assert_page_is_ok(aTarget, pDb->pgsz);
    hctDbRemoveCell(pDb, p, aTarget, pOp->iInsert);
    assert_page_is_ok(aTarget, pDb->pgsz);
    if( pOp->iInsert==0 ){
      assert( pOp->iPg>0 );
      pOp->iPg--;
      aTarget = p->writepg.aPg[pOp->iPg].aNew;
      assert( hctPagenentry(aTarget)>0 );
      pOp->iInsert = ((HctDbPageHdr*)aTarget)->nEntry - 1;
    }else{
      pOp->iInsert--;
    }

    /* Load the cell immediately before the one just removed */
    pPrev = hctDbEntryEntry(aTarget, pOp->iInsert);
    pOp->nEntrySize = pPrev->nSize;
    prevFlags = pPrev->flags;

    hctDbCellGet(pDb, &aTarget[pPrev->iOff], pPrev->flags, &prev);
    nLocalSz = hctDbLocalsize(aTarget, pDb->pgsz, pOp->nEntrySize);
  }

  /* Update the range-tid and range-oldpg fields. There are several 
  ** possibilities:
  **
  **   1) The left-hand-cell already has the desired range-pointer values
  **      (both TID and old-page-number).
  **
  **   2) The left-hand-cell does not have a range-pointer. Or else
  **      has a range-pointer so old it can be overwritten with impunity.
  **
  **   3) The left-hand-cell has a range-pointer to a fan-page that was
  **      created by the current HctDbWriter batch, and that fan-page
  **      is not already full.
  **
  **   4) None of the above are true. A new fan-page must be created.
  */
  if( prev.iRangeTid==iTidValue && prev.iRangeOld==pOp->iOldPg ){
    /* Possibility (1) */
    pOp->bFullDel = 1;
    pOp->iInsert = -1;
  }
  else if( prev.iRangeTid==0 || (prev.iRangeTid & HCT_TID_MASK)<=iSafeTid ){
    /* Possibility (2) */
    prev.iRangeTid = iTidValue;
    prev.iRangeOld = pOp->iOldPg;
  }else if( prev.iRangeOld==p->fanpg.iNewPg 
        && ((HctDbHistoryFan*)p->fanpg.aNew)->iRangeTid1==iTidValue
  ){
    /* Possibility (3) */
    HctDbHistoryFan *pFan = (HctDbHistoryFan*)p->fanpg.aNew;
    if( pFan->aPgOld1[pFan->pg.nEntry-2]!=pOp->iOldPg ){
      const int nMax = ((pDb->pgsz - sizeof(HctDbHistoryFan))/sizeof(u32));
      assert( pFan->pg.nEntry<nMax );
      pFan->aPgOld1[pFan->pg.nEntry-1] = pOp->iOldPg;
      pFan->pg.nEntry++;
      if( pFan->pg.nEntry==nMax ){
        rc = sqlite3HctFilePageRelease(&p->fanpg);
      }
    }
    pOp->bFullDel = 1;
    pOp->iInsert = -1;
  }else{
    /* Possibility (4) */
    rc = sqlite3HctFilePageRelease(&p->fanpg);
    if( rc==SQLITE_OK ){
      rc = sqlite3HctFilePageNewPhysical(pDb->pFile, &p->fanpg);
    }
    if( rc==SQLITE_OK ){
      rc = sqlite3HctFileClearPhysInUse(pDb->pFile, p->fanpg.iNewPg, 0);
    }
    if( rc==SQLITE_OK ){
      int bDummy = 0;
      HctDbHistoryFan *pFan = (HctDbHistoryFan*)p->fanpg.aNew;
      memset(pFan, 0, pDb->pgsz);
      pFan->pg.hdrFlags = HCT_PAGETYPE_HISTORY;
      pFan->pg.nEntry = 2;
      pFan->iRangeTid0 = prev.iRangeTid;
      pFan->iFollowTid0 = prev.iRangeTid;
      pFan->pgOld0 = prev.iRangeOld;
      rc = hctDbLeafSearch(
          pDb, pOp->aOldPg, pOp->iIntkey, pRec, &pFan->iSplit0, &bDummy
      );
      assert( bDummy );
      pFan->iRangeTid1 = iTidValue;
      pFan->aPgOld1[0] = pOp->iOldPg;
      prev.iRangeOld = p->fanpg.iNewPg;
      if( (prev.iRangeTid & HCT_TID_MASK)<(iTidValue & HCT_TID_MASK) ){
        prev.iRangeTid = iTidValue;
      }
    }
  }

  if( rc==SQLITE_OK && pOp->bFullDel==0 ){
    prev.iRangeTid |= iTidOr;
    pOp->aEntry = pDb->aTmp;
    pOp->nEntry = hctDbCellPut(pOp->aEntry, &prev, nLocalSz);
    pOp->entryFlags = prevFlags | HCTDB_HAS_RANGETID | HCTDB_HAS_RANGEOLD;
    if( hctPagetype(aTarget)==HCT_PAGETYPE_INTKEY ){
      if( bLeftmost ){
        pOp->iIntkey = SMALLEST_INT64;
      }else{
        pOp->iIntkey = ((HctDbIntkeyLeaf*)aTarget)->aEntry[pOp->iInsert].iKey;
      }
    }
  }

  assert_page_is_ok(aTarget, pDb->pgsz);
  if( aNull ) sqlite3_free(aNull);
  return rc;
}

static int hctDbInsertFindPosition(
  HctDatabase *pDb,
  HctDbWriter *p,
  u32 iRoot,
  UnpackedRecord *pRec,
  i64 iKey,
  HctDbInsertOp *pOp,
  int *pbClobber
){
  const RecordCompare xCompare = pRec ? sqlite3VdbeFindCompare(pRec) : 0;
  int rc = SQLITE_OK;

  if( p->writepg.nPg==0 ){
    if( p->writecsr.iRoot!=iRoot ){
      hctDbCsrInit(pDb, iRoot, 0, &p->writecsr);
    }else{
      hctDbCsrReset(&p->writecsr);
    }
    if( pRec ){
      p->writecsr.pKeyInfo = sqlite3KeyInfoRef(pRec->pKeyInfo);
    }
    rc = hctDbCsrSeek(
        &p->writecsr, &p->fp, p->iHeight, xCompare, pRec, iKey, pbClobber
    );
    if( rc ) return rc;
    pOp->iInsert = p->writecsr.iCell;
    if( *pbClobber==0 ) pOp->iInsert++;

    p->writepg.aPg[0] = p->writecsr.pg;
    memset(&p->writecsr.pg, 0, sizeof(HctFilePage));

    assert( p->bDoCleanup );
    p->writepg.nPg = 1;
    rc = sqlite3HctFilePageWrite(&p->writepg.aPg[0]);
    if( rc ) return rc;
    hctMemcpy(p->writepg.aPg[0].aNew, p->writepg.aPg[0].aOld, pDb->pgsz);
    if( p->fp.iKey==0 ){
      rc = hctDbSetWriteFpKey(pDb, p);
    }
    if( rc ) return rc;
  }else if( pRec ){
    HctBuffer buf = {0,0,0};
    for(pOp->iPg=p->writepg.nPg-1; pOp->iPg>0; pOp->iPg--){
      const u8 *aK;
      int nK;
      rc = hctDbLoadRecord(
          pDb, &buf, p->writepg.aPg[pOp->iPg].aNew, 0, &nK, &aK
      );
      if( rc!=SQLITE_OK ){
        sqlite3HctBufferFree(&buf);
        return rc;
      }
      if( xCompare(nK, aK, pRec)<=0 ) break;
    }
    sqlite3HctBufferFree(&buf);
    rc = hctDbIndexSearch(pDb,
        p->writepg.aPg[pOp->iPg].aNew, xCompare, pRec, &pOp->iInsert, pbClobber
    );
    if( rc!=SQLITE_OK ) return rc;
  }else{
    for(pOp->iPg=p->writepg.nPg-1; pOp->iPg>0; pOp->iPg--){
      if( hctDbIntkeyFPKey(p->writepg.aPg[pOp->iPg].aNew)<=iKey ) break;
    }
    if( p->iHeight==0 ){
      pOp->iInsert = hctDbIntkeyLeafSearch(
          p->writepg.aPg[pOp->iPg].aNew, iKey, pbClobber
      );
    }else{
      pOp->iInsert = hctDbIntkeyNodeSearch(
          p->writepg.aPg[pOp->iPg].aNew, iKey, pbClobber
      );
    }
  }

  return rc;
}

static int hctDbWriteWriteConflict(
  HctDatabase *pDb, 
  HctDbWriter *p,
  HctDbInsertOp *pOp,
  UnpackedRecord *pKey, 
  i64 iKey,
  int bClobber
){
  int rc = SQLITE_OK;
  const u8 *aTarget = p->writepg.aPg[pOp->iPg].aNew;

  assert( p->iHeight==0 && pDb->eMode==HCT_MODE_NORMAL );

  if( bClobber ){
    HctDbIndexEntry *pE;
    if( pKey ){
      pE = &((HctDbIndexLeaf*)aTarget)->aEntry[pOp->iInsert];
    }else{
      pE = (HctDbIndexEntry*)&((HctDbIntkeyLeaf*)aTarget)->aEntry[pOp->iInsert];
    }
    if( pE->flags & HCTDB_HAS_TID ){
      u64 iTid;
      hctMemcpy(&iTid, &aTarget[pE->iOff], sizeof(u64));
      if( hctDbTidIsConflict(pDb, iTid) ){
        rc = HCT_SQLITE_BUSY;
      }

    }
  }else if( pOp->iInsert>0 ){
    int iCell = 0;
    int bMerge = 0;
    HctRangePtr ptr;

    iCell = (pOp->iInsert - 1);
    hctDbGetRange(aTarget, iCell, &ptr);
    while( hctDbFollowRangeOld(pDb, &ptr, &bMerge) ){
      HctFilePage pg;
      const u8 *aOld = 0;

      if( ptr.iOld==pDb->pa.fanpg.iNewPg ){
        aOld = pDb->pa.fanpg.aNew;
        memset(&pg, 0, sizeof(pg));
      }else{
        rc = hctDbGetPhysical(pDb, ptr.iOld, &pg);
        aOld = pg.aOld;
      }

      /* assert( bMerge==0 || iRangeTid!=pDb->iTid ); */
      if( rc==SQLITE_OK ){
        int iCell = 0;
        if( hctPagetype(aOld)==HCT_PAGETYPE_HISTORY ){
          iCell = hctDbFanSearch(&rc, pDb, aOld, pKey, iKey);
        }else{
          int bExact = 0;
          rc = hctDbLeafSearch(pDb, aOld, iKey, pKey, &iCell, &bExact);
          if( rc==SQLITE_OK && bExact ){
            if( bMerge ){
              HctDbCell cell;
              hctDbCellGetByIdx(pDb, aOld, iCell, &cell);
              if( hctDbTidIsVisible(pDb, cell.iTid, 0) ) rc = HCT_SQLITE_BUSY;
            }
            sqlite3HctFilePageRelease(&pg);
            break;
          }else{
            iCell--;
          }
          if( rc ){
            sqlite3HctFilePageRelease(&pg);
            break;
          }
        }

        hctDbGetRange(aOld, iCell, &ptr);
        sqlite3HctFilePageRelease(&pg);
      }else{
        break;
      }
    }
  }

  return rc;
}

static int hctDbInsert(
  HctDatabase *pDb,
  HctDbWriter *p,
  u32 iRoot,
  UnpackedRecord *pRec,           /* The key value for index tables */
  i64 iKey,                       /* For intkey tables, the key value */
  u32 iChildPg,                   /* For internal node ops, the child pgno */
  int bDel,                       /* True for a delete operation */
  int nData, const u8 *aData      /* Record/key to insert */
){
  const RecordCompare xCompare = (pRec ? sqlite3VdbeFindCompare(pRec) : 0);
  int rc = SQLITE_OK;
  int bClobber = 0;
  u8 *aTarget;                    /* Page to write new entry to */
  HctDbInsertOp op = {0,0,0,0,0,0,0,0,0,0,0};
  int bUpdateInPlace = 0;

  p->nWriteKey++;

  assert( pDb->eMode==HCT_MODE_NORMAL || pDb->eMode==HCT_MODE_ROLLBACK );

  /* Check if any existing dirty pages need to be flushed to disk before 
  ** this key can be inserted. If they do, flush them. */
  assert( p->writepg.nPg==0 || iRoot==p->writecsr.iRoot );
  assert( p->writepg.nPg>0 || p->bAppend==0 );
  if( p->writepg.nPg ){
    assert( p->bDoCleanup );
    if( p->writepg.nPg>HCTDB_MAX_DIRTY 
     || p->discardpg.nPg>=HCTDB_MAX_DIRTY 
     || hctDbTestWriteFpKey(p, xCompare, pRec, iKey) 
    ){
      rc = hctDbInsertFlushWrite(pDb, p);
      if( rc ) return rc;
      p->nWriteKey = 1;
    }
  }

  p->bDoCleanup = 1;
  hctDbWriterGrow(p);

  /* This block sets stack variables:
  **
  **   op.iPg:     Index of page in HctDbWriter.writepg.aPg[] to write to.
  **   op.iInsert: The index of the new, overwritten, or deleted entry 
  **               within the page.
  **   bClobber:   True if this write clobbers (or deletes, if bDel) an 
  **               existing entry.
  **   aTarget:    The aNew[] buffer of the page that will be written.
  **
  ** It also checks if the current key is a write-write conflict. And 
  ** returns early if so.
  */
  if( p->bAppend ){
    assert( bClobber==0 );
    assert( p->writepg.nPg>0 );
    op.iPg = p->writepg.nPg-1;
    aTarget = p->writepg.aPg[op.iPg].aNew;
    op.iInsert = hctPagenentry(aTarget);
  }else{
    /* If the page array is empty, seek the write cursor to find the leaf
    ** page on which to insert this new entry or delete key.
    **
    ** Otherwise, figure out which page in the HctDbWriter.aWritePg[] array the
    ** new entry belongs on.  */
    rc = hctDbInsertFindPosition(pDb, p, iRoot, pRec, iKey, &op, &bClobber);
    if( rc ) return rc;
    aTarget = p->writepg.aPg[op.iPg].aNew;
    assert( aTarget );

    /* If this is a write to a leaf page, and not part of a rollback, 
    ** check for a write-write conflict here. */
    if( 0==p->iHeight 
     && pDb->eMode==HCT_MODE_NORMAL
     && (rc=hctDbWriteWriteConflict(pDb, p, &op, pRec, iKey, bClobber))
    ){
      return rc;
    }
  }

  if( bClobber==0 && bDel ){
    return SQLITE_OK;
  }

  /* At this point, once the page that will be modified has been loaded
  ** and marked as writable, if the operation is on an internal list:
  **
  **   1) For an insert, check if the child page has already been marked
  **      as EVICTED by some other client. If so, return early.
  **
  **   2) For a delete, check that there is an entry to delete. And if so,
  **      that the value of its child-page field matches iChildPg. If
  **      not, return early. Note that the page marked as writable will
  **      still be flushed to disk in this case - even though it may be
  **      unmodified.
  **
  ** This resolves a race condition that may occur if client B starts 
  ** removing page X from a list before client A has finished inserting
  ** the corresponding entry into the parent list. Specifically:
  **
  **   + when client A gets here, if the EVICTED flag is not set on page X,
  **     then client B will try to delete the corresponding entry from
  **     the parent list at some point in the future. This will either 
  **     occur after client A has updated the list, in which case no
  **     problem, or it will cause client A's attempt to flush the modified
  **     page to disk to fail. Client A will retry, see the EVICTED flag
  **     is set, and continue.
  **
  **   + or, if EVICTED is set, then there is no point in writing the
  **     entry into the parent list.
  */
  assert( rc==SQLITE_OK );
  if( p->iHeight>0 ){
    if( bDel==0 && sqlite3HctFilePageIsEvicted(pDb->pFile, iChildPg) ){
      return SQLITE_OK;
    }
    if( bDel ){
      u32 iChild = hctDbGetChildPage(aTarget, op.iInsert);
      if( iChild!=iChildPg ) return SQLITE_OK;
    }
  }

  /* Writes to an intkey internal node are handled separately. They are
  ** different because they used fixed size key/data pairs. All other types
  ** of page use variably sized key/data entries. */
  if( pRec==0 && p->iHeight>0 ){
    return hctDbInsertIntkeyNode(
        pDb, p, op.iPg, op.iInsert, iKey, iChildPg, bClobber, bDel
    );
  }

  if( p->iHeight>0 ){
    op.bFullDel = bDel;
  }

  if( rc ){
    assert( !"is this really possible?" );
    return rc;
  }

  /* If this is a clobber or delete operation and the entry being removed
  ** has an overflow chain, add an entry to HctDbWriter.delOvfl. */
  if( bClobber ){
    hctDbRemoveOverflow(pDb, p, aTarget, op.iInsert);
  }

  /* Populate the following variables:
  **
  **   entryFlags
  **   aEntry
  **   nEntry
  **   nEntrySize
  **
  ** This block populates the above variables. It also inserts overflow pages.
  */
  op.iIntkey = iKey;
  if( op.bFullDel==0 ){

    if( p->iHeight==0 && (bClobber || bDel) ){
      rc = hctDbFindOldPage(pDb, p, pRec, iKey, &op.iOldPg, &op.aOldPg);
      if( rc!=SQLITE_OK ) goto insert_out;
      assert( op.iOldPg!=0 );
    }

    if( bDel && p->iHeight==0 ){
      assert( bClobber );
      rc = hctDbDelete(pDb, p, pRec, &op);
      aTarget = p->writepg.aPg[op.iPg].aNew;
      assert_page_is_ok(aTarget, pDb->pgsz);
      if( op.bFullDel ) bClobber = 0;
    }else{
      HctDbCell cell;
      int nLocal = 0;
      memset(&cell, 0, sizeof(cell));

      if( p->iHeight==0 ){
      
        cell.iTid = pDb->iTid;
        if( pDb->eMode==HCT_MODE_ROLLBACK ){
          cell.iTid |= HCT_TID_ROLLBACK_OVERRIDE;
        }

        if( bClobber ){
          u64 iOldRangeTid = hctDbGetRangeTidByIdx(pDb, aTarget, op.iInsert);
          if( (iOldRangeTid & HCT_TID_MASK)>pDb->iTid ){
            cell.iRangeOld = hctDbMakeFollowPtr(&rc,pDb,iOldRangeTid,op.iOldPg);
            cell.iRangeTid = iOldRangeTid;
          }else{
            cell.iRangeTid = pDb->iTid;
            cell.iRangeOld = op.iOldPg;
          }
        }else if( op.iInsert>0 ){
          HctDbCell prev;
          hctDbCellGetByIdx(pDb, aTarget, op.iInsert-1, &prev);
          cell.iRangeTid = prev.iRangeTid;
          cell.iRangeOld = prev.iRangeOld;
          assert( cell.iRangeTid==0 || cell.iRangeOld!=0 );
        }
      }
      rc = hctDbInsertOverflow(
          pDb, p, aTarget, nData, aData, &nLocal, &cell.iOvfl
      );
      cell.aPayload = aData;

      op.aEntry = pDb->aTmp;
      op.nEntry = hctDbCellPut(op.aEntry, &cell, nLocal);
      op.nEntrySize = nData;
      op.entryFlags = hctDbCellToFlags(&cell);
    }

    assert( rc!=SQLITE_OK || op.bFullDel || op.aEntry==pDb->aTmp );
    if( rc!=SQLITE_OK ) goto insert_out;
  }

  assert( op.aEntry==0 || op.aEntry==pDb->aTmp );

  /* There are now two choices - either the aTarget[] page can be updated
  ** directly (if the new entry fits on the page), or the balance-tree()
  ** routine runs to redistribute cells between aTarget[] and its peers,
  ** writing the new entry at the same time. A balance is required if:
  **
  **   1) there is insufficient space in the free-gap for any new
  **      cell and array entry, or
  **
  **   2) this is a full-delete of the fpkey of the page (iInsert==0), or
  **
  **   3) this operation would leave the page underfull, and it is not
  **      the only page in its list.
  */
  if( op.eBalance==BALANCE_NONE ){
    int szEntry = hctDbPageEntrySize(aTarget);
    int nFree = hctDbFreebytes(aTarget);
    int nReq = 0;
    int nSpace = 0;               /* Space freed by removing cell */ 

    if( bClobber ){
      nSpace = hctDbPageRecordSize(aTarget, pDb->pgsz, op.iInsert);
      nFree += szEntry;
      nFree += nSpace;
    }

    if( op.bFullDel==0 ){
      if( nSpace>=op.nEntry ) bUpdateInPlace = 1;
      nFree -= op.nEntry;
      nFree -= szEntry;
      nReq = op.nEntry + (bClobber ? 0 : szEntry);
    }

    /* If (a) this is a clobber operation, and (b) either the first
    ** key on the page is being deleted or else the page will be less
    ** than 1/3 full following the update, and (c) the page is not
    ** the only page in its linked list, rebalance! */
    if( (bClobber || bDel)                                           /* (a) */ 
     && ((op.iInsert==0 && op.bFullDel) || (nFree>(2*pDb->pgsz/3)))  /* (b) */
     && (hctIsLeftmost(aTarget)==0 || hctPagePeer(aTarget)!=0)       /* (c) */
    ){
      /* Target page will be underfull following this op. Rebalance! */
      op.eBalance = BALANCE_REQUIRED;
      bUpdateInPlace = 0;
    }else if( hctDbFreegap(aTarget)<nReq && bUpdateInPlace==0 ){
      op.eBalance = BALANCE_OPTIONAL;
    }
  }

  if( op.eBalance!=BALANCE_NONE ){
    assert( op.bFullDel==0 || op.aEntry==0 );
    assert( op.bFullDel==0 || op.nEntry==0 );
    assert_all_pages_ok(pDb, p);
    if( p->bAppend ){
      rc = hctDbBalanceAppend(pDb, p, &op);
    }else{
      rc = hctDbBalance(pDb, p, &op, bClobber);
    }
    if( rc==SQLITE_OK ) assert_all_pages_ok(pDb, p);
    aTarget = p->writepg.aPg[op.iPg].aNew;
  }else if( bUpdateInPlace ){
    assert_page_is_ok(aTarget, pDb->pgsz);
    hctDbClobberEntry(pDb, aTarget, &op);
    assert_page_is_ok(aTarget, pDb->pgsz);
  }else if( bClobber ){
    assert_page_is_ok(aTarget, pDb->pgsz);
    hctDbRemoveCell(pDb, p, aTarget, op.iInsert);
    assert_page_is_ok(aTarget, pDb->pgsz);
  }

  /* Unless this is a full-delete operation, update rest of the aEntry[]
  ** entry fields for the new cell. */
  if( rc==SQLITE_OK && op.bFullDel==0 ){
    int eType = hctPagetype(aTarget);
    assert_page_is_ok(aTarget, pDb->pgsz);
    assert( op.iInsert>=0 );
    
    /* print_out_page("1", aTarget, pDb->pgsz); */
    if( bUpdateInPlace==0 ){
      hctDbInsertEntry(pDb, aTarget, op.iInsert, op.aEntry, op.nEntry);
    }

    assert( (pRec==0)==(eType==HCT_PAGETYPE_INTKEY) );
    if( eType==HCT_PAGETYPE_INTKEY ){
      HctDbIntkeyEntry *pE = &((HctDbIntkeyLeaf*)aTarget)->aEntry[op.iInsert];
      pE->iKey = op.iIntkey;
      pE->nSize = op.nEntrySize;
      pE->flags = op.entryFlags;
    }else if( p->iHeight==0 ){
      HctDbIndexEntry *pE = &((HctDbIndexLeaf*)aTarget)->aEntry[op.iInsert];
      pE->nSize = op.nEntrySize;
      pE->flags = op.entryFlags;
    }else{
      HctDbIndexNodeEntry *pE = &((HctDbIndexNode*)aTarget)->aEntry[op.iInsert];
      pE->nSize = op.nEntrySize;
      pE->flags = op.entryFlags;
      pE->iChildPg = iChildPg;
    }

    /* print_out_page("2", aTarget, pDb->pgsz); */
    assert_page_is_ok(aTarget, pDb->pgsz);
  }

 insert_out:
  if( rc==SQLITE_OK ){
    assert_all_pages_ok(pDb, p);
    assert_all_pages_nonempty(pDb, p);
  }
  return rc;
}

static int hctDbInsertWithRetry(
  HctDatabase *pDb,
  u32 iRoot,
  UnpackedRecord *pRec,           /* The key value for index tables */
  i64 iKey,                       /* For intkey tables, the key value */
  int bDel,                       /* True for a delete, false for insert */
  int nData, const u8 *aData,     /* Record/key to insert */
  int *pnRetry                    /* OUT: number of operations to retry */
){
  int rc = SQLITE_OK;

  rc = hctDbInsert(pDb, &pDb->pa, iRoot, pRec, iKey, 0, bDel, nData, aData);
  if( rc!=SQLITE_OK ){
    hctDbWriterCleanup(pDb, &pDb->pa, 1);
  }
  if( rc==SQLITE_LOCKED || (rc&0xFF)==SQLITE_BUSY ){
    if( rc==SQLITE_LOCKED ){
      rc = SQLITE_OK;
      pDb->nCasFail++;
    }
    *pnRetry = pDb->pa.nWriteKey;
    pDb->pa.nWriteKey = 0;
  }else{
    *pnRetry = 0;
  }

  return rc;
}

int sqlite3HctDbJrnlWrite(
  HctDatabase *pDb,               /* Database to insert into or delete from */
  u32 iRoot,                      /* Root page of hct_journal table */
  i64 iKey,                       /* intkey value */
  int nData, const u8 *aData,     /* Record to insert */
  int *pnRetry                    /* OUT: number of operations to retry */
){
  int rc = SQLITE_OK;

  assert( pDb->eMode==HCT_MODE_NORMAL );
  pDb->eMode = HCT_MODE_ROLLBACK;
  rc = hctDbInsertWithRetry(pDb, iRoot, 0, iKey, 0, nData, aData, pnRetry);
  pDb->eMode = HCT_MODE_NORMAL;
  return rc;
}

/*
** This function is used while debugging hctree only. 
**
** It returns a string describing the current position of cursor pCsr. It
** is the responsibility of the caller to eventually free the returned
** string using sqlite3_free().
*/
static char *hctDbCsrDebugPosition(HctDbCsr *pCsr){
  char *zRet = 0;
  i64 iPg = 0;
  int iCell = 0;
  if( pCsr->nRange ){
    iCell = pCsr->iCell;
    iPg = (i64)pCsr->pg.iOldPg;
  }else{
    iCell = pCsr->aRange[pCsr->nRange-1].iCell;
    iPg = (i64)pCsr->aRange[pCsr->nRange-1].pg.iOldPg;
  }

  zRet = sqlite3HctMprintf("(phys. page %lld, cell %d)", iPg, iCell);
  return zRet;
}

static void hctDbDebugRollbackOp(
  const char *zFunc,
  int iLine,
  HctDatabase *pDb, 
  UnpackedRecord *pRec, 
  i64 iKey, 
  int op
){
  char *zKey = 0;
  char *zRes = 0;
  if( pRec ){
    zKey = sqlite3HctDbUnpackedToText(pRec);
  }else{
    zKey = sqlite3HctMprintf("[%lld]", iKey);
  }

  if( op<0 ){
    zRes = sqlite3HctMprintf("<delete>");
  }else if( op>0 ){
    char *zPos = hctDbCsrDebugPosition(&pDb->rbackcsr);
    const u8 *aData = 0;
    int nData = 0;
    i64 iTid = (i64)hctDbCsrTid(&pDb->rbackcsr);

    sqlite3HctDbCsrData(&pDb->rbackcsr, &nData, &aData);
    zRes = sqlite3HctDbRecordToText(0, aData, nData);
    zRes = sqlite3HctMprintf("(%z) from %z (tid=%lld)", zRes, zPos, iTid);
  }else{
    zRes = sqlite3HctMprintf("<no-op>");
  }

  hctExtraWriteLogging(zFunc, iLine,
      sqlite3HctMprintf("%p: rollback of (%s) -> %s" , pDb, zKey, zRes)
  );

  sqlite3_free(zKey);
  sqlite3_free(zRes);
}

#define HCT_EXTRA_WR_LOGGING_ROLLBACK(a,b,c,d) if( hct_extra_write_logging ) { hctDbDebugRollbackOp(__func__, __LINE__, a,b,c,d); }

static void hctDbDebugInsertOp(
  const char *zFunc,
  int iLine,
  HctDatabase *pDb, 
  UnpackedRecord *pRec, 
  i64 iKey, 
  int bDel,
  int nData, const u8 *aData
){
  char *zKey = 0;
  char *zRes = 0;
  if( pRec ){
    zKey = sqlite3HctDbUnpackedToText(pRec);
  }else{
    zKey = sqlite3HctMprintf("[%lld]", iKey);
  }

  if( bDel ){
    zRes = sqlite3HctMprintf("<delete>");
  }else{
    zRes = sqlite3HctDbRecordToText(0, aData, nData);
    zRes = sqlite3HctMprintf("(%z)", zRes);
  }

  hctExtraWriteLogging(zFunc, iLine,
      sqlite3HctMprintf("%p: insert of (%s) -> %s" , pDb, zKey, zRes)
  );

  sqlite3_free(zKey);
  sqlite3_free(zRes);
}

#define HCT_EXTRA_WR_LOGGING_INSERT(a,b,c,d,e,f) if( hct_extra_write_logging ) { hctDbDebugInsertOp(__func__, __LINE__, a,b,c,d,e,f); }

int sqlite3HctDbInsert(
  HctDatabase *pDb,               /* Database to insert into or delete from */
  u32 iRoot,                      /* Root page of table to modify */
  UnpackedRecord *pRec,           /* The key value for index tables */
  i64 iKey,                       /* For intkey tables, the key value */
  int bDel,                       /* True for a delete, false for insert */
  int nData, const u8 *aData,     /* Record/key to insert */
  int *pnRetry                    /* OUT: number of operations to retry */
){
  int rc = SQLITE_OK;
  int nRecField = pRec ? pRec->nField : 0;

  if( iRoot==1 ){
    /* If the sqlite_schema table is being written, set iReqSnapshotId to
    ** a very large value. The upper layer will interpret this as the
    ** transaction depending on snapshot (iCid-1) for the purposes of the
    ** hct_journal table, where iCid is (yet to be assigned) CID value
    ** of this transaction.  */
    pDb->iReqSnapshotId = LARGEST_TID;
  }

  /* If this operation is inserting an index entry, figure out how many of
  ** the record fields to consider when determining if a potential write
  ** collision is found in the data structure.  */
  sqlite3HctDbRecordTrim(pRec);

#if 0
  {
    char *zText = sqlite3HctDbRecordToText(0, aData, nData);
    sqlite3HctFileDebugPrint(pDb->pFile, 
        "%p: %s sqlite3HctDbInsert(bDel=%d, iKey=%lld, aData={%s}) iTid=%lld\n",
          pDb, 
          (pDb->eMode==HCT_MODE_ROLLBACK ? "RB" : "  "),
          bDel, iKey, zText, (i64)pDb->iTid
    );
    fflush(stdout);
  }
#endif

  assert( pDb->eMode==HCT_MODE_NORMAL 
       || pDb->eMode==HCT_MODE_ROLLBACK
  );
  if( pDb->eMode==HCT_MODE_ROLLBACK ){
    int op = 0;

    pDb->pa.bDoCleanup = 1;
    if( pDb->rbackcsr.iRoot!=iRoot ){
      hctDbCsrCleanup(&pDb->rbackcsr);
      hctDbCsrInit(pDb, iRoot, 0, &pDb->rbackcsr);
      if( pRec ){
        pDb->rbackcsr.pKeyInfo = sqlite3KeyInfoRef(pRec->pKeyInfo);
      }
    }else{
      hctDbCsrReset(&pDb->rbackcsr);
    }

    rc = sqlite3HctDbCsrRollbackSeek(&pDb->rbackcsr, pRec, iKey, &op);
    if( rc==SQLITE_OK ){
      HCT_EXTRA_WR_LOGGING_ROLLBACK(pDb, pRec, iKey, op);
      if( op<0 ){
        bDel = 1;
        aData = 0;
        nData = 0;
      }else if( op>0 ){
        rc = sqlite3HctDbCsrData(&pDb->rbackcsr, &nData, &aData);
        bDel = 0;

      }else{
        /* TODO: It would be nice to assert( op!=0 ) here, but this fails
        ** if the original op being rolled back was a no-op delete. If
        ** we could note these as they occur, we could bring a form
        ** of this assert() back.  */
        /* assert( op!=0 ); */
        pDb->pa.nWriteKey++;
        goto insert_done;
      }
    }
  }else{
    HCT_EXTRA_WR_LOGGING_INSERT(pDb, pRec, iKey, bDel, nData, aData);
  }

  if( rc==SQLITE_OK ){
    rc = hctDbInsertWithRetry(pDb,iRoot,pRec,iKey, bDel, nData, aData, pnRetry);
    if( rc!=SQLITE_OK ){
      hctDbWriterCleanup(pDb, &pDb->pa, 1);
    }
  }

 insert_done:
  if( pDb->eMode==HCT_MODE_ROLLBACK ){
    hctDbCsrCleanup(&pDb->rbackcsr);
  }
  if( pRec ) pRec->nField = nRecField;
  assert( pDb->rbackcsr.pRec==0 );
  return rc;
}


/*
** Load the key associated with cell iCell1 on page aPg1[] and compare
** it to pKey2. Return an integer less than, equal to or greater than
** zero if the loaded key is less than, equal to or greater than pKey2,
** respectively. i.e.
**
**   ret = key(aPg1, iCell1) - (*pKey2)
**
** Value iDefaultRet is returned if page aPg1 is an index page and 
** pKey2->pKey is NULL.
*/
static int hctDbCompareCellKey(
  int *pRc,
  HctDatabase *pDb,
  const u8 *aPg1,
  int iCell1,
  HctDbKey *pKey2,
  int iDefaultRet
){
  int ret = 0;
  if( *pRc==SQLITE_OK ){

    assert( hctPagetype(aPg1)==HCT_PAGETYPE_INTKEY
        || hctPagetype(aPg1)==HCT_PAGETYPE_INDEX
    );
    if( hctPagetype(aPg1)==HCT_PAGETYPE_INTKEY ){
      i64 iKey = hctDbGetIntkey(aPg1, iCell1);
      if( iKey<pKey2->iKey ){
        ret = -1;
      }else if( iKey>pKey2->iKey ){
        ret = +1;
      }
    }else if( pKey2->pKey==0 ){
      ret = iDefaultRet;
    }else{
      int nRec = 0;
      const u8 *aRec = 0;
      HctBuffer buf = {0,0,0};
      int rc = hctDbLoadRecord(pDb, &buf, aPg1, iCell1, &nRec, &aRec);
      if( rc!=SQLITE_OK ){
        *pRc = rc;
      }else if( nRec==0 ){
        ret = -1;
      }else{
        ret = sqlite3VdbeRecordCompare(nRec, aRec, pKey2->pKey);
      }
      sqlite3HctBufferFree(&buf);
    }
  }

  return ret;
}

/*
** Page pPg is currently the root page of a table being written by a 
** direct-write cursor.
*/
static int hctDbDirectSplitRoot(HctDatabase *pDb, HctFilePage *pPg){
  HctFilePage peer;
  int rc = SQLITE_OK;

  rc = sqlite3HctFilePageNew(pDb->pFile, &peer);
  if( rc==SQLITE_OK ){
    u8 pgtype = hctPagetype(pPg->aOld);

    memcpy(peer.aNew, pPg->aOld, pDb->pgsz);
    hctDbRootPageInit(pgtype==HCT_PAGETYPE_INDEX, 
        hctPageheight(peer.aNew) + 1, peer.iPg, pPg->aOld, pDb->pgsz
    );

    rc = sqlite3HctFilePageRelease(pPg);
    memcpy(pPg, &peer, sizeof(HctFilePage));
  }

  return rc;
}

static int hctDbDirectInsertAt(
  HctDbCsr *pCsr,
  int bStrictAppend,
  int nHeight, int iChildPg,
  UnpackedRecord *pRec, i64 iKey, 
  int nData, const u8 *aData,
  int *pbFail                     /* Set to true if insert NOT handled */
){
  HctDatabase *pDb = pCsr->pDb;
  HctFilePage pg;                 /* Page object to write to */
  u32 iPg = pCsr->iRoot;          /* Logical page number */
  int rc = SQLITE_OK;             /* Return Code */
  int nLocal;                     /* Bytes of local payload */

  memset(&pg, 0, sizeof(pg));

  assert( bStrictAppend==0 || nHeight==0 );

  if( bStrictAppend ){
    memcpy(&pg, &pCsr->pg, sizeof(pg));
    memset(&pCsr->pg, 0, sizeof(pg));
    pCsr->iCell = -1;
  }else{
    /* Set object pg to refer to the rightmost page of the level to write. */
    while( rc==SQLITE_OK ){
      rc = sqlite3HctFilePageGet(pDb->pFile, iPg, &pg);
      if( rc==SQLITE_OK ){
        HctDbPageHdr *pPg = (HctDbPageHdr*)pg.aOld;
        if( pPg->nHeight==nHeight ) break;
        iPg = hctDbGetChildPage((u8*)pPg, pPg->nEntry-1);
        sqlite3HctFilePageRelease(&pg);
      }
    }
    if( rc!=SQLITE_OK ) goto direct_insert_out;

    /* Page pg is now the rightmost page of the b-tree level to write to. 
    ** If nHeight==0, check to see if this operation really is an append. 
    ** If nHeight>0, it is guaranteed to be.  */
    if( nHeight==0 ){
      int nEntry = hctPagenentry(pg.aOld);
      if( nEntry>0 ){
        HctDbKey k;
        memset(&k, 0, sizeof(k));
        k.iKey = iKey;
        k.pKey = pRec;
        if( hctDbCompareCellKey(&rc, pDb, pg.aOld, nEntry-1, &k, -1)>=0 ){
          *pbFail = 1;
          goto direct_insert_out;
        }
        sqlite3HctBufferFree(&k.buf);
      }
    }
  }

  if( hctPagetype(pg.aOld)==HCT_PAGETYPE_INTKEY && hctPageheight(pg.aOld)>0 ){
    int nMax = hctDbMaxCellsPerIntkeyNode(pDb->pgsz);
    HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)pg.aOld;

    if( pNode->pg.nEntry>=nMax ){
      /* The new entry will not fit on this page. */
      HctDbIntkeyNode *pPeer;
      HctFilePage peer;

      if( pg.iPg==pCsr->iRoot ){
        rc = hctDbDirectSplitRoot(pDb, &pg);
        if( rc ) goto direct_insert_out;
        pNode = (HctDbIntkeyNode*)pg.aNew;
      }

      rc = sqlite3HctFilePageNew(pDb->pFile, &peer);
      if( rc ) goto direct_insert_out;
      pNode->pg.iPeerPg = peer.iPg;
      pPeer = (HctDbIntkeyNode*)peer.aNew;

      memset(pPeer, 0, sizeof(*pPeer));
      pPeer->pg.hdrFlags = hctPagetype(pNode);
      pPeer->pg.nHeight = pNode->pg.nHeight;

      rc = hctDbDirectInsertAt(
          pCsr, 0, nHeight+1, peer.iPg, 0, iKey, 0, 0, 0
      );
      if( rc ) goto direct_insert_out;

      rc = sqlite3HctFilePageRelease(&pg);
      if( rc ){
        sqlite3HctFilePageRelease(&peer);
        goto direct_insert_out;
      }

      memcpy(&pg, &peer, sizeof(HctFilePage));
      pNode = pPeer;
    }

    pNode->aEntry[pNode->pg.nEntry].iChildPg = iChildPg;
    pNode->aEntry[pNode->pg.nEntry].iKey = iKey;
    pNode->pg.nEntry++;
  }else{
    HctDbCell cell;                 /* New cell to insert */
    const int szEntry = hctDbPageEntrySize(pg.aOld);
    int nReq = 0;
    int iOff = 0;
    HctDbLeaf *pPg = (HctDbLeaf*)pg.aOld;

    memset(&cell, 0, sizeof(cell));

    /* Write any required overflow pages. Figure out how much space is required
    ** by the new entry at the same time. */
    rc = hctDbInsertOverflow(pDb, 0, pg.aOld, nData, aData,&nLocal,&cell.iOvfl);
    if( rc!=SQLITE_OK ) goto direct_insert_out;

    cell.aPayload = aData;
    nReq = szEntry + nLocal + (cell.iOvfl ? 4 : 0);
    if( nReq>pPg->hdr.nFreeGap ){
      HctDbLeaf *pPeer = 0;
      HctFilePage peer;

      /* If this operation causes the root page to split, handle that. */
      if( pg.iPg==pCsr->iRoot ){
        rc = hctDbDirectSplitRoot(pDb, &pg);
        if( rc ) goto direct_insert_out;
        pPg = (HctDbLeaf*)pg.aNew;
      }

      rc = sqlite3HctFilePageNew(pDb->pFile, &peer);
      if( rc ) goto direct_insert_out;
      pPg->pg.iPeerPg = peer.iPg;
      pPeer = (HctDbLeaf*)peer.aNew;
      memset(pPeer, 0, sizeof(*pPeer));
      pPeer->pg.hdrFlags = hctPagetype(pPg);
      pPeer->pg.nHeight = pPg->pg.nHeight;
      pPeer->hdr.nFreeGap = pDb->pgsz - 
        (hctPageheight(pPg)==0 ? sizeof(HctDbLeaf) : sizeof(HctDbIndexNode));
      pPeer->hdr.nFreeBytes = pPeer->hdr.nFreeGap;

      rc = hctDbDirectInsertAt(
          pCsr, 0, nHeight+1, peer.iPg, pRec, iKey, nData, aData, 0
      );
      if( rc ) goto direct_insert_out;

      rc = sqlite3HctFilePageRelease(&pg);
      if( rc ){
        sqlite3HctFilePageRelease(&peer);
        goto direct_insert_out;
      }
      memcpy(&pg, &peer, sizeof(HctFilePage));
      pPg = pPeer;
    }else{
      rc = sqlite3HctFilePageDirectWrite(&pg);
      if( rc ) goto direct_insert_out;
      pPg = (HctDbLeaf*)pg.aNew;
    }

    if( hctPagetype(pPg)==HCT_PAGETYPE_INTKEY ){
      HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)pPg;
      HctDbIntkeyEntry *pE = &pLeaf->aEntry[pLeaf->pg.nEntry];
      iOff = sizeof(HctDbIntkeyLeaf) 
           + pLeaf->pg.nEntry * sizeof(HctDbIntkeyEntry) 
           + pLeaf->hdr.nFreeGap - (nLocal + (cell.iOvfl ? 4 : 0));

      hctDbCellPut(&((u8*)pLeaf)[iOff], &cell, nLocal);
      pE->nSize = nData;
      pE->iOff = iOff;
      pE->flags = hctDbCellToFlags(&cell);
      pE->iKey = iKey;
    }else if( hctPageheight(pPg)==0 ){
      HctDbIndexLeaf *pLeaf = (HctDbIndexLeaf*)pPg;
      HctDbIndexEntry *pE = &pLeaf->aEntry[pLeaf->pg.nEntry];
      iOff = sizeof(HctDbIndexLeaf) 
           + pLeaf->pg.nEntry * sizeof(HctDbIndexEntry) 
           + pLeaf->hdr.nFreeGap - (nLocal + (cell.iOvfl ? 4 : 0));

      hctDbCellPut(&((u8*)pLeaf)[iOff], &cell, nLocal);
      pE->nSize = nData;
      pE->iOff = iOff;
      pE->flags = hctDbCellToFlags(&cell);
    }else{
      HctDbIndexNode *pNode = (HctDbIndexNode*)pPg;
      HctDbIndexNodeEntry *pE = &pNode->aEntry[pNode->pg.nEntry];
      iOff = sizeof(HctDbIndexNode) 
           + pNode->pg.nEntry * sizeof(HctDbIndexNodeEntry) 
           + pNode->hdr.nFreeGap - (nLocal + (cell.iOvfl ? 4 : 0));
      pE->nSize = nData;
      pE->iOff = iOff;
      pE->flags = hctDbCellToFlags(&cell);
      pE->iChildPg = iChildPg;
    }

    hctDbCellPut(&((u8*)pPg)[iOff], &cell, nLocal);
    pPg->pg.nEntry++;
    pPg->hdr.nFreeGap -= nReq;
    pPg->hdr.nFreeBytes -= nReq;
  }

  {
    u32 iRight = pg.iPg;
    rc = sqlite3HctFilePageRelease(&pg);
    if( nHeight==0 ){
      sqlite3HctFilePageRelease(&pCsr->pg);
      sqlite3HctFilePageGet(pDb->pFile, iRight, &pCsr->pg);
      pCsr->iCell = hctPagenentry(pCsr->pg.aOld)-1;
      assert( sqlite3HctDbCsrIsLast(pCsr) );
    }
  }
  return rc;

 direct_insert_out:
  sqlite3HctFilePageRelease(&pg);
  return rc;
}

int sqlite3HctDbDirectInsert(
  HctDbCsr *pCsr, 
  int bStrictAppend,
  UnpackedRecord *pRec, i64 iKey, 
  int nData, const u8 *aData,
  int *pbFail
){
  int rc = SQLITE_OK;

  /* The TID has not yet been allocated */
  assert( pCsr->pDb->iTid==0 );

  rc = hctDbDirectInsertAt(
      pCsr, bStrictAppend, 0, 0, pRec, iKey, nData, aData, pbFail
  );
  return rc;
}

/*
** Start the write-phase of a transaction.
*/
int sqlite3HctDbStartWrite(HctDatabase *p, u64 *piTid){
  int rc = SQLITE_OK;
  HctTMapClient *pTMapClient = sqlite3HctFileTMapClient(p->pFile);

  assert( p->iTid==0 );
  assert( p->eMode==HCT_MODE_NORMAL );
  memset(&p->pa, 0, sizeof(p->pa));
  hctDbPageArrayReset(&p->pa.writepg);
  hctDbPageArrayReset(&p->pa.discardpg);

  p->nWriteCount = sqlite3HctFileWriteCount(p->pFile);
  p->iTid = sqlite3HctFileAllocateTransid(p->pFile);
  rc = sqlite3HctTMapNewTID(pTMapClient, p->iTid, &p->pTmap);
  *piTid = p->iTid;
  return rc;
}

static u64 *hctDbFindTMapEntry(HctTMap *pTmap, u64 iTid){
  int iMap, iEntry;
  assert( pTmap->iFirstTid<=iTid );
  assert( pTmap->iFirstTid+(pTmap->nMap*HCT_TMAP_PAGESIZE)>iTid );
  iMap = (iTid - pTmap->iFirstTid) / HCT_TMAP_PAGESIZE;
  iEntry = (iTid - pTmap->iFirstTid) % HCT_TMAP_PAGESIZE;

  iEntry = HCT_TMAP_ENTRYSLOT(iEntry);
  return &pTmap->aaMap[iMap][iEntry];
}

/*
** This is called once the current transaction has been completely 
** written to disk and validated. The CID is passed as the second argument.
** Or, if the transaction was abandoned and rolled back, iCid is passed
** zero.
*/
int sqlite3HctDbEndWrite(HctDatabase *p, u64 iCid, int bRollback){
  int rc = SQLITE_OK;
  u64 *pEntry = hctDbFindTMapEntry(p->pTmap, p->iTid);

  assert( p->eMode==HCT_MODE_NORMAL );
  assert( p->pa.writepg.nPg==0 );

  if( iCid==0 ){
    assert( bRollback );
    assert( p->iSnapshotId );
    iCid = p->iSnapshotId;
  }

  HctAtomicStore(pEntry, iCid|(bRollback?HCT_TMAP_ROLLBACK:HCT_TMAP_COMMITTED));
  p->iTid = 0;
  return rc;
}

static void hctDbFreeCsrList(HctDbCsr *pList){
  HctDbCsr *pNext = pList;
  while( pNext ){
    HctDbCsr *pDel = pNext;
    pNext = pNext->pNextScanner;
    hctDbFreeCsr(pDel);
  }
}

int sqlite3HctDbEndRead(HctDatabase *pDb){
  HctTMapClient *pTMapClient = sqlite3HctFileTMapClient(pDb->pFile);
  // assert( (pDb->iSnapshotId==0)==(pDb->pTmap==0) );
  hctDbFreeCsrList(pDb->pScannerList);
  pDb->pScannerList = 0;
  if( pDb->pTmap ){
    sqlite3HctTMapEnd(pTMapClient, pDb->iSnapshotId);
    pDb->pTmap = 0;
    pDb->iSnapshotId = 0;
    pDb->iReqSnapshotId = 0;
    pDb->bConcurrent = 0;
  }
  return SQLITE_OK;
}

/*
** If recovery is still required, this function grabs the file-server
** mutex and returns non-zero. Or, if recovery is not required, returns
** zero without grabbing the mutex.
*/
int sqlite3HctDbStartRecovery(HctDatabase *pDb, int iStage){
  return sqlite3HctFileStartRecovery(pDb->pFile, iStage);
}

void sqlite3HctDbRecoverTid(HctDatabase *pDb, u64 iTid){
  pDb->iTid = iTid;
  pDb->iLocalMinTid = iTid ? iTid-1 : 0;
}

int sqlite3HctDbFinishRecovery(HctDatabase *pDb, int iStage, int rc){
  /* assert( pDb->eMode==HCT_MODE_ROLLBACK ); */
  assert( iStage==0 || iStage==1 );
  assert( pDb->iSnapshotId>0 || rc!=SQLITE_OK );

  pDb->iTid = 0;
  pDb->eMode = HCT_MODE_NORMAL;
  pDb->iSnapshotId = 0;
  pDb->iReqSnapshotId = 0;
  pDb->iLocalMinTid = 0;
  return sqlite3HctFileFinishRecovery(pDb->pFile, iStage, rc);
}

/*
** Open a cursor.
*/
int sqlite3HctDbCsrOpen(
  HctDatabase *pDb, 
  KeyInfo *pKeyInfo,
  u32 iRoot, 
  HctDbCsr **ppCsr
){
  int rc = SQLITE_OK;
  HctDbCsr *p;

  assert( pDb->iSnapshotId!=0 );

  HCT_EXTRA_LOGGING(pDb, (
      "opening cursor with snapshot=%lld, iLocalMinTid=%lld", 
      pDb->iSnapshotId, pDb->iLocalMinTid
  ));

  /* Search for an existing cursor that can be reused. */
  HctDbCsr **pp;
  for(pp=&pDb->pScannerList; *pp; pp=&(*pp)->pNextScanner){
    if( (*pp)->iRoot==iRoot ){
      *ppCsr = *pp;
      *pp = (*pp)->pNextScanner;
      return SQLITE_OK;
    }
  }

  /* If no existing cursor was found, allocate a new one */
  p = (HctDbCsr*)sqlite3MallocZero(sizeof(HctDbCsr));
  if( p==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    p->pDb = pDb;
    p->iRoot = iRoot;
    p->iCell = -1;
    p->pKeyInfo = pKeyInfo;
    sqlite3KeyInfoRef(pKeyInfo);
  }
  *ppCsr = p;
  return rc;
}

/*
** Set the "no-snapshot" flag on the cursor passed as the first argument.
*/
void sqlite3HctDbCsrNosnap(HctDbCsr *pCsr, int bNosnap){
  if( pCsr ) pCsr->bNosnap = bNosnap;
}

void sqlite3HctDbCsrNoscan(HctDbCsr *pCsr, int bNoscan){
  if( pCsr ) pCsr->bNoscan = bNoscan;
}

/*
** Close a cursor opened with sqlite3HctDbCsrOpen().
*/
void sqlite3HctDbCsrClose(HctDbCsr *pCsr){
  if( pCsr ){
    HctDatabase *pDb = pCsr->pDb;
    hctDbCsrScanFinish(pCsr);
    hctDbCsrReset(pCsr);
    if( hctDbIsConcurrent(pDb) && pDb->iTid==0 ){
      pCsr->pNextScanner = pDb->pScannerList;
      pDb->pScannerList = pCsr;
    }else{
      hctDbFreeCsr(pCsr);
    }
  }
}

/*
** The cursor passed as the first argument must be open on an intkey
** table and pointed at a valid entry. This function sets output variable
** (*piKey) to the integer key value associated with that entry before 
** returning.
*/
void sqlite3HctDbCsrKey(HctDbCsr *pCsr, i64 *piKey){
  int iCell = 0;
  const u8 *aPg = 0;

  aPg = hctDbCsrPageAndCell(pCsr, &iCell);
  *piKey = hctDbGetIntkey(aPg, iCell);
}

/*
** Return true if the cursor is at EOF. Otherwise false.
*/
int sqlite3HctDbCsrEof(HctDbCsr *pCsr){
  return pCsr==0 || pCsr->iCell<0;
}

/*
** Set the cursor to point to the first entry in its table. If it is
** stepped, this cursor will be stepped with sqlite3HctDbCsrNext().
*/
int sqlite3HctDbCsrFirst(HctDbCsr *pCsr){
  int rc = SQLITE_OK;

  rc = hctDbCsrScanFinish(pCsr);
  if( rc==SQLITE_OK ){
    hctDbCsrReset(pCsr);
    pCsr->eDir = BTREE_DIR_FORWARD;
    rc = hctDbCsrScanStart(pCsr, 0, SMALLEST_INT64);
  }
  pCsr->eDir = BTREE_DIR_FORWARD;

  if( rc==SQLITE_OK ){
    rc = hctDbCsrFirstValid(pCsr);
  }

  return rc;
}

/*
** Set the cursor to point to the last entry in its table. If it is
** stepped, this cursor will be stepped with sqlite3HctDbCsrPrev().
*/
int sqlite3HctDbCsrLast(HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  HctFile *pFile = pCsr->pDb->pFile;
  u32 iPg = pCsr->iRoot;
  HctDbPageHdr *pPg = 0;
  HctFilePage pg;

  rc = hctDbCsrScanFinish(pCsr);
  if( rc==SQLITE_OK ){
    hctDbCsrReset(pCsr);
    pCsr->eDir = BTREE_DIR_REVERSE;
    rc = hctDbCsrScanStart(pCsr, 0, LARGEST_INT64);
  }

  /* Find the last page in the leaf page list. */
  while( 1 ){
    rc = sqlite3HctFilePageGet(pFile, iPg, &pg);
    if( rc!=SQLITE_OK ) break;

    pPg = (HctDbPageHdr*)pg.aOld;
    if( pPg->iPeerPg ){
      iPg = pPg->iPeerPg;
    }else if( pPg->nHeight==0 ){
      break;
    }else if( hctPagetype(pPg)==HCT_PAGETYPE_INTKEY ){
      HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)pPg;
      iPg = pNode->aEntry[pPg->nEntry-1].iChildPg;
    }else{
      HctDbIndexNode *pNode = (HctDbIndexNode*)pPg;
      iPg = pNode->aEntry[pPg->nEntry-1].iChildPg;
    }
    sqlite3HctFilePageRelease(&pg);
  }

  /* Set the cursor to point to one position past the last entry on the
  ** page located above. Then call sqlite3HctDbCsrPrev() to step back to
  ** the first entry visible to the current transaction.  */
  if( rc==SQLITE_OK ){
    assert( pPg->nHeight==0 && pPg->iPeerPg==0 );
    hctMemcpy(&pCsr->pg, &pg, sizeof(pg));
    if( pPg->nEntry==0 ){
      pCsr->iCell = -1;
    }else{
      pCsr->iCell = pPg->nEntry;
      rc = sqlite3HctDbCsrPrev(pCsr);
    }
  }

  return rc;
}

static int hctDbCsrNext(HctDbCsr *pCsr){
  HctDatabase *pDb = pCsr->pDb;
  HctDbPageHdr *pPg = 0;
  int rc = SQLITE_OK;

  /* Check if the current cell, be it on the linked list of leaves, or
  ** on a history page, has an old-data pointer that should be followed. 
  **
  ** Except, don't do this if pCsr->iCell is less than zero. In that
  ** case this call is supposed to jump to the first cell on the main
  ** page.  */
  if( pCsr->iCell>=0 ){
    do {
      int bMerge = 0;
      HctRangePtr ptr;

      hctDbCsrGetRange(pCsr, &ptr);
      if( hctDbFollowRangeOld(pDb, &ptr, &bMerge) ){
        hctDbCsrDescendRange(&rc, pCsr, ptr.iRangeTid, ptr.iOld, bMerge);
        if( rc==SQLITE_OK ){
          HctDbRangeCsr *p = &pCsr->aRange[pCsr->nRange-1];
          if( hctDbCheckForOverlap(pCsr->pKeyInfo, &p->lowkey, &p->highkey) ){
            HCT_EXTRA_LOGGING_NO_NEWRANGECSR(pCsr);
            hctDbCsrAscendRange(pCsr);
          }else{

            if( p->eRange==HCT_RANGE_FAN 
             || hctDbKeyIsNull(pCsr->pKeyInfo, &p->lowkey)
            ){
              p->iCell = -1;
            }else{
              int bExact = 0;
              hctDbLeafSearch(pDb, p->pg.aOld, 
                  p->lowkey.iKey, p->lowkey.pKey, &p->iCell, &bExact
              );

              if( bExact==0 ) p->iCell--;
              if( p->iCell>=0 ) p->iCell--;
            }

            HCT_EXTRA_LOGGING_NEWRANGECSR(pCsr);
          }
        }
      }

      while( pCsr->nRange ){
        HctDbRangeCsr *p = &pCsr->aRange[pCsr->nRange-1];

        p->iCell++;
        HCT_EXTRA_LOGGING(pDb, (
            "moved range cursor (%d) to: %z",
            pCsr->nRange-1, hctDbCsrDebugCsrPos(pCsr)
        ));
        if( p->iCell<hctPagenentry(p->pg.aOld) && (
            p->eRange==HCT_RANGE_FAN 
         || hctDbCompareCellKey(&rc, pDb, p->pg.aOld, p->iCell, &p->highkey, -1)<0
        )){

          if( p->eRange==HCT_RANGE_MERGE 
           && hctDbCompareCellKey(&rc,pDb,p->pg.aOld,p->iCell,&p->lowkey,1)<0
          ){
            HCT_EXTRA_LOGGING(pDb, ("lowkey breaking..."));
            break;
          }

          if( p->eRange==HCT_RANGE_MERGE ){
            HCT_EXTRA_LOGGING(pDb, ("returning..."));
            return SQLITE_OK;
          }
          break;
        }
        HCT_EXTRA_LOGGING(pDb, ("range cursor %d at EOF", pCsr->nRange-1));
        hctDbCsrAscendRange(pCsr);
      }

    }while( pCsr->nRange );

  }

  pPg = (HctDbPageHdr*)pCsr->pg.aOld;
  assert( pCsr->iCell>=-1 && pCsr->iCell<pPg->nEntry );
  assert( pPg->nHeight==0 );

  pCsr->iCell++;
  HCT_EXTRA_LOGGING(pDb, (
      "moving to cell %d on physical page %lld",
      pCsr->iCell, pCsr->pg.iOldPg
  ));
  if( pCsr->iCell==pPg->nEntry ){
    u32 iPeerPg = pPg->iPeerPg;
    if( iPeerPg==0 ){
      /* Main cursor is now at EOF */
      pCsr->iCell = -1;
      sqlite3HctFilePageRelease(&pCsr->pg);
      HCT_EXTRA_LOGGING(pDb, ("at EOF"));
    }else{
      /* Jump to peer page */
      rc = sqlite3HctFilePageRelease(&pCsr->pg);
      if( rc==SQLITE_OK ){
        rc = sqlite3HctFilePageGet(pDb->pFile, iPeerPg, &pCsr->pg);
        pCsr->iCell = 0;
      }
      HCT_EXTRA_LOGGING(pDb, (
          "jump to peer page %lld (physical %lld)", iPeerPg, pCsr->pg.iOldPg
      ));
    }
  }

  return rc;
}

static int hctDbCsrGoLeft(HctDbCsr *pCsr, int bFullSeek){
  int rc = SQLITE_OK;

  assert( pCsr->nRange==0 );
  if( hctIsLeftmost(pCsr->pg.aOld)==0 ){
    HctDbKey k;

    memset(&k, 0, sizeof(k));
    hctDbGetKey(&rc, pCsr->pDb, pCsr->pKeyInfo, 1, pCsr->pg.aOld, 0, &k);
    hctDbDecrementKey(&k);

    if( rc==SQLITE_OK ){
      int res = 0;
      if( bFullSeek ){
        assert( pCsr->eDir==BTREE_DIR_REVERSE );
        rc = sqlite3HctDbCsrSeek(pCsr, k.pKey, k.iKey, &res);
      }else{
        int nHeight = ((HctDbPageHdr*)pCsr->pg.aOld)->nHeight;
        hctDbCsrSeek(pCsr, 0, nHeight, 0, k.pKey, k.iKey, &res);
      }
      assert( rc!=SQLITE_OK || res<0 || res==0 );
    }

    hctDbFreeKeyContents(&k);

#if 0
    int nHeight = ((HctDbPageHdr*)pCsr->pg.aOld)->nHeight;
    if( pCsr->pKeyInfo ){
      UnpackedRecord *pRec = 0;
      pCsr->iCell = 0;
      rc = sqlite3HctDbCsrLoadAndDecode(pCsr, &pRec);
      if( rc==SQLITE_OK ){
        int bDummy;
        HctFilePage pg = pCsr->pg;
        memset(&pCsr->pg, 0, sizeof(HctFilePage));
        pRec->default_rc = 1;
        hctDbCsrSeek(pCsr, 0, nHeight, 0, pRec, 0, &bDummy);
        pRec->default_rc = 0;
        sqlite3HctFilePageRelease(&pg);
      }
    }else{
      i64 iKey = hctDbIntkeyFPKey(pCsr->pg.aOld);
      sqlite3HctFilePageRelease(&pCsr->pg);
      rc = hctDbCsrSeek(pCsr, 0, nHeight, 0, 0, iKey-1, 0);
    }
#endif
  }

  return rc;
}

static int hctDbCsrPrev(HctDbCsr *pCsr){
  HctDatabase *pDb = pCsr->pDb;
  int rc = SQLITE_OK;

  if( pCsr->nRange ){
    HctDbRangeCsr *pRange = &pCsr->aRange[pCsr->nRange-1];
    pRange->iCell--;
  }else{
    pCsr->iCell--;
    if( pCsr->iCell<0 ){
      return hctDbCsrGoLeft(pCsr, 1);
    }
  }

  if( pCsr->iCell>=0 ){
    do {
      HctRangePtr ptr;
      int bMerge = 0;

      hctDbCsrGetRange(pCsr, &ptr);
      if( hctDbFollowRangeOld(pDb, &ptr, &bMerge) ){
        do {
          hctDbCsrDescendRange(&rc, pCsr, ptr.iRangeTid, ptr.iOld, bMerge);
          memset(&ptr, 0, sizeof(ptr));

          if( rc==SQLITE_OK ){
            HctDbRangeCsr *p = &pCsr->aRange[pCsr->nRange-1];
            if( hctDbCheckForOverlap(pCsr->pKeyInfo, &p->lowkey, &p->highkey) ){
              HCT_EXTRA_LOGGING_NO_NEWRANGECSR(pCsr);
              hctDbCsrAscendRange(pCsr);
            }else{
              if( p->eRange==HCT_RANGE_FAN 
               || hctDbKeyIsNull(pCsr->pKeyInfo, &p->highkey)
              ){
                p->iCell = ((HctDbPageHdr*)p->pg.aOld)->nEntry-1;
              }else{
                int bExact;
                hctDbLeafSearch(pDb, p->pg.aOld, 
                    p->highkey.iKey, p->highkey.pKey, &p->iCell, &bExact
                );
                p->iCell--;
              }

              if( p->iCell>=0 ){
                hctDbCsrGetRange(pCsr, &ptr);
              }
              HCT_EXTRA_LOGGING_NEWRANGECSR(pCsr);
            }
          }
        }while( hctDbFollowRangeOld(pDb, &ptr, &bMerge) );
      }

      while( pCsr->nRange>0 ){
        HctDbRangeCsr *p = &pCsr->aRange[pCsr->nRange-1];
        if( p->iCell>=0 && (
            p->eRange==HCT_RANGE_FAN
         || hctDbCompareCellKey(&rc, pDb, p->pg.aOld, p->iCell, &p->lowkey,1)>=0
        )){
          if( p->eRange==HCT_RANGE_MERGE ){
            return SQLITE_OK;
          }
          p->iCell--;
          break;
        }
        HCT_EXTRA_LOGGING(pDb, ("range cursor %d at EOF", pCsr->nRange-1));
        hctDbCsrAscendRange(pCsr);
      }
    }while( pCsr->nRange );
  }

  return rc;
}

static void hctExtraLoggingLogCsrPos(
  const char *zFunc, 
  int iLine, 
  HctDbCsr *pCsr
){
  char *zMsg = hctDbCsrDebugCsrPos(pCsr);
  hctExtraLogging(zFunc, iLine, pCsr->pDb,
      sqlite3_mprintf("RETURNING: %z", zMsg)
  );
}

#define HCT_EXTRA_LOGGING_CSRPOS(pCsr)                  \
  if( pCsr->pDb->pConfig->bHctExtraLogging ){           \
    hctExtraLoggingLogCsrPos(__func__, __LINE__, pCsr); \
  }

int sqlite3HctDbCsrNext(HctDbCsr *pCsr){
  int rc = SQLITE_OK;

  /* Should not be called while committing, validating or doing rollback. */
  assert( pCsr->pDb->iTid==0 && pCsr->pDb->eMode==HCT_MODE_NORMAL );

  do {
    rc = hctDbCsrNext(pCsr);
  }while( rc==SQLITE_OK && pCsr->iCell>=0 && hctDbCurrentIsVisible(pCsr)==0 );

  HCT_EXTRA_LOGGING_CSRPOS(pCsr);

  return rc;
}

int sqlite3HctDbCsrPrev(HctDbCsr *pCsr){
  int rc = SQLITE_OK;

  assert( pCsr->pDb->eMode==HCT_MODE_NORMAL );
  do {
    rc = hctDbCsrPrev(pCsr);
  }while( rc==SQLITE_OK && pCsr->iCell>=0 && hctDbCurrentIsVisible(pCsr)==0 );

  HCT_EXTRA_LOGGING_CSRPOS(pCsr);

  return rc;
}

void sqlite3HctDbCsrClear(HctDbCsr *pCsr){
  hctDbCsrScanFinish(pCsr);
  hctDbCsrReset(pCsr);
}


int sqlite3HctDbCsrData(HctDbCsr *pCsr, int *pnData, const u8 **paData){
  const u8 *pPg;
  int iCell;

  pPg = hctDbCsrPageAndCell(pCsr, &iCell);
  assert( hctPageheight(pPg)==0 );

#if 0
  if( pCsr->nRange ){
    printf("%p: data from range page %d (from %d) (snapshotid=%lld)\n", 
        pCsr->pDb,
        (int)pCsr->aRange[pCsr->nRange-1].pg.iOldPg, 
        (int)pCsr->pg.iOldPg, pCsr->pDb->iSnapshotId
    );
  }else{
    printf("%p: data from page %d (snapshotid=%lld)\n", 
        pCsr->pDb,
        (int)pCsr->pg.iOldPg, pCsr->pDb->iSnapshotId
    );
  }
  fflush(stdout);
#endif
  
  return hctDbLoadRecord(pCsr->pDb, &pCsr->rec, pPg, iCell, pnData, paData);
}

static int hctDbValidateEntry(HctDatabase *pDb, HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  u8 flags;

  if( pCsr->nRange ){
    /* If the current entry is on a history page, it is not valid (as
    ** it has already been deleted). Later: unless of course it was this
    ** transaction that deleted it!  */
    if( pCsr->aRange[pCsr->nRange-1].iRangeTid!=pDb->iTid ){
      rc = HCT_SQLITE_BUSY;
    }
  }else{
    int iOff = hctDbCellOffset(pCsr->pg.aOld, pCsr->iCell, &flags);
    if( flags & HCTDB_HAS_TID ){
      u64 iTid = hctGetU64(&pCsr->pg.aOld[iOff]);
      if( hctDbTidIsConflict(pCsr->pDb, iTid) ){
        rc = HCT_SQLITE_BUSY;
      }
    }
  }
  return rc;
}

static int hctDbValidateIntkey(HctDatabase *pDb, HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  HctCsrIntkeyOp *pOpList = pCsr->intkey.pOpList;
  HctCsrIntkeyOp *pOp;

  pCsr->intkey.pOpList = 0;
  assert( pCsr->intkey.pCurrentOp==0 );
  for(pOp=pOpList; pOp && rc==SQLITE_OK; pOp=pOp->pNextOp){
    int bDum = 0;
    assert( pOp->iFirst<=pOp->iLast );

    if( pOp->iLogical ){
      int bEvict = 0;

      /* If the physical page associated with the logical page containing
      ** the current key has not changed, and the logical page has not been
      ** evicted, then the current key itself may not have been modified.
      ** Jump to the next iteration of the loop in this case. */
      u32 iPhys = sqlite3HctFilePageMapping(pDb->pFile, pOp->iLogical, &bEvict);
      if( pOp->iPhysical==iPhys && bEvict==0 ) continue;

      /* Alternatively, if the logical page has not been evicted, load it
      ** and seek to the desired key. If the key is found, or if it is not
      ** found but the key would reside on the current page, then load
      ** the page into the cursor. This is faster than the hctDbCsrSeek()
      ** call below.  */
      if( bEvict==0 && pOp->iLogical!=pCsr->iRoot ){
        rc = hctDbGetPhysical(pDb, iPhys, &pCsr->pg);
        if( rc==SQLITE_OK ){
          pCsr->eDir = BTREE_DIR_FORWARD;
          pCsr->iCell = hctDbIntkeyLeafSearch(pCsr->pg.aOld, pOp->iFirst,&bDum);
          if( pCsr->iCell>=((HctDbIntkeyLeaf*)pCsr->pg.aOld)->pg.nEntry ){
            hctDbCsrReset(pCsr);
          }
        }
      }
    }

    if( pCsr->pg.aOld==0 ){
      if( pOp->iFirst==SMALLEST_INT64 ){
        pCsr->eDir = BTREE_DIR_FORWARD;
        rc = hctDbCsrFirst(pCsr);
      }else{
        if( pOp->iFirst==pOp->iLast ){
          pCsr->eDir = BTREE_DIR_NONE;
        }else{
          pCsr->eDir = BTREE_DIR_FORWARD;
        }
        rc = hctDbCsrSeekAndDescend(pCsr, 0, pOp->iFirst, 0, &bDum);
      }
    }

    while( rc==SQLITE_OK && !sqlite3HctDbCsrEof(pCsr) ){
      i64 iKey = 0;
      sqlite3HctDbCsrKey(pCsr, &iKey);
      if( iKey>=pOp->iFirst && iKey<=pOp->iLast ){
        rc = hctDbValidateEntry(pDb, pCsr);
      }
      if( rc!=SQLITE_OK || iKey>=pOp->iLast ) break;
      rc = hctDbCsrNext(pCsr);
    }
    hctDbCsrReset(pCsr);
  }
  assert( pCsr->intkey.pOpList==0 && pCsr->intkey.pCurrentOp==0 );
  pCsr->intkey.pOpList = pOpList;

  return rc;
}

static int hctDbValidateIndex(HctDatabase *pDb, HctDbCsr *pCsr){
  int rc = SQLITE_OK;
  HctCsrIndexOp *pOpList = pCsr->index.pOpList;
  HctCsrIndexOp *pOp;

  pCsr->index.pOpList = 0;
  assert( pCsr->index.pCurrentOp==0 );
  rc = hctDbCsrAllocateUnpacked(pCsr);
  for(pOp=pOpList; pOp && rc==SQLITE_OK; pOp=pOp->pNextOp){
    UnpackedRecord *pRec = pCsr->pRec;
    int bDummy = 0;

    if( pOp->iLogical
     && pOp->iPhysical==sqlite3HctFilePageMapping(pDb->pFile, pOp->iLogical, &bDummy)
    ){
      continue;
    }

    hctDbCsrReset(pCsr);
    pCsr->eDir = (pOp->pFirst==pOp->pLast) ? BTREE_DIR_NONE : BTREE_DIR_FORWARD;
    if( pOp->pFirst==0 ){
      rc = hctDbCsrFirst(pCsr);
    }else{
      int bExact = 0;
      sqlite3VdbeRecordUnpack(pCsr->pKeyInfo, pOp->nFirst, pOp->pFirst, pRec);
      rc = hctDbCsrSeek(pCsr, 0, 0, 0, pRec, 0, &bExact);
      if( rc==SQLITE_OK && bExact==0 ){
        rc = hctDbCsrNext(pCsr);
      }
    }
    if( pOp->pLast && pOp->pLast!=pOp->pFirst ){
      sqlite3VdbeRecordUnpack(pCsr->pKeyInfo, pOp->nLast, pOp->pLast, pRec);
    }else{
      pRec = 0;
    }
    if( rc!=SQLITE_OK ) break;

    if( pOp->pLast==pOp->pFirst ){
      assert( !sqlite3HctDbCsrEof(pCsr) );
      rc = hctDbValidateEntry(pDb, pCsr);
    }else{
      while( !sqlite3HctDbCsrEof(pCsr) ){
        int res = -1;
        if( pRec ){
          const u8 *aKey = 0;
          int nKey = 0;
          rc = sqlite3HctDbCsrData(pCsr, &nKey, &aKey);
          if( rc!=SQLITE_OK ) break;
          if( nKey>0 ){
            res = sqlite3VdbeRecordCompare(nKey, aKey, pRec);
          }
          if( res<0 ) break;
        }
        rc = hctDbValidateEntry(pDb, pCsr);
        if( res==0 || rc!=SQLITE_OK ) break;
        rc = hctDbCsrNext(pCsr);
        if( rc!=SQLITE_OK ) break;
      }
    }
  }

  assert( pCsr->index.pOpList==0 && pCsr->index.pCurrentOp==0 );
  pCsr->index.pOpList = pOpList;
  return rc;
}

/*
** A transaction has recently been committed, or rolled back after data
** was written to the db (i.e. as a result of failed validation). Do a
** tmap scan if required.
*/
void sqlite3HctDbTMapScan(HctDatabase *pDb){
  u64 nFinalWrite = 0;
  int nPageScan = pDb->pConfig->nPageScan;

  /* Set nWrite to the number of pages written by this transaction. It 
  ** doesn't matter if it is slightly inaccurate in some cases.  */
  int nWrite = sqlite3HctFileWriteCount(pDb->pFile) - pDb->nWriteCount;

  assert( nWrite>=0 );
  if( nWrite==0 ) nWrite = 1;
  nFinalWrite = sqlite3HctFileIncrWriteCount(pDb->pFile, nWrite);
  if( (nFinalWrite / nPageScan)!=((nFinalWrite-nWrite) / nPageScan) ){
    sqlite3HctTMapScan(sqlite3HctFileTMapClient(pDb->pFile));
  }
}

int 
__attribute__ ((noinline)) 
sqlite3HctDbValidate(
  sqlite3 *db, 
  HctDatabase *pDb, 
  u64 *piCid
){
  HctDbCsr *pCsr = 0;
  u64 *pEntry = hctDbFindTMapEntry(pDb->pTmap, pDb->iTid);
  u64 iCid = *piCid;
  int rc = SQLITE_OK;

  assert( *pEntry==0 );
  if( iCid==0 ){
    HctAtomicStore(pEntry, HCT_TMAP_VALIDATING);
    iCid = sqlite3HctFileAllocateCID(pDb->pFile, 1);
  }
  HctAtomicStore(pEntry, HCT_TMAP_VALIDATING | iCid);

  assert( pDb->eMode==HCT_MODE_NORMAL );

  /* Invoke the SQLITE_TESTCTRL_HCT_MTCOMMIT hook, if applicable */
  if( db->xMtCommit ) db->xMtCommit(db->pMtCommitCtx, 2);

  /* If iCid is one more than pDb->iSnapshotId, then this transaction is
  ** being applied against the snapshot that it was run against. In this
  ** case we can skip validation entirely. */
  if( iCid!=pDb->iSnapshotId+1 ){
    if( hctDbIsConcurrent(pDb) ){
      pDb->eMode = HCT_MODE_VALIDATE;
      if( hctDbValidateMeta(pDb) ){
        rc = HCT_SQLITE_BUSY;
      }else{
        for(pCsr=pDb->pScannerList; pCsr; pCsr=pCsr->pNextScanner){
          if( pCsr->pKeyInfo==0 ){
            rc = hctDbValidateIntkey(pDb, pCsr);
          }else{
            rc = hctDbValidateIndex(pDb, pCsr);
          }
          if( rc ) break;
        }
      }
      pDb->eMode = HCT_MODE_NORMAL;
    }else{
      assert( pDb->pScannerList==0 );
      rc = HCT_SQLITE_BUSY;
    }
  }

  *piCid = iCid;
  return rc;
}


static int compareName(const u8 *a, const u8 *b, int n){
  int ii;
  for(ii=0; ii<n; ii++){
    if( sqlite3UpperToLower[a[ii]]!=sqlite3UpperToLower[b[ii]] ){
      return 1;
    }
  }
  return 0;
}

int sqlite3HctDbValidateTablename(
  HctDatabase *pDb, 
  const u8 *pName, int nName, 
  u64 iTid
){
  HctDbCsr *pCsr = 0;
  int rc = SQLITE_OK;

  rc = sqlite3HctDbCsrOpen(pDb, 0, 1, &pCsr);
  if( rc==SQLITE_OK ){
    pCsr->eDir = BTREE_DIR_FORWARD;
    for(rc=hctDbCsrFirst(pCsr);
        rc==SQLITE_OK && sqlite3HctDbCsrEof(pCsr)==0;
        rc=hctDbCsrNext(pCsr)
    ){
      u64 iDbTid = hctDbCsrTid(pCsr);
      if( iDbTid && iDbTid!=iTid ){
        int nData = 0;
        const u8 *aData = 0;
        int nDbName = 0;
        const u8 *pDbName = 0;

        rc = sqlite3HctDbCsrData(pCsr, &nData, &aData);
        if( rc!=SQLITE_OK ) break;
        nDbName = sqlite3HctNameFromSchemaRecord(aData, nData, &pDbName);

        if( nDbName==nName && compareName(pDbName, pName, nName)==0 ){
          rc = HCT_SQLITE_BUSY;
          break;
        }
      }
    }
  }
  sqlite3HctDbCsrClose(pCsr);
  return rc;
}

/*************************************************************************
**************************************************************************
** Start of integrity-check implementation.
**
** The code here assumes that the database is quiescent. If it is invoked
** concurrently with database writers, false-positive errors may be reported.
*/

/*
** Walk the tree structure with logical root page iRoot, visiting every
** page and overflow page currently linked in.
**
** For each page in the tree, the supplied callback is invoked. The first
** argument passed to the callback is a copy of the fourth argument to
** this function. The second and third arguments are the logical and
** physical page number, respectively. If there is no logical page number,
** as for overflow pages, the second parameter is passed zero.
**
** It (presumably) makes little sense to call this function without 
** somehow guaranteeing that the tree is not being currently written to.
*/
int sqlite3HctDbWalkTree(
  HctFile *pFile,                 /* File tree resides in */
  u32 iRoot,                      /* Root page of tree */
  int (*x)(void*, u32, u32),      /* Callback function */
  void *pCtx                      /* First argument to pass to x() */
){
  int rc = SQLITE_OK;
  u32 pgno = iRoot;

  u32 iPhys = 0;
  int dummy = 0;

  /* Special case - the root page is not mapped to any physical page. */
  iPhys = sqlite3HctFilePageMapping(pFile, iRoot, &dummy);
  if( iPhys==0 ){
    return x(pCtx, iRoot, 0);
  }

  /* This outer loop runs once for each list in the tree structure - once
  ** for the list of leaves, once for the list of parent, and so on.
  ** Starting from the root page and descending towards the leaves. */
  do {
    HctFilePage pg;
    int nHeight = 0;
    int eType = 0;
    u32 pgnoChild = 0;

    /* Load up page pgno - the leftmost of its list. Then, unless this
    ** is the list of leaves, set pgnoChild to the leftmost child of
    ** the page. Or, if this is a list of leaves, leave pgnoChild set
    ** to zero.  */
    rc = sqlite3HctFilePageGet(pFile, pgno, &pg);
    if( rc!=SQLITE_OK ){
      break;
    }else{
      nHeight = hctPageheight(pg.aOld);
      eType = hctPagetype(pg.aOld);
      if( eType!=HCT_PAGETYPE_INTKEY && eType!=HCT_PAGETYPE_INDEX ){
        rc = SQLITE_CORRUPT_BKPT;
        break;
      }
      else if( nHeight>0 ){
        if( eType==HCT_PAGETYPE_INTKEY ){
          pgnoChild = ((HctDbIntkeyNode*)pg.aOld)->aEntry[0].iChildPg;
        }else{
          pgnoChild = ((HctDbIndexNode*)pg.aOld)->aEntry[0].iChildPg;
        }
      }
    }

    while( pg.aOld ){
      u32 iPeerPg = ((HctDbPageHdr*)pg.aOld)->iPeerPg;
      u32 iLogic = pg.iPg;
      u32 iPhys = pg.iOldPg;

      rc = x(pCtx, iLogic, iPhys);
      if( rc!=SQLITE_OK ) break;

      if( nHeight==0 || eType==HCT_PAGETYPE_INDEX ){
        int iCell = 0;
        int nEntry = ((HctDbPageHdr*)pg.aOld)->nEntry;
        for(iCell=0; iCell<nEntry; iCell++){
          HctDbCell cell;
          hctDbCellGetByIdx(0, pg.aOld, iCell, &cell);

          if( cell.iOvfl ){
            u32 ovfl = cell.iOvfl;
            while( ovfl!=0 ){
              HctFilePage ov;
              rc = x(pCtx, 0, ovfl);
              if( rc!=SQLITE_OK ) break;
              rc = sqlite3HctFilePageGetPhysical(pFile, ovfl, &ov);
              if( rc!=SQLITE_OK ) break;
              ovfl = ((HctDbPageHdr*)ov.aOld)->iPeerPg;
              sqlite3HctFilePageRelease(&ov);
            }
          }
        }
      }

      sqlite3HctFilePageRelease(&pg);
      if( iPeerPg ){
        rc = sqlite3HctFilePageGet(pFile, iPeerPg, &pg);
        if( rc!=SQLITE_OK ) break;
      }
    }

    pgno = pgnoChild;
  }while( rc==SQLITE_OK && pgno!=0 );
  
  return rc;
}

typedef struct IntCheckCtx IntCheckCtx;
struct IntCheckCtx {
  u32 nLogic;                     /* Number of logical pages in db */
  u32 nPhys;                      /* Number of physical pages in db */
  u8 *aLogic;                     
  u8 *aPhys;
  int nErr;
  int nMaxErr;
  char *zErr;
  i64 nEntry;                     /* Number of entries in table */
};

static void hctDbICError(
  IntCheckCtx *p,
  char *zFmt,
  ...
){
  va_list ap;
  char *zErr;
  va_start(ap, zFmt);
  zErr = sqlite3_vmprintf(zFmt, ap);
  p->zErr = sqlite3_mprintf("%z%s%z", p->zErr, (p->zErr ? "\n" : ""), zErr);
  p->nErr++;
  va_end(ap);
}

static int hctDbIntegrityCheckCb(
  void *pCtx,
  u32 iLogic,
  u32 iPhys
){
  IntCheckCtx *p = (IntCheckCtx*)pCtx;
  if( iLogic ){
    if( p->aLogic[iLogic-1] ){
      hctDbICError(p, "multiple refs to logical page %d", (int)iLogic);
    }
    p->aLogic[iLogic-1] = 1;
  }
  if( iPhys ){
    if( p->aPhys[iPhys-1] ){
      hctDbICError(p, "multiple refs to physical page %d", (int)iPhys);
    }
    p->aPhys[iPhys-1] = 1;
  }

  return (p->nErr>=p->nMaxErr) ? -1 : 0;
}

#if 0
/*
** Parameter aVal[] is an array nVal values in size. If this array contains
** the value passed as the 3rd parameter (val), return true. Otherwise return
** false.
*/
static int arrayContainsValue(u32 *aVal, int nVal, u32 val){
  int ii;
  for(ii=0; ii<nVal; ii++){
    if( aVal[ii]==val ) return 1;
  }
  return 0;
}
#endif

char *sqlite3HctDbIntegrityCheck(
  HctDatabase *pDb, 
  u32 *aRoot, 
  Mem *aCnt,
  int nRoot, 
  int *pnErr
){
  HctFile *pFile = pDb->pFile;
  IntCheckCtx c;
  u32 *aFileRoot = 0;
  int nFileRoot = 0;

  int rc = sqlite3HctFileRootArray(pFile, &aFileRoot, &nFileRoot);
  memset(&c, 0, sizeof(c));
  if( rc==SQLITE_OK ){
    c.nErr = *pnErr;
    c.nMaxErr = 100;
    sqlite3HctFileICArrays(pFile, &c.aLogic, &c.nLogic, &c.aPhys, &c.nPhys);
  }
  if( !c.aLogic ){
    c.nErr++;
  }else{
    int ii;

    for(ii=0; c.nErr==0 && ii<nFileRoot; ii++){
      u32 r = aFileRoot[ii];
      c.nEntry = 0;
      sqlite3HctDbWalkTree(pFile, r, hctDbIntegrityCheckCb, (void*)&c);
#if 0
      if( r!=2 ){
        int jj;
        for(jj=0; jj<nRoot && aRoot[jj]!=r; jj++);
        assert( jj<nRoot );
        sqlite3MemSetArrayInt64(aCnt, jj, c.nEntry);
      }
#endif
    }

    /* Check for leaks */
    for(ii=1; c.nErr<c.nMaxErr && ii<=c.nLogic; ii++){
      assert( c.aLogic[ii-1]==0 || c.aLogic[ii-1]==1 );
      if( c.aLogic[ii-1]!=1 ){
        hctDbICError(&c, "logical page %d has been leaked", ii);
      }
    }
    for(ii=1; c.nErr<c.nMaxErr && ii<=c.nPhys; ii++){
      assert( c.aPhys[ii-1]==0 || c.aPhys[ii-1]==1 );
      if( c.aPhys[ii-1]!=1 ){
        hctDbICError(&c, "physical page %d has been leaked", ii);
      }
    }
  }

  *pnErr = c.nErr;
  sqlite3_free(c.aLogic);
  sqlite3_free(aFileRoot);
  return c.zErr;
}

i64 sqlite3HctDbStats(sqlite3 *db, int iStat, const char **pzStat){
  i64 iVal = -1;
  HctDatabase *pDb = sqlite3HctDbFind(db, 0);

  switch( iStat ){
    case 0:
      *pzStat = "balance_intkey";
      iVal = pDb->stats.nBalanceIntkey;
      break;
    case 1:
      *pzStat = "balance_index";
      iVal = pDb->stats.nBalanceIndex;
      break;
    case 2:
      *pzStat = "balance_single";
      iVal = pDb->stats.nBalanceSingle;
      break;
    case 3:
      *pzStat = "tmap_lookup";
      iVal = pDb->stats.nTMapLookup;
      break;
    case 4:
      *pzStat = "update_in_place";
      iVal = pDb->stats.nUpdateInPlace;
      break;
    case 5:
      *pzStat = "internal_retry";
      iVal = pDb->stats.nInternalRetry;
      break;
    default:
      break;
  }

  return iVal;
}

/*************************************************************************
**************************************************************************
** Below are the virtual table implementations. These are debugging 
** aids only.
*/

typedef struct hctdb_vtab hctdb_vtab;
struct hctdb_vtab {
  sqlite3_vtab base;              /* Base class - must be first */
  sqlite3 *db;
};

/* templatevtab_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct hctdb_cursor hctdb_cursor;
struct hctdb_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  HctDatabase *pDb;          /* Database to report on */
  u64 iMaxPgno;              /* Maximum page number for this scan */

  u64 pgno;                  /* The page-number/rowid value */
  const char *zPgtype;
  u32 iPeerPg;
  u32 nEntry;
  u32 nHeight;
  u32 nFree;
  char *zFpKey;
};

/*
** The hctdbConnect() method is invoked to create a new
** template virtual table.
**
** Think of this routine as the constructor for hctdb_vtab objects.
**
** All this routine needs to do is:
**
**    (1) Allocate the hctdb_vtab object and initialize all fields.
**
**    (2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
**        result set of queries against the virtual table will look like.
*/
static int hctdbConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  hctdb_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db,
      "CREATE TABLE x("
        "pgno INTEGER, pgtype TEXT, nheight INTEGER, "
        "peer INTEGER, nentry INTEGER, nfree INTEGER, fpkey TEXT"
      ")"
  );

  if( rc==SQLITE_OK ){
    pNew = sqlite3MallocZero( sizeof(*pNew) );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ) return SQLITE_NOMEM;
    pNew->db = db;
  }
  return rc;
}

/*
** This method is the destructor for hctdb_vtab objects.
*/
static int hctdbDisconnect(sqlite3_vtab *pVtab){
  hctdb_vtab *p = (hctdb_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new hctdb_cursor object.
*/
static int hctdbOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  hctdb_cursor *pCur;
  pCur = sqlite3MallocZero(sizeof(*pCur));
  if( pCur==0 ) return SQLITE_NOMEM;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a hctdb_cursor.
*/
static int hctdbClose(sqlite3_vtab_cursor *cur){
  hctdb_cursor *pCur = (hctdb_cursor*)cur;
  sqlite3_free(pCur->zFpKey);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static char *hex_encode(const u8 *aIn, int nIn){
  char *zRet = sqlite3MallocZero(nIn*2+1);
  if( zRet ){
    static const char aDigit[] = "0123456789ABCDEF";
    int i;
    for(i=0; i<nIn; i++){
      zRet[i*2] = aDigit[ (aIn[i] >> 4) ];
      zRet[i*2+1] = aDigit[ (aIn[i] & 0xF) ];
    }
  }
  return zRet;
}

char *sqlite3HctDbUnpackedToText(UnpackedRecord *pRec){
  char *zRet = 0;
  const char *zSep = "";
  int ii;

  zRet = sqlite3HctMprintf("(");

  for(ii=0; ii<pRec->nField; ii++){
    sqlite3_value *pVal = (sqlite3_value*)&pRec->aMem[ii];
    switch( sqlite3_value_type(pVal) ){
      case SQLITE_NULL:
        zRet = sqlite3_mprintf("%z%sNULL", zRet, zSep);
        break;
      case SQLITE_INTEGER:
        zRet = sqlite3_mprintf("%z%s%lld", zRet,zSep,sqlite3_value_int64(pVal));
        break;
      case SQLITE_FLOAT:
        zRet = sqlite3_mprintf("%z%s%f", zRet,zSep,sqlite3_value_double(pVal));
        break;
      case SQLITE_TEXT: {
        zRet = sqlite3_mprintf("%z%s%.*s", zRet, zSep, pVal->n, pVal->z);
        break;
      }
      default:
        int n = sqlite3_value_bytes(pVal);
        const u8 *a = (const u8*)sqlite3_value_blob(pVal);
        zRet = sqlite3_mprintf("%z%s%z", zRet, zSep, hex_encode(a, n));
        break;
    }

    zSep = ",";
  }

  /*
  ** sqlite3VdbeRecordCompare
  */
  if( pRec->default_rc ){
    zRet = sqlite3HctMprintf("%z)%s", zRet, (pRec->default_rc>0 ? "-1" : "+1"));
  }else{
    zRet = sqlite3HctMprintf( "%z)", zRet);
  }

  return zRet;
}

char *sqlite3HctDbRecordToText(sqlite3 *db, const u8 *aRec, int nRec){
  char *zRet = 0;
  const char *zSep = "";
  const u8 *pEndHdr;              /* Points to one byte past record header */
  const u8 *pHdr;                 /* Current point in record header */
  const u8 *pBody;                /* Current point in record data */
  u64 nHdr;                       /* Bytes in record header */

  if( nRec==0 ){
    return sqlite3_mprintf("");
  }

  pHdr = aRec + sqlite3GetVarint(aRec, &nHdr);
  pBody = pEndHdr = &aRec[nHdr];
  while( pHdr<pEndHdr ){
    u64 iSerialType = 0;
    int nByte = 0;

    pHdr += sqlite3GetVarint(pHdr, &iSerialType);
    nByte = sqlite3VdbeSerialTypeLen((u32)iSerialType);

    switch( iSerialType ){
      case 0: {  /* Null */
        zRet = sqlite3_mprintf("%z%sNULL", zRet, zSep);
        break;
      }
      case 1: case 2: case 3: case 4: case 5: case 6: {
        i64 iVal = 0;

        switch( iSerialType ){
          case 1: 
            iVal = (i64)pBody[0];
            break;
          case 2: 
            iVal = ((i64)pBody[0] << 8) + (i64)pBody[1];
            break;
          case 3: 
            iVal = ((i64)pBody[0] << 16) + ((i64)pBody[1] << 8) + (i64)pBody[2];
            break;
          case 4: 
            iVal = ((i64)pBody[0] << 24) + ((i64)pBody[1] << 16) 
                 + ((i64)pBody[2] << 8) + (i64)pBody[3];
            break;
          case 5: 
            iVal = ((i64)pBody[0] << 40) + ((i64)pBody[1] << 32) 
                 + ((i64)pBody[2] << 24) + ((i64)pBody[3] << 16) 
                 + ((i64)pBody[4] << 8) + (i64)pBody[5];
            break;
          case 6: 
            iVal = ((i64)pBody[0] << 56) + ((i64)pBody[1] << 48) 
                 + ((i64)pBody[2] << 40) + ((i64)pBody[3] << 32) 
                 + ((i64)pBody[4] << 24) + ((i64)pBody[5] << 16) 
                 + ((i64)pBody[6] << 8) + (i64)pBody[7];
            break;
        }

        zRet = sqlite3_mprintf("%z%s%lld", zRet, zSep, iVal);
        break;
      }
      case 7: {
        double d;
        u64 i = ((u64)pBody[0] << 56) + ((u64)pBody[1] << 48) 
            + ((u64)pBody[2] << 40) + ((u64)pBody[3] << 32) 
            + ((u64)pBody[4] << 24) + ((u64)pBody[5] << 16) 
            + ((u64)pBody[6] << 8) + (u64)pBody[7];
        hctMemcpy(&d, &i, 8);
        zRet = sqlite3_mprintf("%z%s%f", zRet, zSep, d);
        break;
      }

      case 8: {  /* 0 */
        zRet = sqlite3_mprintf("%z%s0", zRet, zSep);
        break;
      }
      case 9: {  /* 1 */
        zRet = sqlite3_mprintf("%z%s1", zRet, zSep);
        break;
      }

      default: {
        if( (iSerialType % 2) ){
          /* A text value */
          zRet = sqlite3_mprintf("%z%s%.*Q", zRet, zSep, nByte, pBody);
        }else{  
          /* A blob value */
          char *zHex = hex_encode(pBody, nByte);
          zRet = sqlite3_mprintf("%z%sX'%z'", zRet, zSep, zHex);
        }
        break;
      }
    }
    pBody += nByte;
    zSep = ",";
  }

/* printf("%s\n", zRet); */
  return zRet;
}

/*
** Load the values for xColumn() associated with the current value of
** hctdb_cursor.pgno into memory.
*/
static int hctdbLoadPage(
  hctdb_cursor *pCur,
  const u8 *aPg
){
  static const char *azType[] = {
    "!INVALID!",                /* 0x00 */
    "intkey",                   /* 0x01 */
    "0x02",                     /* 0x02 */
    "index",                    /* 0x03 */
    "0x03",                     /* 0x04 */
    "overflow",                 /* 0x05 */
    "history",                  /* 0x06 */
  };
  int rc = SQLITE_OK;
  HctDbPageHdr *pHdr = (HctDbPageHdr*)aPg;
  sqlite3 *db = ((hctdb_vtab*)pCur->base.pVtab)->db;
  int eType;

  assert( 0==sqlite3_stricmp("intkey", azType[HCT_PAGETYPE_INTKEY]) );
  assert( 0==sqlite3_stricmp("index", azType[HCT_PAGETYPE_INDEX]) );
  assert( 0==sqlite3_stricmp("overflow", azType[HCT_PAGETYPE_OVERFLOW]) );

  sqlite3_free(pCur->zFpKey);
  pCur->zFpKey = 0;

  eType = hctPagetype(pHdr);
  if( eType<ArraySize(azType) ){
    pCur->zPgtype = azType[hctPagetype(pHdr)];
  }else{
    pCur->zPgtype = "!INVALID!";
  }
  pCur->iPeerPg = pHdr->iPeerPg;
  pCur->nEntry = pHdr->nEntry;
  pCur->nHeight = pHdr->nHeight;

  if( eType==HCT_PAGETYPE_INTKEY ){
    if( pHdr->nHeight==0 ){
      HctDbIntkeyLeaf *pLeaf = (HctDbIntkeyLeaf*)aPg;
      char *zFpKey = sqlite3_mprintf("%lld", pLeaf->aEntry[0].iKey);
      if( zFpKey==0 ) rc = SQLITE_NOMEM_BKPT;
      pCur->zFpKey = zFpKey;
      pCur->nFree = (int)pLeaf->hdr.nFreeBytes;
    }else{
      HctDbIntkeyNode *pNode = (HctDbIntkeyNode*)aPg;
      char *zFpKey = sqlite3_mprintf("%lld", pNode->aEntry[0].iKey);
      if( zFpKey==0 ) rc = SQLITE_NOMEM_BKPT;
      pCur->zFpKey = zFpKey;
      pCur->nFree = (
        hctDbMaxCellsPerIntkeyNode(pCur->pDb->pgsz) - pNode->pg.nEntry
      ) * sizeof(HctDbIntkeyNodeEntry);
    }

  }else if( eType==HCT_PAGETYPE_INDEX ){
    HctBuffer buf = {0,0,0};
    const u8 *aRec = 0;
    int nRec = 0;

    rc = hctDbLoadRecord(pCur->pDb, &buf, aPg, 0, &nRec, &aRec);
    if( rc==SQLITE_OK ){
      char *zFpKey = sqlite3HctDbRecordToText(db, aRec, nRec);
      if( zFpKey==0 ) rc = SQLITE_NOMEM_BKPT;
      pCur->zFpKey = zFpKey;
    }

    pCur->nFree = (int)(((HctDbIndexNode*)pHdr)->hdr.nFreeBytes);
    sqlite3HctBufferFree(&buf);
  }
  return rc;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int hctdbEof(sqlite3_vtab_cursor *cur){
  hctdb_cursor *pCur = (hctdb_cursor*)cur;
  return pCur->pgno>pCur->iMaxPgno;
}

/*
** Advance a hctdb_cursor to its next row of output.
*/
static int hctdbNext(sqlite3_vtab_cursor *cur){
  hctdb_cursor *pCur = (hctdb_cursor*)cur;
  int rc = SQLITE_OK;
  HctFilePage pg;

  memset(&pg, 0, sizeof(pg));
  do {
    sqlite3HctFilePageRelease(&pg);
    pCur->pgno++;
    if( hctdbEof(cur) ) return SQLITE_OK;
    rc = sqlite3HctFilePageGetPhysical(pCur->pDb->pFile, pCur->pgno, &pg);
  }while( rc==SQLITE_OK && pg.aOld==0 );

  if( pg.aOld ){
    rc = hctdbLoadPage(pCur, pg.aOld);
  }
  return rc;
}

/*
** Return values of columns for the row at which the hctdb_cursor
** is currently pointing.
*/
static int hctdbColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  hctdb_cursor *pCur = (hctdb_cursor*)cur;
  assert( i>=0 && i<=6 );
  switch( i ){
    case 0: /* pgno */
      sqlite3_result_int64(ctx, (i64)pCur->pgno);
      break;
    case 1: /* pgtype */
      sqlite3_result_text(ctx, pCur->zPgtype, -1, SQLITE_TRANSIENT);
      break;
    case 2: /* nHeight */
      sqlite3_result_int64(ctx, (i64)pCur->nHeight);
      break;
    case 3: /* peer */
      sqlite3_result_int64(ctx, (i64)pCur->iPeerPg);
      break;
    case 4: /* nEntry */
      sqlite3_result_int64(ctx, (i64)pCur->nEntry);
      break;
    case 5: /* nfree */
      sqlite3_result_int64(ctx, (i64)pCur->nFree);
      break;
    case 6: /* fpkey */
      sqlite3_result_text(ctx, pCur->zFpKey, -1, SQLITE_TRANSIENT);
      break;
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int hctdbRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  hctdb_cursor *pCur = (hctdb_cursor*)cur;
  *pRowid = pCur->pgno;
  return SQLITE_OK;
}

/*
** This method is called to "rewind" the hctdb_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to hctdbColumn() or hctdbRowid() or 
** hctdbEof().
*/
static int hctdbFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  hctdb_cursor *pCur = (hctdb_cursor*)pVtabCursor;
  hctdb_vtab *pTab = (hctdb_vtab*)(pCur->base.pVtab);
 
  pCur->pDb = sqlite3HctDbFind(pTab->db, 0);
  if( argc==1 ){
    u32 iVal = (u32)sqlite3_value_int64(argv[0]);
    pCur->iMaxPgno = iVal;
    pCur->pgno = iVal-1;
  }else{
    pCur->pgno = 0;
    pCur->iMaxPgno = sqlite3HctFileMaxpage(pCur->pDb->pFile);
  }
  return hctdbNext(pVtabCursor);
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int hctdbBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;
  pIdxInfo->estimatedCost = (double)10000;
  pIdxInfo->estimatedRows = 10000;

  for(i=0; i<pIdxInfo->nConstraint; i++){
    struct sqlite3_index_constraint *p = &pIdxInfo->aConstraint[i];
    if( p->iColumn!=0 ) continue;
    if( p->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( !p->usable ) continue;
    pIdxInfo->aConstraintUsage[i].argvIndex = 1;
    pIdxInfo->idxNum = 1;
    pIdxInfo->estimatedCost = (double)10;
    pIdxInfo->estimatedRows = 10;
    break;
  }

  return SQLITE_OK;
}

typedef struct hctentry_vtab hctentry_vtab;
struct hctentry_vtab {
  sqlite3_vtab base;              /* Base class - must be first */
  sqlite3 *db;
};

/* templatevtab_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct hctentry_cursor hctentry_cursor;
struct hctentry_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  HctDatabase *pDb;          /* Database to report on */
  int iEntry;
  HctFilePage pg;
  u32 iPg;                   /* Current physical page number */
  u32 iLastPg;               /* Last physical page to report on */
};

/*
** The hctentryConnect() method is invoked to create a new
** template virtual table.
**
** Think of this routine as the constructor for hctentry_vtab objects.
**
** All this routine needs to do is:
**
**    (1) Allocate the hctentry_vtab object and initialize all fields.
**
**    (2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
**        result set of queries against the virtual table will look like.
*/
static int hctentryConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  hctentry_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db,
      "CREATE TABLE x("
        "pgno INTEGER, entry INTEGER, "
        "ikey INTEGER, size INTEGER, offset INTEGER, "
        "child INTEGER, "
        "tid INTEGER, rangetid INTEGER, "
        /* "oldpg INTEGER, " */
        "rangeoldpg INTEGER, ovfl INTEGER, record TEXT"
      ")"
  );

  if( rc==SQLITE_OK ){
    pNew = sqlite3MallocZero( sizeof(*pNew) );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ) return SQLITE_NOMEM;
    pNew->db = db;
  }
  return rc;
}

/*
** This method is the destructor for hctentry_vtab objects.
*/
static int hctentryDisconnect(sqlite3_vtab *pVtab){
  hctentry_vtab *p = (hctentry_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new hctentry_cursor object.
*/
static int hctentryOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  hctentry_cursor *pCur;
  pCur = sqlite3MallocZero(sizeof(*pCur));
  if( pCur==0 ) return SQLITE_NOMEM;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a hctentry_cursor.
*/
static int hctentryClose(sqlite3_vtab_cursor *cur){
  hctentry_cursor *pCur = (hctentry_cursor*)cur;
  sqlite3HctFilePageRelease(&pCur->pg);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int hctentryEof(sqlite3_vtab_cursor *cur){
  hctentry_cursor *pCur = (hctentry_cursor*)cur;
  return pCur->pg.aOld==0;
}

/*
** Advance a hctentry_cursor to its next row of output.
*/
static int hctentryNext(sqlite3_vtab_cursor *cur){
  int rc = SQLITE_OK;
  hctentry_cursor *pCur = (hctentry_cursor*)cur;

  while( rc==SQLITE_OK ){
    HctDbPageHdr *pPg = (HctDbPageHdr*)pCur->pg.aOld;
    int eType = hctPagetype(pPg);
    if( eType==HCT_PAGETYPE_INTKEY 
     || eType==HCT_PAGETYPE_INDEX 
     || eType==HCT_PAGETYPE_HISTORY 
    ){
      pCur->iEntry++;
      if( pCur->iEntry<pPg->nEntry ) break;
    }
    pCur->iEntry = -1;
    pCur->iPg++;
    sqlite3HctFilePageRelease(&pCur->pg);
    if( pCur->iPg>pCur->iLastPg ) break;
    rc = sqlite3HctFilePageGetPhysical(pCur->pDb->pFile, pCur->iPg, &pCur->pg);
  }

  return rc;
}

/*
** Return values of columns for the row at which the hctentry_cursor
** is currently pointing.
*/
static int hctentryColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  hctentry_cursor *pCur = (hctentry_cursor*)cur;
  int eType = hctPagetype(pCur->pg.aOld);
  int nHeight = hctPageheight(pCur->pg.aOld);

  HctDbIntkeyEntry *pIntkey = 0;
  HctDbIntkeyNodeEntry *pIntkeyNode = 0;
  HctDbIndexEntry *pIndex = 0;
  HctDbIndexNodeEntry *pIndexNode = 0;
  HctDbHistoryFan *pFan = 0;

  switch( eType ){
    case HCT_PAGETYPE_INTKEY:
      if( nHeight==0 ){
        pIntkey = &((HctDbIntkeyLeaf*)pCur->pg.aOld)->aEntry[pCur->iEntry];
      }else{
        pIntkeyNode = &((HctDbIntkeyNode*)pCur->pg.aOld)->aEntry[pCur->iEntry];
      }
      break;

    case HCT_PAGETYPE_INDEX:
      if( nHeight==0 ){
        pIndex = &((HctDbIndexLeaf*)pCur->pg.aOld)->aEntry[pCur->iEntry];
      }else{
        pIndexNode = &((HctDbIndexNode*)pCur->pg.aOld)->aEntry[pCur->iEntry];
      }
      break;

    case HCT_PAGETYPE_HISTORY:
      pFan = (HctDbHistoryFan*)pCur->pg.aOld;
      break;
      
  }

  switch( i ){
    case 0: /* pgno */
      sqlite3_result_int64(ctx, (i64)pCur->iPg);
      break;
    case 1: /* iEntry */
      sqlite3_result_int64(ctx, (i64)pCur->iEntry);
      break;
    case 2: /* ikey */
      if( pIntkey ) sqlite3_result_int64(ctx, pIntkey->iKey);
      if( pIntkeyNode ) sqlite3_result_int64(ctx, pIntkeyNode->iKey);
      break;
    case 3: /* size */
      if( pIntkey ) sqlite3_result_int64(ctx, pIntkey->nSize);
      if( pIndex ) sqlite3_result_int64(ctx, pIndex->nSize);
      if( pIndexNode ) sqlite3_result_int64(ctx, pIndexNode->nSize);
      break;
    case 4: /* offset */
      if( pIntkey ) sqlite3_result_int64(ctx, pIntkey->iOff);
      if( pIndex ) sqlite3_result_int64(ctx, pIndex->iOff);
      if( pIndexNode ) sqlite3_result_int64(ctx, pIndexNode->iOff);
      break;
    case 5: /* child */
      if( pIndexNode ) sqlite3_result_int64(ctx, pIndexNode->iChildPg);
      if( pIntkeyNode ) sqlite3_result_int64(ctx, pIntkeyNode->iChildPg);
      break;

    case 6: /* tid */
    case 7: /* rangetid */
    case 8: /* rangeoldpg */
    case 9: /* ovfl */
      if( pIntkey || pIndex || pIndexNode ){
        u8 *aPg = pCur->pg.aOld;
        HctDbCell cell;
        HctDbIndexEntry *p = hctDbEntryEntry(aPg, pCur->iEntry);
        hctDbCellGet(pCur->pDb, &aPg[p->iOff], p->flags, &cell);

        if( i==6 && cell.iTid ){
          i64 iVal = (cell.iTid & HCT_TID_MASK);
          if( cell.iTid & HCT_TID_ROLLBACK_OVERRIDE ) iVal = iVal*-1;
          sqlite3_result_int64(ctx, iVal);
        }
        if( i==7 && cell.iRangeTid ){
          i64 iVal = (cell.iRangeTid & HCT_TID_MASK);
          if( cell.iRangeTid & HCT_TID_ROLLBACK_OVERRIDE ) iVal = iVal*-1;
          sqlite3_result_int64(ctx, iVal);
        }
        if( i==8 && cell.iRangeOld ){
          sqlite3_result_int64(ctx, (i64)cell.iRangeOld);
        }
        if( i==9 && cell.iOvfl ){
          sqlite3_result_int64(ctx, (i64)cell.iOvfl);
        }
      }else if( pFan ){
        if( i==7 ){   /* rangetid */
          u64 iVal = ((pCur->iEntry==0) ? pFan->iRangeTid0 : pFan->iRangeTid1);
          if( iVal & HCT_TID_ROLLBACK_OVERRIDE ){
            sqlite3_result_int64(ctx, ((i64)(iVal & HCT_TID_MASK)) * -1);
          }else{
            sqlite3_result_int64(ctx, (i64)iVal);
          }
        }else if( i==8 ){ /* rangeoldpg */
          u32 iRangeOldPg = 
            ((pCur->iEntry==0) ? pFan->pgOld0 : pFan->aPgOld1[pCur->iEntry-1]);
          sqlite3_result_int64(ctx, (i64)iRangeOldPg);
        }
      }
      break;
    case 10: /* record */
      if( pIntkey || pIndex || pIndexNode ){
        sqlite3 *db = sqlite3_context_db_handle(ctx);
        u8 *aPg = pCur->pg.aOld;
        char *zRec;
        int sz;
        const u8 *aRec = 0;
        HctBuffer buf = {0,0,0};

        hctDbLoadRecord(pCur->pDb, &buf, aPg, pCur->iEntry, &sz, &aRec);

        zRec = sqlite3HctDbRecordToText(db, aRec, sz);
        if( zRec ){
          sqlite3_result_text(ctx, zRec, -1, SQLITE_TRANSIENT);
          sqlite3_free(zRec);
        }
        sqlite3HctBufferFree(&buf);
      }else if( pFan ){
        char *zRec = sqlite3_mprintf("iSplit0=%d", pFan->iSplit0);
        if( zRec ){
          sqlite3_result_text(ctx, zRec, -1, SQLITE_TRANSIENT);
          sqlite3_free(zRec);
        }
      }
      break;
  }

  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int hctentryRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  hctentry_cursor *pCur = (hctentry_cursor*)cur;
  *pRowid = (((i64)pCur->iPg) << 32) + pCur->iEntry;
  return SQLITE_OK;
}

/*
** This method is called to "rewind" the hctentry_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to hctentryColumn() or hctentryRowid() or 
** hctentryEof().
*/
static int hctentryFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  int rc;
  hctentry_cursor *pCur = (hctentry_cursor*)pVtabCursor;
  hctentry_vtab *pTab = (hctentry_vtab*)(pCur->base.pVtab);
  u32 iLastPg; 
 
  pCur->pDb = sqlite3HctDbFind(pTab->db, 0);
  pCur->iEntry = -1;
  iLastPg = sqlite3HctFileMaxpage(pCur->pDb->pFile);

  if( idxNum==1 ){
    u32 iPg = (u32)sqlite3_value_int64(argv[0]);
    assert( argc==1 );
    if( iPg<1 || iPg>iLastPg ) return SQLITE_OK;
    pCur->iPg = pCur->iLastPg = iPg;
  }else{
    pCur->iPg = 1;
    pCur->iLastPg = iLastPg;
  }

  rc = sqlite3HctFilePageGetPhysical(pCur->pDb->pFile, pCur->iPg, &pCur->pg);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  return hctentryNext(pVtabCursor);
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int hctentryBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;
  int iPgnoEq = -1;

  pIdxInfo->estimatedCost = (double)1000000;
  pIdxInfo->estimatedRows = 1000000;

  /* Search for a pgno=? constraint */
  for(i=0; i<pIdxInfo->nConstraint; i++){
    struct sqlite3_index_constraint *p = &pIdxInfo->aConstraint[i];
    if( p->usable && p->iColumn==0 && p->op==SQLITE_INDEX_CONSTRAINT_EQ ){
      iPgnoEq = i;
    }
  }

  if( iPgnoEq>=0 ){
    pIdxInfo->aConstraintUsage[iPgnoEq].argvIndex = 1;
    pIdxInfo->idxNum = 1;
    pIdxInfo->estimatedCost = (double)1000;
    pIdxInfo->estimatedRows = 1000;
  }

  return SQLITE_OK;
}

typedef struct hctvalid_vtab hctvalid_vtab;
typedef struct hctvalid_cursor hctvalid_cursor;
struct hctvalid_vtab {
  sqlite3_vtab base;              /* Base class - must be first */
  sqlite3 *db;
};
struct hctvalid_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  HctDatabase *pDb;          /* Database to report on */
  int iEntry;                /* Current entry (i.e. rowid) */

  u32 rootpgno;              /* Value of rootpgno column */
  char *zFirst;
  char *zLast;
  char *zPglist;
};
static int hctvalidConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  hctvalid_vtab *pNew = 0;
  int rc = SQLITE_OK;

  *ppVtab = 0;
  rc = sqlite3_declare_vtab(db,
      "CREATE TABLE x(rootpgno, first, last, pglist)"
  );

  if( rc==SQLITE_OK ){
    pNew = sqlite3MallocZero( sizeof(*pNew) );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ) return SQLITE_NOMEM;
    pNew->db = db;
  }
  return rc;
}
static int hctvalidBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  pIdxInfo->estimatedCost = (double)10000;
  pIdxInfo->estimatedRows = 10000;
  return SQLITE_OK;
}
static int hctvalidDisconnect(sqlite3_vtab *pVtab){
  hctvalid_vtab *p = (hctvalid_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}
static int hctvalidOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  hctvalid_cursor *pCur;
  pCur = sqlite3MallocZero(sizeof(*pCur));
  if( pCur==0 ) return SQLITE_NOMEM;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}
static int hctvalidClose(sqlite3_vtab_cursor *cur){
  hctvalid_cursor *pCur = (hctvalid_cursor*)cur;
  sqlite3_free(pCur);
  return SQLITE_OK;
}
static int hctvalidNext(sqlite3_vtab_cursor *cur){
  hctvalid_cursor *pCsr = (hctvalid_cursor*)cur;
  hctvalid_vtab *pTab = (hctvalid_vtab*)(pCsr->base.pVtab);
  int ii;
  HctDbCsr *pDbCsr = 0;
  HctCsrIntkeyOp *pIntkeyOp = 0;
  HctCsrIndexOp *pIndexOp = 0;

  sqlite3_free(pCsr->zFirst);
  sqlite3_free(pCsr->zLast);
  sqlite3_free(pCsr->zPglist);
  pCsr->zFirst = 0;
  pCsr->zLast = 0;
  pCsr->zPglist = 0;
  pCsr->rootpgno = 0;
  pCsr->iEntry++;
  pDbCsr = pCsr->pDb->pScannerList;
  pIntkeyOp = pDbCsr->intkey.pOpList;
  pIndexOp = pDbCsr->index.pOpList;
  ii = 0;
  if( pIntkeyOp==0 && pIndexOp==0 ) ii--;
  for(/*noop*/; pDbCsr && ii<pCsr->iEntry; ii++){
    if( pIntkeyOp ) pIntkeyOp = pIntkeyOp->pNextOp;
    if( pIndexOp ) pIndexOp = pIndexOp->pNextOp;
    if( pIntkeyOp==0 && pIndexOp==0 ){
      pDbCsr = pDbCsr->pNextScanner;
      if( pDbCsr ){
        pIntkeyOp = pDbCsr->intkey.pOpList;
        pIndexOp = pDbCsr->index.pOpList;
        if( pIntkeyOp==0 && pIndexOp==0 ) ii--;
      }
    }
  }

  if( pDbCsr ){
    pCsr->rootpgno = pDbCsr->iRoot;
    if( pIntkeyOp ){
      if( pIntkeyOp->iFirst!=SMALLEST_INT64 ){
        pCsr->zFirst = sqlite3_mprintf("%lld", pIntkeyOp->iFirst);
      }
      if( pIntkeyOp->iFirst!=LARGEST_INT64 ){
        pCsr->zLast = sqlite3_mprintf("%lld", pIntkeyOp->iLast);
      }
      if( pIntkeyOp->iLogical ){
        pCsr->zPglist = sqlite3_mprintf(
            "%lld/%lld", pIntkeyOp->iLogical, pIntkeyOp->iPhysical
        );
      }
    }else{
      if( pIndexOp->pFirst ){
        pCsr->zFirst = sqlite3HctDbRecordToText(
            pTab->db, pIndexOp->pFirst, pIndexOp->nFirst
        );
      }
      if( pIndexOp->pLast ){
        pCsr->zLast = sqlite3HctDbRecordToText(
            pTab->db, pIndexOp->pLast, pIndexOp->nLast
        );
      }
      if( pIndexOp->iLogical ){
        pCsr->zPglist = sqlite3_mprintf(
            "%lld/%lld", pIndexOp->iLogical, pIndexOp->iPhysical
        );
      }
    }
  }

  return SQLITE_OK;
}
static int hctvalidFilter(
  sqlite3_vtab_cursor *cur, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  hctvalid_cursor *pCsr = (hctvalid_cursor*)cur;
  hctvalid_vtab *pTab = (hctvalid_vtab*)(pCsr->base.pVtab);
 
  pCsr->pDb = sqlite3HctDbFind(pTab->db, 0);
  pCsr->iEntry = -1;
  return hctvalidNext(cur);
}
static int hctvalidEof(sqlite3_vtab_cursor *cur){
  hctvalid_cursor *pCsr = (hctvalid_cursor*)cur;
  return (pCsr->rootpgno==0);
}
static int hctvalidColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  hctvalid_cursor *pCsr = (hctvalid_cursor*)cur;
  switch( i ){
    case 0:
      sqlite3_result_int64(ctx, (i64)pCsr->rootpgno);
      break;
    case 1:
      sqlite3_result_text(ctx, pCsr->zFirst, -1, SQLITE_TRANSIENT);
      break;
    case 2:
      sqlite3_result_text(ctx, pCsr->zLast, -1, SQLITE_TRANSIENT);
      break;
    case 3:
      sqlite3_result_text(ctx, pCsr->zPglist, -1, SQLITE_TRANSIENT);
      break;
  }
  return SQLITE_OK;
}
static int hctvalidRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  hctvalid_cursor *pCsr = (hctvalid_cursor*)cur;
  *pRowid = pCsr->iEntry;
  return SQLITE_OK;
}



int sqlite3HctVtabInit(sqlite3 *db){
  static sqlite3_module hctdbModule = {
    /* iVersion    */ 0,
    /* xCreate     */ 0,
    /* xConnect    */ hctdbConnect,
    /* xBestIndex  */ hctdbBestIndex,
    /* xDisconnect */ hctdbDisconnect,
    /* xDestroy    */ 0,
    /* xOpen       */ hctdbOpen,
    /* xClose      */ hctdbClose,
    /* xFilter     */ hctdbFilter,
    /* xNext       */ hctdbNext,
    /* xEof        */ hctdbEof,
    /* xColumn     */ hctdbColumn,
    /* xRowid      */ hctdbRowid,
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

  static sqlite3_module hctentryModule = {
    /* iVersion    */ 0,
    /* xCreate     */ 0,
    /* xConnect    */ hctentryConnect,
    /* xBestIndex  */ hctentryBestIndex,
    /* xDisconnect */ hctentryDisconnect,
    /* xDestroy    */ 0,
    /* xOpen       */ hctentryOpen,
    /* xClose      */ hctentryClose,
    /* xFilter     */ hctentryFilter,
    /* xNext       */ hctentryNext,
    /* xEof        */ hctentryEof,
    /* xColumn     */ hctentryColumn,
    /* xRowid      */ hctentryRowid,
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

  static sqlite3_module hctvalidModule = {
    /* iVersion    */ 0,
    /* xCreate     */ 0,
    /* xConnect    */ hctvalidConnect,
    /* xBestIndex  */ hctvalidBestIndex,
    /* xDisconnect */ hctvalidDisconnect,
    /* xDestroy    */ 0,
    /* xOpen       */ hctvalidOpen,
    /* xClose      */ hctvalidClose,
    /* xFilter     */ hctvalidFilter,
    /* xNext       */ hctvalidNext,
    /* xEof        */ hctvalidEof,
    /* xColumn     */ hctvalidColumn,
    /* xRowid      */ hctvalidRowid,
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

  int rc;

  rc = sqlite3_create_module(db, "hctdb", &hctdbModule, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_module(db, "hctentry", &hctentryModule, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_module(db, "hctvalid", &hctvalidModule, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFileVtabInit(db);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctPManVtabInit(db);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctStatsInit(db);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctJrnlInit(db);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctTmapVtabInit(db);
  }
  return rc;
}
