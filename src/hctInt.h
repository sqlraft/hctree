
#include <sqliteInt.h>
#include "sqlite3hct.h"

typedef sqlite3_int64 i64;
typedef unsigned char u8;
typedef unsigned int u32;

/*
** Primitives for atomic load and store.
*/
#define HctAtomicStore(PTR,VAL)  __atomic_store_n((PTR),(VAL), __ATOMIC_SEQ_CST)
#define HctAtomicLoad(PTR)  __atomic_load_n((PTR), __ATOMIC_SEQ_CST)

#define HctCASBool(PTR,OLD,NEW) \
    (int)__sync_bool_compare_and_swap((PTR),(OLD),(NEW))


/*
*/
typedef struct HctConfig HctConfig;
struct HctConfig {
  int nDbFile;                    /* Number of files (hct_file.c) */
  int nPageSet;                   /* Used by hct_pman.c */
  int nPageScan;                  /* Used by hct_pman.c */
  int szLogChunk;                 /* Used by hctree.c */
  int nTryBeforeUnevict;
  int bQuiescentIntegrityCheck;   /* PRAGMA hct_quiescent_integrity_check */
  int pgsz;
  sqlite3 *db;
};

#define HCT_TID_MASK  ((((u64)0x00FFFFFF) << 32)|0xFFFFFFFF)
#define HCT_PGNO_MASK ((u64)0xFFFFFFFF)

#define HCT_MAX_NDBFILE                128

#define HCT_DEFAULT_NDBFILE              1
#define HCT_DEFAULT_NPAGESET           256
#define HCT_DEFAULT_NTRYBEFOREUNEVICT  100
#define HCT_DEFAULT_NPAGESCAN         1024
#define HCT_DEFAULT_SZLOGCHUNK       16384
#define HCT_DEFAULT_PAGESIZE          4096



#include <hctTMapInt.h>
#include <hctFileInt.h>

#ifdef SQLITE_DEBUG
# define SQLITE_LOCKED_ERR(x,y) sqlite3HctLockedErr(x,y)
 int sqlite3HctLockedErr(u32 pgno, const char *zReason);
#else
# define SQLITE_LOCKED_ERR(x,y) SQLITE_LOCKED
#endif

#define HCT_TREE_SCHEMAOP_ROOT 3

/* 
** Growable buffer type used for various things.
*/
typedef struct HctBuffer HctBuffer;
struct HctBuffer {
  u8 *aBuf;
  int nBuf;
  int nAlloc;
};
int sqlite3HctBufferGrow(HctBuffer *pBuf, int nSize);
void sqlite3HctBufferFree(HctBuffer *pBuf);



/*************************************************************************
** Interface to code in hct_tree.c
*/
typedef struct HctTree HctTree;
typedef struct HctTreeCsr HctTreeCsr;

int sqlite3HctTreeNew(HctTree **ppTree);
void sqlite3HctTreeFree(HctTree *pTree);

int sqlite3HctTreeInsert(HctTreeCsr*, UnpackedRecord*, i64, int, const u8*,int);
int sqlite3HctTreeAppend(HctTreeCsr*, KeyInfo*, i64, int, const u8*,int);
int sqlite3HctTreeDelete(HctTreeCsr *pCsr);
int sqlite3HctTreeDeleteKey(HctTreeCsr *, UnpackedRecord *, i64, int,const u8*);

/* 
** These functions are used to open and close transactions and nested 
** sub-transactions.
**
** The Begin() function is used to open transactions and sub-transactions. 
** A successful call to Begin() ensures that there are at least iLevel 
** nested transactions open. To open a top-level transaction, pass iLevel=1. 
** To open a sub-transaction within the top-level transaction, iLevel=2. 
** Passing iLevel=0 is a no-op.
**
** Release() is used to commit transactions and sub-transactions. A
** successful call to Release() ensures that there are at most iLevel 
** nested transactions open. To commit a top-level transaction, pass iLevel=0. 
** To commit all sub-transactions inside the main transaction, pass iLevel=1.
**
** Function lsm_rollback() is used to roll back transactions and
** sub-transactions. A successful call to lsm_rollback() restores the database 
** to the state it was in when the iLevel'th nested sub-transaction (if any) 
** was first opened. And then closes transactions to ensure that there are 
** at most iLevel nested transactions open. Passing iLevel=0 rolls back and 
** closes the top-level transaction. iLevel=1 also rolls back the top-level 
** transaction, but leaves it open. iLevel=2 rolls back the sub-transaction 
** nested directly inside the top-level transaction (and leaves it open).
*/
int sqlite3HctTreeBegin(HctTree *pTree, int iStmt);
int sqlite3HctTreeRelease(HctTree *pTree, int iStmt);
int sqlite3HctTreeRollbackTo(HctTree *pTree, int iStmt);

int sqlite3HctTreeClearOne(HctTree *pTree, u32 iRoot, i64 *pnRow);

int sqlite3HctTreeCsrOpen(HctTree *pTree, u32 iRoot, HctTreeCsr **ppCsr);
int sqlite3HctTreeCsrClose(HctTreeCsr *pCsr);

int sqlite3HctTreeCsrNext(HctTreeCsr *pCsr);
int sqlite3HctTreeCsrPrev(HctTreeCsr *pCsr);
int sqlite3HctTreeCsrEof(HctTreeCsr *pCsr);

int sqlite3HctTreeCsrSeek(HctTreeCsr*, UnpackedRecord*, i64 iKey, int *pRes);
int sqlite3HctTreeCsrFirst(HctTreeCsr *pCsr);
int sqlite3HctTreeCsrLast(HctTreeCsr *pCsr);

int sqlite3HctTreeCsrKey(HctTreeCsr *pCsr, i64 *piKey);
int sqlite3HctTreeCsrData(HctTreeCsr *pCsr, int *pnData, const u8 **paData);
int sqlite3HctTreeCsrIsDelete(HctTreeCsr *pCsr);

void sqlite3HctTreeCsrPin(HctTreeCsr *pCsr);
void sqlite3HctTreeCsrUnpin(HctTreeCsr *pCsr);

int sqlite3HctTreeCsrHasMoved(HctTreeCsr *pCsr);
int sqlite3HctTreeCsrRestore(HctTreeCsr *pCsr, int *pIsDifferent);
void sqlite3HctTreeCsrClear(HctTreeCsr *pCsr);

u32 sqlite3HctTreeCsrRoot(HctTreeCsr *pCsr);

int sqlite3HctTreeCsrIsEmpty(HctTreeCsr *pCsr);
int sqlite3HctTreeCsrTblIsEmpty(HctTree *p, u32 pgnoRoot);

/* 
** Iterate through non-empty tables/indexes within an HctTree structure. Used
** when flushing contents to disk.  
**
** If parameter bSchemaOp is false, then no callback is issued for the table
** with root page number HCT_TREE_SCHEMAOP_ROOT. If bSchemaOp is non-zero,
** then HCT_TREE_SCHEMAOP_ROOT is treated like any other table.
*/

int sqlite3HctTreeForeach(
  HctTree *pTree,
  int bSchemOp,
  void *pCtx,
  int (*x)(void *, u32, KeyInfo*)
);
void sqlite3HctTreeClear(HctTree *pTree);

void sqlite3HctTreeCsrIncrblob(HctTreeCsr *pCsr);
int sqlite3HctTreeCsrReseek(HctTreeCsr *pCsr, int*);

int sqlite3HctTreeUpdateMeta(HctTree*, const u8*, int);

/*************************************************************************
** Interface to code in hct_database.c
*/
typedef struct HctDatabase HctDatabase;
typedef struct HctDbCsr HctDbCsr;

typedef struct HctJournal HctJournal;

HctDatabase *sqlite3HctDbFind(sqlite3*, int);
int sqlite3HctDetectJournals(sqlite3 *db);

HctDatabase *sqlite3HctDbOpen(int*, const char *zFile, HctConfig*);
void sqlite3HctDbClose(HctDatabase *pDb);

int sqlite3HctDbRootNew(HctDatabase *p, u32 *piRoot);

int sqlite3HctDbRootInit(HctDatabase *p, int bIndex, u32 iRoot);
void sqlite3HctDbRootPageInit(int bIndex, u8 *aPage, int szPage);
int sqlite3HctDbGetMeta(HctDatabase *p, u8 *aBuf, int nBuf);

int sqlite3HctDbInsert(
  HctDatabase *pDb, 
  u32 iRoot,
  UnpackedRecord *pRec, i64 iKey, 
  int bDel, int nData, const u8 *aData,
  int *pnRetry
);
int sqlite3HctDbInsertFlush(HctDatabase *pDb, int *pnRetry);
int sqlite3HctDbStartRead(HctDatabase*,HctJournal*);
int sqlite3HctDbStartWrite(HctDatabase*, u64*);
int sqlite3HctDbEndWrite(HctDatabase*, u64, int);
int sqlite3HctDbEndRead(HctDatabase*);
int sqlite3HctDbValidate(sqlite3*, HctDatabase*, u64 *piCid, int*);

i64 sqlite3HctDbTid(HctDatabase *);

void sqlite3HctDbRollbackMode(HctDatabase*,int);

int sqlite3HctDbCsrOpen(HctDatabase*, struct KeyInfo*, u32 iRoot, HctDbCsr**);
void sqlite3HctDbCsrClose(HctDbCsr *pCsr);

void sqlite3HctDbCsrNosnap(HctDbCsr *pCsr, int bNosnap);
void sqlite3HctDbCsrNoscan(HctDbCsr *pCsr, int bNoscan);

void sqlite3HctDbCsrDir(HctDbCsr*, int eDir);
int sqlite3HctDbCsrSeek(HctDbCsr*, UnpackedRecord*, i64 iKey, int *pRes);

int sqlite3HctDbCsrEof(HctDbCsr*);
int sqlite3HctDbCsrFirst(HctDbCsr*);
int sqlite3HctDbCsrLast(HctDbCsr*);
int sqlite3HctDbCsrNext(HctDbCsr*);
int sqlite3HctDbCsrPrev(HctDbCsr*);
void sqlite3HctDbCsrClear(HctDbCsr*);

void sqlite3HctDbCsrKey(HctDbCsr*, i64 *piKey);
int sqlite3HctDbCsrData(HctDbCsr *pCsr, int *pnData, const u8 **paData);
int sqlite3HctDbCsrLoadAndDecode(HctDbCsr *pCsr, UnpackedRecord **ppRec);

int sqlite3HctDbIsIndex(HctDatabase *pDb, u32 iRoot, int *pbIndex);

int sqlite3HctDbStartRecovery(HctDatabase *pDb, int iStage);
int sqlite3HctDbFinishRecovery(HctDatabase *db, int iStage, int rc);
void sqlite3HctDbRecoverTid(HctDatabase *db, u64 iTid);

char *sqlite3HctDbLogFile(HctDatabase*);

i64 sqlite3HctDbNCasFail(HctDatabase*);

char *sqlite3HctDbIntegrityCheck(HctDatabase*, u32 *aRoot,Mem*,int nRoot, int*);
i64 sqlite3HctDbStats(sqlite3 *db, int iStat, const char **pzStat);

int sqlite3HctDbCsrRollbackSeek(HctDbCsr*, UnpackedRecord*, i64, int *pOp);

void sqlite3HctDbSetSavePhysical(
  HctDatabase *pDb,
  int (*xSave)(void*, i64 iPhys),
  void *pSave
);

char *sqlite3HctDbRecordToText(sqlite3 *db, const u8 *aRec, int nRec);

void sqlite3HctDbTMapScan(HctDatabase *pDb);

void sqlite3HctDbTransIsConcurrent(HctDatabase *pDb, int bConcurrent);

HctFile *sqlite3HctDbFile(HctDatabase *pDb);

int sqlite3HctDbWalkTree(
  HctFile *pFile,                 /* File tree resides in */
  u32 iRoot,                      /* Root page of tree */
  int (*x)(void*, u32, u32),      /* Callback function */
  void *pCtx                      /* First argument to pass to x() */
);

int sqlite3HctDbPagesize(HctDatabase *pDb);

void sqlite3HctDbRecordTrim(UnpackedRecord *pRec);

/*
** This function returns the current snapshot-id. It may only be called
** when a read transaction is active.
*/
i64 sqlite3HctDbSnapshotId(HctDatabase *pDb);

u64 sqlite3HctDbReqSnapshot(HctDatabase *pDb);

int sqlite3HctDbCsrFindLastWrite(
  HctDbCsr *pCsr,                 /* Cursor to seek */
  UnpackedRecord *pRec,           /* Key for index/without rowid tables */
  i64 iKey,                       /* Key for intkey tables */
  u64 *piCid                      /* Last CID to write to this key */
);

void sqlite3HctDbJrnlWriteCid(HctDatabase *pDb, u64 iVal);

int sqlite3HctDbDirectInsert(
  HctDbCsr *pCsr, 
  int bStrictAppend,
  UnpackedRecord *pRec, i64 iKey, 
  int nData, const u8 *aData,
  int *pbFail
);
int sqlite3HctDbDirectClear(HctDatabase *pDb, u32 iRoot);

int sqlite3HctDbCsrIsLast(HctDbCsr *pCsr);

int sqlite3HctDbValidateTablename(HctDatabase*, const u8*, int, u64);

/*************************************************************************
** Interface to code in hct_file.c
*/

/*************************************************************************
** Interface to code in hct_record.c
*/
int sqlite3HctSerializeRecord(
  UnpackedRecord *pRec,           /* Record to serialize */
  u8 **ppRec,                     /* OUT: buffer containing serialization */
  int *pnRec                      /* OUT: size of (*ppRec) in bytes */
);

int sqlite3HctNameFromSchemaRecord(const u8*, int, const u8**);

/*************************************************************************
** Interface to code in hct_stats.c
*/
int sqlite3HctStatsInit(sqlite3*);

/*************************************************************************
** Utility functions:
*/
void *sqlite3HctMalloc(int *pRc, i64 nByte);

/*************************************************************************
** hctree.c:
**/

#include <hctJrnlInt.h>
HctJournal *sqlite3HctJrnlFind(sqlite3*);

int sqlite3HctBtreeIsNewTable(Btree *pBt, u64 iRoot);
u64 sqlite3HctBtreeSnapshotId(Btree *pBt);

i64 sqlite3HctMainStats(sqlite3 *db, int iStat, const char **pzStat);


