
#include <sqliteInt.h>

typedef sqlite3_int64 i64;
typedef unsigned char u8;
typedef unsigned int u32;

# define HctAtomicStore(PTR,VAL)  __atomic_store_n((PTR),(VAL),__ATOMIC_RELAXED)

#include <hctTMapInt.h>

struct HctConfig {
  int nMaxEdksFan;
};

/*************************************************************************
** Interface to code in hct_tree.c
*/
typedef struct HctTree HctTree;
typedef struct HctTreeCsr HctTreeCsr;

int sqlite3HctTreeNew(HctTree **ppTree);
void sqlite3HctTreeFree(HctTree *pTree);

int sqlite3HctTreeInsert(HctTreeCsr*, UnpackedRecord*, i64, int, const u8*,int);
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

u32 sqlite3HctTreeCsrRoot(HctTreeCsr *pCsr);

/* Iterate through non-empty tables/indexes within an HctTree structure. Used
** when flushing contents to disk.  */
int sqlite3HctTreeForeach(
  HctTree *pTree,
  void *pCtx,
  int (*x)(void *, u32, KeyInfo*)
);
void sqlite3HctTreeClear(HctTree *pTree);

/*************************************************************************
** Interface to code in hct_database.c
*/
typedef struct HctDatabase HctDatabase;
typedef struct HctDbCsr HctDbCsr;

int sqlite3HctDbOpen(const char *zFile, HctDatabase **ppDb);
void sqlite3HctDbClose(HctDatabase *pDb);

int sqlite3HctDbRootNew(HctDatabase *p, u32 *piRoot);
int sqlite3HctDbRootFree(HctDatabase *p, u32 iRoot);

int sqlite3HctDbRootInit(HctDatabase *p, int bIndex, u32 iRoot);
void sqlite3HctDbRootPageInit(int bIndex, u8 *aPage, int szPage);
void sqlite3HctDbMetaPageInit(u8 *aPage, int szPage);
int sqlite3HctDbGetMeta(HctDatabase *p, u8 *aBuf, int nBuf);

int sqlite3HctDbInsert(
  HctDatabase *pDb, 
  u32 iRoot,
  UnpackedRecord *pRec, i64 iKey, 
  int bDel, int nData, const u8 *aData,
  int *pnRetry
);
int sqlite3HctDbInsertFlush(HctDatabase *pDb, int *pnRetry);
int sqlite3HctDbStartWrite(HctDatabase*);
int sqlite3HctDbEndWrite(HctDatabase*);
int sqlite3HctDbEndRead(HctDatabase*);

void sqlite3HctDbRollbackMode(HctDatabase*,int);

int sqlite3HctDbCsrOpen(HctDatabase*, struct KeyInfo*, u32 iRoot, HctDbCsr**);
void sqlite3HctDbCsrClose(HctDbCsr *pCsr);

void sqlite3HctDbCsrDir(HctDbCsr*, int eDir);
int sqlite3HctDbCsrSeek(HctDbCsr*, UnpackedRecord*, i64 iKey, int *pRes);

int sqlite3HctDbCsrEof(HctDbCsr*);
int sqlite3HctDbCsrFirst(HctDbCsr*);
int sqlite3HctDbCsrLast(HctDbCsr*);
int sqlite3HctDbCsrNext(HctDbCsr*);
int sqlite3HctDbCsrPrev(HctDbCsr*);

void sqlite3HctDbCsrKey(HctDbCsr*, i64 *piKey);
int sqlite3HctDbCsrData(HctDbCsr *pCsr, int *pnData, const u8 **paData);
int sqlite3HctDbCsrLoadAndDecode(HctDbCsr *pCsr, UnpackedRecord **ppRec);

int sqlite3HctDbIsIndex(HctDatabase *pDb, u32 iRoot, int *pbIndex);

/*************************************************************************
** Interface to code in hct_file.c
*/

typedef struct HctFile HctFile;
HctFile *sqlite3HctDbFile(HctDatabase *pDb);

int sqlite3HctFileOpen(
  const char *zFile, 
  HctFile **ppFile
);
void sqlite3HctFileClose(HctFile *pFile);

typedef struct HctFileGlobal HctFileGlobal;
struct HctFileGlobal { sqlite3_mutex *pMutex; };
HctFileGlobal *sqlite3HctFileGlobal(
  HctFile *pFile,
  int nGlobal,
  void (*xDelete)(HctFileGlobal*)
);

u32 sqlite3HctFileMaxpage(HctFile *pFile);

/*
** Allocate logical root page numbers. And free the same (required if the
** transaction is rolled back).
*/
int sqlite3HctFileRootNew(HctFile *pFile, u32 *piRoot);
int sqlite3HctFileRootFree(HctFile *pFile, u32 iRoot);

typedef struct HctFilePage HctFilePage;
struct HctFilePage {
  u8 *aOld;                       /* Current buffer, or NULL */
  u8 *aNew;                       /* New buffer (to be populated) */

  /* Used internally by hct_file.c */
  u32 iPg;                        /* logical page number */
  u32 iNewPg;                     /* New physical page number */
  u64 iPagemap;                   /* Value read from page-map slot */
  int bDelete;                    /* True when page marked for deletion */
  HctFile *pFile;
};

int sqlite3HctFilePageNew(HctFile *pFile, u32 iPg, HctFilePage *pPg);

/*
** Obtain a read-only reference to logical page iPg.
*/
int sqlite3HctFilePageGet(HctFile *pFile, u32 iPg, HctFilePage *pPg);

/*
** If the page is not already writable (if pPg->aNew==0), make it writable.
** This involves allocating a new physical page and setting pPg->aNew
** to point to the buffer.
*/
int sqlite3HctFilePageWrite(HctFilePage *pPg);

/*
** This is a no-op if the page is not writable.
**
** If the page is already writable, reverse this so that will not be
** written out when PageRelease() or PageCommit() is called. This reclaims 
** the physical page that was allocated by the earlier PageWrite() call
** and sets pPg->aNew to NULL.
*/
void sqlite3HctFilePageUnwrite(HctFilePage *pPg);

/*
** This is a no-op if the page is not writable.
**
** Commit the new version of the page to disk (i.e. set the page-map entry 
** so that the logical page number now maps to the new version of the page
** in pPg->aNew). Then make pPg a non-writable reference to the logical
** page (so that pPg->aOld points to the new version of the page and
** pPg->aNew is NULL).
*/
int sqlite3HctFilePageCommit(HctFilePage *pPg);

/*
** Release a page reference obtained via an earlier call to 
** sqlite3HctFilePageGet() or sqlite3HctFilePageNew(). After this call
** pPg->aOld is NULL.
**
** If the page is writable, it is committed (see sqlite3HctFilePageCommit)
** before the reference is released.
*/
int sqlite3HctFilePageRelease(HctFilePage *pPg);


void sqlite3HctFilePageZero(HctFilePage *pPg);
int sqlite3HctFilePageDelete(HctFilePage *pPg);
int sqlite3HctFilePageGetPhysical(HctFile *pFile, u32 iPg, HctFilePage *pPg);
int sqlite3HctFilePageNewPhysical(HctFile *pFile, HctFilePage *pPg);

u64 sqlite3HctFileStartTrans(HctFile *pFile);
int sqlite3HctFileFinishTrans(HctFile *pFile);

u64 sqlite3HctFileAllocateTransid(HctFile *pFile);
u64 sqlite3HctFileAllocateCID(HctFile *pFile);
u64 sqlite3HctFileGetSnapshotid(HctFile *pFile);

HctTMapClient *sqlite3HctFileTMapClient(HctFile*);

HctDatabase *sqlite3HctDbFind(sqlite3*, int);

int sqlite3HctFilePgsz(HctFile *pFile);
int sqlite3HctFileVtabInit(sqlite3 *db);

