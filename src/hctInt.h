
#include <sqliteInt.h>

typedef sqlite3_int64 i64;
typedef unsigned char u8;
typedef unsigned int u32;

/*
** Page types. These are the values that may appear in the page-type
** field of a page header.
*/
#define HCT_PAGETYPE_INTKEY_LEAF 0x01
#define HCT_PAGETYPE_INTKEY_NODE 0x02
#define HCT_PAGETYPE_INDEX_LEAF  0x03
#define HCT_PAGETYPE_INDEX_NODE  0x04
#define HCT_PAGETYPE_OVERFLOW    0x05

/* This bit may be set if (ePagetype & 0x07) is HCT_PAGETYPE_INTKEY_NODE
** or HCT_PAGETYPE_INDEX_NODE.  */
#define HCT_PAGETYPE_REVERSE     0x08

/*************************************************************************
** Interface to code in hct_tree.c
*/
typedef struct HctTree HctTree;
typedef struct HctTreeCsr HctTreeCsr;

int sqlite3HctTreeNew(HctTree **ppTree);
void sqlite3HctTreeFree(HctTree *pTree);

int sqlite3HctTreeInsert(HctTreeCsr*, UnpackedRecord*, i64, int, const u8*,int);
int sqlite3HctTreeDelete(HctTreeCsr *pCsr);

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

int sqlite3HctTreeClearOne(HctTree *pTree, u32 iRoot, int *pnRow);

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

int sqlite3HctDbInsert(HctDatabase*, u32, UnpackedRecord*, i64, int, const u8*);
int sqlite3HctDbDelete(HctDatabase*, u32, UnpackedRecord*, i64);
int sqlite3HctDbEndTransaction(HctDatabase *p);

int sqlite3HctDbCsrOpen(HctDatabase *pDb, u32 iRoot, HctDbCsr **ppCsr);
void sqlite3HctDbCsrClose(HctDbCsr *pCsr);

void sqlite3HctDbCsrDir(HctDbCsr*, int eDir);
int sqlite3HctDbCsrSeek(HctDbCsr*, UnpackedRecord*, i64 iKey, int *pRes);

int sqlite3HctDbCsrEof(HctDbCsr*);
int sqlite3HctDbCsrFirst(HctDbCsr*);
int sqlite3HctDbCsrLast(HctDbCsr*);
int sqlite3HctDbCsrNext(HctDbCsr*);

void sqlite3HctDbCsrKey(HctDbCsr*, i64 *piKey);
int sqlite3HctDbCsrData(HctDbCsr *pCsr, int *pnData, const u8 **paData);

/*************************************************************************
** Interface to code in hct_file.c
*/

typedef struct HctFile HctFile;

int sqlite3HctFileOpen(const char *zFile, HctFile **ppFile);
void sqlite3HctFileClose(HctFile *pFile);

/* Return the page-size in bytes */
int sqlite3HctFilePagesize(HctFile *pFile);
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

/*
** Read, write and allocate database pages.
*/
int sqlite3HctFilePageGet(HctFile *pFile, u32 iPg, HctFilePage *pPg);
int sqlite3HctFilePageNew(HctFile *pFile, u32 iPg, HctFilePage *pPg);
int sqlite3HctFilePageWrite(HctFilePage *pPg);
int sqlite3HctFilePageDelete(HctFilePage *pPg);
int sqlite3HctFilePageRelease(HctFilePage *pPg);

u64 sqlite3HctFileStartTrans(HctFile *pFile);
int sqlite3HctFileFinishTrans(HctFile *pFile);

u64 sqlite3HctFileGetTransid(HctFile *pFile);

HctDatabase *sqlite3HctDbFind(sqlite3*, int);

