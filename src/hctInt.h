
#include <sqliteInt.h>

typedef sqlite3_int64 i64;
typedef unsigned char u8;
typedef unsigned int u32;


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

void sqlite3HctTreeClear(HctTree *pTree);
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

/*************************************************************************
** Interface to code in hct_database.c
*/

typedef struct HctDatabase HctDatabase;

int sqlite3HctDbOpen(const char *zFile, HctDatabase **ppDb);
void sqlite3HctDbClose(HctDatabase *pDb);

/*************************************************************************
** Interface to code in hct_file.c
*/

typedef struct HctFile HctFile;

int sqlite3HctFileOpen(const char *zFile, HctFile **ppFile);
void sqlite3HctFileClose(HctFile *pFile);

/* Return the page-size in bytes */
int sqlite3HctFilePagesize(HctFile *pFile);

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
};

/*
** Read, write and allocate database pages.
*/
int sqlite3HctFilePageGet(HctFile *pFile, u32 iPg, HctFilePage *pPg);
int sqlite3HctFilePageNew(HctFile *pFile, HctFilePage *pPg);
int sqlite3HctFilePageWrite(HctFilePage *pPg);
int sqlite3HctFilePageDelete(HctFilePage *pPg);
int sqlite3HctFilePageRelease(HctFilePage *pPg);




