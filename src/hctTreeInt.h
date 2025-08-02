/*
** 2025 July 31
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
** Function RollbackTo() is used to roll back transactions and
** sub-transactions. A successful call to RollbackTo() restores the database 
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

/*
** Return false if the tree contains any keys for root table. Or true 
** otherwise.
*/
int sqlite3HctTreeIsEmpty(HctTree *pTree, i64 iRoot);

/* 
** Iterate through non-empty tables/indexes within an HctTree structure. Used
** when flushing contents to disk.  
*/
int sqlite3HctTreeForeach(
  HctTree *pTree,
  void *pCtx,
  int (*x)(void *, u32, KeyInfo*)
);
void sqlite3HctTreeClear(HctTree *pTree);

void sqlite3HctTreeCsrIncrblob(HctTreeCsr *pCsr);
int sqlite3HctTreeCsrReseek(HctTreeCsr *pCsr, int*);

int sqlite3HctTreeUpdateMeta(HctTree*, const u8*, int);

/*
** If the tree currently contains an entry for database meta-data, copy
** the first nMeta bytes of it into buffer aMeta and return SQLITE_OK. 
** Otherwise, if the tree contains no such entry, return SQLITE_EMPTY.
*/
int sqlite3HctTreeGetMeta(HctTree *pTree, u8 *aMeta, int nMeta);


