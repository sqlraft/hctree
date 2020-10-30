/*
** 2004 April 6
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

#include "sqliteInt.h"
#include "hctInt.h"

#ifdef SQLITE_ENABLE_HCT

typedef struct BtNewRoot BtNewRoot;

/*
** aNewRoot[]:
**   Array of nNewRoot BtNewRoot structures. Each such structure represents
**   a new table or index created by the current transaction. 
**   aNewRoot[x].iSavepoint contains the open savepoint count when the table
**   with root page aNewRoot[x].pgnoRoot was created. The value 
**   Btree.db->nSavepoint.
*/
struct Btree {
  sqlite3 *db;
  HctTree *pHctTree;              /* In-memory part of database */
  HctDatabase *pHctDb;            /* On-disk part of db, if any */
  void *pSchema;                  /* Schema memory from sqlite3BtreeSchema() */
  void(*xSchemaFree)(void*);      /* Function to free pSchema */
  int eTrans;                     /* SQLITE_TXN_NONE, READ or WRITE */
  BtCursor *pCsrList;             /* List of all open cursors */

  int nNewRoot;
  BtNewRoot *aNewRoot;

  u32 iNextRoot;                  /* Next root page to allocate if pHctDb==0 */
  u32 aMeta[16];                  /* 16 database meta values */
};

struct BtNewRoot {
  int iSavepoint;
  int bIndex;
  u32 pgnoRoot;
};

struct BtCursor {
  Btree *pBtree;

  HctTreeCsr *pHctTreeCsr;
  HctDbCsr *pHctDbCsr;
  int bUseTree;                   /* 1 if tree-csr is current entry, else 0 */
  int eDir;                       /* One of BTREE_DIR_NONE, FORWARD, REVERSE */

  KeyInfo *pKeyInfo;              /* For non-intkey tables */
  int errCode;
  int wrFlag;                     /* Value of wrFlag when cursor opened */
  BtCursor *pCsrNext;             /* Next element in Btree.pCsrList list */
};


#ifdef SQLITE_TEST
BtShared *SQLITE_WSD sqlite3SharedCacheList = 0;
#endif

#ifndef SQLITE_OMIT_SHARED_CACHE
/*
** Enable or disable the shared pager and schema features.
**
** This routine has no effect on existing database connections.
** The shared cache setting effects only future calls to
** sqlite3_open(), sqlite3_open16(), or sqlite3_open_v2().
*/
int sqlite3_enable_shared_cache(int enable){
  sqlite3GlobalConfig.sharedCacheEnabled = enable;
  return SQLITE_OK;
}
#endif


#ifdef SQLITE_DEBUG
/*
** Return an reset the seek counter for a Btree object.
*/
sqlite3_uint64 sqlite3BtreeSeekCount(Btree *pBt){
  assert( 0 );
  return 0;
}
#endif

/*
** Clear the current cursor position.
*/
void sqlite3BtreeClearCursor(BtCursor *pCur){
  assert( 0 );
}

/*
** Determine whether or not a cursor has moved from the position where
** it was last placed, or has been invalidated for any other reason.
** Cursors can move when the row they are pointing at is deleted out
** from under them, for example.  Cursor might also move if a btree
** is rebalanced.
**
** Calling this routine with a NULL cursor pointer returns false.
**
** Use the separate sqlite3BtreeCursorRestore() routine to restore a cursor
** back to where it ought to be if this routine returns true.
*/
int sqlite3BtreeCursorHasMoved(BtCursor *pCur){
  return sqlite3HctTreeCsrHasMoved(pCur->pHctTreeCsr);
}

/*
** Return a pointer to a fake BtCursor object that will always answer
** false to the sqlite3BtreeCursorHasMoved() routine above.  The fake
** cursor returned must not be used with any other Btree interface.
*/
BtCursor *sqlite3BtreeFakeValidCursor(void){
  static BtCursor csr = {0,0,0};
  return &csr;
}

/*
** This routine restores a cursor back to its original position after it
** has been moved by some outside activity (such as a btree rebalance or
** a row having been deleted out from under the cursor).  
**
** On success, the *pDifferentRow parameter is false if the cursor is left
** pointing at exactly the same row.  *pDifferntRow is the row the cursor
** was pointing to has been deleted, forcing the cursor to point to some
** nearby row.
**
** This routine should only be called for a cursor that just returned
** TRUE from sqlite3BtreeCursorHasMoved().
*/
int sqlite3BtreeCursorRestore(BtCursor *pCur, int *pDifferentRow){
  return sqlite3HctTreeCsrRestore(pCur->pHctTreeCsr, pDifferentRow);
}

/*
** Return the size of the database file in pages. If there is any kind of
** error, return ((unsigned int)-1).
*/
Pgno sqlite3BtreeLastPage(Btree *p){
  return 0xFFFFFFFF;
}

/*
** Provide flag hints to the cursor.
*/
void sqlite3BtreeCursorHintFlags(BtCursor *pCur, unsigned x){
  /* no-op */
  assert( x==BTREE_SEEK_EQ || x==BTREE_BULKLOAD || x==0 );
}

/*
** Open a database file.
** 
** zFilename is the name of the database file.  If zFilename is NULL
** then an ephemeral database is created.  The ephemeral database might
** be exclusively in memory, or it might use a disk-based memory cache.
** Either way, the ephemeral database will be automatically deleted 
** when sqlite3BtreeClose() is called.
**
** If zFilename is ":memory:" then an in-memory database is created
** that is automatically destroyed when it is closed.
**
** The "flags" parameter is a bitmask that might contain bits like
** BTREE_OMIT_JOURNAL and/or BTREE_MEMORY.
**
** If the database is already opened in the same database connection
** and we are in shared cache mode, then the open will fail with an
** SQLITE_CONSTRAINT error.  We cannot allow two or more BtShared
** objects in the same database connection since doing so will lead
** to problems with locking.
*/
int sqlite3BtreeOpen(
  sqlite3_vfs *pVfs,      /* VFS to use for this b-tree */
  const char *zFilename,  /* Name of the file containing the BTree database */
  sqlite3 *db,            /* Associated database handle */
  Btree **ppBtree,        /* Pointer to new Btree object written here */
  int flags,              /* Options */
  int vfsFlags            /* Flags passed through to sqlite3_vfs.xOpen() */
){
  int rc = SQLITE_OK;
  Btree *pNew;

  pNew = (Btree*)sqlite3_malloc(sizeof(Btree));
  if( pNew ){
    memset(pNew, 0, sizeof(Btree));
    pNew->iNextRoot = 2;
    pNew->db = db;
    rc = sqlite3HctTreeNew(&pNew->pHctTree);
  }else{
    rc = SQLITE_NOMEM;
  }

  if( rc==SQLITE_OK && zFilename && zFilename[0] ){
    rc = sqlite3HctDbOpen(zFilename, &pNew->pHctDb);
  }

  if( rc!=SQLITE_OK ){
    sqlite3BtreeClose(pNew);
    pNew = 0;
  }
  *ppBtree = pNew;
  return rc;
}

/*
** Close an open database and invalidate all cursors.
*/
int sqlite3BtreeClose(Btree *p){
  if( p ){
    while(p->pCsrList){
      sqlite3BtreeCloseCursor(p->pCsrList);
    }
    if( p->xSchemaFree ){
      p->xSchemaFree(p->pSchema);
    }
    sqlite3_free(p->pSchema);
    sqlite3HctTreeFree(p->pHctTree);
    sqlite3HctDbClose(p->pHctDb);
    sqlite3_free(p->aNewRoot);
    sqlite3_free(p);
  }
  return SQLITE_OK;
}

/*
** Change the "soft" limit on the number of pages in the cache.
** Unused and unmodified pages will be recycled when the number of
** pages in the cache exceeds this soft limit.  But the size of the
** cache is allowed to grow larger than this limit if it contains
** dirty pages or pages still in active use.
*/
int sqlite3BtreeSetCacheSize(Btree *p, int mxPage){
  /* no-op in hct */
  return SQLITE_OK;
}

/*
** Change the "spill" limit on the number of pages in the cache.
** If the number of pages exceeds this limit during a write transaction,
** the pager might attempt to "spill" pages to the journal early in
** order to free up memory.
**
** The value returned is the current spill size.  If zero is passed
** as an argument, no changes are made to the spill size setting, so
** using mxPage of 0 is a way to query the current spill size.
*/
int sqlite3BtreeSetSpillSize(Btree *p, int mxPage){
  return 1024;
}

#if SQLITE_MAX_MMAP_SIZE>0
/*
** Change the limit on the amount of the database file that may be
** memory mapped.
*/
int sqlite3BtreeSetMmapLimit(Btree *p, sqlite3_int64 szMmap){
  assert( 0 );
  return SQLITE_OK;
}
#endif /* SQLITE_MAX_MMAP_SIZE>0 */

/*
** Change the way data is synced to disk in order to increase or decrease
** how well the database resists damage due to OS crashes and power
** failures.  Level 1 is the same as asynchronous (no syncs() occur and
** there is a high probability of damage)  Level 2 is the default.  There
** is a very low but non-zero probability of damage.  Level 3 reduces the
** probability of damage to near zero but with a write performance reduction.
*/
#ifndef SQLITE_OMIT_PAGER_PRAGMAS
int sqlite3BtreeSetPagerFlags(
  Btree *p,              /* The btree to set the safety level on */
  unsigned pgFlags       /* Various PAGER_* flags */
){
  /* HCT - does this need fixing? */
  return SQLITE_OK;
}
#endif

/*
** Change the default pages size and the number of reserved bytes per page.
** Or, if the page size has already been fixed, return SQLITE_READONLY 
** without changing anything.
**
** The page size must be a power of 2 between 512 and 65536.  If the page
** size supplied does not meet this constraint then the page size is not
** changed.
**
** Page sizes are constrained to be a power of two so that the region
** of the database file used for locking (beginning at PENDING_BYTE,
** the first byte past the 1GB boundary, 0x40000000) needs to occur
** at the beginning of a page.
**
** If parameter nReserve is less than zero, then the number of reserved
** bytes per page is left unchanged.
**
** If the iFix!=0 then the BTS_PAGESIZE_FIXED flag is set so that the page size
** and autovacuum mode can no longer be changed.
*/
int sqlite3BtreeSetPageSize(Btree *p, int pageSize, int nReserve, int iFix){
  int rc = SQLITE_OK;
  return rc;
}

/*
** Return the currently defined page size
*/
int sqlite3BtreeGetPageSize(Btree *p){
  return 4096;
}

/*
** This function is similar to sqlite3BtreeGetReserve(), except that it
** may only be called if it is guaranteed that the b-tree mutex is already
** held.
**
** This is useful in one special case in the backup API code where it is
** known that the shared b-tree mutex is held, but the mutex on the 
** database handle that owns *p is not. In this case if sqlite3BtreeEnter()
** were to be called, it might collide with some other operation on the
** database handle that owns *p, causing undefined behavior.
*/
int sqlite3BtreeGetReserveNoMutex(Btree *p){
  assert( 0 );
  return 0;
}

/*
** Return the number of bytes of space at the end of every page that
** are intentually left unused.  This is the "reserved" space that is
** sometimes used by extensions.
**
** The value returned is the larger of the current reserve size and
** the latest reserve size requested by SQLITE_FILECTRL_RESERVE_BYTES.
** The amount of reserve can only grow - never shrink.
*/
int sqlite3BtreeGetRequestedReserve(Btree *p){
  return 0;
}


/*
** Set the maximum page count for a database if mxPage is positive.
** No changes are made if mxPage is 0 or negative.
** Regardless of the value of mxPage, return the maximum page count.
*/
Pgno sqlite3BtreeMaxPageCount(Btree *p, Pgno mxPage){
  assert( 0 );
  return 0xFFFFFFFF;
}

/*
** Change the values for the BTS_SECURE_DELETE and BTS_OVERWRITE flags:
**
**    newFlag==0       Both BTS_SECURE_DELETE and BTS_OVERWRITE are cleared
**    newFlag==1       BTS_SECURE_DELETE set and BTS_OVERWRITE is cleared
**    newFlag==2       BTS_SECURE_DELETE cleared and BTS_OVERWRITE is set
**    newFlag==(-1)    No changes
**
** This routine acts as a query if newFlag is less than zero
**
** With BTS_OVERWRITE set, deleted content is overwritten by zeros, but
** freelist leaf pages are not written back to the database.  Thus in-page
** deleted content is cleared, but freelist deleted content is not.
**
** With BTS_SECURE_DELETE, operation is like BTS_OVERWRITE with the addition
** that freelist leaf pages are written back into the database, increasing
** the amount of disk I/O.
*/
int sqlite3BtreeSecureDelete(Btree *p, int newFlag){
  return 0;
}

/*
** Change the 'auto-vacuum' property of the database. If the 'autoVacuum'
** parameter is non-zero, then auto-vacuum mode is enabled. If zero, it
** is disabled. The default value for the auto-vacuum property is 
** determined by the SQLITE_DEFAULT_AUTOVACUUM macro.
*/
int sqlite3BtreeSetAutoVacuum(Btree *p, int autoVacuum){
  return SQLITE_OK;
}

/*
** Return the value of the 'auto-vacuum' property. If auto-vacuum is 
** enabled 1 is returned. Otherwise 0.
*/
int sqlite3BtreeGetAutoVacuum(Btree *p){
  /* hct is never in auto-vacuum mode */
  return 0;
}

/*
** Initialize the first page of the database file (creating a database
** consisting of a single page and no schema objects). Return SQLITE_OK
** if successful, or an SQLite error code otherwise.
*/
int sqlite3BtreeNewDb(Btree *p){
  int rc = SQLITE_OK;
  assert( 0 );
  return rc;
}

/*
** Attempt to start a new transaction. A write-transaction
** is started if the second argument is nonzero, otherwise a read-
** transaction.  If the second argument is 2 or more and exclusive
** transaction is started, meaning that no other process is allowed
** to access the database.  A preexisting transaction may not be
** upgraded to exclusive by calling this routine a second time - the
** exclusivity flag only works for a new transaction.
**
** A write-transaction must be started before attempting any 
** changes to the database.  None of the following routines 
** will work unless a transaction is started first:
**
**      sqlite3BtreeCreateTable()
**      sqlite3BtreeCreateIndex()
**      sqlite3BtreeClearTable()
**      sqlite3BtreeDropTable()
**      sqlite3BtreeInsert()
**      sqlite3BtreeDelete()
**      sqlite3BtreeUpdateMeta()
**
** If an initial attempt to acquire the lock fails because of lock contention
** and the database was previously unlocked, then invoke the busy handler
** if there is one.  But if there was previously a read-lock, do not
** invoke the busy handler - just return SQLITE_BUSY.  SQLITE_BUSY is 
** returned when there is already a read-lock in order to avoid a deadlock.
**
** Suppose there are two processes A and B.  A has a read lock and B has
** a reserved lock.  B tries to promote to exclusive but is blocked because
** of A's read lock.  A tries to promote to reserved but is blocked by B.
** One or the other of the two processes must give way or there can be
** no progress.  By returning SQLITE_BUSY and not invoking the busy callback
** when A already has a read lock, we encourage A to give up and let B
** proceed.
*/
int sqlite3BtreeBeginTrans(Btree *p, int wrflag, int *pSchemaVersion){
  int rc = SQLITE_OK;
  int req = wrflag ? SQLITE_TXN_WRITE : SQLITE_TXN_READ;

  if( pSchemaVersion ) *pSchemaVersion = p->aMeta[1];
  if( wrflag ){
    rc = sqlite3HctTreeBegin(p->pHctTree, 1 + p->db->nSavepoint);
  }
  if( rc==SQLITE_OK && p->eTrans<req ){
    p->eTrans = req;
  }
  return rc;
}

/*
** A write-transaction must be opened before calling this function.
** It performs a single unit of work towards an incremental vacuum.
**
** If the incremental vacuum is finished after this function has run,
** SQLITE_DONE is returned. If it is not finished, but no error occurred,
** SQLITE_OK is returned. Otherwise an SQLite error code. 
*/
int sqlite3BtreeIncrVacuum(Btree *p){
  assert( 0 );
  return SQLITE_OK;
}

/*
** This routine does the first phase of a two-phase commit.  This routine
** causes a rollback journal to be created (if it does not already exist)
** and populated with enough information so that if a power loss occurs
** the database can be restored to its original state by playing back
** the journal.  Then the contents of the journal are flushed out to
** the disk.  After the journal is safely on oxide, the changes to the
** database are written into the database file and flushed to oxide.
** At the end of this call, the rollback journal still exists on the
** disk and we are still holding all locks, so the transaction has not
** committed.  See sqlite3BtreeCommitPhaseTwo() for the second phase of the
** commit process.
**
** This call is a no-op if no write-transaction is currently active on pBt.
**
** Otherwise, sync the database file for the btree pBt. zSuperJrnl points to
** the name of a super-journal file that should be written into the
** individual journal file, or is NULL, indicating no super-journal file 
** (single database transaction).
**
** When this is called, the super-journal should already have been
** created, populated with this journal pointer and synced to disk.
**
** Once this is routine has returned, the only thing required to commit
** the write-transaction for this database file is to delete the journal.
*/
int sqlite3BtreeCommitPhaseOne(Btree *p, const char *zSuperJrnl){
  /* no-op for hct? */
  int rc = SQLITE_OK;
  return rc;
}

static int btreeFlushOneToDisk(void *pCtx, u32 iRoot, KeyInfo *pKeyInfo){
  Btree *p = (Btree*)pCtx;
  HctTreeCsr *pCsr = 0;
  int rc;

  rc = sqlite3HctTreeCsrOpen(p->pHctTree, iRoot, &pCsr);
  if( rc==SQLITE_OK ){
    for(rc=sqlite3HctTreeCsrFirst(pCsr);
        rc==SQLITE_OK && sqlite3HctTreeCsrEof(pCsr)==0;
        rc=sqlite3HctTreeCsrNext(pCsr)
    ){
      i64 iKey = 0;
      int nData = 0;
      const u8 *aData = 0;
      sqlite3HctTreeCsrKey(pCsr, &iKey);
      sqlite3HctTreeCsrData(pCsr, &nData, &aData);
      if( pKeyInfo ){
        assert( 0 );
      }else{
        rc = sqlite3HctDbInsert(p->pHctDb, iRoot, 0, iKey, nData, aData);
      }
      if( rc ) break;
    }
    sqlite3HctTreeCsrClose(pCsr);
  }

  return rc;
}

/*
** Flush the contents of Btree.pHctTree to Btree.pHctDb.
*/
static int btreeFlushToDisk(Btree *p){
  int i;
  int rc = SQLITE_OK;

  /* Initialize any root pages created by this transaction */
  for(i=0; rc==SQLITE_OK && i<p->nNewRoot; i++){
    BtNewRoot *pRoot = &p->aNewRoot[i];
    rc = sqlite3HctDbRootInit(p->pHctDb, pRoot->bIndex, pRoot->pgnoRoot);
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3HctTreeForeach(p->pHctTree, (void*)p, btreeFlushOneToDisk);
  }

  sqlite3HctDbCommit(p->pHctDb);
  return rc;
}

/*
** Commit the transaction currently in progress.
**
** This routine implements the second phase of a 2-phase commit.  The
** sqlite3BtreeCommitPhaseOne() routine does the first phase and should
** be invoked prior to calling this routine.  The sqlite3BtreeCommitPhaseOne()
** routine did all the work of writing information out to disk and flushing the
** contents so that they are written onto the disk platter.  All this
** routine has to do is delete or truncate or zero the header in the
** the rollback journal (which causes the transaction to commit) and
** drop locks.
**
** Normally, if an error occurs while the pager layer is attempting to 
** finalize the underlying journal file, this function returns an error and
** the upper layer will attempt a rollback. However, if the second argument
** is non-zero then this b-tree transaction is part of a multi-file 
** transaction. In this case, the transaction has already been committed 
** (by deleting a super-journal file) and the caller will ignore this 
** functions return code. So, even if an error occurs in the pager layer,
** reset the b-tree objects internal state to indicate that the write
** transaction has been closed. This is quite safe, as the pager will have
** transitioned to the error state.
**
** This will release the write lock on the database file.  If there
** are no active cursors, it also releases the read lock.
*/
int sqlite3BtreeCommitPhaseTwo(Btree *p, int bCleanup){
  int rc = SQLITE_OK;
  if( p->eTrans==SQLITE_TXN_WRITE ){
    sqlite3HctTreeRelease(p->pHctTree, 0);
    if( p->pHctDb ){
      rc = btreeFlushToDisk(p);
      sqlite3HctTreeClear(p->pHctTree);
      p->nNewRoot = 0;
    }
    p->eTrans = SQLITE_TXN_READ;
  }
  return rc;
}

/*
** Do both phases of a commit.
*/
int sqlite3BtreeCommit(Btree *p){
  int rc;
  sqlite3BtreeEnter(p);
  rc = sqlite3BtreeCommitPhaseOne(p, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3BtreeCommitPhaseTwo(p, 0);
  }
  sqlite3BtreeLeave(p);
  return rc;
}

/*
** This routine sets the state to CURSOR_FAULT and the error
** code to errCode for every cursor on any BtShared that pBtree
** references.  Or if the writeOnly flag is set to 1, then only
** trip write cursors and leave read cursors unchanged.
**
** Every cursor is a candidate to be tripped, including cursors
** that belong to other database connections that happen to be
** sharing the cache with pBtree.
**
** This routine gets called when a rollback occurs. If the writeOnly
** flag is true, then only write-cursors need be tripped - read-only
** cursors save their current positions so that they may continue 
** following the rollback. Or, if writeOnly is false, all cursors are 
** tripped. In general, writeOnly is false if the transaction being
** rolled back modified the database schema. In this case b-tree root
** pages may be moved or deleted from the database altogether, making
** it unsafe for read cursors to continue.
**
** If the writeOnly flag is true and an error is encountered while 
** saving the current position of a read-only cursor, all cursors, 
** including all read-cursors are tripped.
**
** SQLITE_OK is returned if successful, or if an error occurs while
** saving a cursor position, an SQLite error code.
*/
int sqlite3BtreeTripAllCursors(Btree *pBtree, int errCode, int writeOnly){
  int rc = SQLITE_OK;
  if( pBtree ){
    BtCursor *p;
    for(p=pBtree->pCsrList; p; p=p->pCsrNext){
      if( writeOnly==0 || p->wrFlag ){
        sqlite3HctTreeCsrClose(p->pHctTreeCsr);
        p->pHctTreeCsr = 0;
        p->errCode = errCode;
      }
    }
  }
  return rc;
}

/*
** Rollback the transaction in progress.
**
** If tripCode is not SQLITE_OK then cursors will be invalidated (tripped).
** Only write cursors are tripped if writeOnly is true but all cursors are
** tripped if writeOnly is false.  Any attempt to use
** a tripped cursor will result in an error.
**
** This will release the write lock on the database file.  If there
** are no active cursors, it also releases the read lock.
*/
int sqlite3BtreeRollback(Btree *p, int tripCode, int writeOnly){
  if( p->eTrans==SQLITE_TXN_WRITE ){
    sqlite3HctTreeRollbackTo(p->pHctTree, 0);
    if( p->pHctDb ){
      sqlite3HctTreeClear(p->pHctTree);
    }
    p->eTrans = SQLITE_TXN_READ;
    p->nNewRoot = 0;
  }
  return SQLITE_OK;
}

/*
** Start a statement subtransaction. The subtransaction can be rolled
** back independently of the main transaction. You must start a transaction 
** before starting a subtransaction. The subtransaction is ended automatically 
** if the main transaction commits or rolls back.
**
** Statement subtransactions are used around individual SQL statements
** that are contained within a BEGIN...COMMIT block.  If a constraint
** error occurs within the statement, the effect of that one statement
** can be rolled back without having to rollback the entire transaction.
**
** A statement sub-transaction is implemented as an anonymous savepoint. The
** value passed as the second parameter is the total number of savepoints,
** including the new anonymous savepoint, open on the B-Tree. i.e. if there
** are no active savepoints and no other statement-transactions open,
** iStatement is 1. This anonymous savepoint can be released or rolled back
** using the sqlite3BtreeSavepoint() function.
*/
int sqlite3BtreeBeginStmt(Btree *p, int iStatement){
  int rc = SQLITE_OK;
  rc = sqlite3HctTreeBegin(p->pHctTree, iStatement+1);
  return rc;
}

static int btreeRollbackRoot(Btree *p, int iSavepoint){
  int i;
  int rc = SQLITE_OK;
  for(i=p->nNewRoot-1; rc==SQLITE_OK && i>=0; i--){
    if( p->aNewRoot[i].iSavepoint<=iSavepoint ) break;
    rc = sqlite3HctDbRootFree(p->pHctDb, p->aNewRoot[i].pgnoRoot);
  }
  p->nNewRoot = i+1;
  return rc;
}

/*
** The second argument to this function, op, is always SAVEPOINT_ROLLBACK
** or SAVEPOINT_RELEASE. This function either releases or rolls back the
** savepoint identified by parameter iSavepoint, depending on the value 
** of op.
**
** Normally, iSavepoint is greater than or equal to zero. However, if op is
** SAVEPOINT_ROLLBACK, then iSavepoint may also be -1. In this case the 
** contents of the entire transaction are rolled back. This is different
** from a normal transaction rollback, as no locks are released and the
** transaction remains open.
*/
int sqlite3BtreeSavepoint(Btree *p, int op, int iSavepoint){
  int rc = SQLITE_OK;
  if( p && p->eTrans==SQLITE_TXN_WRITE ){
    int i;
    assert( op==SAVEPOINT_ROLLBACK || op==SAVEPOINT_RELEASE );
    if( op==SAVEPOINT_RELEASE ){
      for(i=0; i<p->nNewRoot; i++){
        if( p->aNewRoot[i].iSavepoint>iSavepoint ){
          p->aNewRoot[i].iSavepoint = iSavepoint;
        }
      }
      sqlite3HctTreeRelease(p->pHctTree, iSavepoint+1);
    }else{
      sqlite3HctTreeRollbackTo(p->pHctTree, iSavepoint+2);
      btreeRollbackRoot(p, iSavepoint);
    }
  }
  return rc;
}

/*
** Open a new cursor
*/
int sqlite3BtreeCursor(
  Btree *p,                                   /* The btree */
  Pgno iTable,                                /* Root page of table to open */
  int wrFlag,                                 /* 1 to write. 0 read-only */
  struct KeyInfo *pKeyInfo,                   /* First arg to xCompare() */
  BtCursor *pCur                              /* Write new cursor here */
){
  int rc;
  assert( pCur->pHctTreeCsr==0 );
  pCur->pKeyInfo = pKeyInfo;
  rc = sqlite3HctTreeCsrOpen(p->pHctTree, iTable, &pCur->pHctTreeCsr);
  if( rc==SQLITE_OK && p->pHctDb ){
    rc = sqlite3HctDbCsrOpen(p->pHctDb, iTable, &pCur->pHctDbCsr);
  }
  if( rc==SQLITE_OK ){
    pCur->pCsrNext = p->pCsrList;
    pCur->pBtree = p;
    pCur->wrFlag = wrFlag;
    p->pCsrList = pCur;
  }else{
    assert( pCur->pHctTreeCsr==0 );
    pCur->pKeyInfo = 0;
  }
  return rc;
}

/*
** Return the size of a BtCursor object in bytes.
**
** This interfaces is needed so that users of cursors can preallocate
** sufficient storage to hold a cursor.  The BtCursor object is opaque
** to users so they cannot do the sizeof() themselves - they must call
** this routine.
*/
int sqlite3BtreeCursorSize(void){
  return ROUND8(sizeof(BtCursor));
}

/*
** Initialize memory that will be converted into a BtCursor object.
**
** The simple approach here would be to memset() the entire object
** to zero.  But it turns out that the apPage[] and aiIdx[] arrays
** do not need to be zeroed and they are large, so we can save a lot
** of run-time by skipping the initialization of those elements.
*/
void sqlite3BtreeCursorZero(BtCursor *p){
  /* hct takes the simple approach mentioned above */
  memset(p, 0, sizeof(BtCursor));
}

/*
** Close a cursor.  The read lock on the database file is released
** when the last cursor is closed.
*/
int sqlite3BtreeCloseCursor(BtCursor *pCur){
  if( pCur->pBtree ){
    BtCursor **pp;
    sqlite3HctTreeCsrClose(pCur->pHctTreeCsr);
    sqlite3HctDbCsrClose(pCur->pHctDbCsr);
    for(pp=&pCur->pBtree->pCsrList; *pp!=pCur; pp=&(*pp)->pCsrNext);
    *pp = pCur->pCsrNext;
    pCur->pHctTreeCsr = 0;
    pCur->pBtree = 0;
    pCur->pCsrNext = 0;
  }
  return SQLITE_OK;
}

#ifndef NDEBUG  /* The next routine used only within assert() statements */
/*
** Return true if the given BtCursor is valid.  A valid cursor is one
** that is currently pointing to a row in a (non-empty) table.
** This is a verification routine is used only within assert() statements.
*/
int sqlite3BtreeCursorIsValid(BtCursor *pCur){
  return pCur && (
      !sqlite3HctTreeCsrEof(pCur->pHctTreeCsr)
   || !sqlite3HctDbCsrEof(pCur->pHctDbCsr)
  );
}
#endif /* NDEBUG */
int sqlite3BtreeCursorIsValidNN(BtCursor *pCur){
  return (
      !sqlite3HctTreeCsrEof(pCur->pHctTreeCsr)
   || !sqlite3HctDbCsrEof(pCur->pHctDbCsr)
  );
}

/*
** Return the value of the integer key or "rowid" for a table btree.
** This routine is only valid for a cursor that is pointing into a
** ordinary table btree.  If the cursor points to an index btree or
** is invalid, the result of this routine is undefined.
*/
i64 sqlite3BtreeIntegerKey(BtCursor *pCur){
  i64 iKey;
  if( pCur->bUseTree ){
    sqlite3HctTreeCsrKey(pCur->pHctTreeCsr, &iKey);
  }else{
    sqlite3HctDbCsrKey(pCur->pHctDbCsr, &iKey);
  }
  return iKey;
}

/*
** Pin or unpin a cursor.
*/
void sqlite3BtreeCursorPin(BtCursor *pCur){
  sqlite3HctTreeCsrPin(pCur->pHctTreeCsr);
}
void sqlite3BtreeCursorUnpin(BtCursor *pCur){
  sqlite3HctTreeCsrUnpin(pCur->pHctTreeCsr);
}

#ifdef SQLITE_ENABLE_OFFSET_SQL_FUNC
/*
** Return the offset into the database file for the start of the
** payload to which the cursor is pointing.
*/
i64 sqlite3BtreeOffset(BtCursor *pCur){
  assert( 0 );
  return 0;
}
#endif /* SQLITE_ENABLE_OFFSET_SQL_FUNC */

/*
** Return the number of bytes of payload for the entry that pCur is
** currently pointing to.  For table btrees, this will be the amount
** of data.  For index btrees, this will be the size of the key.
**
** The caller must guarantee that the cursor is pointing to a non-NULL
** valid entry.  In other words, the calling procedure must guarantee
** that the cursor has Cursor.eState==CURSOR_VALID.
*/
u32 sqlite3BtreePayloadSize(BtCursor *pCur){
  int nData;
  if( pCur->bUseTree ){
    sqlite3HctTreeCsrData(pCur->pHctTreeCsr, &nData, 0);
  }else{
    sqlite3HctDbCsrData(pCur->pHctDbCsr, &nData, 0);
  }
  return nData;
}

/*
** Return an upper bound on the size of any record for the table
** that the cursor is pointing into.
**
** This is an optimization.  Everything will still work if this
** routine always returns 2147483647 (which is the largest record
** that SQLite can handle) or more.  But returning a smaller value might
** prevent large memory allocations when trying to interpret a
** corrupt datrabase.
**
** The current implementation merely returns the size of the underlying
** database file.
*/
sqlite3_int64 sqlite3BtreeMaxRecordSize(BtCursor *pCur){
  assert( 0 );
  return 0x7FFFFFFF;
}

/*
** Read part of the payload for the row at which that cursor pCur is currently
** pointing.  "amt" bytes will be transferred into pBuf[].  The transfer
** begins at "offset".
**
** pCur can be pointing to either a table or an index b-tree.
** If pointing to a table btree, then the content section is read.  If
** pCur is pointing to an index b-tree then the key section is read.
**
** For sqlite3BtreePayload(), the caller must ensure that pCur is pointing
** to a valid row in the table.  For sqlite3BtreePayloadChecked(), the
** cursor might be invalid or might need to be restored before being read.
**
** Return SQLITE_OK on success or an error code if anything goes
** wrong.  An error is returned if "offset+amt" is larger than
** the available payload.
*/
int sqlite3BtreePayload(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  assert( 0 );
  return SQLITE_OK;
}

/*
** This variant of sqlite3BtreePayload() works even if the cursor has not
** in the CURSOR_VALID state.  It is only used by the sqlite3_blob_read()
** interface.
*/
#ifndef SQLITE_OMIT_INCRBLOB
int sqlite3BtreePayloadChecked(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  assert( 0 );
  return SQLITE_OK;
}
#endif /* SQLITE_OMIT_INCRBLOB */

/*
** For the entry that cursor pCur is point to, return as
** many bytes of the key or data as are available on the local
** b-tree page.  Write the number of available bytes into *pAmt.
**
** The pointer returned is ephemeral.  The key/data may move
** or be destroyed on the next call to any Btree routine,
** including calls from other threads against the same cache.
** Hence, a mutex on the BtShared should be held prior to calling
** this routine.
**
** These routines is used to get quick access to key and data
** in the common case where no overflow pages are used.
*/
const void *sqlite3BtreePayloadFetch(BtCursor *pCur, u32 *pAmt){
  const u8 *aData;
  int nData;
  if( pCur->bUseTree ){
    sqlite3HctTreeCsrData(pCur->pHctTreeCsr, &nData, &aData);
  }else{
    sqlite3HctDbCsrData(pCur->pHctDbCsr, &nData, &aData);
  }
  *pAmt = (u32)nData;
  return aData;
}

static void btreeSetUseTree(BtCursor *pCur){
  int bTreeEof = sqlite3HctTreeCsrEof(pCur->pHctTreeCsr);
  int bDbEof = sqlite3HctDbCsrEof(pCur->pHctDbCsr);

  assert( pCur->eDir==BTREE_DIR_FORWARD || pCur->eDir==BTREE_DIR_REVERSE );

  if( bTreeEof ){
    pCur->bUseTree = 0;
  }else if( bDbEof ){
    pCur->bUseTree = 1;
  }else if( pCur->pKeyInfo==0 ){
    i64 iKeyTree;
    i64 iKeyDb;

    sqlite3HctTreeCsrKey(pCur->pHctTreeCsr, &iKeyTree);
    sqlite3HctDbCsrKey(pCur->pHctDbCsr, &iKeyDb);

    /* TODO - reverse scans */
    pCur->bUseTree = (iKeyTree < iKeyDb);
    if( pCur->eDir==BTREE_DIR_REVERSE ) pCur->bUseTree = !pCur->bUseTree;
  }else{
    /* TODO - index cursors */
    assert( 0 );
  }
}

/* Move the cursor to the first entry in the table.  Return SQLITE_OK
** on success.  Set *pRes to 0 if the cursor actually points to something
** or set *pRes to 1 if the table is empty.
*/
int sqlite3BtreeFirst(BtCursor *pCur, int *pRes){
  int rc = SQLITE_OK;

  sqlite3HctTreeCsrFirst(pCur->pHctTreeCsr);
  rc = sqlite3HctDbCsrFirst(pCur->pHctDbCsr);

  if( rc==SQLITE_OK ){
    int bTreeEof = sqlite3HctTreeCsrEof(pCur->pHctTreeCsr);
    int bDbEof = sqlite3HctDbCsrEof(pCur->pHctDbCsr);
    *pRes = (bTreeEof && bDbEof);
    pCur->eDir = BTREE_DIR_FORWARD;
    btreeSetUseTree(pCur);
  }

  return rc;
}

/* Move the cursor to the last entry in the table.  Return SQLITE_OK
** on success.  Set *pRes to 0 if the cursor actually points to something
** or set *pRes to 1 if the table is empty.
*/
int sqlite3BtreeLast(BtCursor *pCur, int *pRes){
  int rc = SQLITE_OK;
  sqlite3HctTreeCsrLast(pCur->pHctTreeCsr);
  rc = sqlite3HctDbCsrLast(pCur->pHctDbCsr);

  if( rc==SQLITE_OK ){
    int bTreeEof = sqlite3HctTreeCsrEof(pCur->pHctTreeCsr);
    int bDbEof = sqlite3HctDbCsrEof(pCur->pHctDbCsr);
    *pRes = (bTreeEof && bDbEof);
    pCur->eDir = BTREE_DIR_REVERSE;
    btreeSetUseTree(pCur);
  }

  return rc;
}

/* Move the cursor so that it points to an entry near the key 
** specified by pIdxKey or intKey.   Return a success code.
**
** For INTKEY tables, the intKey parameter is used.  pIdxKey 
** must be NULL.  For index tables, pIdxKey is used and intKey
** is ignored.
**
** If an exact match is not found, then the cursor is always
** left pointing at a leaf page which would hold the entry if it
** were present.  The cursor might point to an entry that comes
** before or after the key.
**
** An integer is written into *pRes which is the result of
** comparing the key with the entry to which the cursor is 
** pointing.  The meaning of the integer written into
** *pRes is as follows:
**
**     *pRes<0      The cursor is left pointing at an entry that
**                  is smaller than intKey/pIdxKey or if the table is empty
**                  and the cursor is therefore left point to nothing.
**
**     *pRes==0     The cursor is left pointing at an entry that
**                  exactly matches intKey/pIdxKey.
**
**     *pRes>0      The cursor is left pointing at an entry that
**                  is larger than intKey/pIdxKey.
**
** For index tables, the pIdxKey->eqSeen field is set to 1 if there
** exists an entry in the table that exactly matches pIdxKey.  
*/
int sqlite3BtreeMovetoUnpacked(
  BtCursor *pCur,          /* The cursor to be moved */
  UnpackedRecord *pIdxKey, /* Unpacked index key */
  i64 intKey,              /* The table key */
  int biasRight,           /* If true, bias the search to the high end */
  int *pRes                /* Write search results here */
){
  int rc = SQLITE_OK;
  int res1 = 0;
  int res2 = 0;


  rc = sqlite3HctTreeCsrSeek(pCur->pHctTreeCsr, pIdxKey, intKey, &res1);
  if( rc==SQLITE_OK ){
    rc = sqlite3HctDbCsrSeek(pCur->pHctDbCsr, pIdxKey, intKey, &res2);
  }

  if( pCur->eDir==BTREE_DIR_NONE ){
    if( res1==0 ){
      *pRes = 0;
      pCur->bUseTree = 1;
    }else{
      pCur->bUseTree = 0;
      *pRes = res2;
    }
  }else{
    if( pCur->eDir==BTREE_DIR_FORWARD ){
      if( rc==SQLITE_OK && res2<0 && !sqlite3HctDbCsrEof(pCur->pHctDbCsr) ){
        rc = sqlite3HctDbCsrNext(pCur->pHctDbCsr);
      }
      if( rc==SQLITE_OK && res1<0 && !sqlite3HctTreeCsrEof(pCur->pHctTreeCsr) ){
        rc = sqlite3HctDbCsrNext(pCur->pHctDbCsr);
      }

      if( res1==0 || res2==0 ){
        *pRes = 0;
      }else if( sqlite3HctTreeCsrEof(pCur->pHctTreeCsr)
             && sqlite3HctDbCsrEof(pCur->pHctDbCsr)
      ){
        *pRes = -1;
      }else{
        *pRes = +1;
      }
    }else{
      assert( 0 );
    }
    btreeSetUseTree(pCur);
  }

  return rc;
}

void sqlite3BtreeCursorDir(BtCursor *pCur, int eDir){
  assert( eDir==BTREE_DIR_NONE 
       || eDir==BTREE_DIR_FORWARD 
       || eDir==BTREE_DIR_REVERSE
  );
  pCur->eDir = eDir;
}

/*
** Return TRUE if the cursor is not pointing at an entry of the table.
**
** TRUE will be returned after a call to sqlite3BtreeNext() moves
** past the last entry in the table or sqlite3BtreePrev() moves past
** the first entry.  TRUE is also returned if the table is empty.
*/
int sqlite3BtreeEof(BtCursor *pCur){
  /* TODO: What if the cursor is in CURSOR_REQUIRESEEK but all table entries
  ** have been deleted? This API will need to change to return an error code
  ** as well as the boolean result value.
  */
  return (
      sqlite3HctTreeCsrEof(pCur->pHctTreeCsr)
   && sqlite3HctDbCsrEof(pCur->pHctDbCsr)
  );
}

/*
** Return an estimate for the number of rows in the table that pCur is
** pointing to.  Return a negative number if no estimate is currently 
** available.
*/
i64 sqlite3BtreeRowCountEst(BtCursor *pCur){
  assert( 0 );
  return -1;
}

/*
** Advance the cursor to the next entry in the database. 
** Return value:
**
**    SQLITE_OK        success
**    SQLITE_DONE      cursor is already pointing at the last element
**    otherwise        some kind of error occurred
**
** The main entry point is sqlite3BtreeNext().  That routine is optimized
** for the common case of merely incrementing the cell counter BtCursor.aiIdx
** to the next cell on the current page.  The (slower) btreeNext() helper
** routine is called when it is necessary to move to a different page or
** to restore the cursor.
**
** If bit 0x01 of the F argument in sqlite3BtreeNext(C,F) is 1, then the
** cursor corresponds to an SQL index and this routine could have been
** skipped if the SQL index had been a unique index.  The F argument
** is a hint to the implement. SQLite btree implementation does not use
** this hint, but COMDB2 does.
*/
int sqlite3BtreeNext(BtCursor *pCur, int flags){
  int rc = SQLITE_OK;
  assert( pCur->eDir==BTREE_DIR_FORWARD );
  if( pCur->bUseTree ){
    rc = sqlite3HctTreeCsrNext(pCur->pHctTreeCsr);
  }else{
    rc = sqlite3HctDbCsrNext(pCur->pHctDbCsr);
  }
  if( rc==SQLITE_OK ){
    if( sqlite3BtreeEof(pCur) ){
      rc = SQLITE_DONE;
    }else{
      btreeSetUseTree(pCur);
    }
  }
  return rc;
}

/*
** Step the cursor to the back to the previous entry in the database.
** Return values:
**
**     SQLITE_OK     success
**     SQLITE_DONE   the cursor is already on the first element of the table
**     otherwise     some kind of error occurred
**
** The main entry point is sqlite3BtreePrevious().  That routine is optimized
** for the common case of merely decrementing the cell counter BtCursor.aiIdx
** to the previous cell on the current page.  The (slower) btreePrevious()
** helper routine is called when it is necessary to move to a different page
** or to restore the cursor.
**
** If bit 0x01 of the F argument to sqlite3BtreePrevious(C,F) is 1, then
** the cursor corresponds to an SQL index and this routine could have been
** skipped if the SQL index had been a unique index.  The F argument is a
** hint to the implement.  The native SQLite btree implementation does not
** use this hint, but COMDB2 does.
*/
int sqlite3BtreePrevious(BtCursor *pCur, int flags){
  int rc = SQLITE_OK;
  rc = sqlite3HctTreeCsrPrev(pCur->pHctTreeCsr);
  if( rc==SQLITE_OK && sqlite3HctTreeCsrEof(pCur->pHctTreeCsr) ){
    rc = SQLITE_DONE;
  }
  return rc;
}

/*
** Insert a new record into the BTree.  The content of the new record
** is described by the pX object.  The pCur cursor is used only to
** define what table the record should be inserted into, and is left
** pointing at a random location.
**
** For a table btree (used for rowid tables), only the pX.nKey value of
** the key is used. The pX.pKey value must be NULL.  The pX.nKey is the
** rowid or INTEGER PRIMARY KEY of the row.  The pX.nData,pData,nZero fields
** hold the content of the row.
**
** For an index btree (used for indexes and WITHOUT ROWID tables), the
** key is an arbitrary byte sequence stored in pX.pKey,nKey.  The 
** pX.pData,nData,nZero fields must be zero.
**
** If the seekResult parameter is non-zero, then a successful call to
** MovetoUnpacked() to seek cursor pCur to (pKey,nKey) has already
** been performed.  In other words, if seekResult!=0 then the cursor
** is currently pointing to a cell that will be adjacent to the cell
** to be inserted.  If seekResult<0 then pCur points to a cell that is
** smaller then (pKey,nKey).  If seekResult>0 then pCur points to a cell
** that is larger than (pKey,nKey).
**
** If seekResult==0, that means pCur is pointing at some unknown location.
** In that case, this routine must seek the cursor to the correct insertion
** point for (pKey,nKey) before doing the insertion.  For index btrees,
** if pX->nMem is non-zero, then pX->aMem contains pointers to the unpacked
** key values and pX->aMem can be used instead of pX->pKey to avoid having
** to decode the key.
*/
int sqlite3BtreeInsert(
  BtCursor *pCur,                /* Insert data into the table of this cursor */
  const BtreePayload *pX,        /* Content of the row to be inserted */
  int flags,                     /* True if this is likely an append */
  int seekResult                 /* Result of prior MovetoUnpacked() call */
){
  int rc = SQLITE_OK;
  UnpackedRecord r;
  UnpackedRecord *pRec = 0;
  const u8 *aData;
  int nData;
  int nZero;

  if( pX->pKey ){
    aData = pX->pKey;
    nData = pX->nKey;
    nZero = 0;
    if( pX->nMem ){
      memset(&r, 0, sizeof(r));
      r.pKeyInfo = pCur->pKeyInfo;
      r.aMem = pX->aMem;
      r.nField = pX->nMem;
      pRec = &r;
    }else{
      pRec = sqlite3VdbeAllocUnpackedRecord(pCur->pKeyInfo);
      if( pRec==0 ) return SQLITE_NOMEM_BKPT;
      sqlite3VdbeRecordUnpack(pCur->pKeyInfo, nData, aData, pRec);
    }
  }else{
    aData = pX->pData;
    nData = pX->nData;
    nZero = pX->nZero;
  }
  rc = sqlite3HctTreeInsert(
      pCur->pHctTreeCsr, pRec, pX->nKey, nData, aData, nZero
  );

  if( pRec && pRec!=&r ){
    sqlite3DbFree(pCur->pKeyInfo->db, pRec);
  }
  return rc;
}

/*
** Delete the entry that the cursor is pointing to. 
**
** If the BTREE_SAVEPOSITION bit of the flags parameter is zero, then
** the cursor is left pointing at an arbitrary location after the delete.
** But if that bit is set, then the cursor is left in a state such that
** the next call to BtreeNext() or BtreePrev() moves it to the same row
** as it would have been on if the call to BtreeDelete() had been omitted.
**
** The BTREE_AUXDELETE bit of flags indicates that is one of several deletes
** associated with a single table entry and its indexes.  Only one of those
** deletes is considered the "primary" delete.  The primary delete occurs
** on a cursor that is not a BTREE_FORDELETE cursor.  All but one delete
** operation on non-FORDELETE cursors is tagged with the AUXDELETE flag.
** The BTREE_AUXDELETE bit is a hint that is not used by this implementation,
** but which might be used by alternative storage engines.
*/
int sqlite3BtreeDelete(BtCursor *pCur, u8 flags){
  int rc = SQLITE_OK;
  rc = sqlite3HctTreeDelete(pCur->pHctTreeCsr);
  return rc;
}

/*
** Create a new BTree table.  Write into *piTable the page
** number for the root page of the new table.
**
** The type of type is determined by the flags parameter.  Only the
** following values of flags are currently in use.  Other values for
** flags might not work:
**
**     BTREE_INTKEY|BTREE_LEAFDATA     Used for SQL tables with rowid keys
**     BTREE_ZERODATA                  Used for SQL indices
*/
int sqlite3BtreeCreateTable(Btree *p, Pgno *piTable, int flags){
  Pgno iNew = 0;
  int rc = SQLITE_OK;
  if( p->pHctDb ){
    BtNewRoot *aNewRoot;

    /* Grow the Btree.aNewRoot array */
    aNewRoot = (BtNewRoot*)sqlite3_realloc(
        p->aNewRoot, sizeof(BtNewRoot)*(p->nNewRoot+1)
    );
    if( aNewRoot==0 ) return SQLITE_NOMEM_BKPT;
    p->aNewRoot = aNewRoot;

    /* Allocate a new root page and add it to the array. */
    rc = sqlite3HctDbRootNew(p->pHctDb, &iNew);
    if( rc==SQLITE_OK ){
      int bIndex = (flags & BTREE_INTKEY)==0;
      p->aNewRoot[p->nNewRoot].pgnoRoot = iNew;
      p->aNewRoot[p->nNewRoot].iSavepoint = p->db->nSavepoint;
      p->aNewRoot[p->nNewRoot].bIndex = bIndex;
      p->nNewRoot++;
    }
  }else{
    iNew = p->iNextRoot++;
  }
  *piTable = iNew;
  return rc;
}

/*
** Delete all information from a single table in the database.  iTable is
** the page number of the root of the table.  After this routine returns,
** the root page is empty, but still exists.
**
** This routine will fail with SQLITE_LOCKED if there are any open
** read cursors on the table.  Open write cursors are moved to the
** root of the table.
**
** If pnChange is not NULL, then table iTable must be an intkey table. The
** integer value pointed to by pnChange is incremented by the number of
** entries in the table.
*/
int sqlite3BtreeClearTable(Btree *p, int iTable, int *pnChange){
  return sqlite3HctTreeClearOne(p->pHctTree, iTable, pnChange);
}

/*
** Delete all information from the single table that pCur is open on.
**
** This routine only work for pCur on an ephemeral table.
*/
int sqlite3BtreeClearTableOfCursor(BtCursor *pCur){
  return sqlite3HctTreeClearOne(
      pCur->pBtree->pHctTree, sqlite3HctTreeCsrRoot(pCur->pHctTreeCsr), 0
  );
}

/*
** Erase all information in a table and add the root of the table to
** the freelist.  Except, the root of the principle table (the one on
** page 1) is never added to the freelist.
**
** This routine will fail with SQLITE_LOCKED if there are any open
** cursors on the table.
**
** If AUTOVACUUM is enabled and the page at iTable is not the last
** root page in the database file, then the last root page 
** in the database file is moved into the slot formerly occupied by
** iTable and that last slot formerly occupied by the last root page
** is added to the freelist instead of iTable.  In this say, all
** root pages are kept at the beginning of the database file, which
** is necessary for AUTOVACUUM to work right.  *piMoved is set to the 
** page number that used to be the last root page in the file before
** the move.  If no page gets moved, *piMoved is set to 0.
** The last root page is recorded in meta[3] and the value of
** meta[3] is updated by this procedure.
*/
int sqlite3BtreeDropTable(Btree *p, int iTable, int *piMoved){
  int rc = SQLITE_OK;
  /* HCT - fix this */
  return rc;
}


/*
** This function may only be called if the b-tree connection already
** has a read or write transaction open on the database.
**
** Read the meta-information out of a database file.  Meta[0]
** is the number of free pages currently in the database.  Meta[1]
** through meta[15] are available for use by higher layers.  Meta[0]
** is read-only, the others are read/write.
** 
** The schema layer numbers meta values differently.  At the schema
** layer (and the SetCookie and ReadCookie opcodes) the number of
** free pages is not visible.  So Cookie[0] is the same as Meta[1].
**
** This routine treats Meta[BTREE_DATA_VERSION] as a special case.  Instead
** of reading the value out of the header, it instead loads the "DataVersion"
** from the pager.  The BTREE_DATA_VERSION value is not actually stored in the
** database file.  It is a number computed by the pager.  But its access
** pattern is the same as header meta values, and so it is convenient to
** read it from this routine.
*/
void sqlite3BtreeGetMeta(Btree *p, int idx, u32 *pMeta){
  *pMeta = p->aMeta[idx];
}

/*
** Write meta-information back into the database.  Meta[0] is
** read-only and may not be written.
*/
int sqlite3BtreeUpdateMeta(Btree *p, int idx, u32 iMeta){
  /* HCT: This is a problem - meta values should be subject to normal
  ** transaction/savepoint rollback.  */
  int rc = SQLITE_OK;
  p->aMeta[idx] = iMeta;
  return rc;
}

/*
** The first argument, pCur, is a cursor opened on some b-tree. Count the
** number of entries in the b-tree and write the result to *pnEntry.
**
** SQLITE_OK is returned if the operation is successfully executed. 
** Otherwise, if an error is encountered (i.e. an IO error or database
** corruption) an SQLite error code is returned.
*/
int sqlite3BtreeCount(sqlite3 *db, BtCursor *pCur, i64 *pnEntry){
  i64 nEntry = 0;
  for(
      sqlite3HctTreeCsrFirst(pCur->pHctTreeCsr);
      0==sqlite3HctTreeCsrEof(pCur->pHctTreeCsr);
      sqlite3HctTreeCsrNext(pCur->pHctTreeCsr)
  ){
    nEntry++;
  }
  *pnEntry = nEntry;
  return SQLITE_OK;
}

/*
** Return the pager associated with a BTree.  This routine is used for
** testing and debugging only.
*/
Pager *sqlite3BtreePager(Btree *p){
  return (Pager*)p;
}

#ifndef SQLITE_OMIT_INTEGRITY_CHECK
/*
** This routine does a complete check of the given BTree file.  aRoot[] is
** an array of pages numbers were each page number is the root page of
** a table.  nRoot is the number of entries in aRoot.
**
** A read-only or read-write transaction must be opened before calling
** this function.
**
** Write the number of error seen in *pnErr.  Except for some memory
** allocation errors,  an error message held in memory obtained from
** malloc is returned if *pnErr is non-zero.  If *pnErr==0 then NULL is
** returned.  If a memory allocation error occurs, NULL is returned.
**
** If the first entry in aRoot[] is 0, that indicates that the list of
** root pages is incomplete.  This is a "partial integrity-check".  This
** happens when performing an integrity check on a single table.  The
** zero is skipped, of course.  But in addition, the freelist checks
** and the checks to make sure every page is referenced are also skipped,
** since obviously it is not possible to know which pages are covered by
** the unverified btrees.  Except, if aRoot[1] is 1, then the freelist
** checks are still performed.
*/
char *sqlite3BtreeIntegrityCheck(
  sqlite3 *db,  /* Database connection that is running the check */
  Btree *p,     /* The btree to be checked */
  Pgno *aRoot,  /* An array of root pages numbers for individual trees */
  int nRoot,    /* Number of entries in aRoot[] */
  int mxErr,    /* Stop reporting errors after this many */
  int *pnErr    /* Write number of errors seen to this variable */
){
  *pnErr = 0;
  return 0;
}
#endif /* SQLITE_OMIT_INTEGRITY_CHECK */

/*
** Return the full pathname of the underlying database file.  Return
** an empty string if the database is in-memory or a TEMP database.
**
** The pager filename is invariant as long as the pager is
** open so it is safe to access without the BtShared mutex.
*/
const char *sqlite3BtreeGetFilename(Btree *p){
  return 0;
}

/*
** Return the pathname of the journal file for this database. The return
** value of this routine is the same regardless of whether the journal file
** has been created or not.
**
** The pager journal filename is invariant as long as the pager is
** open so it is safe to access without the BtShared mutex.
*/
const char *sqlite3BtreeGetJournalname(Btree *p){
  return 0;
}

/*
** Return one of SQLITE_TXN_NONE, SQLITE_TXN_READ, or SQLITE_TXN_WRITE
** to describe the current transaction state of Btree p.
*/
int sqlite3BtreeTxnState(Btree *p){
  return p ? p->eTrans : SQLITE_TXN_NONE;
}

#ifndef SQLITE_OMIT_WAL
/*
** Run a checkpoint on the Btree passed as the first argument.
**
** Return SQLITE_LOCKED if this or any other connection has an open 
** transaction on the shared-cache the argument Btree is connected to.
**
** Parameter eMode is one of SQLITE_CHECKPOINT_PASSIVE, FULL or RESTART.
*/
int sqlite3BtreeCheckpoint(Btree *p, int eMode, int *pnLog, int *pnCkpt){
  int rc = SQLITE_OK;
  assert( 0 );
  return rc;
}
#endif

/*
** Return true if there is currently a backup running on Btree p.
*/
int sqlite3BtreeIsInBackup(Btree *p){
  return 0;
}

/*
** This function returns a pointer to a blob of memory associated with
** a single shared-btree. The memory is used by client code for its own
** purposes (for example, to store a high-level schema associated with 
** the shared-btree). The btree layer manages reference counting issues.
**
** The first time this is called on a shared-btree, nBytes bytes of memory
** are allocated, zeroed, and returned to the caller. For each subsequent 
** call the nBytes parameter is ignored and a pointer to the same blob
** of memory returned. 
**
** If the nBytes parameter is 0 and the blob of memory has not yet been
** allocated, a null pointer is returned. If the blob has already been
** allocated, it is returned as normal.
**
** Just before the shared-btree is closed, the function passed as the 
** xFree argument when the memory allocation was made is invoked on the 
** blob of allocated memory. The xFree function should not call sqlite3_free()
** on the memory, the btree layer does that.
*/
void *sqlite3BtreeSchema(Btree *p, int nBytes, void(*xFree)(void *)){
  void *pRet = 0;
  if( p->pSchema ){
    pRet = p->pSchema;
  }else if( nBytes>0 ){
    pRet = p->pSchema = sqlite3_malloc(nBytes);
    if( pRet ){
      memset(pRet, 0, nBytes);
      p->xSchemaFree = xFree;
    }
  }
  return pRet;
}

/*
** Return SQLITE_LOCKED_SHAREDCACHE if another user of the same shared 
** btree as the argument handle holds an exclusive lock on the 
** sqlite_schema table. Otherwise SQLITE_OK.
*/
int sqlite3BtreeSchemaLocked(Btree *p){
  return SQLITE_OK;
}


#ifndef SQLITE_OMIT_SHARED_CACHE
/*
** Obtain a lock on the table whose root page is iTab.  The
** lock is a write lock if isWritelock is true or a read lock
** if it is false.
*/
int sqlite3BtreeLockTable(Btree *p, int iTab, u8 isWriteLock){
  int rc = SQLITE_OK;
  assert( 0 );
  return rc;
}
#endif

#ifndef SQLITE_OMIT_INCRBLOB
/*
** Argument pCsr must be a cursor opened for writing on an 
** INTKEY table currently pointing at a valid table entry. 
** This function modifies the data stored as part of that entry.
**
** Only the data content may only be modified, it is not possible to 
** change the length of the data stored. If this function is called with
** parameters that attempt to write past the end of the existing data,
** no modifications are made and SQLITE_CORRUPT is returned.
*/
int sqlite3BtreePutData(BtCursor *pCsr, u32 offset, u32 amt, void *z){
  assert( 0 );
  return SQLITE_OK;
}

/* 
** Mark this cursor as an incremental blob cursor.
*/
void sqlite3BtreeIncrblobCursor(BtCursor *pCur){
  assert( 0 );
}
#endif

/*
** Set both the "read version" (single byte at byte offset 18) and 
** "write version" (single byte at byte offset 19) fields in the database
** header to iVersion.
*/
int sqlite3BtreeSetVersion(Btree *pBtree, int iVersion){
  assert( 0 );
  return SQLITE_OK;
}

/*
** Return true if the cursor has a hint specified.  This routine is
** only used from within assert() statements
*/
int sqlite3BtreeCursorHasHint(BtCursor *pCsr, unsigned int mask){
  return 0;
}

/*
** Return true if the given Btree is read-only.
*/
int sqlite3BtreeIsReadonly(Btree *p){
  assert( 0 );
  return 0;
}

/*
** Return the size of the header added to each page by this module.
*/
int sqlite3HeaderSizeBtree(void){ 
  assert( 0 );
  return 0;
}

#if !defined(SQLITE_OMIT_SHARED_CACHE)
/*
** Return true if the Btree passed as the only argument is sharable.
*/
int sqlite3BtreeSharable(Btree *p){
  assert( 0 );
  return 0;
}

/*
** Return the number of connections to the BtShared object accessed by
** the Btree handle passed as the only argument. For private caches 
** this is always 1. For shared caches it may be 1 or greater.
*/
int sqlite3BtreeConnectionCount(Btree *p){
  assert( 0 );
  return 1;
}
#endif


int sqlite3PagerIsMemdb(Pager *pPager){
  return 1;
}
int sqlite3PagerExclusiveLock(Pager *pPager){
  /* hct - noop */
  return SQLITE_OK;
}
int sqlite3PagerGetJournalMode(Pager *pPager){
  return PAGER_JOURNALMODE_OFF;
}
int sqlite3PagerLockingMode(Pager *pPager, int m){
  /* no-op */
  return SQLITE_OK;
}


int sqlite3PagerGet(Pager *pPager, Pgno pgno, DbPage **ppPage, int clrFlag){
  assert( 0 );
  return SQLITE_ERROR;
}
int sqlite3PagerWrite(DbPage *pPg){
  assert( 0 );
  return SQLITE_ERROR;
}
void *sqlite3PagerGetData(DbPage *pPg){
  assert( 0 );
  return 0;
}
void *sqlite3PagerGetExtra(DbPage *pPg){
  assert( 0 );
  return 0;
}
sqlite3_backup **sqlite3PagerBackupPtr(Pager *pPager){
  assert( 0 );
  return 0;
}
void sqlite3PagerUnref(DbPage *pPg){
  assert( 0 );
}
sqlite3_file *sqlite3PagerFile(Pager *pPager){
  static sqlite3_file f = { 0 };
  return &f;
}
void sqlite3PagerPagecount(Pager *pPager, int *pnPg){
  assert( 0 );
}
int sqlite3PagerCommitPhaseOne(Pager *pPager, const char *zSuper, int f){
  assert( 0 );
  return SQLITE_ERROR;
}
int sqlite3PagerSync(Pager *pPager, const char *zSuper){
  assert( 0 );
  return SQLITE_ERROR;
}
void sqlite3PagerTruncateImage(Pager *pPager, Pgno pgno){
  assert( 0 );
}
void sqlite3PagerClearCache(Pager *pPager){
  assert( 0 );
}
void sqlite3PagerUnrefPageOne(DbPage *pPg){
  assert( 0 );
}
void sqlite3PagerShrink(Pager *pPager){
  assert( 0 );
}
int sqlite3PagerFlush(Pager *pPager){
  assert( 0 );
}
sqlite3_vfs *sqlite3PagerVfs(Pager *pPager){
  assert( 0 );
  return 0;
}
sqlite3_file *sqlite3PagerJrnlFile(Pager *pPager){
  static sqlite3_file f = { 0 };
  assert( 0 );
  return &f;
}
u32 sqlite3PagerDataVersion(Pager *pPager){
  return 0;
}
i64 sqlite3PagerJournalSizeLimit(Pager *pPager, i64 ii){
  assert( 0 );
  return 0;
}
int sqlite3PagerOkToChangeJournalMode(Pager *pPager){
  assert( 0 );
  return 1;
}
int sqlite3PagerSetJournalMode(Pager *pPager, int m){
  assert( 0 );
  return SQLITE_OK;
}
int sqlite3PagerMemUsed(Pager *pPager){
  assert( 0 );
  return 0;
}
void sqlite3PagerCacheStat(Pager *pPager, int a, int b, int *c){
  assert( 0 );
}

/*
** The following set of routines are used to disable the simulated
** I/O error mechanism.  These routines are used to avoid simulated
** errors in places where we do not care about errors.
**
** Unless -DSQLITE_TEST=1 is used, these routines are all no-ops
** and generate no code.
*/
#ifdef SQLITE_TEST
extern int sqlite3_io_error_pending;
extern int sqlite3_io_error_hit;
static int saved_cnt;
void disable_simulated_io_errors(void){
  saved_cnt = sqlite3_io_error_pending;
  sqlite3_io_error_pending = -1;
}
void enable_simulated_io_errors(void){
  sqlite3_io_error_pending = saved_cnt;
}
int *sqlite3PagerStats(Pager *pPager){
  assert( 0 );
  return 0;
}
#else
# define disable_simulated_io_errors()
# define enable_simulated_io_errors()
#endif

/*
** Return the sqlite3_file for the main database given the name
** of the corresonding WAL or Journal name as passed into
** xOpen.
*/
sqlite3_file *sqlite3_database_file_object(const char *zName){
  assert( 0 );
  return 0;
}

#ifdef SQLITE_TEST
int sqlite3_opentemp_count = 0;
int sqlite3_pager_readdb_count = 0;
int sqlite3_pager_writedb_count = 0;
int sqlite3_pager_writej_count = 0;

int sqlite3PagerOpen(
    sqlite3_vfs *pVfs, Pager **ppPager, const char *zFile,
    int a, int b, int c, void(*x)(DbPage*)
){
  assert( 0 );
  return SQLITE_ERROR;
}
int sqlite3PagerClose(Pager *pPager, sqlite3 *db){
  assert( 0 );
  return SQLITE_ERROR;
}
void sqlite3PagerSetCachesize(Pager *pPager, int n){
  assert( 0 );
}
int sqlite3PagerSetPagesize(Pager *pPager, u32 *p, int n){
  assert( 0 );
  return SQLITE_ERROR;
}
int sqlite3PagerRollback(Pager *pPager){
  assert( 0 );
  return SQLITE_ERROR;
}
int sqlite3PagerCommitPhaseTwo(Pager *pPager){
  assert( 0 );
  return SQLITE_ERROR;
}
int sqlite3PagerOpenSavepoint(Pager *pPager, int n){
  assert( 0 );
  return SQLITE_ERROR;
}
int sqlite3PagerSavepoint(Pager *pPager, int op, int iSavepoint){
  assert( 0 );
  return SQLITE_ERROR;
}
int sqlite3PagerSharedLock(Pager *pPager){
  assert( 0 );
  return SQLITE_ERROR;
}
DbPage *sqlite3PagerLookup(Pager *pPager, Pgno pgno){
  assert( 0 );
  return 0;
}
Pgno sqlite3PagerPagenumber(DbPage *pPg){
  assert( 0 );
  return 0;
}
#endif
#endif /* SQLITE_ENABLE_HCT */
