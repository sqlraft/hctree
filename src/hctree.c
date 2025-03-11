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

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>

#ifdef SQLITE_ENABLE_HCT

typedef struct BtSchemaOp BtSchemaOp;

typedef struct HBtree HBtree;
typedef struct HBtCursor HBtCursor;
typedef struct HctLogFile HctLogFile;
typedef struct HctMainStats HctMainStats;


/*
** An object to help with writing a log file.
*/
struct HctLogFile {
  int fd;                         /* File descriptor open on log file */
  char *zLogFile;                 /* Full path to log file */
  u8 *aBuf;                       /* malloc'd buffer for writing log file */
  int nBuf;                       /* Size of aBuf[] in bytes */
  i64 iFileOff;                   /* Current write offset in file */
  int iBufferOff;                 /* Current write offset in buffer */
};

struct HctMainStats {
  i64 nRetry;
  i64 nRetryKey;
  i64 nKeyOp;
};

/*
** aSchemaOp[]:
**   Array of nSchemaOp BtSchemaOp structures. Each such structure represents
**   a new table or index created by the current transaction. 
**   aSchemaOp[x].iSavepoint contains the open savepoint count when the table
**   with root page aSchemaOp[x].pgnoRoot was created. The value 
**   HBtree.db->nSavepoint.
**
** eTrans:
**   Set to SQLITE_TXN_NONE, READ or WRITE to indicate the type of 
**   transaction that is open. This is set by the following functions:
**
**     sqlite3HctBtreeBeginTrans()
**     sqlite3HctBtreeCommitPhaseTwo()
**     sqlite3HctBtreeRollback()
*/
struct HBtree {
  BtreeMethods *pMethods;

  HctConfig config;               /* Configuration for this connection */
  HctTree *pHctTree;              /* In-memory part of database */
  HctDatabase *pHctDb;            /* On-disk part of db, if any */
  void *pSchema;                  /* Memory from sqlite3HctBtreeSchema() */
  void(*xSchemaFree)(void*);      /* Function to free pSchema */
  int eTrans;                     /* SQLITE_TXN_NONE, READ or WRITE */
  HBtCursor *pCsrList;            /* List of all open cursors */

  int nSchemaOp;
  BtSchemaOp *aSchemaOp;
  int nRollbackOp;

  int openFlags;
  HctLogFile *pLog;               /* Object for writing to log file */
  u32 iNextRoot;                  /* Next root page to allocate if pHctDb==0 */
  u32 aMeta[SQLITE_N_BTREE_META]; /* 16 database meta values */
  int eMetaState;                 /* HCT_METASTATE_XXX value */

  int bRecoveryDone;
#if 0
  u64 iJrnlRoot;                  /* Root of sqlite_hct_journal */
  u64 iBaseRoot;                  /* Root of sqlite_hct_baseline */
#endif
  HctJournal *pHctJrnl;

  Pager *pFakePager;
  HctMainStats stats;
};

/* 
** Another candidate value for HBtree.eTrans. Must be different from
** SQLITE_TXN_NONE, SQLITE_TXN_READ and SQLITE_TXN_WRITE.
*/
#define SQLITE_TXN_ERROR 4

/*
** Candidate values for HBtree.eMetaState.
*/
#define HCT_METASTATE_NONE  0
#define HCT_METASTATE_READ  1

/*
** A schema op. An ordered list of these objects is stored in the
** HBtree.aSchemaOp[] array.
**
** eSchemaOp:
**   An HCT_SCHEMAOP_XXX constant (see below).
**
** iSavepoint:
**   The number of open savepoints + statement transactions when 
**   the schema operation was performed.
*/ 
struct BtSchemaOp {
  int eSchemaOp;
  int iSavepoint;
  u32 pgnoRoot;                   /* Root page created/dropped or populated */
};

/*
** Candidate values for BtSchemaOp.eSchemaOp
*/
#define HCT_SCHEMAOP_DROP          1
#define HCT_SCHEMAOP_CREATE_INTKEY 2
#define HCT_SCHEMAOP_CREATE_INDEX  3
#define HCT_SCHEMAOP_POPULATE      4


struct HBtCursor {
  BtCursorMethods *pMethods;

  HBtree *pBtree;
  HctTreeCsr *pHctTreeCsr;
  HctDbCsr *pHctDbCsr;
  int bUseTree;                   /* 1 if tree-csr is current entry, else 0 */
  int eDir;                       /* One of BTREE_DIR_NONE, FORWARD, REVERSE */

  int isLast;                     /* Csr has not moved since BtreeLast() */
  int isDirectWrite;              /* True to write directly to db */

  KeyInfo *pKeyInfo;              /* For non-intkey tables */
  int errCode;
  int wrFlag;                     /* Value of wrFlag when cursor opened */
  HBtCursor *pCsrNext;            /* Next element in Btree.pCsrList list */
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


/*
** Return an reset the seek counter for a Btree object.
*/
sqlite3_uint64 sqlite3HctBtreeSeekCount(Btree *pBt){
  assert( 0 );
  return 0;
}

/*
** Clear the current cursor position.
*/
void sqlite3HctBtreeClearCursor(BtCursor *pCur){
  HBtCursor *pCsr = (HBtCursor*)pCur;
  sqlite3HctDbCsrClear(pCsr->pHctDbCsr);
  sqlite3HctTreeCsrClear(pCsr->pHctTreeCsr);
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
** Use the separate sqlite3HctBtreeCursorRestore() routine to restore a cursor
** back to where it ought to be if this routine returns true.
*/
int sqlite3HctBtreeCursorHasMoved(BtCursor *pCursor){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  return sqlite3HctTreeCsrHasMoved(pCur->pHctTreeCsr);
}

/*
** Return a pointer to a fake BtCursor object that will always answer
** false to the sqlite3HctBtreeCursorHasMoved() routine above.  The fake
** cursor returned must not be used with any other Btree interface.
*/
#if 0 
BtCursor *sqlite3HctBtreeFakeValidCursor(void){
  static BtCursor csr = {0,0,0};
  return &csr;
}
#endif

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
** TRUE from sqlite3HctBtreeCursorHasMoved().
*/
int sqlite3HctBtreeCursorRestore(BtCursor *pCursor, int *pDifferentRow){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  return sqlite3HctTreeCsrRestore(pCur->pHctTreeCsr, pDifferentRow);
}

/*
** Return the size of the database file in pages. If there is any kind of
** error, return ((unsigned int)-1).
*/
Pgno sqlite3HctBtreeLastPage(Btree *p){
  return 0xFFFFFFFF;
}

/*
** Provide flag hints to the cursor.
*/
void sqlite3HctBtreeCursorHintFlags(BtCursor *pCur, unsigned x){
  /* no-op */
  assert( x==BTREE_SEEK_EQ || x==BTREE_BULKLOAD || x==0 );
}

typedef struct RecoverCsr RecoverCsr;
struct RecoverCsr {
  HctDbCsr *pCsr;                 /* Cursor to read from database on disk */
  HctTreeCsr *pTreeCsr;           /* Cursor to write to in-memory tree */
  UnpackedRecord *pRec;           /* Used to seek both cursors */
  KeyInfo *pKeyInfo;
};

static void hctRecoverCursorClose(HBtree *p, RecoverCsr *pCsr){
  sqlite3HctDbCsrClose(pCsr->pCsr);
  sqlite3HctTreeCsrClose(pCsr->pTreeCsr);
  sqlite3DbFree(p->config.db, pCsr->pRec);
  sqlite3KeyInfoUnref(pCsr->pKeyInfo);
  memset(pCsr, 0, sizeof(RecoverCsr));
}

static int hctFindKeyInfo(HBtree *p, u32 iRoot, KeyInfo **ppKeyInfo){
  Schema *pSchema = (Schema*)p->pSchema;
  int rc = SQLITE_OK;
  HashElem *pE = 0;
  KeyInfo *pKeyInfo = 0;

  /* Search the database schema for an index with root page iRoot. If
  ** one is found, extract a KeyInfo reference. */
  for(pE=sqliteHashFirst(&pSchema->tblHash); pE; pE=sqliteHashNext(pE)){
    Index *pIdx = 0;
    Table *pTab = (Table*)sqliteHashData(pE);
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      if( pIdx->tnum==iRoot ){
        Parse sParse;
        Parse *pSave = 0;
        memset(&sParse, 0, sizeof(sParse));
        sParse.db = p->config.db;
        pSave = sParse.db->pParse;
        sParse.db->pParse = &sParse;
        pKeyInfo = sqlite3KeyInfoOfIndex(&sParse, pIdx);
        sParse.db->pParse = pSave;
        rc = sParse.rc;
        sqlite3DbFree(sParse.db, sParse.zErrMsg);
        break;
      }
    }
    if( pTab->tnum==iRoot ) break;
  }

  *ppKeyInfo = pKeyInfo;
  return rc;
}

/*
** 
*/
static int hctRecoverCursorOpen(
  HBtree *p,
  u32 iRoot,
  RecoverCsr *pCsr
){
  int rc = SQLITE_OK;
  memset(pCsr, 0, sizeof(RecoverCsr));

  rc = hctFindKeyInfo(p, iRoot, &pCsr->pKeyInfo);
  assert( rc==SQLITE_OK || pCsr->pKeyInfo==0 );
  if( pCsr->pKeyInfo ){
    pCsr->pRec = sqlite3VdbeAllocUnpackedRecord(pCsr->pKeyInfo);
    if( pCsr->pRec==0 ) rc = SQLITE_NOMEM_BKPT;
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctDbCsrOpen(p->pHctDb, pCsr->pKeyInfo, iRoot, &pCsr->pCsr);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctTreeCsrOpen(p->pHctTree, iRoot, &pCsr->pTreeCsr);
  }

  return rc;
}

#if 1
# define hctRecoverDebug(v,w,x,y,z)
#else
static void hctRecoverDebug(
  RecoverCsr *p, 
  const char *zType, 
  i64 iKey, 
  const u8 *aKey, 
  int nKey
){
  if( p->pRec==0 ){
    printf("recover-%s: %lld\n", zType, iKey);
  }else{
    char *zText = sqlite3HctDbRecordToText(0, aKey, nKey);
    printf("recover-%s: %s\n", zType, zText);
    sqlite3_free(zText);
  }
  fflush(stdout);
}
#endif

/*
** This object is used to read a log file from disk. It is manipulated using
** the following API:
**
**     hctLogReaderOpen()
**     hctLogReaderNext()
**     hctLogReaderClose()
**
** Log file format consists of an 8-byte TID value followed by one or more
** records. Each record is:
**
**   * 32-bit root page number,
**   * 32-bit size of key field (nKey),
**   * if( nKey==0 ) 64-bit rowid key,
**   * if( nKey!=0 ) nKey byte blob key.
*/
typedef struct HctLogReader HctLogReader;
struct HctLogReader {
  u8 *aFile;                      /* Buffer containing log file contents */
  int nFile;                      /* Size of aFile[] in bytes */
  int iFile;                      /* Offset of next record in aFile[] */

  i64 iTid;                       /* TID value for log file */
  int bEof;                       /* True if reader has hit EOF */

  /* Valid only if bEof==0 */
  i64 iRoot;                      /* Root page for current entry */
  i64 iKey;                       /* Integer key for current entry (aKey==0) */
  int nKey;                       /* Size of aKey[] buffer */
  u8 *aKey;                       /* Blob key for current entry */
};

static void hctLogReaderNext(HctLogReader *pReader){
  u32 aInt[2];

  if( (pReader->iFile + sizeof(aInt))>pReader->nFile ){
    pReader->bEof = 1;
  }else{
    memcpy(aInt, &pReader->aFile[pReader->iFile], sizeof(aInt));
    pReader->iRoot = (i64)aInt[0];
    if( pReader->iRoot==0 ){
      pReader->bEof = 1;
    }else{
      pReader->nKey = (int)aInt[1];
      pReader->iFile += sizeof(aInt);
      if( pReader->nKey==0 ){
        pReader->aKey = 0;
        if( pReader->iFile+sizeof(i64)>pReader->nFile ){
          pReader->bEof = 1;
        }else{
          memcpy(&pReader->iKey, &pReader->aFile[pReader->iFile], sizeof(i64));
          pReader->iFile += sizeof(i64);
        }
      }else{
        pReader->iKey = 0;
        if( pReader->iFile+pReader->nKey>pReader->nFile ){
          pReader->bEof = 1;
        }else{
          pReader->aKey = &pReader->aFile[pReader->iFile];
          pReader->iFile += pReader->nKey;
        }
      }
    }
  }
}

static void hctLogReaderClose(HctLogReader *pReader){
  sqlite3_free(pReader->aFile);
  memset(pReader, 0, sizeof(*pReader));
}

static int hctLogReaderOpen(const char *zFile, HctLogReader *pReader){
  int rc = SQLITE_OK;
  int fd = -1;

  memset(pReader, 0, sizeof(*pReader));
  fd = open(zFile, O_RDONLY);
  if( fd<0 ){
    rc = sqlite3HctIoerr(SQLITE_IOERR);
  }else{
    struct stat sStat;

    memset(&sStat, 0, sizeof(sStat));
    fstat(fd, &sStat);
    pReader->nFile = (int)sStat.st_size;
    pReader->aFile = (u8*)sqlite3HctMalloc(&rc, pReader->nFile + 8);
    if( pReader->aFile ){
      int nRead = read(fd, pReader->aFile, pReader->nFile);
      if( nRead!=pReader->nFile ){
        rc = sqlite3HctIoerr(SQLITE_IOERR);
      }else{
        memcpy(&pReader->iTid, pReader->aFile, sizeof(i64));
        pReader->iFile = sizeof(i64);
        if( pReader->iTid==0 ){
          pReader->bEof = 1;
        }else{
          hctLogReaderNext(pReader);
        }
      }
    }

    close(fd);
  }

  return rc;
}


static int btreeFlushData(HBtree *p, int bRollback);

static int hctRecoverOne(void *pCtx, const char *zFile){
  HBtree *p = (HBtree*)pCtx;
  int rc = SQLITE_OK;
  u32 iPrevRoot = 0;
  RecoverCsr csr;
  HctLogReader rdr;

  memset(&csr, 0, sizeof(csr));
  rc = hctLogReaderOpen(zFile, &rdr);
  if( rc==SQLITE_OK && rdr.bEof==0 ){

    assert( rdr.iTid!=0 );
    sqlite3HctDbRollbackMode(p->pHctDb, 2);
    sqlite3HctDbRecoverTid(p->pHctDb, rdr.iTid);
    for(/* no-op */; rdr.bEof==0; hctLogReaderNext(&rdr)){
      int op = 0;

      if( rdr.iRoot!=iPrevRoot ){
        iPrevRoot = rdr.iRoot;
        hctRecoverCursorClose(p, &csr);
        rc = hctRecoverCursorOpen(p, rdr.iRoot, &csr);
      }

      if( rdr.nKey ){
        sqlite3VdbeRecordUnpack(csr.pKeyInfo, rdr.nKey, rdr.aKey, csr.pRec);
      }
      rc = sqlite3HctDbCsrRollbackSeek(csr.pCsr, csr.pRec, rdr.iKey, &op);

      if( rc==SQLITE_OK && op!=0 ){
        HctTreeCsr *pTCsr = csr.pTreeCsr;
        if( op<0 ){
          /* rollback requires deleting the key */
          hctRecoverDebug(&csr, "delete", rdr.iKey, rdr.aKey, rdr.nKey);
          rc = sqlite3HctTreeDeleteKey(
              pTCsr, csr.pRec, rdr.iKey, rdr.nKey, rdr.aKey
          );
        }else if( op>0 ){
          const u8 *aOld = 0;
          int nOld = 0;
          rc = sqlite3HctDbCsrData(csr.pCsr, &nOld, &aOld);
          if( rc==SQLITE_OK ){
            hctRecoverDebug(&csr, "insert", rdr.iKey, aOld, nOld);
            rc = sqlite3HctTreeInsert(pTCsr, csr.pRec, rdr.iKey, nOld, aOld, 0);
          }
        }
      }
    }
    hctRecoverCursorClose(p, &csr);

    if( rc==SQLITE_OK ){
      rc = btreeFlushData(p, 0);
    }
    sqlite3HctDbRollbackMode(p->pHctDb, 0);
    if( rc==SQLITE_OK && p->pHctJrnl ){
      rc = sqlite3HctJrnlRollbackEntry(p->pHctJrnl, rdr.iTid);
    }
    sqlite3HctDbRecoverTid(p->pHctDb, 0);
  }

  if( rc==SQLITE_OK ){
    /* TODO!!! */
    unlink(zFile);
  }
  hctLogReaderClose(&rdr);
  return rc;
}

static int hctRecoverLogs(HBtree *p){
  HctFile *pFile = sqlite3HctDbFile(p->pHctDb);
  return sqlite3HctFileFindLogs(pFile, (void*)p, hctRecoverOne);
}


/*
** Free a pLog object and close the associated log file handle. If parameter
** bUnlink is true, also unlink() the log file.
*/
static void hctLogFileClose(HctLogFile *pLog, int bUnlink){
  if( pLog ){
    close(pLog->fd);
    if( bUnlink ) unlink(pLog->zLogFile);
    sqlite3_free(pLog->zLogFile);
    sqlite3_free(pLog->aBuf);
    sqlite3_free(pLog);
  }
}

/*
** Open a log file object.
*/
static int hctLogFileOpen(char *zLogFile, int nBuf, HctLogFile **ppLog){
  int rc = SQLITE_OK;
  HctLogFile *pLog;

  pLog = (HctLogFile*)sqlite3HctMalloc(&rc, sizeof(HctLogFile));
  if( pLog ){
    pLog->zLogFile = zLogFile;
    pLog->fd = open(zLogFile, O_CREAT|O_RDWR, 0644);
    if( pLog->fd<0 ){
      rc = SQLITE_CANTOPEN_BKPT;
    }else{
      pLog->nBuf = nBuf;
      pLog->aBuf = sqlite3HctMalloc(&rc, nBuf);
    }
  }

  if( rc!=SQLITE_OK ){
    hctLogFileClose(pLog, 0);
    pLog = 0;
  }

  *ppLog = pLog;
  return rc;
}

static int hctLogFileWrite(HctLogFile *pLog, const void *aData, int nData){
  int nRem = nData;
  const u8 *aRem = (u8*)aData;
  
  assert( pLog->iBufferOff<=pLog->nBuf );
  while( 1 ){

    int nCopy = MIN(pLog->nBuf - pLog->iBufferOff, nRem);
    if( nCopy>0 ){
      memcpy(&pLog->aBuf[pLog->iBufferOff], aRem, nCopy);
      pLog->iBufferOff += nCopy;
      nRem -= nCopy;
      if( nRem==0 ) break;
      aRem += nCopy;
    }

    if( write(pLog->fd, pLog->aBuf, pLog->nBuf)!=pLog->nBuf ){
      return sqlite3HctIoerr(SQLITE_IOERR_WRITE);
    }
    pLog->iFileOff += pLog->nBuf;
    pLog->iBufferOff = 0;
  }

  return SQLITE_OK;
}


static void hctLogFileRestart(HctLogFile *pLog){
  memset(pLog->aBuf, 0, 8);
  lseek(pLog->fd, 0, SEEK_SET);
  pLog->iFileOff = 0;
  pLog->iBufferOff = 8;
}


static int hctLogFileWriteTid(HctLogFile *pLog, u64 iTid){
  lseek(pLog->fd, 0, SEEK_SET);
  if( write(pLog->fd, &iTid, sizeof(iTid))!=sizeof(iTid) ){
    return sqlite3HctIoerr(SQLITE_IOERR_WRITE);
  }
  return SQLITE_OK;
}

static int hctLogFileFinish(HctLogFile *pLog, u64 iTid){
  int rc = SQLITE_OK;
  int bDone = 0;
  if( pLog->iFileOff==0 ){
    bDone = 1;
    memcpy(pLog->aBuf, &iTid, sizeof(iTid));
  }
  if( rc==SQLITE_OK ){
    static const u8 aZero[8] = {0,0,0,0, 0,0,0,0};
    rc = hctLogFileWrite(pLog, aZero, sizeof(aZero));
    if( rc==SQLITE_OK ){
      assert( pLog->iBufferOff>0 );
      if( write(pLog->fd, pLog->aBuf, pLog->iBufferOff)!=pLog->iBufferOff ){
        rc = sqlite3HctIoerr(SQLITE_IOERR_WRITE);
      }
    }
  }
  if( bDone==0 && rc==SQLITE_OK ){
    rc = hctLogFileWriteTid(pLog, iTid);
  }
  return rc;
}

static int btreeLogFileZero(HctLogFile *pLog){
  return hctLogFileWriteTid(pLog, 0);
}


/*
** Open a database file.
** 
** zFilename is the name of the database file.  If zFilename is NULL
** then an ephemeral database is created.  The ephemeral database might
** be exclusively in memory, or it might use a disk-based memory cache.
** Either way, the ephemeral database will be automatically deleted 
** when sqlite3HctBtreeClose() is called.
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
int sqlite3HctBtreeOpen(
  sqlite3_vfs *pVfs,      /* VFS to use for this b-tree */
  const char *zFilename,  /* Name of the file containing the BTree database */
  sqlite3 *db,            /* Associated database handle */
  Btree **ppBtree,        /* Pointer to new Btree object written here */
  int flags,              /* Options */
  int vfsFlags            /* Flags passed through to sqlite3_vfs.xOpen() */
){
  int rc = SQLITE_OK;
  HBtree *pNew;

  assert( (flags & BTREE_SINGLE)==0 && zFilename && zFilename[0] );

  pNew = (HBtree*)sqlite3_malloc(sizeof(HBtree));
  if( pNew ){
    memset(pNew, 0, sizeof(HBtree));
    pNew->iNextRoot = 2;
    pNew->config.db = db;
    pNew->openFlags = flags;
    pNew->config.nDbFile = HCT_DEFAULT_NDBFILE;
    pNew->config.nPageSet = HCT_DEFAULT_NPAGESET;
    pNew->config.nTryBeforeUnevict = HCT_DEFAULT_NTRYBEFOREUNEVICT;
    pNew->config.nPageScan = HCT_DEFAULT_NPAGESCAN;
    pNew->config.szLogChunk = HCT_DEFAULT_SZLOGCHUNK;
    pNew->config.pgsz = HCT_DEFAULT_PAGESIZE;
    rc = sqlite3HctTreeNew(&pNew->pHctTree);
    pNew->pFakePager = (Pager*)sqlite3HctMalloc(&rc, 4096);
  }else{
    rc = SQLITE_NOMEM;
  }

  if( rc==SQLITE_OK && zFilename && zFilename[0] ){
    pNew->pHctDb = sqlite3HctDbOpen(&rc, zFilename, &pNew->config);
  }

  if( rc!=SQLITE_OK ){
    sqlite3HctBtreeClose((Btree*)pNew);
    pNew = 0;
  }
  *ppBtree = (Btree*)pNew;
  return rc;
}

/*
** Close an open database and invalidate all cursors.
*/
int sqlite3HctBtreeClose(Btree *pBt){
  HBtree *const p = (HBtree*)pBt;
  if( p ){
    while(p->pCsrList){
      sqlite3HctBtreeCloseCursor((BtCursor*)p->pCsrList);
    }
    hctLogFileClose(p->pLog, 1);
    sqlite3HctBtreeRollback((Btree*)p, SQLITE_OK, 0);
    sqlite3HctBtreeCommit((Btree*)p);
    if( p->xSchemaFree ){
      p->xSchemaFree(p->pSchema);
    }
    sqlite3_free(p->pSchema);
    sqlite3HctJournalClose(p->pHctJrnl);
    sqlite3HctTreeFree(p->pHctTree);
    sqlite3HctDbClose(p->pHctDb);
    sqlite3_free(p->aSchemaOp);
    sqlite3_free(p->pFakePager);
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
int sqlite3HctBtreeSetCacheSize(Btree *p, int mxPage){
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
int sqlite3HctBtreeSetSpillSize(Btree *p, int mxPage){
  return 1024;
}

#if SQLITE_MAX_MMAP_SIZE>0
/*
** Change the limit on the amount of the database file that may be
** memory mapped.
*/
int sqlite3HctBtreeSetMmapLimit(Btree *p, sqlite3_int64 szMmap){
  /* assert( 0 ); */
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
int sqlite3HctBtreeSetPagerFlags(
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
int sqlite3HctBtreeSetPageSize(Btree *pBt, int pgsz, int nReserve, int iFix){
  HBtree *const p = (HBtree*)pBt;
  int rc = SQLITE_READONLY;
  if( p->pHctDb && pgsz>=512 && pgsz<=32768 && 0==(pgsz & (pgsz-1)) ){
    int orig = sqlite3HctDbPagesize(p->pHctDb);
    if( orig==0 ){
      p->config.pgsz = pgsz;
      rc = SQLITE_OK;
    }
  }
  return rc;
}

/*
** Return the currently defined page size
*/
int sqlite3HctBtreeGetPageSize(Btree *pBt){
  HBtree *const p = (HBtree*)pBt;
  int pgsz = 1024;
  if( p->pHctDb ){
    pgsz = sqlite3HctDbPagesize(p->pHctDb);
    if( pgsz==0 ){
      pgsz = p->config.pgsz;
    }
  }
  p->config.pgsz = pgsz;
  return pgsz;
}

/*
** This function is similar to sqlite3HctBtreeGetReserve(), except that it
** may only be called if it is guaranteed that the b-tree mutex is already
** held.
**
** This is useful in one special case in the backup API code where it is
** known that the shared b-tree mutex is held, but the mutex on the 
** database handle that owns *p is not. In this case if sqlite3HctBtreeEnter()
** were to be called, it might collide with some other operation on the
** database handle that owns *p, causing undefined behavior.
*/
int sqlite3HctBtreeGetReserveNoMutex(Btree *p){
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
int sqlite3HctBtreeGetRequestedReserve(Btree *p){
  return 0;
}


/*
** Set the maximum page count for a database if mxPage is positive.
** No changes are made if mxPage is 0 or negative.
** Regardless of the value of mxPage, return the maximum page count.
*/
Pgno sqlite3HctBtreeMaxPageCount(Btree *p, Pgno mxPage){
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
int sqlite3HctBtreeSecureDelete(Btree *p, int newFlag){
  return 0;
}

/*
** Change the 'auto-vacuum' property of the database. If the 'autoVacuum'
** parameter is non-zero, then auto-vacuum mode is enabled. If zero, it
** is disabled. The default value for the auto-vacuum property is 
** determined by the SQLITE_DEFAULT_AUTOVACUUM macro.
*/
int sqlite3HctBtreeSetAutoVacuum(Btree *p, int autoVacuum){
  return SQLITE_OK;
}

/*
** Return the value of the 'auto-vacuum' property. If auto-vacuum is 
** enabled 1 is returned. Otherwise 0.
*/
int sqlite3HctBtreeGetAutoVacuum(Btree *p){
  /* hct is never in auto-vacuum mode */
  return 0;
}

/*
** Initialize the first page of the database file (creating a database
** consisting of a single page and no schema objects). Return SQLITE_OK
** if successful, or an SQLite error code otherwise.
*/
int sqlite3HctBtreeNewDb(Btree *p){
  int rc = SQLITE_OK;
  assert( 0 );
  return rc;
}

static int hctDetectJournals(HBtree *p){
  int rc = SQLITE_OK;
  if( p->pHctJrnl==0 ){
    rc = sqlite3HctJournalNewIf(
        (Schema*)p->pSchema, p->pHctTree, p->pHctDb, &p->pHctJrnl
    );
  }
  return rc;
}

/*
** This is called by sqlite3_hct_journal_init() after the journal and
** baseline tables have been created in the database to initialize the
** journal sub-system.
**
** Return SQLITE_OK if successful, or an SQLite error code if an error
** occurs.
*/
int sqlite3HctDetectJournals(sqlite3 *db){
  HBtree *p = (HBtree*)db->aDb[0].pBt;
  int rc = hctDetectJournals(p);
  if( rc==SQLITE_OK ){
    rc = sqlite3HctDbStartRead(p->pHctDb, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctJrnlRecovery(p->pHctJrnl, p->pHctDb);
  }
  sqlite3HctDbEndRead(p->pHctDb);
  return rc;
}

typedef struct HctFreelistCtx HctFreelistCtx;
struct HctFreelistCtx {
  /* Physical pages that need to be preserved for log and journal rollback */
  int nAlloc;
  int nPg;
  i64 *aPg;

  /* Root pages in the current schema */
  int nRootAlloc;
  int nRoot;
  i64 *aRoot;

  HBtree *p;
};

static int hctTopDownMerge(
  i64 *aB, 
  int iBegin1, int iEnd1, 
  int iBegin2, int iEnd2, 
  i64 *aA
){
  int i = iBegin1;
  int j = iBegin2;
  int k;
  for(k=iBegin1; i<iEnd1 || j<iEnd2; k++){
    if( i<iEnd1 && (j>=iEnd2 || aA[i]<=aA[j]) ){
      if( j<iEnd2 && aA[i]==aA[j] ) j++;
      aB[k] = aA[i];
      i++;
    }else{
      aB[k] = aA[j];
      j++;
    }
  }
  return k;
}

/*
** Sort the integers in aA[] into array aB[].
*/
static int hctTopDownSplitMerge(i64 *aB, int iBegin, int iEnd, i64 *aA){
  if( (iEnd-iBegin)>1 ){
    int iMid = (iEnd + iBegin) / 2;
    int i1 = hctTopDownSplitMerge(aA, iBegin, iMid, aB);
    int i2 = hctTopDownSplitMerge(aA, iMid, iEnd, aB);
    return hctTopDownMerge(aB, iBegin, i1, iMid, i2, aA);
  }
  return iEnd;
}

/*
** Sort the array of aPg[] page numbers in ascending order. Discard 
** any duplicates. 
*/
static void hctFreelistSort(int *pRc, HctFreelistCtx *p){
  if( *pRc==SQLITE_OK && p->nPg>1 ){
    i64 *aWork = (i64*)sqlite3HctMalloc(pRc, p->nPg * sizeof(i64));
    if( aWork ){
      memcpy(aWork, p->aPg, p->nPg * sizeof(i64));
      p->nPg = hctTopDownSplitMerge(p->aPg, 0, p->nPg, aWork);
      sqlite3_free(aWork);
#ifdef SQLITE_DEBUG
      {
        int ii;
        for(ii=1; ii<p->nPg; ii++){
          assert( p->aPg[ii]>p->aPg[ii-1] );
        }
      }
#endif
    }
  }
}

static int hctSavePhysical(void *pCtx, i64 iPhys){
  HctFreelistCtx *p = (HctFreelistCtx*)pCtx;
  if( p->nPg==p->nAlloc ){
    int nNew = (p->nPg>0) ? p->nPg * 4 : 64;
    i64 *aNew = (i64*)sqlite3_realloc(p->aPg, nNew*sizeof(i64));;
    if( aNew==0 ) return SQLITE_NOMEM;
    p->aPg = aNew;
    p->nAlloc = nNew;
  }
  p->aPg[p->nPg++] = iPhys;
  return SQLITE_OK;
}

static int hctScanOne(void *pCtx, const char *zFile){
  HctFreelistCtx *p = (HctFreelistCtx*)pCtx;
  int rc = SQLITE_OK;
  HctLogReader rdr;

  sqlite3HctDbSetSavePhysical(p->p->pHctDb, hctSavePhysical, pCtx);

  rc = hctLogReaderOpen(zFile, &rdr);
  if( rc==SQLITE_OK && rdr.bEof==0 ){
    u32 iPrevRoot =0;
    RecoverCsr csr;
    memset(&csr, 0, sizeof(csr));
    sqlite3HctDbRecoverTid(p->p->pHctDb, rdr.iTid);
    for(/* no-op */; rc==SQLITE_OK && rdr.bEof==0; hctLogReaderNext(&rdr)){

      if( rdr.iRoot!=iPrevRoot ){
        hctRecoverCursorClose(p->p, &csr);
        rc = hctRecoverCursorOpen(p->p, rdr.iRoot, &csr);
        iPrevRoot = rdr.iRoot;
      }

      if( rc==SQLITE_OK ){
        int dummy = 0;
        if( rdr.nKey ){
          sqlite3VdbeRecordUnpack(csr.pKeyInfo, rdr.nKey, rdr.aKey, csr.pRec);
        }
        rc = sqlite3HctDbCsrRollbackSeek(csr.pCsr, csr.pRec, rdr.iKey, &dummy);
      }
    }

    hctRecoverCursorClose(p->p, &csr);
  }

  sqlite3HctDbSetSavePhysical(p->p->pHctDb, 0, 0);
  hctLogReaderClose(&rdr);
  return rc;
}

static void hctRootpageAdd(int *pRc, HctFreelistCtx *pCtx, i64 iRoot){
  if( *pRc==SQLITE_OK ){
    if( pCtx->nRoot==pCtx->nRootAlloc ){
      int nNew = (pCtx->nRoot>0) ? pCtx->nRoot * 4 : 64;
      i64 *aNew = (i64*)sqlite3_realloc(pCtx->aRoot, nNew*sizeof(i64));;
      if( aNew==0 ){
        *pRc = SQLITE_NOMEM;
        return;
      }
      pCtx->aRoot = aNew;
      pCtx->nRootAlloc = nNew;
    }

    pCtx->aRoot[pCtx->nRoot++] = iRoot;
  }
}

/*
** Assemble a list of the root pages in the current schema in the 
** pCtx->aRoot[] array.
*/
static void hctRootpageList(int *pRc, HctFreelistCtx *pCtx){
  Schema *pSchema = (Schema*)pCtx->p->pSchema;
  HashElem *pE = 0;
  for(pE=sqliteHashFirst(&pSchema->tblHash); pE; pE=sqliteHashNext(pE)){
    Table *pTab = (Table*)sqliteHashData(pE);
    Index *pIdx = 0;
    hctRootpageAdd(pRc, pCtx, pTab->tnum);
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      hctRootpageAdd(pRc, pCtx, pIdx->tnum);
    }
  }
}

/*
** This is called as part of recovery, before any log files are rolled back,
** to rebuild the free-page list (or, if you like, to initialize the
** page-manager). This involves the following:
**
**   1) Scanning the sqlite_hct_journal table, if any, from the first hole
**      to the last entry to determine the list of physical database pages
**      that will be required if sqlite3_hct_journal_rollback() is called.
**
**   2) Scanning each log file that will be rolled back, accumulating a 
**      list of the physical database pages that will be required to find
**      the "old" values required to roll them back.
**
**   3) Scanning the page map, checking for pages with the PHYSICAL_IN_USE
**      flag clear. Each such page is added to the free-page list. If the
**      page was one of those found in the scans in steps (1) or (2), then
**      it is not available for reuse until after tid $TID, and all previous
**      tids, have been committed. Otherwise, it is available for reuse 
**      immediately.
**
**      $TID is set to the TID of the next transaction that will be written
**      to this database (page-map entry TRANSID_EOF+1).
**
** This is a complicated procedure.
*/
static int hctRecoverFreeList(HBtree *p){
  HctFreelistCtx ctx;
  HctFile *pFile = sqlite3HctDbFile(p->pHctDb);
  int rc = SQLITE_OK;

  memset(&ctx, 0, sizeof(ctx));
  ctx.p = p;

  /* If this is a replication database, scan all journal entries that may
  ** be rolled back using a call to sqlite3_hct_journal_rollback(). Record
  ** the set of physical pages that may be required by this call in the 
  ** ctx.aPg[] array.  */
  if( p->pHctJrnl ){
    void *pCtx = (void*)&ctx;
    rc = sqlite3HctJrnlSavePhysical(
        p->config.db, p->pHctJrnl, hctSavePhysical, pCtx
    );
  }

  /* Also scan any log files, adding the list of physical pages that must
  ** be preserved to the ctx.aPg[] array.  */
  if( rc==SQLITE_OK ){
    sqlite3HctDbRollbackMode(p->pHctDb, 2);
    rc = sqlite3HctFileFindLogs(pFile, (void*)&ctx, hctScanOne);
    sqlite3HctDbRollbackMode(p->pHctDb, 0);
  }

  /* Sort the list of physical page numbers accumulated above. */
  hctFreelistSort(&rc, &ctx);

  /* Assemble a list of root pages. */
  hctRootpageList(&rc, &ctx);

  /* Scan the page-map, taking into account the physical pages that must
  ** be preserved, and the set of root pages in the current db schema. */
  if( rc==SQLITE_OK ){
    rc = sqlite3HctFileRecoverFreelists(
        pFile, ctx.nRoot, ctx.aRoot, ctx.nPg, ctx.aPg
    );
  }

  sqlite3_free(ctx.aPg);
  sqlite3_free(ctx.aRoot);
  return rc;
}

static int hctAttemptRecovery(HBtree *p){
  int rc = SQLITE_OK;
  if( p->bRecoveryDone==0 ){
    HctFile *pFile = sqlite3HctDbFile(p->pHctDb);
    if( p->pHctDb && sqlite3HctFileStartRecovery(pFile, 0) ){
      p->bRecoveryDone = 1;
      rc = hctRecoverFreeList(p);

      if( rc==SQLITE_OK ){
        rc = hctRecoverLogs(p);
      }

      if( rc==SQLITE_OK && p->pHctJrnl ){
        sqlite3HctDbRollbackMode(p->pHctDb, 0);
        rc = sqlite3HctJrnlRecovery(p->pHctJrnl, p->pHctDb);
      }
      rc = sqlite3HctDbFinishRecovery(p->pHctDb, 0, rc);
    }

    p->bRecoveryDone = (rc==SQLITE_OK);
  }

  return rc;
}

/*
** Like sqlite3BtreeGetMeta(), but may return an error.
*/
static int hctBtreeGetMeta(HBtree *p, int idx, u32 *pMeta){
  int rc = SQLITE_OK;

  assert( idx>=0 && idx<SQLITE_N_BTREE_META );
  assert( p->pHctDb );

  if( idx==BTREE_DATA_VERSION ){
    /* TODO: Fix this so that the data_version does not change when the
    ** database is written by the current connection. */
    i64 iSnapshot = sqlite3HctDbSnapshotId(p->pHctDb);
    *pMeta = (u32)iSnapshot;
  }else{
    if( p->eMetaState==HCT_METASTATE_NONE && p->eTrans!=SQLITE_TXN_ERROR ){
      if( p->eTrans==SQLITE_TXN_NONE ){
        rc = sqlite3HctDbGetMeta(
            p->pHctDb, (u8*)p->aMeta, SQLITE_N_BTREE_META*4
        );
      }else{
        int res = 0;
        HBtCursor csr;
        BtCursor *pCsr = (BtCursor*)&csr;
        memset(&csr, 0, sizeof(csr));

        rc = sqlite3HctBtreeCursor((Btree*)p, 2, 0, 0, pCsr);
        if( rc==SQLITE_OK ){
          rc = sqlite3HctBtreeTableMoveto(pCsr, 0, 0, &res);
        }
        /* assert( rc==SQLITE_OK ); */
        if( rc==SQLITE_OK && res==0 ){
          const void *aMeta = 0;
          u32 nMeta = 0;
          aMeta = sqlite3HctBtreePayloadFetch(pCsr, &nMeta);
          memcpy(p->aMeta, aMeta, MAX(nMeta, SQLITE_N_BTREE_META*4));
          p->eMetaState = HCT_METASTATE_READ;
        }
        sqlite3HctBtreeCloseCursor(pCsr);
      }
      sqlite3HctJournalSchemaVersion(
          p->pHctJrnl, &p->aMeta[BTREE_SCHEMA_VERSION]
      );
    }
    *pMeta = p->aMeta[idx];
  }

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
**      sqlite3HctBtreeCreateTable()
**      sqlite3HctBtreeCreateIndex()
**      sqlite3HctBtreeClearTable()
**      sqlite3HctBtreeDropTable()
**      sqlite3HctBtreeInsert()
**      sqlite3HctBtreeDelete()
**      sqlite3HctBtreeIdxDelete()
**      sqlite3HctBtreeUpdateMeta()
*/
int sqlite3HctBtreeBeginTrans(Btree *pBt, int wrflag, int *pSchemaVersion){
  HBtree *const p = (HBtree*)pBt;
  int rc = SQLITE_OK;
  int req = wrflag ? SQLITE_TXN_WRITE : SQLITE_TXN_READ;

  assert( wrflag==0 || p->pHctDb==0 || pSchemaVersion );

  if( p->eTrans==SQLITE_TXN_ERROR ) return SQLITE_BUSY_SNAPSHOT;

  if( rc==SQLITE_OK ){
    rc = sqlite3HctDbStartRead(p->pHctDb, p->pHctJrnl);
  }

  if( rc==SQLITE_OK && pSchemaVersion ){
    rc = hctBtreeGetMeta(p, 1, (u32*)pSchemaVersion);
    sqlite3HctDbTransIsConcurrent(p->pHctDb, p->config.db->eConcurrent);
  }

  if( rc==SQLITE_OK && wrflag ){
    rc = sqlite3HctTreeBegin(p->pHctTree, 1 + p->config.db->nSavepoint);
  }
  if( rc==SQLITE_OK && p->eTrans<req ){
    p->eTrans = req;
  }
  return rc;
}

/*
** This is called just after the schema is loaded for b-tree pBt.
*/
int sqlite3HctBtreeSchemaLoaded(Btree *pBt){
  int rc = SQLITE_OK;
  HBtree *const p = (HBtree*)pBt;
  if( p->bRecoveryDone==0 ){
    rc = hctDetectJournals(p);
    if( rc==SQLITE_OK ){
      rc = hctAttemptRecovery(p);
    }
    if( rc==SQLITE_OK ){
      sqlite3HctDbEndRead(p->pHctDb);
    }
  }
  if( rc==SQLITE_OK && p->pHctJrnl ){
    sqlite3HctJournalFixSchema(p->pHctJrnl, p->config.db, p->pSchema);
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
int sqlite3HctBtreeIncrVacuum(Btree *p){
  return SQLITE_DONE;
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
** committed.  See sqlite3HctBtreeCommitPhaseTwo() for the second phase of the
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
int sqlite3HctBtreeCommitPhaseOne(Btree *p, const char *zSuperJrnl){
  /* Everything happens in sqlite3HctBtreeCommitPhaseTwo() */
  return SQLITE_OK;
}

typedef struct FlushOneCtx FlushOneCtx;
struct FlushOneCtx {
  HBtree *p;
  int bRollback;
};

static int btreeFlushOneToDisk(void *pCtx, u32 iRoot, KeyInfo *pKeyInfo){
  FlushOneCtx *pFC = (FlushOneCtx*)pCtx;
  HBtree *p = pFC->p;
  int iRollbackDir = pFC->bRollback ? -1 : 1;

  HctDatabase *pDb = p->pHctDb;
  HctTreeCsr *pCsr = 0;
  int rc;
  UnpackedRecord *pRec = 0;

  if( pKeyInfo ){
    pRec = sqlite3VdbeAllocUnpackedRecord(pKeyInfo);
    if( pRec==0 ) return SQLITE_NOMEM_BKPT;
  }

  rc = sqlite3HctTreeCsrOpen(p->pHctTree, iRoot, &pCsr);
  if( rc==SQLITE_OK ){
    for(rc=sqlite3HctTreeCsrFirst(pCsr); rc==SQLITE_OK ; /* no-op */){
      int nRetry = 0;
      int ii;
      i64 iKey = 0;
      int nData = 0;
      int bDel = 0;
      const u8 *aData = 0;
      sqlite3HctTreeCsrKey(pCsr, &iKey);
      sqlite3HctTreeCsrData(pCsr, &nData, &aData);
      bDel = sqlite3HctTreeCsrIsDelete(pCsr);
      if( pRec ) sqlite3VdbeRecordUnpack(pKeyInfo, nData, aData, pRec);
      rc = sqlite3HctDbInsert(pDb, iRoot, pRec, iKey, bDel,nData,aData,&nRetry);
      p->nRollbackOp += (iRollbackDir * (1 - nRetry));
      if( rc ) break;
      p->stats.nKeyOp++;

      if( pFC->bRollback && p->nRollbackOp==0 ){
        assert( nRetry==0 );
        rc = sqlite3HctDbInsertFlush(pDb, &nRetry);
        if( rc ) break;
        if( nRetry==0 ){
          rc = SQLITE_DONE;
          break;
        }
        p->nRollbackOp = nRetry;
        if( sqlite3HctTreeCsrEof(pCsr) ){
          sqlite3HctTreeCsrLast(pCsr);
        }
      }

      if( nRetry==0 ){
        sqlite3HctTreeCsrNext(pCsr);
        if( sqlite3HctTreeCsrEof(pCsr) ){
          rc = sqlite3HctDbInsertFlush(pDb, &nRetry);
          if( nRetry ){
            sqlite3HctTreeCsrLast(pCsr);
            assert( sqlite3HctTreeCsrEof(pCsr)==0 );
            p->nRollbackOp -= (iRollbackDir * nRetry);
          }else{
            /* Done - the table has been successfully flushed to disk */
            break;
          }
        }
      }else{
        p->stats.nRetry++;
        p->stats.nRetryKey += nRetry;
      }
      for(ii=1; ii<nRetry; ii++){
        assert( sqlite3HctTreeCsrEof(pCsr)==0 );
        sqlite3HctTreeCsrPrev(pCsr);
        assert( sqlite3HctTreeCsrEof(pCsr)==0 );
      }
    }

    sqlite3HctTreeCsrClose(pCsr);
  }

  if( pKeyInfo ){
    sqlite3DbFree(pKeyInfo->db, pRec);
  }
  return rc;
}

static int btreeLogIntkey(HctLogFile *pLog, u32 iRoot, i64 iRowid){
  u8 aBuf[16];
  memcpy(&aBuf[0], &iRoot, sizeof(u32));
  memset(&aBuf[4], 0, sizeof(u32));
  memcpy(&aBuf[8], &iRowid, sizeof(i64));
  return hctLogFileWrite(pLog, aBuf, sizeof(aBuf));
}

static int btreeLogIndex(
  HctLogFile *pLog, 
  u32 iRoot, 
  const u8 *aData, int nData
){
  if( hctLogFileWrite(pLog, &iRoot, sizeof(iRoot))
   || hctLogFileWrite(pLog, &nData, sizeof(nData))
   || hctLogFileWrite(pLog, aData, nData)
  ){
    return sqlite3HctIoerr(SQLITE_IOERR_WRITE);
  }
  return SQLITE_OK;
}

static int btreeLogOneToDisk(void *pCtx, u32 iRoot, KeyInfo *pKeyInfo){
  HBtree *p = (HBtree*)pCtx;
  HctTreeCsr *pCsr = 0;
  int rc;

  rc = sqlite3HctTreeCsrOpen(p->pHctTree, iRoot, &pCsr);
  if( rc==SQLITE_OK ){
    for(rc=sqlite3HctTreeCsrFirst(pCsr); 
        rc==SQLITE_OK && sqlite3HctTreeCsrEof(pCsr)==0; 
        rc=sqlite3HctTreeCsrNext(pCsr)
    ){
      if( pKeyInfo ){
        int nData = 0;
        const u8 *aData = 0;
        sqlite3HctTreeCsrData(pCsr, &nData, &aData);
        rc = btreeLogIndex(p->pLog, iRoot, aData, nData);
      }else{
        i64 iRowid = 0;
        sqlite3HctTreeCsrKey(pCsr, &iRowid);
        rc = btreeLogIntkey(p->pLog, iRoot, iRowid);
      }

      if( rc!=SQLITE_OK ) break;
    }
    sqlite3HctTreeCsrClose(pCsr);
  }

  return rc;
}

static int btreeFlushData(HBtree *p, int bRollback){
  int rc = SQLITE_OK;

  if( bRollback ) sqlite3HctDbRollbackMode(p->pHctDb, 1);
  if( bRollback && p->nRollbackOp==0 ){
    rc = SQLITE_DONE;
  }

  if( rc==SQLITE_OK ){
    FlushOneCtx ctx;
    ctx.p = p;
    ctx.bRollback = bRollback;
    rc = sqlite3HctTreeForeach(p->pHctTree, 0, (void*)&ctx,btreeFlushOneToDisk);
  }
  if( bRollback ) sqlite3HctDbRollbackMode(p->pHctDb, 0);
  return rc;
}

static int btreeWriteLog(HBtree *p){
  int rc = SQLITE_OK;

  if( p->pLog==0 ){
    char *zLog = sqlite3HctDbLogFile(p->pHctDb);
    if( zLog==0 ){
      rc = SQLITE_NOMEM_BKPT;
    }else{
      rc = hctLogFileOpen(zLog, p->config.szLogChunk, &p->pLog);
    }
  }

  if( rc==SQLITE_OK ){
    hctLogFileRestart(p->pLog);
    rc = sqlite3HctTreeForeach(p->pHctTree, 0, (void*)p, btreeLogOneToDisk);
  }

  return rc;
}

/*
** This function is called to validate CREATE TABLE statements made as
** part of the transaction being validated.
*/
static int hctValidateSchema(HBtree *p, u64 iTid){
  int rc = SQLITE_OK;
  HctTreeCsr *pCsr = 0;

  rc = sqlite3HctTreeCsrOpen(p->pHctTree, 1, &pCsr);
  if( rc==SQLITE_OK ){
    for(rc=sqlite3HctTreeCsrFirst(pCsr); 
        rc==SQLITE_OK && !sqlite3HctTreeCsrEof(pCsr);
        rc=sqlite3HctTreeCsrNext(pCsr)
    ){
      int nData = 0;
      const u8 *aData = 0;
      const u8 *pName = 0;
      int nName = 0;

      sqlite3HctTreeCsrData(pCsr, &nData, &aData);
      nName = sqlite3HctNameFromSchemaRecord(aData, nData, &pName);
      if( nName>0 ){
        rc = sqlite3HctDbValidateTablename(p->pHctDb, pName, nName, iTid);
        if( rc!=SQLITE_OK ) break;
      }
    }
  }
  sqlite3HctTreeCsrClose(pCsr);

  return rc;
}

/*
** Flush the contents of Btree.pHctTree to Btree.pHctDb.
*/
static int btreeFlushToDisk(HBtree *p){
  int i;
  int rc = SQLITE_OK;
  int rcok = SQLITE_OK;
  u64 iTid = 0;
  u64 iCid = 0;
  int bTmapScan = 0;
  int bCustomValid = 0;           /* True if xValidate() was invoked */

  /* Write a log file for this transaction. The TID field is still set
  ** to zero at this point.  */
  rc = btreeWriteLog(p);

  if( rc==SQLITE_OK ){
    /* Obtain the TID for this transaction.  */
    iTid = sqlite3HctJrnlWriteTid(p->pHctJrnl, &iCid);
    if( iTid==0 ){
      sqlite3HctDbStartWrite(p->pHctDb, &iTid);
    }

    /* Invoke the SQLITE_TESTCTRL_HCT_MTCOMMIT hook, if applicable */
    if( p->config.db->xMtCommit ){
      p->config.db->xMtCommit(p->config.db->pMtCommitCtx, 0);
    }

    assert( iTid>0 );
    if( p->pLog ) rc = hctLogFileFinish(p->pLog, iTid);
  }

  /* Write all the new database entries to the database. Any write/write
  ** conflicts are detected here - SQLITE_BUSY is returned in that case.  */
  p->nRollbackOp = 0;
  if( rc==SQLITE_OK ){
    rc = btreeFlushData(p, 0);
  }

  /* Assuming the data has been flushed to disk without error or a
  ** write/write conflict, allocate a CID and validate the transaction. */
  if( rc==SQLITE_OK ){
    /* Invoke the SQLITE_TESTCTRL_HCT_MTCOMMIT hook, if applicable */
    if( p->config.db->xMtCommit ){
      p->config.db->xMtCommit(p->config.db->pMtCommitCtx, 1);
    }

    /* Validate the transaction. */
    rc = sqlite3HctDbValidate(p->config.db, p->pHctDb, &iCid, &bTmapScan);
    if( rc==SQLITE_OK && p->nSchemaOp ){
      /* If there have been any schema operations, there may have been
      ** CREATE TABLE statements. For each CREATE TABLE statement, check
      ** that the write to the sqlite_schema table did not create a duplicate
      ** table name.  */
      rc = hctValidateSchema(p, iTid);
    }

    /* If validation passed and this database is configured for replication,
    ** write the journal entry and invoke the custom validation hook */
    if( rc==SQLITE_OK && p->pHctJrnl ){
      rc = sqlite3HctJrnlLog(
        p->pHctJrnl,
        p->config.db,
        (Schema*)p->pSchema,
        iCid, iTid, &bCustomValid
      );
    }
  }

  /* If conflicts have been detected, roll back the transaction */
  assert( rc!=SQLITE_BUSY );
  if( rc==SQLITE_BUSY_SNAPSHOT ){
    rcok = SQLITE_BUSY_SNAPSHOT;
    rc = btreeFlushData(p, 1);
    if( rc==SQLITE_DONE ) rc = SQLITE_OK;
    if( iCid>0 && p->pHctJrnl ){
      rc = sqlite3HctJrnlWriteEmpty(p->pHctJrnl, iCid, iTid, 
          (bCustomValid ? 0 : p->config.db)
      );
    }
  }

  /* Do any DROP TABLE commands */
  for(i=0; rc==SQLITE_OK && i<p->nSchemaOp; i++){
    BtSchemaOp *pOp = &p->aSchemaOp[i];
    if( (rcok==SQLITE_OK && pOp->eSchemaOp==HCT_SCHEMAOP_DROP)
    ){
      HctFile *pFile = sqlite3HctDbFile(p->pHctDb);
      rc = sqlite3HctFileTreeFree(pFile, pOp->pgnoRoot, rcok!=SQLITE_OK);
    }
  }

  /* Zero the log file and set the entry in the transaction-map to 
  ** finish the transaction. */
  if( rc==SQLITE_OK && p->pLog ){
    rc = btreeLogFileZero(p->pLog);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctDbEndWrite(p->pHctDb, iCid, rcok!=SQLITE_OK);
  }
  assert( rc==SQLITE_OK );
  if( bTmapScan ){
    sqlite3HctDbTMapScan(p->pHctDb);
  }

  sqlite3HctJrnlInvokeHook(p->pHctJrnl, p->config.db);
  return (rc==SQLITE_OK ? rcok : rc);
}

static void hctEndTransaction(HBtree *p){
  if( p->eTrans>SQLITE_TXN_NONE 
   && p->pCsrList==0 
   && p->config.db->nVdbeRead<=1 
  ){
    if( p->pHctDb ){
      sqlite3HctDbEndRead(p->pHctDb);
    }
    p->eTrans = SQLITE_TXN_NONE;
    p->eMetaState = HCT_METASTATE_NONE;
  }
}

/*
** Cursor pCur is a direct-write cursor. This function writes the new entry
** specified by the following 4 parameters to the database b-tree opened 
** by the cursor.
**
** SQLITE_OK is returned if successful, or an SQLite error code otherwise.
*/
static int hctBtreeDirectInsert(
  HBtCursor *pCur,
  int bDirectAppend,
  UnpackedRecord *pRec,           /* Unpacked record for index b-tree */
  i64 iKey,                       /* Key value for table b-tree */
  int nData,                      /* Size of aData[] in bytes */
  const u8 *aData                 /* Pointer to record to insert */
){
  int rc = SQLITE_OK;
  int bFail = 0;

  assert( pCur->isDirectWrite );

  rc = sqlite3HctDbDirectInsert(
      pCur->pHctDbCsr, bDirectAppend,
      pRec, iKey, nData, aData, &bFail
  );
  if( bFail ){
    pCur->isDirectWrite = 0;
  }

  return rc;
}

/*
** Roll back any schema operations performed during the current transaction.
**
** iSavepoint==0 means rollback the entire transaction. iSavepoint=1 means
** roll back the outermost savepoint, etc.
*/
static void hctreeRollbackSchema(HBtree *p, int iSavepoint){
  int ii;
  assert( p->eTrans>=SQLITE_TXN_WRITE );

  for(ii=p->nSchemaOp-1; ii>=0; ii--){
    BtSchemaOp *pOp = &p->aSchemaOp[ii];

    if( iSavepoint>pOp->iSavepoint ) break;
    p->nSchemaOp = ii;

    if( pOp->eSchemaOp==HCT_SCHEMAOP_POPULATE ){
      sqlite3HctDbDirectClear(p->pHctDb, pOp->pgnoRoot);
    }

    if( pOp->eSchemaOp==HCT_SCHEMAOP_CREATE_INTKEY
     || pOp->eSchemaOp==HCT_SCHEMAOP_CREATE_INDEX
    ){
      HctFile *pFile = sqlite3HctDbFile(p->pHctDb);
      sqlite3HctFileTreeFree(pFile, pOp->pgnoRoot, 1);
    }
  }
}

/*
** Commit the transaction currently in progress.
**
** This routine implements the second phase of a 2-phase commit.  The
** sqlite3HctBtreeCommitPhaseOne() routine does the first phase and should
** be invoked prior to calling this routine.  The sqlite3HctBtreeCommitPhaseOne()
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
int sqlite3HctBtreeCommitPhaseTwo(Btree *pBt, int bCleanup){
  HBtree *const p = (HBtree*)pBt;
  int rc = SQLITE_OK;

  if( p->eTrans==SQLITE_TXN_ERROR ) return SQLITE_BUSY_SNAPSHOT;

  if( p->eTrans==SQLITE_TXN_WRITE ){
    if( p->pCsrList ){
      /* Cannot commit with open cursors in hctree */
      return SQLITE_LOCKED;
    }

    sqlite3HctTreeRelease(p->pHctTree, 0);
    if( p->pHctDb ){
      rc = btreeFlushToDisk(p);
      sqlite3HctTreeClear(p->pHctTree);
      if( rc!=SQLITE_OK ){
        hctreeRollbackSchema(p, 0);
      }
      p->nSchemaOp = 0;
    }
    p->eTrans = SQLITE_TXN_READ;
  }

  if( rc==SQLITE_OK ){
    hctEndTransaction(p);
  }else{
    p->eTrans = SQLITE_TXN_ERROR;
  }
  return rc;
}

/*
** Do both phases of a commit.
*/
int sqlite3HctBtreeCommit(Btree *pBt){
  int rc;
  HBtree *const p = (HBtree*)pBt;
  rc = sqlite3HctBtreeCommitPhaseOne((Btree*)p, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3HctBtreeCommitPhaseTwo((Btree*)p, 0);
  }
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
int sqlite3HctBtreeTripAllCursors(Btree *pBt, int errCode, int writeOnly){
  HBtree *const p = (HBtree*)pBt;
  int rc = SQLITE_OK;
  if( p ){
    HBtCursor *pCur;
    for(pCur=p->pCsrList; pCur; pCur=pCur->pCsrNext){
      if( writeOnly==0 || pCur->wrFlag ){
        sqlite3HctTreeCsrClose(pCur->pHctTreeCsr);
        pCur->pHctTreeCsr = 0;
        pCur->errCode = errCode;
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
int sqlite3HctBtreeRollback(Btree *pBt, int tripCode, int writeOnly){
  HBtree *const p = (HBtree*)pBt;

  assert( SQLITE_TXN_ERROR==4 && SQLITE_TXN_WRITE==2 );
  assert( SQLITE_TXN_READ==1 && SQLITE_TXN_NONE==0 );
  assert( p->eTrans!=SQLITE_TXN_ERROR || p->pCsrList==0 );

  if( p->eTrans>=SQLITE_TXN_WRITE ){
    hctreeRollbackSchema(p, 0);
    sqlite3HctTreeRollbackTo(p->pHctTree, 0);
    if( p->pHctDb ){
      sqlite3HctTreeClear(p->pHctTree);
    }
    p->eTrans = SQLITE_TXN_READ;
  }
  hctEndTransaction(p);
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
** using the sqlite3HctBtreeSavepoint() function.
*/
int sqlite3HctBtreeBeginStmt(Btree *pBt, int iStatement){
  HBtree *const p = (HBtree*)pBt;
  int rc = SQLITE_OK;
  assert( p->eTrans!=SQLITE_TXN_ERROR );
  rc = sqlite3HctTreeBegin(p->pHctTree, iStatement+1);
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
int sqlite3HctBtreeSavepoint(Btree *pBt, int op, int iSavepoint){
  HBtree *const p = (HBtree*)pBt;
  int rc = SQLITE_OK;
  if( p && p->eTrans==SQLITE_TXN_WRITE ){
    int i;
    assert( op==SAVEPOINT_ROLLBACK || op==SAVEPOINT_RELEASE );
    if( op==SAVEPOINT_RELEASE ){
      assert( iSavepoint>=0 );
      for(i=0; i<p->nSchemaOp; i++){
        if( p->aSchemaOp[i].iSavepoint>iSavepoint ){
          p->aSchemaOp[i].iSavepoint = iSavepoint;
        }
      }
      sqlite3HctTreeRelease(p->pHctTree, iSavepoint+1);
    }else{
      sqlite3HctTreeRollbackTo(p->pHctTree, iSavepoint+2);
      hctreeRollbackSchema(p, iSavepoint+1);
      p->eMetaState = HCT_METASTATE_NONE;
    }
  }
  return rc;
}

int sqlite3HctBtreeIsNewTable(Btree *pBt, u64 iRoot){
  HBtree *const p = (HBtree*)pBt;
  int ii;
  for(ii=0; ii<p->nSchemaOp && p->aSchemaOp[ii].pgnoRoot!=iRoot; ii++);
  return ii<p->nSchemaOp;
}

u64 sqlite3HctBtreeSnapshotId(Btree *pBt){
  HBtree *const p = (HBtree*)pBt;
  return sqlite3HctDbSnapshotId(p->pHctDb);
}

static int hctreeAddNewSchemaOp(HBtree *p, u32 iRoot, int eOp){
  BtSchemaOp *aSchemaOp;
  sqlite3 *db = p->config.db;
  int iSavepoint = db->nSavepoint + db->nStatement;

  /* Grow the Btree.aSchemaOp array */
  assert( p->pHctDb );
  aSchemaOp = (BtSchemaOp*)sqlite3_realloc(
      p->aSchemaOp, sizeof(BtSchemaOp)*(p->nSchemaOp+1)
  );
  if( aSchemaOp==0 ) return SQLITE_NOMEM_BKPT;

  p->aSchemaOp = aSchemaOp;
  p->aSchemaOp[p->nSchemaOp].pgnoRoot = iRoot;
  p->aSchemaOp[p->nSchemaOp].iSavepoint = iSavepoint;
  p->aSchemaOp[p->nSchemaOp].eSchemaOp = eOp;
  p->nSchemaOp++;

  return SQLITE_OK;
}

static int hctreeAddNewRoot(HBtree *p, u32 iRoot, int bIndex){
  int eOp = bIndex ? HCT_SCHEMAOP_CREATE_INDEX : HCT_SCHEMAOP_CREATE_INTKEY;
  return hctreeAddNewSchemaOp(p, iRoot, eOp);
}

/*
** Open a new cursor
*/
int sqlite3HctBtreeCursor(
  Btree *pBt,                                 /* The btree */
  Pgno iTable,                                /* Root page of table to open */
  int wrFlag,                                 /* 1 to write. 0 read-only */
  struct KeyInfo *pKeyInfo,                   /* First arg to xCompare() */
  BtCursor *pCursor                           /* Write new cursor here */
){ 
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  HBtree *const p = (HBtree*)pBt;
  int rc = SQLITE_OK;
  int bNosnap = 0;
  int bNoscan = (iTable==1);
  int bReadonly = sqlite3HctJournalIsReadonly(p->pHctJrnl, iTable, &bNosnap);

  assert( p->eTrans!=SQLITE_TXN_NONE );
  assert( p->eTrans!=SQLITE_TXN_ERROR );
  assert( pCur->pHctTreeCsr==0 );

  /* If this is an attempt to open a read/write cursor on either the
  ** sqlite_hct_journal or sqlite_hct_baseline tables, return an error
  ** immediately.  */
  if( wrFlag && bReadonly ){
    return SQLITE_READONLY;
  }

  pCur->pKeyInfo = pKeyInfo;
  rc = sqlite3HctTreeCsrOpen(p->pHctTree, iTable, &pCur->pHctTreeCsr);
  if( rc==SQLITE_OK && p->pHctDb ){
    int bTreeOnly = 0;
    BtSchemaOp *pOp;

    if( p->nSchemaOp>0 ){
      for(pOp=&p->aSchemaOp[p->nSchemaOp-1]; 1; pOp--){
  
        if( pOp->pgnoRoot==iTable ){
          assert( pOp->eSchemaOp==HCT_SCHEMAOP_CREATE_INTKEY
               || pOp->eSchemaOp==HCT_SCHEMAOP_CREATE_INDEX 
               || pOp->eSchemaOp==HCT_SCHEMAOP_POPULATE 
          );
          if( pOp->eSchemaOp!=HCT_SCHEMAOP_POPULATE ){
  
            if( wrFlag==0 ){
              bTreeOnly = 1;
            }else{
              if( rc==SQLITE_OK ){
                hctreeAddNewSchemaOp(p, iTable, HCT_SCHEMAOP_POPULATE);
                pOp = &p->aSchemaOp[p->nSchemaOp-1];
              }
            }
          }
  
          break;
        }
  
        if( pOp==p->aSchemaOp ){
          pOp = 0;
          break;
        }
      }
  
      if( wrFlag && pOp ){
        sqlite3 *db = p->config.db;
        if( pOp->iSavepoint==(db->nSavepoint+db->nStatement)
         && sqlite3HctTreeCsrIsEmpty(pCur->pHctTreeCsr)
        ){
          pCur->isDirectWrite = 1;
          bNoscan = 1;
        }
      }
    }

    if( bTreeOnly==0 ){
      rc = sqlite3HctDbCsrOpen(p->pHctDb, pKeyInfo, iTable, &pCur->pHctDbCsr);
      sqlite3HctDbCsrNosnap(pCur->pHctDbCsr, bNosnap);
      sqlite3HctDbCsrNoscan(pCur->pHctDbCsr, bNoscan);
    }
  }
  if( rc==SQLITE_OK ){
    pCur->pCsrNext = p->pCsrList;
    pCur->pBtree = p;
    pCur->wrFlag = wrFlag;
    p->pCsrList = pCur;
  }else{
    sqlite3HctTreeCsrClose(pCur->pHctTreeCsr);
    pCur->pHctTreeCsr = 0;
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
int sqlite3HctBtreeCursorSize(void){
  return ROUND8(sizeof(HBtCursor));
}

/*
** Initialize memory that will be converted into a BtCursor object.
**
** The simple approach here would be to memset() the entire object
** to zero.  But it turns out that the apPage[] and aiIdx[] arrays
** do not need to be zeroed and they are large, so we can save a lot
** of run-time by skipping the initialization of those elements.
*/
void sqlite3HctBtreeCursorZero(BtCursor *p){
  /* hct takes the simple approach mentioned above */
  memset(p, 0, sizeof(HBtCursor));
}

/*
** Close a cursor.  The read lock on the database file is released
** when the last cursor is closed.
*/
int sqlite3HctBtreeCloseCursor(BtCursor *pCursor){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  HBtree *const pBtree = pCur->pBtree;
  if( pBtree ){
    HBtCursor **pp;
    sqlite3HctTreeCsrClose(pCur->pHctTreeCsr);
    sqlite3HctDbCsrClose(pCur->pHctDbCsr);
    for(pp=&pBtree->pCsrList; *pp!=pCur; pp=&(*pp)->pCsrNext);
    *pp = pCur->pCsrNext;
    pCur->pHctTreeCsr = 0;
    pCur->pBtree = 0;
    pCur->pCsrNext = 0;
    if( (pBtree->openFlags & BTREE_SINGLE) && pBtree->pCsrList==0 ){
      sqlite3HctBtreeClose((Btree*)pBtree);
    }
  }
  return SQLITE_OK;
}

/*
** Return true if the given BtCursor is valid.  A valid cursor is one
** that is currently pointing to a row in a (non-empty) table.
** This is a verification routine is used only within assert() statements.
*/
int sqlite3HctBtreeCursorIsValid(BtCursor *pCursor){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  return pCur && (
      !sqlite3HctTreeCsrEof(pCur->pHctTreeCsr)
   || !sqlite3HctDbCsrEof(pCur->pHctDbCsr)
  );
}
int sqlite3HctBtreeCursorIsValidNN(BtCursor *pCursor){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
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
i64 sqlite3HctBtreeIntegerKey(BtCursor *pCursor){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
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
void sqlite3HctBtreeCursorPin(BtCursor *pCursor){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  sqlite3HctTreeCsrPin(pCur->pHctTreeCsr);
}
void sqlite3HctBtreeCursorUnpin(BtCursor *pCursor){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  sqlite3HctTreeCsrUnpin(pCur->pHctTreeCsr);
}

#ifdef SQLITE_ENABLE_OFFSET_SQL_FUNC
/*
** Return the offset into the database file for the start of the
** payload to which the cursor is pointing.
*/
i64 sqlite3HctBtreeOffset(BtCursor *pCur){
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
u32 sqlite3HctBtreePayloadSize(BtCursor *pCursor){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
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
sqlite3_int64 sqlite3HctBtreeMaxRecordSize(BtCursor *pCur){
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
** For sqlite3HctBtreePayload(), the caller must ensure that pCur is pointing
** to a valid row in the table.  For sqlite3HctBtreePayloadChecked(), the
** cursor might be invalid or might need to be restored before being read.
**
** Return SQLITE_OK on success or an error code if anything goes
** wrong.  An error is returned if "offset+amt" is larger than
** the available payload.
*/
int sqlite3HctBtreePayload(BtCursor *pCur, u32 offset, u32 amt, void *pBuf){
  u32 n = 0;
  const u8 *p = 0;

  p = (const u8*)sqlite3HctBtreePayloadFetch(pCur, &n);
  assert( offset+amt<=n );
  memcpy(pBuf, &p[offset], amt);

  return SQLITE_OK;
}


static int btreeSetUseTree(HBtCursor *pCur){
  int rc = SQLITE_OK;
  int bTreeEof = sqlite3HctTreeCsrEof(pCur->pHctTreeCsr);
  int bDbEof = sqlite3HctDbCsrEof(pCur->pHctDbCsr);

  assert( pCur->eDir==BTREE_DIR_FORWARD || pCur->eDir==BTREE_DIR_REVERSE );
  assert( pCur->pHctTreeCsr );

  if( bTreeEof ){
    pCur->bUseTree = 0;
  }else if( bDbEof ){
    pCur->bUseTree = 1;
  }else if( pCur->pKeyInfo==0 ){
    i64 iKeyTree;
    i64 iKeyDb;

    sqlite3HctTreeCsrKey(pCur->pHctTreeCsr, &iKeyTree);
    sqlite3HctDbCsrKey(pCur->pHctDbCsr, &iKeyDb);

    if( iKeyTree==iKeyDb ){
      pCur->bUseTree = 2;
    }else{
      pCur->bUseTree = (iKeyTree < iKeyDb);
      if( pCur->eDir==BTREE_DIR_REVERSE ) pCur->bUseTree = !pCur->bUseTree;
    }
  }else{
    UnpackedRecord *pKeyDb = 0;
    const u8 *aKeyTree = 0;
    int nKeyTree = 0;

    rc = sqlite3HctDbCsrLoadAndDecode(pCur->pHctDbCsr, &pKeyDb);
    if( rc==SQLITE_OK ){
      int res;
      int nSave = pKeyDb->nField;
      sqlite3HctDbRecordTrim(pKeyDb);
      sqlite3HctTreeCsrData(pCur->pHctTreeCsr, &nKeyTree, &aKeyTree);
      res = sqlite3VdbeRecordCompare(nKeyTree, aKeyTree, pKeyDb);
      pKeyDb->nField = nSave;
      if( res==0 ){
        pCur->bUseTree = 2;
      }else{
        pCur->bUseTree = (res<0);
        if( pCur->eDir==BTREE_DIR_REVERSE ) pCur->bUseTree = !pCur->bUseTree;
      }
    }
  }

  return rc;
}

static int hctReseekBlobCsr(HBtCursor *pCsr){
  int rc = SQLITE_OK;
  assert( pCsr->pKeyInfo==0 );
  if( sqlite3HctTreeCsrHasMoved(pCsr->pHctTreeCsr) ){
    int res = 0;
    rc = sqlite3HctTreeCsrReseek(pCsr->pHctTreeCsr, &res);
    if( rc==SQLITE_OK && res==0 ){
      pCsr->bUseTree = 1;
    }
  }
  return rc;
}

/*
** This variant of sqlite3HctBtreePayload() works even if the cursor has not
** in the CURSOR_VALID state.  It is only used by the sqlite3_blob_read()
** interface.
*/
#ifndef SQLITE_OMIT_INCRBLOB
int sqlite3HctBtreePayloadChecked(
  BtCursor *pCur, 
  u32 offset, 
  u32 amt, 
  void *pBuf
){
  HBtCursor *pCsr = (HBtCursor*)pCur;
  int rc = SQLITE_OK;
  rc = hctReseekBlobCsr(pCsr);
  if( rc==SQLITE_OK ){
    rc = sqlite3HctBtreePayload(pCur, offset, amt, pBuf);
  }
  return rc;
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
const void *sqlite3HctBtreePayloadFetch(BtCursor *pCursor, u32 *pAmt){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
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

/* Move the cursor to the first entry in the table.  Return SQLITE_OK
** on success.  Set *pRes to 0 if the cursor actually points to something
** or set *pRes to 1 if the table is empty.
*/
int sqlite3HctBtreeFirst(BtCursor *pCursor, int *pRes){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  int rc = SQLITE_OK;

  sqlite3HctTreeCsrFirst(pCur->pHctTreeCsr);
  if( pCur->pHctDbCsr ){
    rc = sqlite3HctDbCsrFirst(pCur->pHctDbCsr);
  }
  if( rc==SQLITE_OK ){
    pCur->eDir = BTREE_DIR_FORWARD;
    btreeSetUseTree(pCur);
    if( pCur->bUseTree && sqlite3HctTreeCsrIsDelete(pCur->pHctTreeCsr) ){
      rc = sqlite3HctBtreeNext((BtCursor*)pCur, 0);
      if( rc==SQLITE_DONE ) rc = SQLITE_OK;
    }
    *pRes = sqlite3HctBtreeEof((BtCursor*)pCur);
  }

  return rc;
}

/* Move the cursor to the last entry in the table.  Return SQLITE_OK
** on success.  Set *pRes to 0 if the cursor actually points to something
** or set *pRes to 1 if the table is empty.
*/
int sqlite3HctBtreeLast(BtCursor *pCursor, int *pRes){
  int rc = SQLITE_OK;
  HBtCursor *const pCur = (HBtCursor*)pCursor;

  if( pCur->isLast==0 ){
    sqlite3HctTreeCsrLast(pCur->pHctTreeCsr);
    if( pCur->pHctDbCsr ){
      rc = sqlite3HctDbCsrLast(pCur->pHctDbCsr);
    }
    if( rc==SQLITE_OK ){
      int bTreeEof = sqlite3HctTreeCsrEof(pCur->pHctTreeCsr);
      int bDbEof = sqlite3HctDbCsrEof(pCur->pHctDbCsr);
      *pRes = (bTreeEof && bDbEof);
      pCur->eDir = BTREE_DIR_REVERSE;
      btreeSetUseTree(pCur);
      if( pCur->bUseTree ){
        if( sqlite3HctTreeCsrIsDelete(pCur->pHctTreeCsr) ){
          rc = sqlite3HctBtreePrevious((BtCursor*)pCur, 0);
          if( rc==SQLITE_DONE ){
            *pRes = sqlite3HctBtreeEof((BtCursor*)pCur);
            rc = SQLITE_OK;
          }
        }else{
          pCur->isLast = 1;
        }
      }
    }
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
static int hctBtreeMovetoUnpacked(
  HBtCursor *pCur,         /* The cursor to be moved */
  UnpackedRecord *pIdxKey, /* Unpacked index key */
  i64 intKey,              /* The table key */
  int biasRight,           /* If true, bias the search to the high end */
  int *pRes                /* Write search results here */
){
  int rc = SQLITE_OK;
  int res1 = 0;
  int res2 = -1;

  pCur->isLast = 0;
  rc = sqlite3HctTreeCsrSeek(pCur->pHctTreeCsr, pIdxKey, intKey, &res1);
  if( rc==SQLITE_OK && pCur->pHctDbCsr ){
    rc = sqlite3HctDbCsrSeek(pCur->pHctDbCsr, pIdxKey, intKey, &res2);
  }

  if( pCur->eDir==BTREE_DIR_NONE ){
    if( res1==0 || pCur->pHctDbCsr==0 ){
      *pRes = res1;
      pCur->bUseTree = 1;
      if( sqlite3HctTreeCsrIsDelete(pCur->pHctTreeCsr) ){
        *pRes = -1;
      }
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
        rc = sqlite3HctTreeCsrNext(pCur->pHctTreeCsr);
      }

      if( res1==0 || (res2==0 && pCur->pHctDbCsr) ){
        *pRes = 0;
      }else if( sqlite3HctTreeCsrEof(pCur->pHctTreeCsr)
             && sqlite3HctDbCsrEof(pCur->pHctDbCsr)
      ){
        *pRes = -1;
      }else{
        *pRes = +1;
      }
    }else{
      assert( pCur->eDir==BTREE_DIR_REVERSE );
      assert( res2<=0 );
      if( rc==SQLITE_OK && res1>0 && !sqlite3HctTreeCsrEof(pCur->pHctTreeCsr) ){
        rc = sqlite3HctTreeCsrPrev(pCur->pHctTreeCsr);
      }
      if( res1==0 || res2==0 ){
        *pRes = 0;
      }else{
        *pRes = -1;
      }
    }

    btreeSetUseTree(pCur);
    if( pCur->bUseTree && sqlite3HctTreeCsrIsDelete(pCur->pHctTreeCsr) ){
      if( pCur->eDir==BTREE_DIR_FORWARD ){
        rc = sqlite3HctBtreeNext((BtCursor*)pCur, 0);
        if( rc==SQLITE_DONE ){
          /* Cursor points at EOF. *pRes must be -ve in this case. */
          rc = SQLITE_OK;
          *pRes = -1;
        }else if( pIdxKey==0 ){
          *pRes = 1;
        }else{
          u32 nKey;
          const void *a = sqlite3HctBtreePayloadFetch((BtCursor*)pCur, &nKey);
          *pRes = sqlite3VdbeRecordCompareWithSkip(nKey, a, pIdxKey, 0);
        }
      }else{
        rc = sqlite3HctBtreePrevious((BtCursor*)pCur, 0);
        if( rc==SQLITE_DONE ) rc = SQLITE_OK;
        *pRes = -1;
      }
    }
  }

  return rc;
}

int sqlite3HctBtreeTableMoveto(
  BtCursor *pCursor,       /* The cursor to be moved */
  i64 intKey,              /* The table key */
  int biasRight,           /* If true, bias the search to the high end */
  int *pRes                /* Write search results here */
){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  if( pCur->isLast && sqlite3HctBtreeIntegerKey(pCursor)<intKey ){
    *pRes = -1;
    return SQLITE_OK;
  }
  return hctBtreeMovetoUnpacked(pCur, 0, intKey, biasRight, pRes);
}
int sqlite3HctBtreeIndexMoveto(
  BtCursor *pCursor,       /* The cursor to be moved */
  UnpackedRecord *pIdxKey, /* Unpacked index key */
  int *pRes                /* Write search results here */
){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  return hctBtreeMovetoUnpacked(pCur, pIdxKey, 0, 0, pRes);
}

void sqlite3HctBtreeCursorDir(BtCursor *pCursor, int eDir){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  assert( eDir==BTREE_DIR_NONE 
       || eDir==BTREE_DIR_FORWARD 
       || eDir==BTREE_DIR_REVERSE
  );
  pCur->eDir = eDir;
  if( pCur->pHctDbCsr ){
    sqlite3HctDbCsrDir(pCur->pHctDbCsr, eDir);
  }
}

/*
** Return TRUE if the cursor is not pointing at an entry of the table.
**
** TRUE will be returned after a call to sqlite3HctBtreeNext() moves
** past the last entry in the table or sqlite3HctBtreePrev() moves past
** the first entry.  TRUE is also returned if the table is empty.
*/
int sqlite3HctBtreeEof(BtCursor *pCursor){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
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
i64 sqlite3HctBtreeRowCountEst(BtCursor *pCur){
  /* TODO: Fix this so that it returns a meaningful value. */
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
** The main entry point is sqlite3HctBtreeNext().  That routine is optimized
** for the common case of merely incrementing the cell counter BtCursor.aiIdx
** to the next cell on the current page.  The (slower) btreeNext() helper
** routine is called when it is necessary to move to a different page or
** to restore the cursor.
**
** If bit 0x01 of the F argument in sqlite3HctBtreeNext(C,F) is 1, then the
** cursor corresponds to an SQL index and this routine could have been
** skipped if the SQL index had been a unique index.  The F argument
** is a hint to the implement. SQLite btree implementation does not use
** this hint, but COMDB2 does.
*/
int sqlite3HctBtreeNext(BtCursor *pCursor, int flags){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  int rc = SQLITE_OK;
  int bDummy;

  assert( pCur->isLast==0 );
  rc = sqlite3HctBtreeCursorRestore((BtCursor*)pCur, &bDummy);
  if( rc!=SQLITE_OK ) return rc;

  if( sqlite3HctBtreeEof((BtCursor*)pCur) ){
    rc = SQLITE_DONE;
  }else{
    assert( pCur->eDir==BTREE_DIR_FORWARD );
    do{
      if( pCur->bUseTree ){
        rc = sqlite3HctTreeCsrNext(pCur->pHctTreeCsr);
      }
      if( rc==SQLITE_OK && (pCur->bUseTree==0 || pCur->bUseTree==2) ){
        rc = sqlite3HctDbCsrNext(pCur->pHctDbCsr);
      }
      if( rc==SQLITE_OK ){
        if( sqlite3HctBtreeEof((BtCursor*)pCur) ){
          rc = SQLITE_DONE;
        }else{
          btreeSetUseTree(pCur);
        }
      }
    }while( rc==SQLITE_OK 
        && pCur->bUseTree && sqlite3HctTreeCsrIsDelete(pCur->pHctTreeCsr) 
    );
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
** The main entry point is sqlite3HctBtreePrevious().  That routine is optimized
** for the common case of merely decrementing the cell counter BtCursor.aiIdx
** to the previous cell on the current page.  The (slower) btreePrevious()
** helper routine is called when it is necessary to move to a different page
** or to restore the cursor.
**
** If bit 0x01 of the F argument to sqlite3HctBtreePrevious(C,F) is 1, then
** the cursor corresponds to an SQL index and this routine could have been
** skipped if the SQL index had been a unique index.  The F argument is a
** hint to the implement.  The native SQLite btree implementation does not
** use this hint, but COMDB2 does.
*/
int sqlite3HctBtreePrevious(BtCursor *pCursor, int flags){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  int rc = SQLITE_OK;
  int bDummy;
  assert( pCur->eDir==BTREE_DIR_REVERSE );

  pCur->isLast = 0;
  rc = sqlite3HctBtreeCursorRestore((BtCursor*)pCur, &bDummy);
  if( rc!=SQLITE_OK ) return rc;

  do{
    if( pCur->bUseTree ){
      rc = sqlite3HctTreeCsrPrev(pCur->pHctTreeCsr);
    }
    if( rc==SQLITE_OK && (pCur->bUseTree==0 || pCur->bUseTree==2) ){
      rc = sqlite3HctDbCsrPrev(pCur->pHctDbCsr);
    }
    if( rc==SQLITE_OK ){
      if( sqlite3HctBtreeEof((BtCursor*)pCur) ){
        rc = SQLITE_DONE;
      }else{
        btreeSetUseTree(pCur);
      }
    }
  }while( rc==SQLITE_OK 
      && pCur->bUseTree && sqlite3HctTreeCsrIsDelete(pCur->pHctTreeCsr) 
  );
  return rc;
}

static void hctBtreeClearIsLast(HBtree *pBt, HBtCursor *pExcept){
  HBtCursor *p;
  for(p=pBt->pCsrList; p; p=p->pCsrNext){
    if( p!=pExcept ) p->isLast = 0;
  }
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
int sqlite3HctBtreeInsert(
  BtCursor *pCursor,             /* Insert data into the table of this cursor */
  const BtreePayload *pX,        /* Content of the row to be inserted */
  int flags,                     /* True if this is likely an append */
  int seekResult                 /* Result of prior MovetoUnpacked() call */
){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  HctTreeCsr *pTreeCsr = pCur->pHctTreeCsr;
  int rc = SQLITE_OK;
  UnpackedRecord r;
  UnpackedRecord *pRec = 0;
  const u8 *aData;
  int nData;
  int nZero;
  i64 iKey = 0;
  int bDirectAppend = 0;

  hctBtreeClearIsLast(pCur->pBtree, pCur);

  if( pCur->isDirectWrite 
   && seekResult<0
   && sqlite3HctDbCsrIsLast(pCur->pHctDbCsr)
  ){
    bDirectAppend = 1;
  }

  if( pX->pKey ){
    aData = pX->pKey;
    nData = pX->nKey;
    nZero = 0;
    iKey = 0;
    if( bDirectAppend==0 ){
      /* If bDirectAppend is set, then it is already known that this will 
      ** be an append to the table. So there is no need for an unpacked 
      ** record.  */
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
    }
  }else{
    aData = pX->pData;
    nData = pX->nData;
    nZero = pX->nZero;
    iKey = pX->nKey;
  }

  if( pCur->isDirectWrite ){
    rc = hctBtreeDirectInsert(pCur, bDirectAppend, pRec, iKey, nData, aData);
    assert( bDirectAppend==0 || pCur->isDirectWrite );
  }

  if( pCur->isDirectWrite==0 ){
    if( pCur->isLast && seekResult<0 ){
      rc = sqlite3HctTreeAppend(
          pTreeCsr, pCur->pKeyInfo, iKey, nData, aData, nZero
      );
    }else{
      rc = sqlite3HctTreeInsert(pTreeCsr, pRec, iKey, nData, aData, nZero);
      pCur->isLast = 0;
    }
  }

  if( pRec && pRec!=&r ){
    sqlite3DbFree(pCur->pKeyInfo->db, pRec);
  }
  return rc;
}

int sqlite3HctSchemaOp(Btree *pBt, const char *zSql){
  int rc = SQLITE_OK;
  HBtree *const p = (HBtree*)pBt;
  if( p->pHctJrnl ){
    HctTreeCsr *pCsr = 0;

    rc = sqlite3HctTreeCsrOpen(p->pHctTree, HCT_TREE_SCHEMAOP_ROOT, &pCsr);
    if( rc==SQLITE_OK ){
      int nSql = sqlite3Strlen30(zSql);
      i64 iRowid = 1;
      sqlite3HctTreeCsrLast(pCsr);
      if( sqlite3HctTreeCsrEof(pCsr)==0 ){
        sqlite3HctTreeCsrKey(pCsr, &iRowid);
        iRowid++;
      }

      rc = sqlite3HctTreeInsert(pCsr, 0, iRowid, nSql, (const u8*)zSql, 0);
      sqlite3HctTreeCsrClose(pCsr);
    }
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
int sqlite3HctBtreeDelete(BtCursor *pCursor, u8 flags){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  int rc = SQLITE_OK;

  /* Switch the cursor out of direct write mode before proceeding with 
  ** the rest of this function. */
  pCur->isDirectWrite = 0;

  hctBtreeClearIsLast(pCur->pBtree, 0);
  if( pCur->pHctDbCsr==0 ){
    rc = sqlite3HctTreeDelete(pCur->pHctTreeCsr);
  }else if( pCur->pKeyInfo==0 ){
    i64 iKey = sqlite3HctBtreeIntegerKey((BtCursor*)pCur);
    rc = sqlite3HctTreeDeleteKey(pCur->pHctTreeCsr, 0, iKey, 0, 0);
  }else{
    u32 nKey;
    const u8 *aKey = (u8*)sqlite3HctBtreePayloadFetch((BtCursor*)pCur, &nKey);
    UnpackedRecord *pRec = sqlite3VdbeAllocUnpackedRecord(pCur->pKeyInfo);

    if( pRec==0 ){
      rc = SQLITE_NOMEM_BKPT;
    }else{
      sqlite3VdbeRecordUnpack(pCur->pKeyInfo, nKey, aKey, pRec);
      rc = sqlite3HctTreeDeleteKey(pCur->pHctTreeCsr, pRec, 0, nKey, aKey);
      sqlite3DbFree(pCur->pBtree->config.db, pRec);
    }
  }
  return rc;
}

int sqlite3HctBtreeIdxDelete(BtCursor *pCursor, UnpackedRecord *pKey){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  int rc = SQLITE_OK;

  /* Switch the cursor out of direct write mode before proceeding with 
  ** the rest of this function. */
  pCur->isDirectWrite = 0;

  hctBtreeClearIsLast(pCur->pBtree, 0);
  if( pCur->pHctDbCsr ){
    u8 *aRec = 0;
    int nRec = 0;
    rc = sqlite3HctSerializeRecord(pKey, &aRec, &nRec);
    if( rc==SQLITE_OK ){
      rc = sqlite3HctTreeDeleteKey(pCur->pHctTreeCsr, pKey, 0, nRec, aRec);
      sqlite3_free(aRec);
    }
  }else{
    int res = 0;
    rc = sqlite3HctTreeCsrSeek(pCur->pHctTreeCsr, pKey, 0, &res);
    if( res==0 ){
      rc = sqlite3HctTreeDelete(pCur->pHctTreeCsr);
    }
  }
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
int sqlite3HctBtreeCreateTable(Btree *pBt, Pgno *piTable, int flags){
  HBtree *const p = (HBtree*)pBt;
  Pgno iNew = 0;
  int rc = SQLITE_OK;
  if( p->pHctDb ){
    rc = sqlite3HctDbRootNew(p->pHctDb, &iNew);
    if( rc==SQLITE_OK ){
      rc = sqlite3HctDbRootInit(p->pHctDb, (flags & BTREE_INTKEY)==0, iNew);
    }
    if( rc==SQLITE_OK ){
      rc = hctreeAddNewRoot(p, iNew, (flags & BTREE_INTKEY)==0);
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
int sqlite3HctBtreeClearTable(Btree *pBt, int iTable, i64 *pnChange){
  HBtree *const p = (HBtree*)pBt;
  int rc = SQLITE_OK;
  KeyInfo  *pKeyInfo = 0;

  rc = hctFindKeyInfo(p, iTable, &pKeyInfo);
  if( rc==SQLITE_OK ){
    i64 nChange = 0;
    BtCursor *pCsr = 0;
    HctTreeCsr *pTreeCsr = 0;
    UnpackedRecord *pRec = 0;

    if( pKeyInfo ){
      pRec = sqlite3VdbeAllocUnpackedRecord(pKeyInfo);
      if( pRec==0 ) rc = SQLITE_NOMEM_BKPT;
    }
    pCsr = (BtCursor*)sqlite3HctMalloc(&rc, sizeof(HBtCursor));
    if( rc==SQLITE_OK ){
      rc = sqlite3HctBtreeCursor(pBt, iTable, 0, pKeyInfo, pCsr);
    }
    if( rc==SQLITE_OK ){
      rc = sqlite3HctTreeCsrOpen(p->pHctTree, iTable, &pTreeCsr);
    }

    if( rc==SQLITE_OK ){
      int res = 0;
      rc = sqlite3HctBtreeFirst(pCsr, &res);
      if( res==0 ){
        while( rc==SQLITE_OK ){
          nChange++;
          if( pKeyInfo ){
            const u8 *aData = 0;
            u32 nData = 0;
            aData = (const u8*)sqlite3HctBtreePayloadFetch(pCsr, &nData);
            sqlite3VdbeRecordUnpack(pKeyInfo, nData, aData, pRec);
            rc = sqlite3HctTreeDeleteKey(pTreeCsr, pRec, 0, nData, aData);
          }else{
            i64 iKey = sqlite3HctBtreeIntegerKey((BtCursor*)pCsr);
            rc = sqlite3HctTreeDeleteKey(pTreeCsr, 0, iKey, 0, 0);
          }
          rc = sqlite3HctBtreeNext(pCsr, 0);
        }
        if( rc==SQLITE_DONE ) rc = SQLITE_OK;
      }
    }
    if( pnChange ) *pnChange = nChange;

    sqlite3KeyInfoUnref(pKeyInfo);
    sqlite3HctBtreeCloseCursor(pCsr);
    sqlite3HctTreeCsrClose(pTreeCsr);
    sqlite3DbFree(p->config.db, pRec);
    sqlite3_free(pCsr);
  }
  return rc;
}

/*
** Delete all information from the single table that pCur is open on.
**
** This routine only work for pCur on an ephemeral table.
*/
int sqlite3HctBtreeClearTableOfCursor(BtCursor *pCursor){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  return sqlite3HctTreeClearOne(
      pCur->pBtree->pHctTree, sqlite3HctTreeCsrRoot(pCur->pHctTreeCsr), 0
  );
}

/*
** Drop the table with root page iTable. Set (*piMoved) to 0 before
** returning.
*/
int sqlite3HctBtreeDropTable(Btree *pBt, int iTable, int *piMoved){
  HBtree *const p = (HBtree*)pBt;
  *piMoved = 0;
  return hctreeAddNewSchemaOp(p, iTable, HCT_SCHEMAOP_DROP);
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
void sqlite3HctBtreeGetMeta(Btree *pBt, int idx, u32 *pMeta){
  HBtree *const p = (HBtree*)pBt;
  (void)hctBtreeGetMeta(p, idx, pMeta);
}

/*
** Write meta-information back into the database.  Meta[0] is
** read-only and may not be written.
*/
int sqlite3HctBtreeUpdateMeta(Btree *pBt, int idx, u32 iMeta){
  int rc = SQLITE_OK;
  HBtree *const p = (HBtree*)pBt;
  u32 dummy;
  sqlite3HctBtreeGetMeta((Btree*)p, 0, &dummy);
  if( p->aMeta[idx]!=iMeta ){
    p->aMeta[BTREE_SCHEMA_VERSION] += 1234;
    p->aMeta[idx] = iMeta;
    rc = sqlite3HctTreeUpdateMeta(
        p->pHctTree, (u8*)p->aMeta, SQLITE_N_BTREE_META*4
    );
  }
  return rc;
}

static char *hctDbMPrintf(int *pRc, const char *zFormat, ...){
  char *zRet = 0;
  if( *pRc==SQLITE_OK ){
    va_list ap;
    va_start(ap, zFormat);
    zRet = sqlite3_vmprintf(zFormat, ap);
    va_end(ap);
    if( !zRet ) *pRc = SQLITE_NOMEM_BKPT;
  }
  return zRet;
}

int sqlite3HctBtreePragma(Btree *pBt, char **aFnctl){
  HBtree *const p = (HBtree*)pBt;
  int rc = SQLITE_OK;
  const char *zLeft = aFnctl[1];
  const char *zRight = aFnctl[2];
  char *zRet = 0;

  if( 0==sqlite3_stricmp("hct_ndbfile", zLeft) ){
    HctFile *pFile = sqlite3HctDbFile(p->pHctDb);
    int iCurrent = 0;
    int bFixed = 0;
    if( zRight ){
      int iVal = sqlite3Atoi(zRight);
      if( iVal<1 || iVal>HCT_MAX_NDBFILE ){
        rc = SQLITE_RANGE;
      }else{
        p->config.nDbFile = iVal;
      }
    }
    if( rc==SQLITE_OK ){
      iCurrent = sqlite3HctFileNFile(pFile, &bFixed);
      if( bFixed==0 ) iCurrent = p->config.nDbFile;
      zRet = hctDbMPrintf(&rc, "%d", iCurrent);
    }
  }

  else if( 0==sqlite3_stricmp("hct_try_before_unevict", zLeft) ){
    int iVal = 0;
    if( zRight ){
      iVal = sqlite3Atoi(zRight);
    }
    if( iVal>0 ){
      p->config.nTryBeforeUnevict = iVal;
    }
    zRet = hctDbMPrintf(&rc, "%d", p->config.nTryBeforeUnevict);
  }
  else if( 0==sqlite3_stricmp("hct_npageset", zLeft) ){
    int iVal = 0;
    if( zRight ){
      iVal = sqlite3Atoi(zRight);
    }
    if( iVal>0 ){
      p->config.nPageSet = iVal;
    }
    zRet = hctDbMPrintf(&rc, "%d", p->config.nPageSet);
  }
  else if( 0==sqlite3_stricmp("hct_ncasfail", zLeft) ){
    zRet = hctDbMPrintf(&rc, "%lld", sqlite3HctDbNCasFail(p->pHctDb));
  }
  else if( p->pHctDb && 0==sqlite3_stricmp("hct_npagescan", zLeft) ){
    int iVal = 0;
    if( zRight ){
      iVal = sqlite3Atoi(zRight);
    }
    if( iVal>0 ){
      p->config.nPageScan = iVal;
    }
    zRet = hctDbMPrintf(&rc, "%d", p->config.nPageScan);
  }
  else if( 0==sqlite3_stricmp("hct_quiescent_integrity_check", zLeft) ){
    int iVal = 0;
    if( zRight ){
      iVal = sqlite3Atoi(zRight);
    }
    if( iVal>0 ){
      p->config.bQuiescentIntegrityCheck = (iVal==0 ? 0 : 1);
    }
    zRet = hctDbMPrintf(&rc, "%d", p->config.bQuiescentIntegrityCheck);
  }else if( 0==sqlite3_stricmp("hct_create_table_no_cookie", zLeft) ){
    int iVal = 0;
    if( zRight ){
      iVal = sqlite3Atoi(zRight);
    }
    if( iVal>0 ){
      p->config.db->bCTNoCookie = (iVal==0 ? 0 : 1);
    }
    zRet = hctDbMPrintf(&rc, "%d", p->config.db->bCTNoCookie);
  }else{
    rc = SQLITE_NOTFOUND;
  }

  aFnctl[0] = zRet;
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
int sqlite3HctBtreeCount(sqlite3 *db, BtCursor *pCursor, i64 *pnEntry){
  HBtCursor *const pCur = (HBtCursor*)pCursor;
  i64 nEntry = 0;
  int dummy = 0;
  int rc;
  for(rc = sqlite3HctBtreeFirst((BtCursor*)pCur, &dummy);
      rc==SQLITE_OK && 0==sqlite3HctBtreeEof((BtCursor*)pCur);
      rc = sqlite3HctBtreeNext((BtCursor*)pCur, 0)
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
Pager *sqlite3HctBtreePager(Btree *pBt){
  HBtree *const p = (HBtree*)pBt;
  return p->pFakePager;
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
int sqlite3HctBtreeIntegrityCheck(
  sqlite3 *db,  /* Database connection that is running the check */
  Btree *pBt,   /* The btree to be checked */
  Pgno *aRoot,  /* An array of root pages numbers for individual trees */
  Mem *aCnt,
  int nRoot,    /* Number of entries in aRoot[] */
  int mxErr,    /* Stop reporting errors after this many */
  int *pnErr,   /* Write number of errors seen to this variable */
  char **pzErr
){
  HBtree *const p = (HBtree*)pBt;
  char *zRet = 0;                 /* Return value */
  *pnErr = 0;
  int ii;
  for(ii=0; ii<nRoot; ii++){
    sqlite3MemSetArrayInt64(aCnt, ii, 0);
  }
  if( p->config.bQuiescentIntegrityCheck && nRoot>0 && aRoot[0]!=0 ){
    zRet = sqlite3HctDbIntegrityCheck(p->pHctDb, aRoot, aCnt, nRoot, pnErr);
    assert( zRet==0 || (*pnErr)>0 );
  }
  *pzErr = zRet;
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
const char *sqlite3HctBtreeGetFilename(Btree *p){
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
const char *sqlite3HctBtreeGetJournalname(Btree *p){
  return 0;
}

/*
** Return one of SQLITE_TXN_NONE, SQLITE_TXN_READ, or SQLITE_TXN_WRITE
** to describe the current transaction state of Btree p.
*/
int sqlite3HctBtreeTxnState(Btree *pBt){
  HBtree *const p = (HBtree*)pBt;
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
int sqlite3HctBtreeCheckpoint(Btree *p, int eMode, int *pnLog, int *pnCkpt){
  return SQLITE_OK;
}
#endif

/*
** Return true if there is currently a backup running on Btree p.
*/
int sqlite3HctBtreeIsInBackup(Btree *p){
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
void *sqlite3HctBtreeSchema(Btree *pBt, int nBytes, void(*xFree)(void *)){
  HBtree *const p = (HBtree*)pBt;
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
int sqlite3HctBtreeSchemaLocked(Btree *p){
  return SQLITE_OK;
}

HctDatabase *sqlite3HctDbFind(sqlite3 *db, int iDb){
  Btree *pBt = db->aDb[iDb].pBt;
  return sqlite3IsHct(pBt) ? ((HBtree*)pBt)->pHctDb : 0;
}
HctJournal *sqlite3HctJrnlFind(sqlite3 *db){
  Btree *pBt = db->aDb[0].pBt;
  return sqlite3IsHct(pBt) ? ((HBtree*)pBt)->pHctJrnl : 0;
}

#ifndef SQLITE_OMIT_SHARED_CACHE
/*
** Obtain a lock on the table whose root page is iTab.  The
** lock is a write lock if isWritelock is true or a read lock
** if it is false.
*/
int sqlite3HctBtreeLockTable(Btree *p, int iTab, u8 isWriteLock){
  int rc = SQLITE_OK;
  assert( 0 );
  return rc;
}
#endif

#ifndef SQLITE_OMIT_INCRBLOB
/*
** Argument pCur must be a cursor opened for writing on an 
** INTKEY table currently pointing at a valid table entry. 
** This function modifies the data stored as part of that entry.
**
** Only the data content may only be modified, it is not possible to 
** change the length of the data stored. If this function is called with
** parameters that attempt to write past the end of the existing data,
** no modifications are made and SQLITE_CORRUPT is returned.
*/
int sqlite3HctBtreePutData(BtCursor *pCur, u32 offset, u32 amt, void *z){
  HBtCursor *pCsr = (HBtCursor*)pCur;
  int rc = SQLITE_OK;

  if( pCsr->wrFlag==0 ){
    rc = SQLITE_READONLY;
  }else{
    rc = hctReseekBlobCsr(pCsr);
  }
  if( rc==SQLITE_OK ){
    u32 nData = 0;
    const void *aData = sqlite3HctBtreePayloadFetch(pCur, &nData);
    if( offset+amt>nData ){
      rc = SQLITE_CORRUPT_BKPT;
    }else{
      u8 *aBuf = (u8*)sqlite3_malloc(nData+1);
      if( aBuf ){
        BtreePayload payload;
        memcpy(aBuf, aData, nData);
        memcpy(&aBuf[offset], z, amt);

        memset(&payload, 0, sizeof(payload));
        payload.nKey = sqlite3HctBtreeIntegerKey(pCur);
        payload.pData = (const void*)aBuf;
        payload.nData = nData;
        rc = sqlite3HctBtreeInsert(pCur, &payload, 0, 0);
        if( rc==SQLITE_OK ){
          int dummy = 0;
          rc = sqlite3HctBtreeTableMoveto(pCur, payload.nKey, 0, &dummy);
          assert( dummy==0 );
        }
        sqlite3_free(aBuf);
      }else{
        rc = SQLITE_NOMEM;
      }
    }
  }

  return rc;
}

/* 
** Mark this cursor as an incremental blob cursor.
*/
void sqlite3HctBtreeIncrblobCursor(BtCursor *pCur){
  HBtCursor *pCsr = (HBtCursor*)pCur;
  sqlite3HctTreeCsrIncrblob(pCsr->pHctTreeCsr);
}
#endif

/*
** Set both the "read version" (single byte at byte offset 18) and 
** "write version" (single byte at byte offset 19) fields in the database
** header to iVersion.
*/
int sqlite3HctBtreeSetVersion(Btree *pBtree, int iVersion){
  assert( 0 );
  return SQLITE_OK;
}

/*
** Return true if the cursor has a hint specified.  This routine is
** only used from within assert() statements
*/
int sqlite3HctBtreeCursorHasHint(BtCursor *pCsr, unsigned int mask){
  return 0;
}

/*
** Return true if the given Btree is read-only.
*/
int sqlite3HctBtreeIsReadonly(Btree *p){
  return 0;
}

#if !defined(SQLITE_OMIT_SHARED_CACHE)
/*
** Return true if the Btree passed as the only argument is sharable.
*/
int sqlite3HctBtreeSharable(Btree *p){
  assert( 0 );
  return 0;
}

/*
** Return the number of connections to the BtShared object accessed by
** the Btree handle passed as the only argument. For private caches 
** this is always 1. For shared caches it may be 1 or greater.
*/
int sqlite3HctBtreeConnectionCount(Btree *p){
  assert( 0 );
  return 1;
}
#endif

int sqlite3HctBtreeExclusiveLock(Btree *p){
  return SQLITE_OK;
}

int sqlite3HctBtreeTransferRow(BtCursor *p1, BtCursor *p2, i64 iKey){
  assert( 0 );
  return SQLITE_LOCKED;
}

int sqlite3HctLockedErr(u32 pgno, const char *zReason){
  return SQLITE_LOCKED;
}

i64 sqlite3HctMainStats(sqlite3 *db, int iStat, const char **pzStat){
  Btree *pBt = db->aDb[0].pBt;

  i64 iRet = 0;

  if( sqlite3IsHct(pBt) ){
    HBtree *pHct = (HBtree*)pBt;
    switch( iStat ){
      case 0:
        *pzStat = "nretry";
        iRet = pHct->stats.nRetry;
        break;
      case 1:
        *pzStat = "nretrykey";
        iRet = pHct->stats.nRetryKey;
        break;
      case 2:
        *pzStat = "nkeyop";
        iRet = pHct->stats.nKeyOp;
        break;
    }
  }

  return iRet;
}


#endif /* SQLITE_ENABLE_HCT */
