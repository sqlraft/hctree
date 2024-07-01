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

#define HCT_JOURNAL_SCHEMA                  \
"CREATE TABLE sqlite_hct_journal("          \
    "cid INTEGER PRIMARY KEY,"              \
    "schema TEXT,"                          \
    "data BLOB,"                            \
    "schemacid INTEGER,"                    \
    "hash BLOB,"                            \
    "tid INTEGER,"                          \
    "validcid INTEGER"                      \
");"


#define HCT_BASELINE_SCHEMA                 \
"CREATE TABLE sqlite_hct_baseline("         \
    "cid INTEGER,"                          \
    "schemacid INTEGER,"                    \
    "hash BLOB"                             \
");"

/*
** In follower mode, it is not possible to call sqlite3_hct_journal_write()
** for the transaction with CID (N + HCT_MAX_LEADING_WRITE) until all 
** transactions with CID values of N or less have been committed.
*/
#define HCT_MAX_LEADING_WRITE (8*1024)

typedef struct HctJrnlServer HctJrnlServer;
typedef struct HctJrnlPendingHook HctJrnlPendingHook;

/*
** One object of this type is shared by all connections to the same 
** database. Managed by the HctFileServer object (see functions
** sqlite3HctFileGetJrnlPtr() and SetJrnlPtr()).
**
** iSchemaCid:
**   This contains the current schema version of the database. Even though
**   this value may be concurrently accessed, there is no need for an
**   advanced or versioned data structure. Because:
**
**   1) In LEADER mode, this value is only accessed when writing an entry
**      to the journal table, from within sqlite3HctJrnlLog(). It is only
**      written to if the transaction has modified the database schema.
**
**      The call to sqlite3HctJrnlLog() comes after the transaction has been
**      successfully validated. And a transaction that modifies the schema
**      only passes validation if there have been no writes at all to the
**      the database since its snapshot was opened - i.e. if the CID for the
**      transaction is one greater than the CID of its snapshot. This 
**      guarantees that there are no transactions with CID values less than
**      that of the schema transaction concurrently accessing iSchemaCid.
**
**      Also, since schema transactions modify the schema cookie, and all other
**      transactions check the schema cookie during validation, it is
**      guaranteed that no transaction started before the schema transaction
**      is committed may successfully validate with a CID value greater than
**      that of the schema transaction.
**    
**      Therefore, if a schema transaction has passed validation, it is
**      guaranteed exclusive access to the iSchemaCid variable.
**
**   2) In FOLLOWER mode, the value is:
**
**      * read from within sqlite3_hct_journal_write(), just after opening
**        a snapshot, and
**
**      * written from within the same call, following successful validation
**        of a schema transaction.
**
**      A schema transaction is only started once all transactions with CID
**      values less than that of the schema transaction have finished
**      committing. This alone ensures that there is at most a single
**      writer to the iSchemaCid variable at any one time.
**
** eMode:
**   The current database mode - either SQLITE_HCT_JOURNAL_MODE_FOLLOWER or
**   SQLITE_HCT_JOURNAL_MODE_LEADER.
**
** iSnapshot:
**   This is meaningful in FOLLOWER mode only.
**
**   This is set to a CID value for which it and all prior transactions are
**   committed. It may be written by any client using an atomic CAS operation,
**   but may only be increased, never decreased. No transaction with a CID
**   greater than (iSnapshot + HCT_MAX_LEADING_WRITE) may be started - 
**   iSnapshot must be increased first.
**
** nCommit:
**   Size of aCommit[] array.
**
** aCommit:
**   This array is only populated if the object is in FOLLOWER mode.
**
**   Say the size of the array is N (actually HctJrnlServer.nCommit). Then,
**   when transaction X is committed, slot aCommit[X % N] is set to X. Or,
**   if transaction X is committed but no snapshot is valid until Y (for Y>X),
**   then instead slot aCommit[X % N] is set to Y.
*/
struct HctJrnlServer {
  u64 iSchemaCid;
  int eMode;
  u64 iSnapshot;
  int nSchemaVersionIncr;
  int nCommit;
  u64 *aCommit;                   /* Array of size nCommit */
};

struct HctJrnlPendingHook {
  u64 iCid;
  u64 iSCid;
  HctBuffer data;
  HctBuffer schema;
};

/*
** There is one instance of this structure for each database handle (HBtree*)
** open on a replication-enabled hctree database.
**
** eInWrite:
**   Set to true while the database connection is in a call to
**   sqlite3_hct_journal_write().
*/
struct HctJournal {
  u64 iJrnlRoot;                  /* Root page of journal table */
  u64 iBaseRoot;                  /* Root page of base table */
  int eInWrite;
  u64 iWriteTid;
  u64 iWriteCid;
  u64 iRollbackSnapshot;
  HctDatabase *pDb;
  HctTree *pTree;
  HctJrnlServer *pServer;
  HctJrnlPendingHook pending;
};

#define HCT_JOURNAL_NONE          0
#define HCT_JOURNAL_INWRITE       1
#define HCT_JOURNAL_INROLLBACK    2

static void hctJournalSetDbError(
  sqlite3 *db,                    /* Database on which to set error */
  int rc,                         /* Error code */
  const char *zFormat, ...        /* Printf() error string and arguments */
){
  char *zErr = 0;
  sqlite3_mutex_enter( sqlite3_db_mutex(db) );
  if( zFormat ){
    va_list ap;
    va_start(ap, zFormat);
    zErr = sqlite3_vmprintf(zFormat, ap);
    va_end(ap);
  }
  if( zErr ){
    sqlite3ErrorWithMsg(db, rc, "%s", zErr);
    sqlite3_free(zErr);
  }else{
    sqlite3ErrorWithMsg(db, rc, 0, 0);
  }
  sqlite3_mutex_leave( sqlite3_db_mutex(db) );
}

/*
** Initialize the main database for replication.
*/
int sqlite3_hct_journal_init(sqlite3 *db){
  const char *zTest1 = "PRAGMA hct_ndbfile";
  const char *zTest2 = "SELECT 1 WHERE (SELECT count(*) FROM sqlite_schema)=0";
  sqlite3_stmt *pTest = 0;
  int rc = SQLITE_OK;

  /* Test that there is not already an open transaction on this database. */
  if( sqlite3_get_autocommit(db)==0 ){
    hctJournalSetDbError(db, SQLITE_ERROR, "open transaction on database");
    return SQLITE_ERROR;
  }

  /* Test that the main db really is an hct database. Leave rc set to 
  ** something other than SQLITE_OK and an error message in the database
  ** handle if it is not. */
  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(db, zTest1, -1, &pTest, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_step(pTest);
    sqlite3_finalize(pTest);
    if( rc==SQLITE_DONE ){
      hctJournalSetDbError(db, SQLITE_ERROR, "not an hct database");
    }else if( rc==SQLITE_ROW ){
      rc = SQLITE_OK;
    }
  }

  /* Open a transaction on the db */
  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(db, "BEGIN", 0, 0, 0);
  }

  /* Test that the main db really is empty */
  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(db, zTest2, -1, &pTest, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_step(pTest);
    sqlite3_finalize(pTest);
    if( rc==SQLITE_DONE ){
      hctJournalSetDbError(db, SQLITE_ERROR, "not an empty database");
      rc = SQLITE_ERROR;
    }else if( rc==SQLITE_ROW ){
      rc = SQLITE_OK;
    }
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(db, 
        "PRAGMA writable_schema = 1;"
        HCT_JOURNAL_SCHEMA ";"
        HCT_BASELINE_SCHEMA ";"
        "INSERT INTO sqlite_hct_baseline VALUES(6, 0, zeroblob(16));"
        "PRAGMA writable_schema = 0;"
        ,0 ,0 ,0
    );
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
  }
  if( rc!=SQLITE_OK ){
    char *zErr = sqlite3_mprintf("%s", sqlite3_errmsg(db));
    sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
    hctJournalSetDbError(db, rc, "%s", zErr);
    sqlite3_free(zErr);
  }else{
    rc = sqlite3HctDetectJournals(db);
  }

  return rc;
}

/*
** Register a custom validation callback with the database handle.
*/
int sqlite3_hct_journal_hook(
  sqlite3 *db,
  void *pArg,
  int(*xValidate)(
    void *pCopyOfArg,
    sqlite3_int64 iCid,
    const char *zSchema,
    const void *pData, int nData,
    sqlite3_int64 iSchemaCid
  )
){
  db->xValidate = xValidate;
  db->pValidateArg = pArg;
  return SQLITE_OK;
}

/*
** Value iVal is to be stored as an integer in an SQLite record. This 
** function returns the number of bytes that it will use for storage.
*/
static int hctJrnlIntSize(u64 iVal){
#define MAX_6BYTE ((((i64)0x00008000)<<32)-1)
  if( iVal<=127 ) return 1;
  if( iVal<=32767 ) return 2;
  if( iVal<=8388607 ) return 3;
  if( iVal<=2147483647 ) return 4;
  if( iVal<=MAX_6BYTE ) return 6;
  return 8;
}

/*
** Store an (nByte*8) bit big-endian integer, value iVal, in buffer a[].
*/
static void hctJrnlIntPut(u8 *a, u64 iVal, int nByte){
  int i;
  for(i=1; i<=nByte; i++){
    a[nByte-i] = (iVal & 0xFF);
    iVal = (iVal >> 8);
  }
}

/*
** Return the byte value that should be stored in the SQLite record
** header for an nSize byte integer field.
*/
static u8 hctJrnlIntHdr(int nSize){
  if( nSize==8 ) return 6;
  if( nSize==6 ) return 5;
  return nSize;
}

/*
** Compose an SQLite record suitable for the sqlite_hct_journal table.
*/
static u8 *hctJrnlComposeRecord(
  u64 iCid,
  const char *zSchema,
  const u8 *pData, int nData,
  u64 iSchemaCid,
  u64 iTid,
  u64 iValidCid,
  int *pnRec
){
  u8 *pRec = 0;
  int nRec = 0;
  int nHdr = 0;
  int nBody = 0;
  int nSchema = 0;                /* Length of zSchema, in bytes */
  int nTidByte = 0;
  int nSchemaCidByte = 0;
  int nValidCidByte = 0;
  u8 aHash[SQLITE_HCT_JOURNAL_HASHSIZE];

  nSchema = sqlite3Strlen30(zSchema);
  nTidByte = hctJrnlIntSize(iTid);
  nSchemaCidByte = hctJrnlIntSize(iSchemaCid);
  nValidCidByte = hctJrnlIntSize(iValidCid);

  sqlite3_hct_journal_hashentry(
      aHash, iCid, zSchema, pData, nData, iSchemaCid
  );

  /* First figure out how large the eventual record will be */
  nHdr = 1                                     /* size of header varint */
       + 1                                     /* "cid" - always NULL */
       + sqlite3VarintLen((nSchema * 2) + 13)  /* "schema" - TEXT */
       + sqlite3VarintLen((nData * 2) + 12)    /* "data" - BLOB */
       + 1                                     /* "schemacid" - INTEGER */
       + 1                                     /* "hash" - BLOB */
       + 1                                     /* "tid" - INTEGER */
       + 1;                                    /* "validcid" - INTEGER */

  nBody = 0                                    /* "cid" - always NULL */
       + nSchema                               /* "schema" - TEXT */
       + nData                                 /* "data" - BLOB */
       + nSchemaCidByte                        /* "schemacid" - INTEGER */
       + SQLITE_HCT_JOURNAL_HASHSIZE           /* "hash" - BLOB */
       + nTidByte                              /* "tid" - INTEGER */
       + nValidCidByte;                        /* "validcid" - INTEGER */

  nRec = nBody+nHdr;
  pRec = (u8*)sqlite3_malloc(nRec);
  if( pRec ){
    u8 *pHdr = pRec;
    u8 *pBody = &pRec[nHdr];

    *pHdr++ = (u8)nHdr;           /* size-of-header varint */
    *pHdr++ = 0x00;               /* "cid" - NULL */

    /* "schema" field - TEXT */
    pHdr += sqlite3PutVarint(pHdr, (nSchema*2) + 13);
    if( nSchema>0 ){
      memcpy(pBody, zSchema, nSchema);
      pBody += nSchema;
    }

    /* "data" field - BLOB */
    pHdr += sqlite3PutVarint(pHdr, (nData*2) + 12);
    if( nData>0 ){
      memcpy(pBody, pData, nData);
      pBody += nData;
    }

    /* "schemacid" field - INTEGER */
    *pHdr++ = hctJrnlIntHdr(nSchemaCidByte);
    hctJrnlIntPut(pBody, iSchemaCid, nSchemaCidByte);
    pBody += nSchemaCidByte;

    /* "hash" field - SQLITE_HCT_JOURNAL_HASHSIZE byte BLOB */
    *pHdr++ = (u8)((SQLITE_HCT_JOURNAL_HASHSIZE * 2) + 12);
    memcpy(pBody, aHash, SQLITE_HCT_JOURNAL_HASHSIZE);
    pBody += SQLITE_HCT_JOURNAL_HASHSIZE;

    /* "tid" field - INTEGER */
    *pHdr++ = hctJrnlIntHdr(nTidByte);
    hctJrnlIntPut(pBody, iTid, nTidByte);
    pBody += nTidByte;

    /* "validcid" field - INTEGER */
    *pHdr++ = hctJrnlIntHdr(nValidCidByte);
    hctJrnlIntPut(pBody, iValidCid, nValidCidByte);
    pBody += nValidCidByte;

    assert( pHdr==&pRec[nHdr] );
    assert( pBody==&pRec[nRec] );
  }else{
    nRec = 0;
  }

  *pnRec = nRec;
  return pRec;
}

typedef struct JrnlCtx JrnlCtx;
struct JrnlCtx {
  Schema *pSchema;
  HctTree *pTree;
  HctBuffer *pBuf;
  HctBuffer *pSchemaSql;
};

typedef struct JrnlTree JrnlTree;
struct JrnlTree {
  const char *zName;
};

static int hctJrnlFindTree(Schema *pSchema, u32 iRoot, JrnlTree *pJTree){
  HashElem *k;
  if( iRoot==1 ) return 0;
  for(k=sqliteHashFirst(&pSchema->tblHash); k; k=sqliteHashNext(k)){
    Table *pTab = (Table*)sqliteHashData(k);
    if( pTab->tnum==iRoot ){
      pJTree->zName = pTab->zName;
      return 1;
    }
  }
  return 0;
}

static void hctJrnlRecordPrefix(
  HctBuffer *pBuf, 
  int nData,                      /* Size of buffer aData[] in bytes */
  const u8 *aData,                /* Buffer containing SQLite record */
  int nField                      /* Number of prefix fields requested */
){
  int iHdr = 0;
  int iBody = 0;
  int ii = 0;
  int szHdr = 0;                  /* Size of output header */
  int szBody = 0;                 /* Size of output record body */
  u8 *aHdrOut = 0;
  u8 *aBodyOut = 0;

  iHdr = getVarint32(aData, iBody);

  /* Figure out the aggregate sizes of the header and body fields for the
  ** required number of prefix fields. */
  for(ii=0; ii<nField; ii++){
    u32 t;
    szHdr += getVarint32(&aData[iHdr+szHdr], t);
    szBody += sqlite3VdbeSerialTypeLen(t);
  }

  /* Determine the size in bytes of header of the output record. This is
  ** the value that should be encoded as a varint at the very start of
  ** the output record.  */
  if( szHdr<=126 ){
    szHdr++;
  }else if( szHdr>126 ){
    int nVarint = sqlite3VarintLen(szHdr);
    szHdr += nVarint;
    if( sqlite3VarintLen(szHdr)!=nVarint ) szHdr++;
  }

  /* Size of record field */
  pBuf->nBuf += sqlite3PutVarint(&pBuf->aBuf[pBuf->nBuf], szHdr+szBody);

  aHdrOut = &pBuf->aBuf[pBuf->nBuf];
  aBodyOut = &aHdrOut[szHdr];

  /* Write the size-of-header field for the output record */
  aHdrOut += sqlite3PutVarint(aHdrOut, szHdr);

  /* Write the other fields to both the header and body of the output record */
  for(ii=0; ii<nField; ii++){
    u32 t = 0;
    u32 nBody = 0;
    iHdr += getVarint32(&aData[iHdr], t);
    nBody = sqlite3VdbeSerialTypeLen(t);
    aHdrOut += sqlite3PutVarint(aHdrOut, t);
    if( nBody>0 ){
      memcpy(aBodyOut, &aData[iBody], nBody);
      iBody += nBody;
      aBodyOut += nBody;
    }
  }

  pBuf->nBuf = (aBodyOut - pBuf->aBuf);
}

static int hctBufferExtend(HctBuffer *pBuf, int nExtend){
  i64 nDesire = pBuf->nBuf + nExtend;
  if( pBuf->nAlloc<nDesire ){
    i64 nNew = 256;
    while( nNew<nDesire ) nNew = nNew*4;
    if( sqlite3HctBufferGrow(pBuf, nNew) ) return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

static int hctBufferAppend(HctBuffer *pBuf, const char *zFmt, ...){
  va_list ap;
  char *zApp = 0;
  int nApp = 0;

  va_start(ap, zFmt);
  zApp = sqlite3_vmprintf(zFmt, ap);
  va_end(ap);

  nApp = sqlite3Strlen30(zApp);
  if( hctBufferExtend(pBuf, nApp+1) ) return SQLITE_NOMEM;
  memcpy(&pBuf->aBuf[pBuf->nBuf], zApp, nApp+1);
  pBuf->nBuf += nApp;
  sqlite3_free(zApp);
  return SQLITE_OK;
}


static int hctJrnlLogTree(void *pCtx, u32 iRoot, KeyInfo *pKeyInfo){
  int rc = SQLITE_OK;
  JrnlCtx *pJrnl = (JrnlCtx*)pCtx;
  HctBuffer *pBuf = pJrnl->pBuf;

  if( iRoot==HCT_TREE_SCHEMAOP_ROOT ){
    HctTreeCsr *pCsr = 0;
    rc = sqlite3HctTreeCsrOpen(pJrnl->pTree, iRoot, &pCsr);
    if( rc==SQLITE_OK ){
      for(rc=sqlite3HctTreeCsrFirst(pCsr);
          rc==SQLITE_OK && sqlite3HctTreeCsrEof(pCsr)==0;
          rc=sqlite3HctTreeCsrNext(pCsr)
      ){
        int nData = 0;
        const u8 *aData = 0;
        sqlite3HctTreeCsrData(pCsr, &nData, &aData);
        rc = hctBufferAppend(pJrnl->pSchemaSql, "%s%.*s", 
            (pJrnl->pSchemaSql->nBuf>0 ? ";" : ""), nData, (const char*)aData
        );
      }
      sqlite3HctTreeCsrClose(pCsr);
    }
  }else{
    JrnlTree jrnltree;
    memset(&jrnltree, 0, sizeof(jrnltree));
    if( hctJrnlFindTree(pJrnl->pSchema, iRoot, &jrnltree) ){
      int nName = sqlite3Strlen30(jrnltree.zName);
  
      rc = hctBufferExtend(pBuf, 1+nName+1);
      if( rc==SQLITE_OK ){
        HctTreeCsr *pCsr = 0;
  
        pBuf->aBuf[pBuf->nBuf++] = 'T';
        memcpy(&pBuf->aBuf[pBuf->nBuf], jrnltree.zName, nName+1);
        pBuf->nBuf += nName+1;
        rc = sqlite3HctTreeCsrOpen(pJrnl->pTree, iRoot, &pCsr);
  
        if( rc==SQLITE_OK ){
          for(rc=sqlite3HctTreeCsrFirst(pCsr);
              rc==SQLITE_OK && sqlite3HctTreeCsrEof(pCsr)==0;
              rc=sqlite3HctTreeCsrNext(pCsr)
          ){
            i64 iKey = 0;
            int nData = 0;
            const u8 *aData = 0;
            int bDel = 0;
  
            sqlite3HctTreeCsrKey(pCsr, &iKey);
            sqlite3HctTreeCsrData(pCsr, &nData, &aData);
            bDel = sqlite3HctTreeCsrIsDelete(pCsr);
  
            rc = hctBufferExtend(pBuf, 1+9+9+nData);
            if( rc!=SQLITE_OK ) break;
  
            if( pKeyInfo==0 ){
              pBuf->aBuf[pBuf->nBuf++] = bDel ? 'd' : 'i';
              pBuf->nBuf += sqlite3PutVarint(&pBuf->aBuf[pBuf->nBuf], iKey);
            }else{
              pBuf->aBuf[pBuf->nBuf++] = bDel ? 'D' : 'I';
              if( bDel ){
                hctJrnlRecordPrefix(pBuf, nData, aData, pKeyInfo->nUniqField);
              }
            }
            if( bDel==0 ){
              pBuf->nBuf += sqlite3PutVarint(&pBuf->aBuf[pBuf->nBuf], nData);
              memcpy(&pBuf->aBuf[pBuf->nBuf], aData, nData);
              pBuf->nBuf += nData;
            }
          }
        }
  
        sqlite3HctTreeCsrClose(pCsr);
      }
    }
  }

  return rc;
}

static int hctJrnlWriteRecord(
  HctJournal *pJrnl,
  u64 iCid,
  const char *zSchema,
  const void *pData, int nData,
  u64 iSchemaCid,
  u64 iTid
){
  int rc = SQLITE_OK;
  u8 *pRec = 0;
  int nRec = 0;

  pRec = hctJrnlComposeRecord(
      iCid, zSchema, pData, nData, iSchemaCid, iTid, 0, &nRec
  );
  if( pRec==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    int nRetry = 0;
    do {
      nRetry = 0;
      rc = sqlite3HctDbInsert(
          pJrnl->pDb, (u32)pJrnl->iJrnlRoot, 0, iCid, 0, nRec, pRec, &nRetry
      );
      if( rc!=SQLITE_OK ) break;
      assert( nRetry==0 || nRetry==1 );
      if( nRetry==0 ){
        rc = sqlite3HctDbInsertFlush(pJrnl->pDb, &nRetry);
        if( rc!=SQLITE_OK ) break;
      }
    }while( nRetry );
  }
  sqlite3_free(pRec);

  return rc;
}

int sqlite3HctJrnlWriteEmpty(
  HctJournal *pJrnl, 
  u64 iCid, 
  u64 iTid, 
  sqlite3 *db                     /* If non-NULL, invoke custom validation */
){
  int rc = SQLITE_OK;
  if( pJrnl->eInWrite==HCT_JOURNAL_NONE ){
    rc = hctJrnlWriteRecord(pJrnl, iCid, "", 0, 0, 0, iTid);

    /* If argument db is not NULL and there is a custom validation hook
    ** configured, invoke it now. This is just to propagate the empty
    ** transaction to any follower databases, not to actually validate
    ** an empty transaction - the return code is ignored.  */
    if( rc==SQLITE_OK && db && db->xValidate ){
      // (void)db->xValidate(db->pValidateArg, iCid, "", 0, 0, 0);
      pJrnl->pending.iCid = iCid;
      pJrnl->pending.iSCid = 0;
      pJrnl->pending.data.nBuf = 0;
      pJrnl->pending.schema.nBuf = 0;
    }
  }
  return rc;
}

void sqlite3HctJrnlInvokeHook(HctJournal *pJrnl, sqlite3 *db){
  if( pJrnl ){
    HctJrnlPendingHook *pPending = &pJrnl->pending;
    if( pPending->iCid>0 ){
      if( db->xValidate ){
        const char *zSchema = "";
        if( pJrnl->pending.schema.nBuf>0 ){
          zSchema = (const char*)pJrnl->pending.schema.aBuf;
        }
        (void)db->xValidate(
            db->pValidateArg, pPending->iCid, 
            zSchema, pPending->data.aBuf, pPending->data.nBuf, 
            pPending->iSCid
            );
      }

      pPending->iCid = 0;
    }
  }
}

int sqlite3HctJrnlLog(
  HctJournal *pJrnl,
  sqlite3 *db,
  Schema *pSchema,
  u64 iCid,
  u64 iTid,
  int *pbValidateCalled
){
  int rc = SQLITE_OK;
  JrnlCtx jrnlctx;
  const char *zSchema = "";
  u64 iSchemaCid = HctAtomicLoad(&pJrnl->pServer->iSchemaCid);

  assert( *pbValidateCalled==0 );
  if( pJrnl->eInWrite!=HCT_JOURNAL_NONE ) return SQLITE_OK;

  memset(&jrnlctx, 0, sizeof(jrnlctx));
  jrnlctx.pSchema = pSchema;
  jrnlctx.pTree = pJrnl->pTree;
  jrnlctx.pBuf = &pJrnl->pending.data;
  jrnlctx.pSchemaSql = &pJrnl->pending.schema;

  jrnlctx.pBuf->nBuf = 0;
  jrnlctx.pSchemaSql->nBuf = 0;

  rc = sqlite3HctTreeForeach(pJrnl->pTree, 1, (void*)&jrnlctx, hctJrnlLogTree);
  if( jrnlctx.pSchemaSql->nBuf ){
    zSchema =(const char*)jrnlctx.pSchemaSql->aBuf;
  }

  if( rc==SQLITE_OK ){
    rc = hctJrnlWriteRecord(pJrnl, iCid, zSchema, 
        jrnlctx.pBuf->aBuf, jrnlctx.pBuf->nBuf, iSchemaCid, iTid
    );
  }

  /* If one is registered, invoke the validation hook */
  if( rc==SQLITE_OK && db->xValidate ){
#if 0
    int res = db->xValidate(db->pValidateArg, iCid, zSchema, 
        jrnlctx.buf.aBuf, jrnlctx.buf.nBuf, iSchemaCid
    );
    if( res!=0 ){
      rc = SQLITE_BUSY_SNAPSHOT;
    }
    *pbValidateCalled = 1;
#endif
    pJrnl->pending.iCid = iCid;
    pJrnl->pending.iSCid = iSchemaCid;
  }

  if( zSchema[0] && rc==SQLITE_OK ){
    HctAtomicStore(&pJrnl->pServer->iSchemaCid, iCid);
  }

  return rc;
}

static void hctJrnlDelServer(void *p){
  if( p ){
    HctJrnlServer *pServer = (HctJrnlServer*)p;
    sqlite3_free(pServer->aCommit);
    sqlite3_free(pServer);
  }
}

typedef struct HctJournalRecord HctJournalRecord;
struct HctJournalRecord {
  i64 iCid;
  const char *zSchema; int nSchema;
  const void *pData; int nData;
  i64 iSchemaCid;
  const void *pHash;
  i64 iTid;
  i64 iValidCid;
};

/*
** Structure containing values read from the sqlite_hct_baseline table.
*/
typedef struct HctBaselineRecord HctBaselineRecord;
struct HctBaselineRecord {
  i64 iCid;
  u8 aHash[SQLITE_HCT_JOURNAL_HASHSIZE];
  i64 iSchemaCid;
};


typedef struct HctRecordReader HctRecordReader;
struct HctRecordReader {
  const u8 *aRec;
  int nRec;
  int nHdr;
  const u8 *pHdr;
  const u8 *pBody;
};

static void hctJrnlReadInit(
  HctRecordReader *p,
  int nRec,
  const u8 *aRec
){
  memset(p, 0, sizeof(*p));
  p->aRec = aRec;
  p->nRec = nRec;
  p->pHdr = p->aRec + getVarint32(aRec, p->nHdr);
  p->pBody = &p->aRec[p->nHdr];
}

static const u8 *hctJrnlReadBlobText(
  int *pRc, 
  HctRecordReader *p, 
  int bText, 
  int *pnData
){
  const u8 *pRet = 0;
  if( *pRc==SQLITE_OK ){
    u64 iType = 0;
    p->pHdr += sqlite3GetVarint(p->pHdr, &iType);
    if( iType<12 || (iType % 2)!=bText ){
      *pRc = SQLITE_CORRUPT_BKPT;
    }else{
      *pnData = (iType - 12) / 2;
      pRet = p->pBody;
      p->pBody += (*pnData);
    }
  }
  return pRet;
}

static const char *hctJrnlReadText(
  int *pRc, 
  HctRecordReader *p, 
  int *pnText
){
  return (const char*)hctJrnlReadBlobText(pRc, p, 1, pnText);
}
static const u8 *hctJrnlReadBlob(
  int *pRc, 
  HctRecordReader *p, 
  int *pnText
){
  return hctJrnlReadBlobText(pRc, p, 0, pnText);
}

static i64 hctJrnlReadInteger(int *pRc, HctRecordReader *p){
  i64 iRet = 0;
  if( *pRc==SQLITE_OK ){
    u64 iType = 0;
    p->pHdr += sqlite3GetVarint(p->pHdr, &iType);
    switch( iType ){
      case 1:
        iRet = p->pBody[0];
        p->pBody++;
        break;
      case 2:
        iRet = ((u64)p->pBody[0] << 8) 
             + ((u64)p->pBody[1] << 0);
        p->pBody += 2;
        break;
      case 3:
        iRet = ((u64)p->pBody[0] << 16)
             + ((u64)p->pBody[1] << 8) 
             + ((u64)p->pBody[2] << 0);
        p->pBody += 3;
        break;
      case 4:
        iRet = ((u64)p->pBody[0] << 24)
             + ((u64)p->pBody[1] << 16) 
             + ((u64)p->pBody[2] << 8) 
             + ((u64)p->pBody[3] << 0);
        p->pBody += 4;
        break;
      case 5:
        iRet = ((u64)p->pBody[0] << 40)
             + ((u64)p->pBody[1] << 32) 
             + ((u64)p->pBody[2] << 24) 
             + ((u64)p->pBody[3] << 16) 
             + ((u64)p->pBody[4] << 8) 
             + ((u64)p->pBody[5] << 0);
        p->pBody += 6;
        break;
      case 6:
        iRet = ((u64)p->pBody[0] << 56)
             + ((u64)p->pBody[1] << 48) 
             + ((u64)p->pBody[2] << 40) 
             + ((u64)p->pBody[3] << 32) 
             + ((u64)p->pBody[4] << 24) 
             + ((u64)p->pBody[5] << 16) 
             + ((u64)p->pBody[6] << 8) 
             + ((u64)p->pBody[7] << 0);
        p->pBody += 6;
        break;
      case 8:
        iRet = 0;
        break;
      case 9:
        iRet = 1;
        break;
      default:
        *pRc = SQLITE_CORRUPT_BKPT;
        break;
    }
  }

  return iRet;
}

static void hctJrnlReadHash(
  int *pRc,                       /* IN/OUT: Error code */
  HctRecordReader *p,             /* Record reader */
  u8 *aHash                       /* Pointer to buffer to populate */
){
  int nHash = 0;
  const u8 *a = 0;
  a = hctJrnlReadBlob(pRc, p, &nHash);
  if( *pRc==SQLITE_OK && nHash!=SQLITE_HCT_JOURNAL_HASHSIZE ){
    *pRc = SQLITE_CORRUPT_BKPT;
  }
  if( *pRc==SQLITE_OK ){
    memcpy(aHash, a, SQLITE_HCT_JOURNAL_HASHSIZE);
  }
}

static int hctJrnlReadJournalRecord(HctDbCsr *pCsr, HctJournalRecord *pRec){
  int rc = SQLITE_OK;
  int nData = 0;
  const u8 *aData = 0;

  memset(pRec, 0, sizeof(*pRec));

  sqlite3HctDbCsrKey(pCsr, (i64*)&pRec->iCid);
  rc = sqlite3HctDbCsrData(pCsr, &nData, &aData);
  if( rc==SQLITE_OK ){
    int nHash = 0;
    HctRecordReader rdr;
    hctJrnlReadInit(&rdr, nData, aData);

    /* "cid" field - always NULL */
    if( *rdr.pHdr++!=0 ) return SQLITE_CORRUPT_BKPT;

    /* "schema" field - always TEXT. */
    pRec->zSchema = hctJrnlReadText(&rc, &rdr, &pRec->nSchema);

    /* "data" field - always BLOB */
    pRec->pData = hctJrnlReadBlob(&rc, &rdr, &pRec->nData);
    
    /* "schemacid" field - always INTEGER */
    pRec->iSchemaCid = hctJrnlReadInteger(&rc, &rdr);

    /* "hash" field - SQLITE_HCT_JOURNAL_HASHSIZE byte BLOB */
    pRec->pHash = (const void*)hctJrnlReadBlob(&rc, &rdr, &nHash);
    if( nHash!=SQLITE_HCT_JOURNAL_HASHSIZE ) rc = SQLITE_CORRUPT_BKPT;

    /* "tid" field - an INTEGER */
    pRec->iTid = hctJrnlReadInteger(&rc, &rdr);

    /* "valid_cid" field - an INTEGER */
    pRec->iValidCid = hctJrnlReadInteger(&rc, &rdr);
  }
  return rc;
}

/*
** Read the contents of the sqlite_hct_baseline table into structure
** (*pRec). Return SQLITE_OK if successful, or an SQLite error code
** otherwise.
*/
static int hctJrnlReadBaseline(
  HctJournal *pJrnl,              /* Database to read from */
  HctBaselineRecord *pRec         /* Populate this structure before returning */
){
  HctDbCsr *pCsr = 0;
  int rc = SQLITE_OK;

  memset(pRec, 0, sizeof(HctBaselineRecord));

  /* Open a cursor on the baseline table */
  rc = sqlite3HctDbCsrOpen(pJrnl->pDb, 0, (u32)pJrnl->iBaseRoot, &pCsr);

  /* Move the cursor to the first record in the table. */
  if( rc==SQLITE_OK ){
    rc = sqlite3HctDbCsrFirst(pCsr);
  }
  if( rc==SQLITE_OK && sqlite3HctDbCsrEof(pCsr) ){
    rc = SQLITE_CORRUPT_BKPT;
  }

  if( rc==SQLITE_OK ){
    int nData = 0;
    const u8 *aData = 0;

    rc = sqlite3HctDbCsrData(pCsr, &nData, &aData);
    if( rc==SQLITE_OK ){
      HctRecordReader rdr;
      hctJrnlReadInit(&rdr, nData, aData);

      /* "cid" field - an INTEGER */
      pRec->iCid = hctJrnlReadInteger(&rc, &rdr);

      /* "schemacid" field - an INTEGER */
      pRec->iSchemaCid = hctJrnlReadInteger(&rc, &rdr);

      /* "hash" field - SQLITE_HCT_JOURNAL_HASHSIZE byte BLOB */
      hctJrnlReadHash(&rc, &rdr, pRec->aHash);
    }
  }
  sqlite3HctDbCsrClose(pCsr);

  return rc;
}

static int hctJrnlGetJrnlShape( 
  sqlite3 *db,
  i64 *piLast,                    /* Out: Last entry in journal */
  i64 *piLastCont                 /* Out: Last contiguous entry in journal */
){
  const char *z1 = "SELECT max(cid) FROM sqlite_hct_journal";
  const char *z2 = "SELECT cid FROM sqlite_hct_journal ORDER BY 1 DESC";

  int rc = SQLITE_OK;
  sqlite3_stmt *pStmt = 0;
  i64 iLast = 0;
  i64 iLastCont = 0;

  rc = sqlite3_prepare_v2(db, z1, -1, &pStmt, 0);
  if( rc==SQLITE_OK ){
    if( SQLITE_ROW==sqlite3_step(pStmt) ){
      iLast = sqlite3_column_int64(pStmt, 0);
    }
    rc = sqlite3_finalize(pStmt);
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(db, z2, -1, &pStmt, 0);
  }
  if( rc==SQLITE_OK ){
    i64 iPrev = iLast;
    iLastCont = iLast;
    while( sqlite3_step(pStmt)==SQLITE_ROW ){
      i64 iThis = sqlite3_column_int64(pStmt, 0);
      if( iThis!=iPrev-1 ){
        iLastCont = iThis;
      }
      if( (iLast-iThis)>HCT_MAX_LEADING_WRITE*2 ) break;
      iPrev = iThis;
    }
    rc = sqlite3_finalize(pStmt);
  }

  *piLast = iLast;
  *piLastCont = iLastCont;
  return rc;
}

static sqlite3_stmt *hctPreparePrintf(
  int *pRc, 
  sqlite3 *db, 
  const char *zFmt, ...
){
  sqlite3_stmt *pRet = 0;
  va_list ap;
  char *zSql = 0;

  va_start(ap, zFmt);
  zSql = sqlite3_vmprintf(zFmt, ap);
  va_end(ap);

  if( *pRc==SQLITE_OK ){
    if( zSql==0 ){
      *pRc = SQLITE_NOMEM;
    }else{
      *pRc = sqlite3_prepare_v2(db, zSql, -1, &pRet, 0);
    }
  }
  sqlite3_free(zSql);
  return pRet;
}

/*
** Iterator for reading a blob from the "data" column of a journal entry.
*/
typedef struct HctDataReader HctDataReader;
struct HctDataReader {
  const u8 *aData;
  int nData;
  int iData;
  
  int bEof;
  char eType;

  /* Valid for all values of eType */
  const char *zTab;

  /* For eType==HCT_TYPE_INSERT_ROWID, HCT_TYPE_DELETE_ROWID */
  i64 iRowid;

  /* For eType==HCT_TYPE_INSERT_ROWID */
  int nRecord;
  const u8 *aRecord;
};

#define HCT_TYPE_TABLE        'T'
#define HCT_TYPE_INSERT_ROWID 'i'
#define HCT_TYPE_DELETE_ROWID 'd'

static int hctDataReaderNext(HctDataReader *p){
  if( p->iData>=p->nData ){
    p->bEof = 1;
  }else{
    p->eType = (char)(p->aData[p->iData++]);
    switch( p->eType ){
      case 'T': {
        p->zTab = (const char*)&p->aData[p->iData];
        p->iData += sqlite3Strlen30(p->zTab) + 1;
        break;
      }

      case 'd': {
        p->iData += sqlite3GetVarint(&p->aData[p->iData], (u64*)&p->iRowid);
        break;
      }

      case 'i': {
        p->iData += sqlite3GetVarint(&p->aData[p->iData], (u64*)&p->iRowid);
        p->iData += getVarint32(&p->aData[p->iData], p->nRecord);
        p->aRecord = &p->aData[p->iData];
        p->iData += p->nRecord;
        break;
      }

      default: {
        return SQLITE_CORRUPT_BKPT;
      }
    }
  }

  return SQLITE_OK;
}

/*
** Initialize an HctDataReader object to iterate through the nData byte 
** 'data' blob in buffer pData. Leave the iterator pointing at the first
** entry in the blob.
*/
static int hctDataReaderInit(const void *pData, int nData, HctDataReader *pRdr){
  memset(pRdr, 0, sizeof(*pRdr));
  pRdr->aData = (const u8*)pData;
  pRdr->nData = nData;
  return hctDataReaderNext(pRdr);
}

int sqlite3HctJrnlSavePhysical( 
  sqlite3 *db,
  HctJournal *pJrnl, 
  int (*xSave)(void*, i64 iPhys), 
  void *pSave
){
  const char *zSql = "SELECT data FROM sqlite_hct_journal WHERE cid>?";
  int rc = SQLITE_OK;
  i64 iLast = 0;
  i64 iLastCont = 0;
  sqlite3_stmt *pStmt = 0;

  rc = hctJrnlGetJrnlShape(db, &iLast, &iLastCont);
  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  }
  if( rc==SQLITE_OK ){
    sqlite3_bind_int64(pStmt, 1, iLastCont);
    while( rc==SQLITE_OK && sqlite3_step(pStmt)==SQLITE_ROW ){
      const void *pData = sqlite3_column_blob(pStmt, 0);
      int nData = sqlite3_column_bytes(pStmt, 0);
      sqlite3_stmt *pQuery = 0;
      HctDataReader rdr;

      sqlite3HctDbSetSavePhysical(pJrnl->pDb, xSave, pSave);
      for(rc=hctDataReaderInit(pData, nData, &rdr);
          rc==SQLITE_OK && rdr.bEof==0;
          rc=hctDataReaderNext(&rdr)
      ){
        switch( rdr.eType ){
          case HCT_TYPE_TABLE: {
            rc = sqlite3_finalize(pQuery);
            pQuery = hctPreparePrintf(
                &rc, db, "SELECT * FROM %Q WHERE _rowid_=?", rdr.zTab
            );
            break;
          }

          case HCT_TYPE_INSERT_ROWID:
          case HCT_TYPE_DELETE_ROWID: {
            sqlite3_bind_int64(pQuery, 1, rdr.iRowid);
            sqlite3_step(pQuery);
            rc = sqlite3_reset(pQuery);
            break;
          }

          default: assert( 0 );
        }
        if( rc ) break;
      }
      sqlite3HctDbSetSavePhysical(pJrnl->pDb, 0, 0);
      sqlite3_finalize(pQuery);
    }
    rc = sqlite3_finalize(pStmt);
  }

  return rc;
}

/*
** Do special recovery (startup) processing for replication-enabled databases.
** This function is called during stage 1 recovery - after any log files have
** been processed (and the database schema + contents restored), but before the
** free-page-lists are recovered.
*/
int sqlite3HctJrnlRecovery(HctJournal *pJrnl, HctDatabase *pDb){
  HctBaselineRecord base;         /* sqlite_hct_baseline data */
  HctJrnlServer *pServer = 0;
  HctFile *pFile = sqlite3HctDbFile(pDb);
  int rc = SQLITE_OK;
  HctDbCsr *pCsr = 0;

  i64 iMaxCid = 0;                
  i64 iSchemaCid = 0;

  /* Read the contents of the sqlite_hct_baseline table. */
  rc = hctJrnlReadBaseline(pJrnl, &base);

  /* Allocate the new HctJrnlServer structure */
  pServer = (HctJrnlServer*)sqlite3HctMalloc(&rc, sizeof(HctJrnlServer));

  /* Read the last record of the sqlite_hct_journal table. Specifically,
  ** the value of fields "cid" and "schema_version". Store these values
  ** in stack variables iMaxCid and aSchema, respectively.  Or, if the
  ** sqlite_hct_journal table is empty, populate iMaxCid and aSchema[] with
  ** values from the baseline table.  */
  if( rc==SQLITE_OK ){
    rc = sqlite3HctDbCsrOpen(pDb, 0, (u32)pJrnl->iJrnlRoot, &pCsr);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3HctDbCsrLast(pCsr);
  }
  if( rc==SQLITE_OK ){
    if( sqlite3HctDbCsrEof(pCsr)==0 ){
      HctJournalRecord rec;
      rc = hctJrnlReadJournalRecord(pCsr, &rec);
      if( rc==SQLITE_OK ){
        iMaxCid = rec.iCid;
        iSchemaCid = (rec.zSchema[0] ? rec.iCid : rec.iSchemaCid);
      }
    }else{
      iMaxCid = base.iCid;
      iSchemaCid = base.iSchemaCid;
    }
  }

  /* Scan the sqlite_hct_journal table from beginning to end. When
  ** the first missing entry is found, calculate the size of the 
  ** HctJrnlServer.aCommit[] API and allocate it. Then continue
  ** scanning the sqlite_hct_journal table, populating aCommit[] along
  ** the way.  */
  if( rc==SQLITE_OK ){
    HctTMapClient *pTClient = sqlite3HctFileTMapClient(pFile);
    i64 iPrev = base.iCid;
    int nTrans = 0;
    u64 *aCommit = 0;

    /* Scan until the first missing entry. Set nTrans to the number of
    ** number of entries between the first missing one and the last
    ** present, or to HCT_MAX_LEADING_WRITE, whichever is greater.
    ** Set iPrev to the largest CID value for which it and all previous
    ** CIDs have been written into the journal table.  */
    for(rc = sqlite3HctDbCsrFirst(pCsr);
        rc==SQLITE_OK && 0==sqlite3HctDbCsrEof(pCsr);
        rc = sqlite3HctDbCsrNext(pCsr)
    ){
      i64 iCid = 0;
      sqlite3HctDbCsrKey(pCsr, &iCid);
      if( iPrev!=0 && iCid!=iPrev+1 ){
        nTrans = iMaxCid - iPrev;
        break;
      }
      iPrev = iCid;
    }
    nTrans = MAX(HCT_MAX_LEADING_WRITE, nTrans);

    pServer->nCommit = nTrans*2;
    aCommit = (u64*)sqlite3HctMalloc(&rc, pServer->nCommit*sizeof(u64));
    pServer->aCommit = aCommit;
    pServer->iSnapshot = iPrev;

    /* Scan through whatever is left of the sqlite_hct_journal table, 
    ** populating the aCommit[] array and the transaction-map (hct_tmap.c)
    ** along the way. */
    while( rc==SQLITE_OK && 0==sqlite3HctDbCsrEof(pCsr) ){
      HctJournalRecord rec;
      rc = hctJrnlReadJournalRecord(pCsr, &rec);
      if( rc==SQLITE_OK ){
        i64 iVal = rec.iValidCid ? rec.iValidCid : rec.iCid;
        pServer->aCommit[rec.iCid % pServer->nCommit] = iVal;
        rc = sqlite3HctTMapRecoverySet(pTClient, rec.iTid, rec.iCid);
      }
      if( rc==SQLITE_OK ){
        rc = sqlite3HctDbCsrNext(pCsr);
      }
    }
    sqlite3HctTMapRecoveryFinish(pTClient, rc);
  }

  if( rc==SQLITE_OK ){
    HctAtomicStore(&pServer->iSchemaCid, iSchemaCid);
    pJrnl->pServer = pServer;
    sqlite3HctFileSetJrnlPtr(pFile, (void*)pServer, hctJrnlDelServer);
    if( iMaxCid>0 ) sqlite3HctFileSetCID(pFile, iMaxCid);
  }else{
    hctJrnlDelServer((void*)pServer);
  }

  sqlite3HctDbCsrClose(pCsr);
  return rc;
}

static u64 hctFindRootByName(Schema *pSchema, const char *zName){
  u64 iRet = 0;
  Table *pTab = (Table*)sqlite3HashFind(&pSchema->tblHash, zName);
  if( pTab ){
    iRet = pTab->tnum;
  }
  return iRet;
}

int sqlite3HctJournalNewIf(
  Schema *pSchema, 
  HctTree *pTree, 
  HctDatabase *pDb, 
  HctJournal **pp
){
  int rc = SQLITE_OK;
  u64 iJrnlRoot = hctFindRootByName(pSchema, "sqlite_hct_journal");
  u64 iBaseRoot = hctFindRootByName(pSchema, "sqlite_hct_baseline");

  assert( *pp==0 );

  if( (iJrnlRoot==0)!=(iBaseRoot==0) ){
    return SQLITE_CORRUPT_BKPT;
  }
  if( iJrnlRoot ){
    HctJournal *pNew = sqlite3HctMalloc(&rc, sizeof(HctJournal));
    if( pNew ){
      HctFile *pFile = sqlite3HctDbFile(pDb);
      pNew->iJrnlRoot = iJrnlRoot;
      pNew->iBaseRoot = iBaseRoot;
      pNew->pDb = pDb;
      pNew->pTree = pTree;
      pNew->pServer = (HctJrnlServer*)sqlite3HctFileGetJrnlPtr(pFile);
      *pp = pNew;
    }
  }

  return rc;
}

void sqlite3HctJournalClose(HctJournal *pJrnl){
  sqlite3_free(pJrnl);
}

/*
** See description in hctJrnlInt.h.
*/
int sqlite3HctJournalIsReadonly(
  HctJournal *pJrnl, 
  u64 iTable,
  int *pbNosnap
){
  if( pJrnl ){
    HctJrnlServer *p = pJrnl->pServer;
    int bNosnap = (pJrnl->iJrnlRoot==iTable || pJrnl->iBaseRoot==iTable);
    *pbNosnap = bNosnap;
    return (pJrnl->eInWrite==HCT_JOURNAL_NONE && (
        bNosnap || !p || p->eMode==SQLITE_HCT_JOURNAL_MODE_FOLLOWER
    ));
  }
  return 0;
}

/*
** Called during log file recovery to remove the entry with "tid" (not CID!) 
** value  iTid from the sqlite_hct_journal table.
*/
int sqlite3HctJrnlRollbackEntry(HctJournal *pJrnl, i64 iTid){
  i64 iDel = 0;
  HctDbCsr *pCsr = 0;
  int rc = SQLITE_OK;

  rc = sqlite3HctDbCsrOpen(pJrnl->pDb, 0, (u32)pJrnl->iJrnlRoot, &pCsr);
  if( rc==SQLITE_OK ){
    HctJournalRecord rec;
    sqlite3HctDbCsrNosnap(pCsr, 1);
    for(rc=sqlite3HctDbCsrLast(pCsr);
        iDel==0 && rc==SQLITE_OK && sqlite3HctDbCsrEof(pCsr)==0;
        rc=sqlite3HctDbCsrPrev(pCsr)
    ){
      hctJrnlReadJournalRecord(pCsr, &rec);
      if( rec.iTid==iTid ) iDel = rec.iCid;
    }

    if( iDel!=0 && rc==SQLITE_OK ){
      rc = hctJrnlWriteRecord(pJrnl, iDel, "", 0, 0, 0, iTid);
    }

    sqlite3HctDbCsrClose(pCsr);
  }

  return rc;
}

/*
** Find the HctJournal object associated with the "main" database of the
** connection passed as the only argument. If successful, set (*ppJrnl)
** to point to said object and return SQLITE_OK. Or, if the database is
** not a replication-enabled db, set (*ppJrnl) to NULL and return SQLITE_OK.
** Or, if an error occurs, return an SQLite error code. The final value
** of (*ppJrnl) is undefined in this case.
*/
static int hctJrnlFind(sqlite3 *db, HctJournal **ppJrnl){
  int rc = SQLITE_OK;
  HctJournal *pJrnl = sqlite3HctJrnlFind(db);

  if( pJrnl==0 ){
    /* If the journal was not found, it might be because the database is
    ** not yet initialized. Run a query to ensure it is, then try to retrieve
    ** the journal object again.  */
    rc = sqlite3_exec(db, "SELECT 1 FROM sqlite_schema LIMIT 1", 0, 0, 0);
    if( rc==SQLITE_OK ){
      pJrnl = sqlite3HctJrnlFind(db);
    }
  }

  if( rc==SQLITE_OK && pJrnl==0 ){
    hctJournalSetDbError(db, SQLITE_ERROR, "not a journaled hct database");
    rc = SQLITE_ERROR;
  }

  *ppJrnl = pJrnl;
  return rc;
}


/*
** Return the current journal mode - SQLITE_HCT_JOURNAL_MODE_FOLLOWER or
** SQLITE_HCT_JOURNAL_MODE_LEADER - for the main database of the connection
** passed as the only argument. Or, if the main database is not a
** replication-enabled hct database, return -1;
*/
int sqlite3_hct_journal_mode(sqlite3 *db){
  int eRet = -1;
  HctJournal *pJrnl = sqlite3HctJrnlFind(db);
  if( pJrnl ){
    eRet = pJrnl->pServer->eMode;
  }
  return eRet;
}

/*
** Return true if the journal is complete - contains no holes. Or false
** otherwise. This function is not threadsafe. Results are undefined
** if there are concurrent transactions running on the database.
*/
static int hctJrnlIsComplete(HctJournal *pJrnl){
  HctJrnlServer *pServer = pJrnl->pServer;
  u64 iSnapshot = pServer->iSnapshot;
  int ii;

  assert( pServer->eMode==SQLITE_HCT_JOURNAL_MODE_FOLLOWER );

  /* Set iSnapshot to the CID of the last contiguous commit */
  while( 1 ){
    int iNext = (iSnapshot+1) % pServer->nCommit;
    u64 iVal = HctAtomicLoad(&pServer->aCommit[iNext]);
    if( iVal<=iSnapshot ) break;
    iSnapshot++;
  }

  /* See if there are any transactions yet committed with CID values greater
  ** than iSnapshot. If there are, then the journal is not complete.  */
  for(ii=0; ii<pServer->nCommit; ii++){
    u64 iVal = HctAtomicLoad(&pServer->aCommit[ii]);
    if( iVal>iSnapshot ){
      return 0;
    }
  }

  return 1;
}

/*
** Set the LEADER/FOLLOWER setting of the main database of the connection 
** passed as the first argument.
*/
int sqlite3_hct_journal_setmode(sqlite3 *db, int eMode){
  int rc = SQLITE_OK;
  HctJournal *pJrnl = sqlite3HctJrnlFind(db);

  if( pJrnl==0 ){
    rc = sqlite3_exec(db, "SELECT 1 FROM sqlite_schema LIMIT 1", 0, 0, 0);
    if( rc==SQLITE_OK ){
      pJrnl = sqlite3HctJrnlFind(db);
    }
  }

  if( rc==SQLITE_OK ){
    if( eMode!=SQLITE_HCT_JOURNAL_MODE_LEADER
     && eMode!=SQLITE_HCT_JOURNAL_MODE_FOLLOWER
    ){
      return SQLITE_MISUSE_BKPT;
    }else if( pJrnl==0 ){
      hctJournalSetDbError(db, SQLITE_ERROR, "not a journaled hct database");
      rc = SQLITE_ERROR;
    }else{
      HctFile *pFile = sqlite3HctDbFile(pJrnl->pDb);
      HctJrnlServer *pServer = pJrnl->pServer;
      if( eMode!=pServer->eMode ){
        if( eMode==SQLITE_HCT_JOURNAL_MODE_LEADER ){
          /* Switch from FOLLOWER to LEADER mode. This is only allowed if
          ** there are no holes in the journal.  */
          if( hctJrnlIsComplete(pJrnl)==0 ){
            hctJournalSetDbError(db, SQLITE_ERROR, "incomplete journal");
            rc = SQLITE_ERROR;
          }else{
            u64 iCid = sqlite3HctJournalSnapshot(pJrnl);
            pServer->eMode = SQLITE_HCT_JOURNAL_MODE_LEADER;
            if( iCid>0 ){
              sqlite3HctFileSetCID(sqlite3HctDbFile(pJrnl->pDb), iCid);
            }
          }
          pServer->nSchemaVersionIncr++;
        }else{
          /* Switch from LEADER to FOLLOWER mode. This is always possible. */
          void *pSchema = sqlite3HctBtreeSchema(db->aDb[0].pBt, 0, 0);
          u64 iSnapshotId = sqlite3HctFileGetSnapshotid(pFile);
          memset(pServer->aCommit, 0, pServer->nCommit*sizeof(u64));
          pServer->iSnapshot = iSnapshotId;
          pServer->eMode = SQLITE_HCT_JOURNAL_MODE_FOLLOWER;
          sqlite3HctJournalFixSchema(pJrnl, db, pSchema);
        }
      }
    }
  }

  return rc;
}

static void hctJrnlFixTable(Table *pTab){
  Index *pIdx;
  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    if( pIdx->idxType==SQLITE_IDXTYPE_UNIQUE 
     || pIdx->idxType==SQLITE_IDXTYPE_PRIMARYKEY
    ){
      pIdx->idxType = SQLITE_IDXTYPE_APPDEF;
    }
    pIdx->uniqNotNull = 0;
    pIdx->onError = OE_None;
  }


}

/*
** This function is used to "fix" a schema so that it can be used in
** a FOLLOWER mode database. Specifically:
**
**   * All UNIQUE indexes are marked as not-unique.
**   * All triggers are removed from the schema.
**   * All FK definitions are removed from the schema.
*/
void sqlite3HctJournalFixSchema(HctJournal *pJrnl, sqlite3 *db, void *pS){
  HctJrnlServer *pServer = pJrnl->pServer;
  if( pServer==0 || pServer->eMode==SQLITE_HCT_JOURNAL_MODE_FOLLOWER ){
    Schema *pSchema = (Schema*)pS;
    HashElem *k;

    for(k=sqliteHashFirst(&pSchema->tblHash); k; k=sqliteHashNext(k)){
      Table *pTab = (Table*)sqliteHashData(k);
      hctJrnlFixTable(pTab);
      while( pTab->pTrigger ){
        Trigger *pTrig = pTab->pTrigger;
        pTab->pTrigger = pTrig->pNext;
        sqlite3DeleteTrigger(db, pTrig);
      }
      if( IsOrdinaryTable(pTab) ){
        sqlite3FkDelete(db, pTab);
      }
    }
    sqlite3HashClear(&pSchema->trigHash);
  }
}

void sqlite3HctJournalSchemaVersion(HctJournal *pJrnl, u32 *pSchemaVersion){
  if( pJrnl && pJrnl->pServer ){
    *pSchemaVersion += HctAtomicLoad(&pJrnl->pServer->nSchemaVersionIncr);
  }
}

#ifdef SQLITE_DEBUG
/*
** assert() that the schema associated with table pTab has been "fixed",
** according to the definition used by sqlite3HctJournalFixSchema().
*/
static void assert_schema_is_fixed(Table *pTab){
  Index *pIdx;
  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    assert( pIdx->idxType==SQLITE_IDXTYPE_APPDEF );
    assert( pIdx->uniqNotNull==0 );
    assert( pIdx->onError==OE_None );
  }
  assert( pTab->pTrigger==0 );
  assert( pTab->u.tab.pFKey==0 );
}
#else
# define assert_schema_is_fixed(x)
#endif

static int hctJrnlGetInsertStmt(
  sqlite3 *db, 
  const char *zTab, 
  int *piPk,
  sqlite3_stmt **ppStmt
){
  sqlite3_str *pStr;
  Schema *pSchema = db->aDb[0].pSchema;
  Table *pTab = (Table*)sqlite3HashFind(&pSchema->tblHash, zTab);
  char *zSql = 0;
  int rc = SQLITE_OK;
  int ii;

  assert( pTab );
  assert_schema_is_fixed(pTab);

  *ppStmt = 0;
  pStr = sqlite3_str_new(0);
  sqlite3_str_appendf(pStr, "REPLACE INTO main.%Q(", zTab);
  if( pTab->iPKey<0 ){
    sqlite3_str_appendf(pStr, "_rowid_, ");
  }
  for(ii=0; ii<pTab->nCol; ii++){
    const char *zSep = (ii==pTab->nCol-1) ? ") VALUES (" : ",";
    sqlite3_str_appendf(pStr, "%Q%s ", pTab->aCol[ii].zCnName, zSep);
  }
  if( pTab->iPKey<0 ){
    sqlite3_str_appendf(pStr, "?%d, ", pTab->nCol+1);
    *piPk = pTab->nCol+1;
  }else{
    *piPk = pTab->iPKey+1;
  }
  for(ii=0; ii<pTab->nCol; ii++){
    const char *zSep = (ii==pTab->nCol-1) ? ")" : ", ";
    sqlite3_str_appendf(pStr, "?%d%s", ii+1, zSep);
  }

  zSql = sqlite3_str_finish(pStr);
  if( zSql==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    rc = sqlite3_prepare_v2(db, zSql, -1, ppStmt, 0);
    sqlite3_free(zSql);
  }

  return rc;
}

static int hctJrnlGetDeleteStmt(
  sqlite3 *db, 
  const char *zTab, 
  sqlite3_stmt **ppStmt
){
  Schema *pSchema = db->aDb[0].pSchema;
  Table *pTab = (Table*)sqlite3HashFind(&pSchema->tblHash, zTab);
  int rc = SQLITE_OK;
  char *zSql = 0;
  const char *zRowid = "_rowid_";

  assert( pTab );
  assert_schema_is_fixed(pTab);

  if( pTab->iPKey>=0 ){
    zRowid = pTab->aCol[pTab->iPKey].zCnName;
  }

  *ppStmt = 0;
  zSql = sqlite3_mprintf(
      "DELETE FROM main.%Q WHERE main.%Q.%Q = ?", 
      pTab->zName, pTab->zName, zRowid
  );
  if( zSql==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    rc = sqlite3_prepare_v2(db, zSql, -1, ppStmt, 0);
    sqlite3_free(zSql);
  }

  return rc;
}

/*
** Parameter aData[] points to a record encoded in SQLite format. Bind
** each value in the record to the statement passed as the second argument.
*/
static int hctJrnlBindRecord(int *pRc, sqlite3_stmt *pStmt, const u8 *aData){
  int rc = *pRc;
  int ret = 0;
  if( rc==SQLITE_OK ){
    const u8 *pHdr = aData;
    const u8 *pData = 0;
    int nHdr;
    int iBind;

    pHdr += getVarint32(pHdr, nHdr);
    pData = &aData[nHdr];
    for(iBind=1; pHdr<&aData[nHdr]; iBind++){
      u32 t;
      pHdr += getVarint32(pHdr, t);
      switch( t ){
        case 10:
        case 11:
        case 0:   /* NULL */
          sqlite3_bind_null(pStmt, iBind);
          break;

        case 1: { /* 1 byte integer */
          i64 iVal = pData[0];
          pData += 1;
          sqlite3_bind_int64(pStmt, iBind, iVal);
          break;
        }
        case 2: { /* 2 byte integer */
          i64 iVal = ((i64)pData[0]<<8) + (i64)pData[1];
          pData += 2;
          sqlite3_bind_int64(pStmt, iBind, iVal);
          break;
        }
        case 3: { /* 3 byte integer */
          i64 iVal = ((i64)pData[0]<<16) + ((i64)pData[1]<<8) + (i64)pData[2];
          pData += 3;
          sqlite3_bind_int64(pStmt, iBind, iVal);
          break;
        }
        case 4: { /* 4 byte integer */
          i64 iVal = ((i64)pData[0]<<24) 
                   + ((i64)pData[1]<<16) 
                   + ((i64)pData[2]<<8) 
                   + (i64)pData[3];
          pData += 4;
          sqlite3_bind_int64(pStmt, iBind, iVal);
          break;
        }
        case 5: { /* 6 byte integer */
          i64 iVal = ((i64)pData[0]<<40) 
                   + ((i64)pData[1]<<32) 
                   + ((i64)pData[2]<<24) 
                   + ((i64)pData[3]<<16) 
                   + ((i64)pData[4]<<8) 
                   + (i64)pData[5];
          pData += 6;
          sqlite3_bind_int64(pStmt, iBind, iVal);
          break;
        }

        case 6: case 7: { /* 8 byte integer, 8 byte real value */
          u64 iVal = ((u64)pData[0]<<56) 
                   + ((u64)pData[1]<<48) 
                   + ((u64)pData[2]<<40) 
                   + ((u64)pData[3]<<32) 
                   + ((u64)pData[4]<<24) 
                   + ((u64)pData[5]<<16) 
                   + ((u64)pData[6]<<8) 
                   + (u64)pData[7];
          pData += 8;
          if( t==6 ){
            i64 iVal2;
            memcpy(&iVal2, &iVal, sizeof(iVal));
            sqlite3_bind_int64(pStmt, iBind, iVal2);
          }else{
            double rVal2;
            memcpy(&rVal2, &iVal, sizeof(iVal));
            sqlite3_bind_double(pStmt, iBind, rVal2);
          }
          break;
        }

        case 8:   /* integer value 0 */
          sqlite3_bind_int(pStmt, iBind, 0);
          break;

        case 9:   /* integer value 1 */
          sqlite3_bind_int(pStmt, iBind, 1);
          break;

        default: {
          int nByte = (t - 12) / 2;
          if( t & 0x01 ){
            sqlite3_bind_text(
                pStmt, iBind, (const char*)pData, nByte, SQLITE_TRANSIENT
            );
          }else{
            sqlite3_bind_blob(
                pStmt, iBind, (const void*)pData, nByte, SQLITE_TRANSIENT
            );
          }
          pData += nByte;
          break;
        };
      }
    }

    ret = pData - aData;
  }
  return ret;
}

u64 sqlite3HctJrnlWriteTid(HctJournal *pJrnl, u64 *piCid){
  u64 iRet = 0;
  assert( *piCid==0 );
  if( pJrnl && pJrnl->eInWrite!=HCT_JOURNAL_NONE ){
    iRet = pJrnl->iWriteTid;
    *piCid = pJrnl->iWriteCid;
  }
  return iRet;
}

u64 sqlite3HctJournalSnapshot(HctJournal *pJrnl){
  u64 iRet = 0;
  if( pJrnl ){
    if( pJrnl->eInWrite==HCT_JOURNAL_INROLLBACK ){
      return pJrnl->iRollbackSnapshot;
    }
    HctJrnlServer *pServer = pJrnl->pServer;
    if( pServer && pServer->eMode==SQLITE_HCT_JOURNAL_MODE_FOLLOWER ){
      u64 iTest = 0;
      u64 iValid = 0;
      u64 iSnap = HctAtomicLoad(&pServer->iSnapshot);
      iRet = iSnap;
      for(iTest=iRet+1; 1; iTest++){
        u64 iVal = HctAtomicLoad(&pServer->aCommit[iTest % pServer->nCommit]);
        if( iVal<iTest ) break;
        if( iVal==iTest ){
          if( iTest>=iValid ) iRet = iTest;
        }else{
          iValid = MAX(iVal, iValid);
        }
      }

      /* Update HctJrnlServer.iSnapshot if required */
      if( iRet>=iSnap+16 ){
        (void)HctCASBool(&pServer->iSnapshot, iSnap, iRet);
      }

      /* If we are in an sqlite3_hct_journal_write() call, it is fine (and
      ** necessary) to read snapshots that are invalid to the application.
      ** So ignore any entries in the aCommit[] array that indicate such.  */
      if( pJrnl->eInWrite==HCT_JOURNAL_INWRITE ){
        assert( (iTest-1)>=iRet );
        iRet = (iTest-1);
      }
    }
  }
  return iRet;
}

/*
** Set output variable (*piCid) to the CID of the newest available 
** database snapshot. Return SQLITE_OK if successful, or an SQLite
** error code if something goes wrong.
*/
int sqlite3_hct_journal_snapshot(sqlite3 *db, sqlite3_int64 *piCid){
  int rc = SQLITE_OK;
  HctJournal *pJrnl = 0;

  rc = hctJrnlFind(db, &pJrnl);
  if( rc==SQLITE_OK ){
    *piCid = (i64)sqlite3HctJournalSnapshot(pJrnl);
  }else{
    *piCid = 0;
  }
  return rc;
}

static sqlite3_stmt *hctJrnlPrepare(int *pRc, sqlite3 *db, const char *zSql){
  sqlite3_stmt *pStmt = 0;
  if( *pRc==SQLITE_OK ){
    *pRc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  }
  return pStmt;
}

static void hctJrnlFinalize(int *pRc, sqlite3_stmt *pStmt){
  int rc = sqlite3_finalize(pStmt);
  if( *pRc==SQLITE_OK ){
    *pRc = rc;
  }
}

int sqlite3_hct_journal_truncate(sqlite3 *db, i64 iMinCid){
  int rc = SQLITE_OK;
  HctJournal *pJrnl = 0;
  sqlite3_stmt *pSelJrnl = 0;
  sqlite3_stmt *pSelBaseline = 0;
  sqlite3_stmt *pDelete = 0;
  sqlite3_stmt *pUpdate = 0;

  if( 0==sqlite3_get_autocommit(db) ){
    hctJournalSetDbError(db, SQLITE_ERROR, 
        "cannot truncate journal from within a transaction"
    );
    return SQLITE_ERROR;
  }

  rc = hctJrnlFind(db, &pJrnl);
  if( rc==SQLITE_OK 
   && pJrnl->pServer->eMode==SQLITE_HCT_JOURNAL_MODE_FOLLOWER 
  ){
    u64 iCid = sqlite3HctJournalSnapshot(pJrnl);
    if( iCid<iMinCid ){
      rc = SQLITE_RANGE;
    }
  }

  if( rc==SQLITE_OK ){
    pJrnl->eInWrite = HCT_JOURNAL_INWRITE;
    rc = sqlite3_exec(db, "BEGIN CONCURRENT", 0, 0, 0);
  }

  pSelBaseline = hctJrnlPrepare(&rc, db, 
      "SELECT cid, schemacid, hash FROM sqlite_hct_baseline"
  );
  pSelJrnl = hctJrnlPrepare(&rc, db, 
      "SELECT cid, schemacid, hash FROM sqlite_hct_journal WHERE cid<? "
      "ORDER BY cid ASC"
  );
  pDelete = hctJrnlPrepare(&rc, db, 
      "DELETE FROM sqlite_hct_journal WHERE cid<?"
  );
  pUpdate = hctJrnlPrepare(&rc, db, 
      "UPDATE sqlite_hct_baseline SET cid=?, schemacid=?, hash=?"
  );

  if( rc==SQLITE_OK ){
    i64 iCid = 0;
    i64 iSchemaCid = 0;
    u8 aHash[SQLITE_HCT_JOURNAL_HASHSIZE];
    int nBaseHash = 0;
    
    memset(aHash, 0, sizeof(aHash));
    if( sqlite3_step(pSelBaseline)==SQLITE_ROW ){
      const u8 *aBaseHash = (const u8*)sqlite3_column_blob(pSelBaseline, 2);
      nBaseHash = sqlite3_column_bytes(pSelBaseline, 2);
      memcpy(aHash, aBaseHash, MIN(nBaseHash, sizeof(aHash)));
      iCid = sqlite3_column_int64(pSelBaseline, 0);
      iSchemaCid = sqlite3_column_int64(pSelBaseline, 1);
    }
    if( nBaseHash!=SQLITE_HCT_JOURNAL_HASHSIZE ){
      rc = SQLITE_CORRUPT_BKPT;
    }else{
      rc = sqlite3_finalize(pSelBaseline);
      pSelBaseline = 0;
      sqlite3_bind_int64(pSelJrnl, 1, iMinCid);
    }
    while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pSelJrnl) ){
      i64 iJrnlSchemaCid = sqlite3_column_int64(pSelJrnl, 1);
      int nJrnlHash = sqlite3_column_bytes(pSelJrnl, 2);
      const u8 *aJrnlHash = (const u8*)sqlite3_column_blob(pSelJrnl, 2);
      iCid = sqlite3_column_int64(pSelJrnl, 0);

      if( nJrnlHash!=SQLITE_HCT_JOURNAL_HASHSIZE ){
        rc = SQLITE_CORRUPT_BKPT;
      }else{
        sqlite3_hct_journal_hash(aHash, aJrnlHash);
      }
      iSchemaCid = MAX(iSchemaCid, iJrnlSchemaCid);
    }
    if( rc==SQLITE_OK ){
      rc = sqlite3_finalize(pSelJrnl);
      pSelJrnl = 0;
    }

    if( rc==SQLITE_OK ){
      sqlite3_bind_int64(pDelete, 1, iMinCid);
      sqlite3_step(pDelete);
      rc = sqlite3_finalize(pDelete);
      pDelete = 0;
    }
    if( rc==SQLITE_OK ){
      sqlite3_bind_int64(pUpdate, 1, iCid);
      sqlite3_bind_int64(pUpdate, 2, iSchemaCid);
      sqlite3_bind_blob(pUpdate, 3, aHash, SQLITE_HCT_JOURNAL_HASHSIZE, 0);
      sqlite3_step(pUpdate);
      rc = sqlite3_finalize(pUpdate);
      pUpdate = 0;
    }
  }

  assert( rc!=SQLITE_OK || sqlite3_get_autocommit(db)==0 );
  if( sqlite3_get_autocommit(db)==0 ){
    if( rc==SQLITE_OK ){
      rc = sqlite3HctDbStartWrite(pJrnl->pDb, &pJrnl->iWriteTid);
      pJrnl->iWriteCid = 1;
    }
    if( rc==SQLITE_OK ){
      rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
    }
    if( rc!=SQLITE_OK ){
      sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
    }
  }

  hctJrnlFinalize(&rc, pSelJrnl);
  hctJrnlFinalize(&rc, pSelBaseline);
  hctJrnlFinalize(&rc, pDelete);
  hctJrnlFinalize(&rc, pUpdate);
  pJrnl->eInWrite = HCT_JOURNAL_NONE;
  pJrnl->iWriteTid = 0;
  pJrnl->iWriteCid = 0;
  return rc;
}

static int hctBufferAppendInsert(
  HctBuffer *pBuf, 
  i64 iRowid,
  Table *pTab, 
  sqlite3_stmt *pQuery
){
  int ii;
  int rc = SQLITE_OK;

  rc = hctBufferAppend(pBuf, "REPLACE INTO %Q(_rowid_", pTab->zName);
  for(ii=0; rc==SQLITE_OK && ii<pTab->nCol; ii++){
    if( ii!=pTab->iPKey ){
      rc = hctBufferAppend(pBuf, ", %Q", pTab->aCol[ii].zCnName);
    }
  }

  if( rc==SQLITE_OK ){
    rc = hctBufferAppend(pBuf, ") VALUES(%lld", iRowid);
  }

  for(ii=0; rc==SQLITE_OK && ii<sqlite3_column_count(pQuery); ii++){
    rc = hctBufferAppend(pBuf, ", %s", sqlite3_column_text(pQuery, ii));
  }
  if( rc==SQLITE_OK ){
    rc = hctBufferAppend(pBuf, ");\n");
  }

  return rc;
}

static sqlite3_stmt *hctGetQuoteQuery(int *pRc, sqlite3 *db, Table *pTab){
  HctBuffer buf = {0,0,0};
  int ii;
  int rc = *pRc;
  sqlite3_stmt *pRet = 0;
  const char *zSep = "";

  if( rc==SQLITE_OK ){
    rc = hctBufferAppend(&buf, "SELECT ");
  }
  for(ii=0; rc==SQLITE_OK && ii<pTab->nCol; ii++){
    if( ii!=pTab->iPKey ){
      rc = hctBufferAppend(&buf, "%squote(x.%Q)", zSep, pTab->aCol[ii].zCnName);
      zSep = ", ";
    }
  }
  if( rc==SQLITE_OK ){
    rc = hctBufferAppend(&buf, "FROM %Q AS x WHERE _rowid_=?", pTab->zName);
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(db, (const char*)buf.aBuf, -1, &pRet, 0);
  }
  sqlite3_free(buf.aBuf);

  *pRc = rc;
  return pRet;
}

/*
** Rollback transactions that follow the first hole in the journal.
*/
int sqlite3_hct_journal_rollback(sqlite3 *db, sqlite3_int64 iCid){
  int rc = SQLITE_OK;
  HctJournal *pJrnl = 0;
  i64 iLast = 0;
  i64 iLastCont = 0;
  sqlite3_stmt *pStmt = 0;
  Schema *pSchema = 0;

  rc = hctJrnlFind(db, &pJrnl);
  if( rc!=SQLITE_OK ) return rc;
  pSchema = db->aDb[0].pSchema;

  /*
  ** 1. Find the location of the first hole in the journal.
  **
  ** 2. Loop through journal entries, from the newest back to the
  **    first hole in the journal.
  **
  ** 3. Work through each of the transactions identified in step (1).
  **    For each, write a log file, make the required modifications to
  **    the db and journal file, then delete the log file.
  */

  /* Cannot call this with an open transaction. */
  if( 0==sqlite3_get_autocommit(db) ){
    hctJournalSetDbError(db, SQLITE_ERROR, 
        "cannot rollback journal from within a transaction"
    );
    return SQLITE_ERROR;
  }

  /* Cannot call this in LEADER mode. */
  if( pJrnl->pServer->eMode==SQLITE_HCT_JOURNAL_MODE_LEADER ){
    hctJournalSetDbError(db, SQLITE_ERROR, 
        "cannot rollback journal in leader database"
    );
    return SQLITE_ERROR;
  }

  /* Find the location of the first hole in the journal. If there are no
  ** holes in the journal, this call is a no-op. */
  rc = hctJrnlGetJrnlShape(db, &iLast, &iLastCont);
  assert( iLastCont<=iLast );
  if( rc!=SQLITE_OK || iLastCont>=iLast ) return rc;

  /* Loop through all of the journal entries that will be rolled back.
  ** For each, extract the primary keys from the "data" blob. Query the
  ** current database snapshot for each of these keys, generating an SQL
  ** script with a "REPLACE INTO" for each row present in the db and a 
  ** "DELETE" for each not.  */
  rc = sqlite3_prepare_v2(db, 
      "SELECT data FROM sqlite_hct_journal WHERE cid>?", -1, &pStmt, 0
  );
  if( rc==SQLITE_OK ){
    HctBuffer sql = {0, 0, 0};
    sqlite3_bind_int64(pStmt, 1, iLastCont);

    rc = hctBufferAppend(&sql, "BEGIN CONCURRENT;\n");
    while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pStmt) ){
      const void *pData = sqlite3_column_blob(pStmt, 0);
      int nData = sqlite3_column_bytes(pStmt, 0);
      sqlite3_stmt *pQuery = 0;
      Table *pTab = 0;
      HctDataReader rdr;

      for(rc=hctDataReaderInit(pData, nData, &rdr);
          rc==SQLITE_OK && rdr.bEof==0;
          rc=hctDataReaderNext(&rdr)
      ){
        switch( rdr.eType ){
          case HCT_TYPE_TABLE: {
            pTab = (Table*)sqlite3HashFind(&pSchema->tblHash, rdr.zTab);
            if( pTab==0 ){
              rc = SQLITE_CORRUPT_BKPT;
            }else{
              rc = sqlite3_finalize(pQuery);
              pQuery = hctGetQuoteQuery(&rc, db, pTab);
            }
            break;
          }

          case HCT_TYPE_INSERT_ROWID:
          case HCT_TYPE_DELETE_ROWID: {
            sqlite3_bind_int64(pQuery, 1, rdr.iRowid);
            if( SQLITE_ROW==sqlite3_step(pQuery) ){
              rc = hctBufferAppendInsert(&sql, rdr.iRowid, pTab, pQuery);
            }else{
              rc = hctBufferAppend(&sql, 
                  "DELETE FROM %Q WHERE _rowid_=%lld;\n", rdr.zTab, rdr.iRowid
              );
            }
            rc = sqlite3_reset(pQuery);
            break;
          }

          default: assert( 0 );
        }
        if( rc ) break;
      }
      sqlite3_finalize(pQuery);
    }
    if( rc==SQLITE_OK ){
      rc = hctBufferAppend(&sql, 
          "DELETE FROM sqlite_hct_journal WHERE cid>%lld;\n", iLastCont
      );
    }

    if( rc==SQLITE_OK ){
      assert( pJrnl->eInWrite==HCT_JOURNAL_NONE );
      pJrnl->eInWrite = HCT_JOURNAL_INROLLBACK;
      rc = sqlite3_exec(db, (const char*)sql.aBuf, 0, 0, 0);
      if( rc==SQLITE_OK ){
        rc = sqlite3HctDbStartWrite(pJrnl->pDb, &pJrnl->iWriteTid);
      }
      if( rc==SQLITE_OK ){
        pJrnl->iWriteCid = iLastCont;
        pJrnl->iRollbackSnapshot = iLast;
        rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
      }
      if( rc!=SQLITE_OK ){
        sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
      }
      pJrnl->eInWrite = HCT_JOURNAL_NONE;
    }

    sqlite3_free(sql.aBuf);
    sqlite3_finalize(pStmt);
  }

  return rc;
}

static u64 hctJournalFindLastWrite(
  int *pRc,                       /* IN/OUT: Error code */
  HctJournal *pJrnl,              /* Journal object */
  u64 iRoot,                      /* Root page of table */
  i64 iRowid                      /* Key (for rowid tables) */
){
  int rc = *pRc;
  u64 iRet = 0;
  if( rc==SQLITE_OK ){
    HctDbCsr *pCsr = 0;
    rc = sqlite3HctDbCsrOpen(pJrnl->pDb, 0, iRoot, &pCsr);
    if( rc==SQLITE_OK ){
      rc = sqlite3HctDbCsrFindLastWrite(pCsr, 0, iRowid, &iRet);
      sqlite3HctDbCsrClose(pCsr);
    }
    *pRc = rc;
  }
  return iRet;
}

/*
** Write a transaction into the database.
*/
int sqlite3_hct_journal_write(
  sqlite3 *db,                    /* Write to "main" db of this handle */
  sqlite3_int64 iCid,
  const char *zSchema,
  const void *pData, int nData,
  sqlite3_int64 iSchemaCid
){
  int rc = SQLITE_OK;
  char *zErr = 0;                 /* Error message, if any */
  HctJournal *pJrnl = 0;
  u64 iValidCid = 0;
  u64 iSnapshotId = 0;
  Btree *pBt = db->aDb[0].pBt;
  Schema *pSchema = db->aDb[0].pSchema;
  u64 iRoot = 0;                          /* Root page of zTab */
  HctJrnlServer *pServer = 0;

  HctDataReader rdr;              /* For iterating through pData/nData */

  rc = hctJrnlFind(db, &pJrnl);
  if( rc!=SQLITE_OK ) return rc;
  pJrnl->eInWrite = HCT_JOURNAL_INWRITE;
  pServer = pJrnl->pServer;

  /* Check that the journal is in follower mode */
  if( pServer->eMode!=SQLITE_HCT_JOURNAL_MODE_FOLLOWER ){
    hctJournalSetDbError(db, SQLITE_ERROR, "database is not in FOLLOWER mode");
    return SQLITE_ERROR;
  }

  /* Check that there is no transaction open on the connection */
  if( rc==SQLITE_OK && sqlite3_get_autocommit(db)==0 ){
    hctJournalSetDbError(db, SQLITE_ERROR, "open transaction on database");
    return SQLITE_ERROR;
  }

  /* Open a concurrent transaction on the db handle. Then ensure that the
  ** snapshot on the main database has also been opened.  */
  rc = sqlite3_exec(db, "BEGIN CONCURRENT", 0, 0, 0);
  if( rc==SQLITE_OK ){
    int dummy = 0;
    rc = sqlite3BtreeBeginTrans(pBt, 1, &dummy);
  }

  /* Check that the snapshot that was just opened has a schema new enough
  ** for this transaction to be applied. */
  if( rc==SQLITE_OK ){
    iSnapshotId = sqlite3HctBtreeSnapshotId(pBt);
    if( iSchemaCid>iSnapshotId ){
      rc = SQLITE_BUSY;
      zErr = sqlite3_mprintf(
          "change may not be applied yet (requires newer schema)"
      );
    }else if( (iSnapshotId+HCT_MAX_LEADING_WRITE)<iCid ){
      rc = SQLITE_BUSY;
      zErr = sqlite3_mprintf(
          "change may not be applied yet (leading write limit of %d)",
          HCT_MAX_LEADING_WRITE
      );
    }
  }

  if( rc==SQLITE_OK && zSchema[0] ){
    if( iSnapshotId!=iCid-1 ){
      rc = SQLITE_BUSY;
      zErr = sqlite3_mprintf(
          "change may not be applied yet (is schema change)"
      );
    }else{
      rc = sqlite3_exec(db, zSchema, 0, 0, 0);
      sqlite3HctJournalFixSchema(pJrnl, db, sqlite3HctBtreeSchema(pBt, 0, 0));
    }
  }

  if( rc==SQLITE_OK ){
    for(rc=hctDataReaderInit(pData, nData, &rdr);
        rc==SQLITE_OK && rdr.bEof==0;
        rc=hctDataReaderNext(&rdr)
    ){
      switch( rdr.eType ){
        case HCT_TYPE_TABLE: {
          iRoot = hctFindRootByName(pSchema, rdr.zTab);
          if( iRoot==0 ){
            rc = sqlite3_exec(db, "SELECT * FROM sqlite_schema", 0, 0, 0);
            if( rc==SQLITE_OK ){
              iRoot = hctFindRootByName(pSchema, rdr.zTab);
            }
          }
          if( iRoot==0 ){
            rc = SQLITE_CORRUPT_BKPT;
          }
          break;
        }

        case HCT_TYPE_INSERT_ROWID: {
          u64 iLastCid = 0;
          if( sqlite3HctBtreeIsNewTable(db->aDb[0].pBt, iRoot)==0 ){
            iLastCid = hctJournalFindLastWrite(&rc, pJrnl, iRoot, rdr.iRowid);
          }
          if( iLastCid>iSnapshotId && iLastCid<iCid ){
            rc = SQLITE_BUSY;
            zErr = sqlite3_mprintf(
                "change may not be applied yet (write/write conflict"
                " iSnapshot=%lld iLastCid=%lld iCid=%lld",
                iSnapshotId, iLastCid, iCid
            );
          }else
          if( iLastCid<=iCid ){
            int iPk = -1;
            sqlite3_stmt *pStmt = 0;
            rc = hctJrnlGetInsertStmt(db, rdr.zTab, &iPk, &pStmt);
            hctJrnlBindRecord(&rc, pStmt, rdr.aRecord);
            sqlite3_bind_int64(pStmt, iPk, rdr.iRowid);
            if( rc==SQLITE_OK ){
              sqlite3_step(pStmt);
              rc = sqlite3_finalize(pStmt);
            }
          }else{
            iValidCid = MAX(iValidCid, iLastCid);
          }
          break;
        }

        case HCT_TYPE_DELETE_ROWID: {
          if( sqlite3HctBtreeIsNewTable(db->aDb[0].pBt, iRoot)==0 ){
            u64 iLastCid = 0;
            iLastCid = hctJournalFindLastWrite(&rc, pJrnl, iRoot, rdr.iRowid);
            if( iLastCid>iSnapshotId && iLastCid<iCid ){
              rc = SQLITE_BUSY;
              zErr = sqlite3_mprintf(
                  "change may not be applied yet (write/write conflict"
                  " iSnapshot=%lld iLastCid=%lld iCid=%lld",
                  iSnapshotId, iLastCid, iCid
              );
            }else
            if( iLastCid<=iCid ){
              sqlite3_stmt *pStmt = 0;
              rc = hctJrnlGetDeleteStmt(db, rdr.zTab, &pStmt);
              sqlite3_bind_int64(pStmt, 1, rdr.iRowid);
              if( rc==SQLITE_OK ){
                sqlite3_step(pStmt);
                rc = sqlite3_finalize(pStmt);
              }
              if( rc==SQLITE_OK 
               && iCid!=iSnapshotId+1 
               && sqlite3_changes(db)==0 
              ){
                rc = SQLITE_BUSY;
                zErr = sqlite3_mprintf(
                    "change may not be applied yet (delete of future key)"
                );
              }
            }else{
              iValidCid = MAX(iValidCid, iLastCid);
            }
          }
          break;
        }
      }
      if( rc ) break;
    }
  }

  /* Allocate a TID value for this transaction. This is done now, instead
  ** of as part of the COMMIT command, so that the TID can be written to
  ** the sqlite_hct_journal table as part of the new record.  */
  if( rc==SQLITE_OK ){
    rc = sqlite3HctDbStartWrite(pJrnl->pDb, &pJrnl->iWriteTid);
    sqlite3HctDbJrnlWriteCid(pJrnl->pDb, iCid);
    pJrnl->iWriteCid = iCid;
  }

  /* Write the sqlite_hct_journal record directly into the HctTree 
  ** structure.  We don't write via the SQL interface here, because
  ** writing to the db once sqlite3HctDbStartWrite() has been called
  ** causes assert() failures. And we don't write directly to the db
  ** either, because the write needs to be rolled back if there is
  ** a conflict.  */
  if( rc==SQLITE_OK ){
    u8 *pRec = 0;
    int nRec = 0;

    /* TODO: "validcid" value */
    pRec = hctJrnlComposeRecord(iCid, zSchema, 
        pData, nData, iSchemaCid, pJrnl->iWriteTid, iValidCid, &nRec
    );
    if( pRec==0 ){
      rc = SQLITE_NOMEM_BKPT;
    }else{
      HctTreeCsr *pCsr = 0;
      u64 root = hctFindRootByName(db->aDb[0].pSchema, "sqlite_hct_journal");

      rc = sqlite3HctTreeCsrOpen(pJrnl->pTree, root, &pCsr);
      if( rc==SQLITE_OK ){
        rc = sqlite3HctTreeInsert(pCsr, 0, iCid, nRec, pRec, 0);
        sqlite3HctTreeCsrClose(pCsr);
      }
    }
    sqlite3_free(pRec);

    if( rc==SQLITE_OK ){
      rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
    }
    if( rc==SQLITE_OK ){
      i64 iVal = iValidCid ? iValidCid : iCid;
      i64 *pPtr = (i64*)&pServer->aCommit[iCid % pServer->nCommit];

      /* If this transaction updated the schema, update the Server.iSchemaCid
      ** field as well. This field is not used in FOLLOWER mode, but may be
      ** if this process switches to LEADER later on.  */
      if( zSchema[0] ){
        HctAtomicStore(&pServer->iSchemaCid, iCid);
      }

      assert( iVal>=iCid );
      while( 1 ){
        i64 iExist = *pPtr;
        if( iExist>=iVal ) break;
        if( HctCASBool(pPtr, iExist, iVal) ) break;
      }
      assert( *pPtr>=iVal );

      if( HctAtomicLoad(&pServer->iSnapshot)==0 ){
        (void)HctCASBool(&pServer->iSnapshot, (u64)0, (u64)iCid);
      }
    }
  }

  if( rc!=SQLITE_OK ){
    sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
    if( zErr ){
      hctJournalSetDbError(db, rc, "%s", zErr);
      sqlite3_free(zErr);
    }else{
      hctJournalSetDbError(db, rc, 0);
    }
  }
  pJrnl->eInWrite = HCT_JOURNAL_NONE;
  sqlite3HctDbJrnlWriteCid(pJrnl->pDb, 0);
  return rc;
}

static int hctBufferAppendIf(HctBuffer *pBuf, const char *zSep){
  int rc = SQLITE_OK;
  if( pBuf->nBuf>0 ){
    rc = hctBufferAppend(pBuf, "%s", zSep);
  }
  return rc;
}

static void hctJournalEntryFunc(
  sqlite3_context *pCtx, 
  int nArg, 
  sqlite3_value **apArg
){
  sqlite3 *db = sqlite3_context_db_handle(pCtx);
  const u8 *aEntry = 0;
  int nEntry = 0;
  int ii = 0;
  HctBuffer buf;
  const char *zTab = "!";
  const char *zSep = " ";

  assert( nArg==1 );
  memset(&buf, 0, sizeof(buf));

  nEntry = sqlite3_value_bytes(apArg[0]);
  aEntry = (const u8*)sqlite3_value_blob(apArg[0]);

  while( ii<nEntry ){
    switch( aEntry[ii++] ){
      case 'T': {
        zTab = (const char*)&aEntry[ii];
        ii += sqlite3Strlen30(zTab) + 1;
        break;
      }

      case 'i': {
        i64 iRowid = 0;
        i64 nByte = 0;
        char *zRec = 0;
        ii += sqlite3GetVarint(&aEntry[ii], (u64*)&iRowid);
        ii += sqlite3GetVarint(&aEntry[ii], (u64*)&nByte);
        zRec = sqlite3HctDbRecordToText(db, &aEntry[ii], (int)nByte);
        ii += nByte;
        hctBufferAppendIf(&buf, zSep);
        hctBufferAppend(&buf, "%s: ins %lld/(%s)", zTab, iRowid, zRec);
        sqlite3_free(zRec);
        break;
      }

      case 'I': {
        i64 nByte = 0;
        char *zRec = 0;
        ii += sqlite3GetVarint(&aEntry[ii], (u64*)&nByte);
        zRec = sqlite3HctDbRecordToText(db, &aEntry[ii], (int)nByte);
        ii += nByte;
        hctBufferAppendIf(&buf, zSep);
        hctBufferAppend(&buf, "%s: ins (%s)", zTab, zRec);
        sqlite3_free(zRec);
        break;
      }

      case 'D': {
        i64 nByte = 0;
        char *zRec = 0;
        ii += sqlite3GetVarint(&aEntry[ii], (u64*)&nByte);
        zRec = sqlite3HctDbRecordToText(db, &aEntry[ii], (int)nByte);
        ii += nByte;
        hctBufferAppendIf(&buf, zSep);
        hctBufferAppend(&buf, "%s: del (%s)", zTab, zRec);
        sqlite3_free(zRec);
        break;
      }

      case 'd': {
        i64 iRowid = 0;
        ii += sqlite3GetVarint(&aEntry[ii], (u64*)&iRowid);
        hctBufferAppendIf(&buf, zSep);
        hctBufferAppend(&buf, "%s: del %lld", zTab, iRowid);
        break;
      }

      default: {
        hctBufferAppend(&buf, "!!!");
        break;
      }
    }
  }

  sqlite3_result_text(pCtx, (const char*)buf.aBuf, -1, SQLITE_TRANSIENT);
  sqlite3HctBufferFree(&buf);
}

int sqlite3HctJrnlInit(sqlite3 *db){
  int rc = SQLITE_OK;
  rc = sqlite3_create_function(db, "hct_journal_entry", 1, SQLITE_UTF8, 0,
      hctJournalEntryFunc, 0, 0
  );
  return rc;
}
