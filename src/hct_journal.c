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
    "schema_version BLOB,"                  \
    "hash BLOB,"                            \
    "tid INTEGER,"                          \
    "validcid INTEGER"                      \
");"


#define HCT_BASELINE_SCHEMA                 \
"CREATE TABLE sqlite_hct_baseline("         \
    "cid INTEGER,"                          \
    "schema_version BLOB,"                  \
    "hash BLOB"                             \
");"

/*
** In follower mode, it is not possible to call sqlite3_hct_journal_write()
** for the transaction with CID (N + HCT_MAX_LEADING_WRITE) until all 
** transactions with CID values of N or less have been committed.
*/
#define HCT_MAX_LEADING_WRITE 1024

typedef struct HctJrnlSchema HctJrnlSchema;
typedef struct HctJrnlServer HctJrnlServer;

/*
** One object of this type is shared by all connections to the same 
** database. Managed by the HctFileServer object (see functions
** sqlite3HctFileGetJrnlPtr() and SetJrnlPtr()).
**
** pSchema:
**   Head of linked-list of schema objects.
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
  HctJrnlSchema *pSchema;
  int eMode;
  u64 iSnapshot;
  int nCommit;
  u64 *aCommit;                   /* Array of size nCommit */
};

struct HctJrnlSchema {
  u8 aVersion[SQLITE_HCT_JOURNAL_HASHSIZE];
  u64 iCid;                       /* CID that created this new schema version */
  u64 iTid;                       /* TID that created it. */
  HctJrnlSchema *pPrev;
};

/*
** There is one instance of this structure for each database handle (HBtree*)
** open on a replication-enabled hctree database.
*/
struct HctJournal {
  u64 iJrnlRoot;                  /* Root page of journal table */
  u64 iBaseRoot;                  /* Base page of journal table */
  HctDatabase *pDb;
  HctJrnlServer *pServer;
};

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
    sqlite3ErrorWithMsg(db, SQLITE_ERROR, "open transaction on database");
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
      sqlite3ErrorWithMsg(db, SQLITE_ERROR, "not an hct database");
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
      sqlite3ErrorWithMsg(db, SQLITE_ERROR, "not an empty database");
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
        "INSERT INTO sqlite_hct_baseline VALUES(0, zeroblob(16), zeroblob(16));"
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
    sqlite3ErrorWithMsg(db, rc, "%s", zErr);
    sqlite3_free(zErr);
  }else{
    rc = sqlite3HctDetectJournals(db);
  }

  return rc;
}

/*
** Register a custom validation callback with the database handle.
*/
int sqlite3_hct_journal_validation_hook(
  sqlite3 *db,
  void *pArg,
  int(*xValidate)(
    void *pCopyOfArg,
    sqlite3_int64 iCid,
    const char *zSchema,
    const void *pData, int nData,
    const void *pSchemaVersion
  )
){
  db->xValidate = xValidate;
  db->pValidateArg = pArg;
  return SQLITE_OK;
}

#define MAX_6BYTE ((((i64)0x00008000)<<32)-1)

static int hctJrnlIntSize(u64 iVal){
  if( iVal<=127 ) return 1;
  if( iVal<=32767 ) return 2;
  if( iVal<=8388607 ) return 3;
  if( iVal<=2147483647 ) return 4;
  if( iVal<=MAX_6BYTE ) return 6;
  return 8;
}

static void hctJrnlIntPut(u8 *a, u64 iVal, int nByte){
  int i;
  for(i=1; i<=nByte; i++){
    a[nByte-i] = (iVal & 0xFF);
    iVal = (iVal >> 8);
  }
}

static u8 hctJrnlIntHdr(int nSize){
  if( nSize==8 ) return 6;
  if( nSize==6 ) return 5;
  return nSize;
}


static u8 *hctJrnlComposeRecord(
  u64 iCid,
  const char *zSchema,
  const u8 *pData, int nData,
  const u8 *pSchemaVersion,
  u64 iTid,
  int *pnRec
){
  u8 *pRec = 0;
  int nRec = 0;
  int nHdr = 0;
  int nBody = 0;
  int nSchema = 0;                /* Length of zSchema, in bytes */
  int nTidByte = 0;
  u8 aHash[SQLITE_HCT_JOURNAL_HASHSIZE];

  nSchema = sqlite3Strlen30(zSchema);
  nTidByte = hctJrnlIntSize(iTid);

  sqlite3_hct_journal_hashentry(
      aHash, iCid, zSchema, pData, nData, pSchemaVersion
  );

  /* First figure out how large the eventual record will be */
  nHdr = 1                                     /* size of header varint */
       + 1                                     /* "cid" - always NULL */
       + sqlite3VarintLen((nSchema * 2) + 13)  /* "schema" - TEXT */
       + sqlite3VarintLen((nData * 2) + 12)    /* "data" - BLOB */
       + 1                                     /* "schema_version" - BLOB */
       + 1                                     /* "hash" - BLOB */
       + 1                                     /* "tid" - INTEGER */
       + 1;                                    /* "validcid" - INTEGER */

  nBody = 0                                    /* "cid" - always NULL */
       + nSchema                               /* "schema" - TEXT */
       + nData                                 /* "data" - BLOB */
       + SQLITE_HCT_JOURNAL_HASHSIZE           /* "schema_version" - BLOB */
       + SQLITE_HCT_JOURNAL_HASHSIZE           /* "hash" - BLOB */
       + nTidByte                              /* "tid" - INTEGER */
       + 0;                                    /* "validcid" - INTEGER */

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

    /* "schema_version" field - SQLITE_HCT_JOURNAL_HASHSIZE byte BLOB */
    *pHdr++ = (u8)((SQLITE_HCT_JOURNAL_HASHSIZE * 2) + 12);
    memcpy(pBody, pSchemaVersion, SQLITE_HCT_JOURNAL_HASHSIZE);
    pBody += SQLITE_HCT_JOURNAL_HASHSIZE;

    /* "hash" field - SQLITE_HCT_JOURNAL_HASHSIZE byte BLOB */
    *pHdr++ = (u8)((SQLITE_HCT_JOURNAL_HASHSIZE * 2) + 12);
    memcpy(pBody, aHash, SQLITE_HCT_JOURNAL_HASHSIZE);
    pBody += SQLITE_HCT_JOURNAL_HASHSIZE;

    /* "tid" field - INTEGER */
    *pHdr++ = hctJrnlIntHdr(nTidByte);
    hctJrnlIntPut(pBody, iTid, nTidByte);
    pBody += nTidByte;

    /* "validcid" field - INTEGER */
    *pHdr++ = (u8)8;

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
  HctBuffer buf;
  HctBuffer schema;
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

static int hctBufferAppend(HctBuffer *pBuf, const char *zFmt, ...){
  va_list ap;
  char *zApp = 0;
  int nApp = 0;

  va_start(ap, zFmt);
  zApp = sqlite3_vmprintf(zFmt, ap);
  va_end(ap);

  nApp = sqlite3Strlen30(zApp);
  if( sqlite3HctBufferGrow(pBuf, pBuf->nBuf+nApp+1) ) return SQLITE_NOMEM;
  memcpy(&pBuf->aBuf[pBuf->nBuf], zApp, nApp+1);
  pBuf->nBuf += nApp;
  sqlite3_free(zApp);
  return SQLITE_OK;
}


static int hctJrnlLogTree(void *pCtx, u32 iRoot, KeyInfo *pKeyInfo){
  int rc = SQLITE_OK;
  JrnlCtx *pJrnl = (JrnlCtx*)pCtx;
  HctBuffer *pBuf = &pJrnl->buf;

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
        rc = hctBufferAppend(&pJrnl->schema, "%s%.*s", 
            (pJrnl->schema.nBuf>0 ? ";" : ""), nData, (const char*)aData
        );
      }
      sqlite3HctTreeCsrClose(pCsr);
    }
  }else{
    JrnlTree jrnltree;
    memset(&jrnltree, 0, sizeof(jrnltree));
    if( hctJrnlFindTree(pJrnl->pSchema, iRoot, &jrnltree) ){
      int nName = sqlite3Strlen30(jrnltree.zName);
  
      rc = sqlite3HctBufferGrow(pBuf, pBuf->nBuf+1+nName+1);
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
  
            rc = sqlite3HctBufferGrow(pBuf, pBuf->nBuf+1+9+9+nData);
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

/*
** Free the linked-list of HctJrnlSchema objects passed as the only argument.
*/
static void hctJrnlSchemaFree(HctJrnlSchema *p){
  HctJrnlSchema *pPrev = 0;
  HctJrnlSchema *pDel = 0;
  for(pDel=p; pDel; pDel=pPrev){
    pPrev = pDel->pPrev;
    sqlite3_free(pDel);
  }
}

/*
** Allocate and return a new schema object. Based on the current schema
** and the SQL script in parameter zSchema. 
**
** The returned object is not yet linked into the HctJournal object. That is
** done by hctJrnlLinkSchema(). If hctJrnlLinkSchema() is not called (e.g.
** because the validation callback failed), the returned schema object 
** should be freed with sqlite3_free().
*/
static HctJrnlSchema *hctJrnlSchemaNew(
  int *pRc,
  HctJournal *pJrnl,
  const char *zSchema,
  u64 iCid,
  u64 iTid
){
  HctJrnlSchema *pPrev = 0;
  HctJrnlSchema *pNew = 0;

  assert( zSchema && zSchema[0] );
  pPrev = HctAtomicLoad(&pJrnl->pServer->pSchema);

  pNew = (HctJrnlSchema*)sqlite3HctMalloc(pRc, sizeof(HctJrnlSchema));
  if( pNew ){
    memcpy(pNew->aVersion, pPrev->aVersion, SQLITE_HCT_JOURNAL_HASHSIZE);
    sqlite3_hct_journal_hashschema(pNew->aVersion, zSchema);
    pNew->iCid = iCid;
    pNew->iTid = iTid;
    pNew->pPrev = pPrev;
  }

  return pNew;
}

static HctJrnlSchema *hctJrnlSchemaFind(
  HctJournal *pJrnl,              /* Journal handle */
  u64 iCid
){
  HctJrnlSchema *p;
  p = HctAtomicLoad(&pJrnl->pServer->pSchema);
  while( p->iCid>iCid ) p = p->pPrev; 
  return p;
}

static void hctJrnlSchemaLink(
  HctJournal *pJrnl,              /* Journal handle */
  HctJrnlSchema *pSchema
){
  HctJrnlSchema *pLast = 0;
  i64 iSafeTid = sqlite3HctFileSafeTID(sqlite3HctDbFile(pJrnl->pDb));
  assert( pSchema->pPrev==HctAtomicLoad(&pJrnl->pServer->pSchema) );

  HctAtomicStore(&pJrnl->pServer->pSchema, pSchema);

  /* All current and future transactions include TID iSafeTid in their
  ** transactions. This means that no client - extant or future - will
  ** need to follow the HctJrnlSchema.pNext pointer from an object
  ** with (HctJrnlSchema.iTid<=iSafeTid). Any such objects can therefore
  ** all be freed now.  */
  for(pLast=pSchema; pLast->pPrev && pLast->iTid>iSafeTid; pLast=pLast->pPrev);
  assert( pLast->pPrev==0 || pLast->iTid<=iSafeTid );
  hctJrnlSchemaFree(pLast->pPrev);
  pLast->pPrev = 0;
}


static int hctJrnlWriteRecord(
  HctJournal *pJrnl,
  u64 iCid,
  const char *zSchema,
  const void *pData, int nData,
  const void *pSV,
  u64 iTid
){
  int rc = SQLITE_OK;
  u8 *pRec = 0;
  int nRec = 0;

  pRec = hctJrnlComposeRecord(iCid, zSchema, pData, nData, pSV, iTid, &nRec);
  if( pRec==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    int nRetry = 0;
    do {
      nRetry = 0;
      rc = sqlite3HctDbInsert(
          pJrnl->pDb, (u32)pJrnl->iJrnlRoot, 0, iCid, 0, nRec, pRec, &nRetry
      );
      assert( nRetry==0 );
      if( rc!=SQLITE_OK ) break;
      rc = sqlite3HctDbInsertFlush(pJrnl->pDb, &nRetry);
      if( rc!=SQLITE_OK ) break;
    }while( nRetry );
  }
  sqlite3_free(pRec);

  return rc;
}

int sqlite3HctJrnlWriteEmpty(HctJournal *pJrnl, u64 iCid, u64 iTid){
  HctJrnlSchema *pJSchema = 0;
  int rc = SQLITE_OK;

  pJSchema = HctAtomicLoad(&pJrnl->pServer->pSchema);
  while( pJSchema->iCid>iCid ) pJSchema = pJSchema->pPrev;

  sqlite3HctDbJournalRbMode(pJrnl->pDb, 1);
  rc = hctJrnlWriteRecord(pJrnl, iCid, "", 0, 0, pJSchema->aVersion, iTid);
  sqlite3HctDbJournalRbMode(pJrnl->pDb, 0);
  return rc;
}

int sqlite3HctJrnlLog(
  HctJournal *pJrnl,
  sqlite3 *db,
  Schema *pSchema,
  u64 iCid,
  u64 iTid,
  HctTree *pTree,
  HctDatabase *pDb
){
  int rc = SQLITE_OK;
  JrnlCtx jrnlctx;
  HctJrnlSchema *pJSchema = 0;
  const char *zSchema = 0;

  memset(&jrnlctx, 0, sizeof(jrnlctx));
  jrnlctx.pSchema = pSchema;
  jrnlctx.pTree = pTree;

  rc = sqlite3HctTreeForeach(pTree, 1, (void*)&jrnlctx, hctJrnlLogTree);
  zSchema = (jrnlctx.schema.nBuf ? (const char*)jrnlctx.schema.aBuf : "");

  if( zSchema[0] ){
    pJSchema = hctJrnlSchemaNew(&rc, pJrnl, zSchema, iCid, iTid);
  }else{
    pJSchema = hctJrnlSchemaFind(pJrnl, iCid);
  }

  if( rc==SQLITE_OK ){
    rc = hctJrnlWriteRecord(pJrnl, iCid, zSchema, 
        jrnlctx.buf.aBuf, jrnlctx.buf.nBuf, pJSchema->aVersion, iTid
    );
  }

  /* If one is registered, invoke the validation hook */
  if( rc==SQLITE_OK && db->xValidate ){
    int res = db->xValidate(db->pValidateArg, iCid, zSchema, 
        jrnlctx.buf.aBuf, jrnlctx.buf.nBuf, pJSchema->aVersion
    );
    if( res!=0 ){
      rc = SQLITE_BUSY_SNAPSHOT;
    }
  }

  if( zSchema[0] ){
    if( rc==SQLITE_OK ){
      hctJrnlSchemaLink(pJrnl, pJSchema);
    }else{
      sqlite3_free(pJSchema);
    }
  }

  sqlite3HctBufferFree(&jrnlctx.buf);
  sqlite3HctBufferFree(&jrnlctx.schema);
  return rc;
}

static void hctJrnlDelServer(void *p){
  if( p ){
    HctJrnlServer *pServer = (HctJrnlServer*)p;
    hctJrnlSchemaFree(pServer->pSchema);
    sqlite3_free(pServer->aCommit);
    sqlite3_free(pServer);
  }
}

typedef struct HctJournalRecord HctJournalRecord;
struct HctJournalRecord {
  i64 iCid;
  const char *zSchema; int nSchema;
  const void *pData; int nData;
  const void *pSchemaVersion;
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
  u8 aSchemaVersion[SQLITE_HCT_JOURNAL_HASHSIZE];
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
    
    /* "schema_version" field - SQLITE_HCT_JOURNAL_HASHSIZE byte BLOB */
    pRec->pSchemaVersion = (const void*)hctJrnlReadBlob(&rc, &rdr, &nHash);
    if( nHash!=SQLITE_HCT_JOURNAL_HASHSIZE ) rc = SQLITE_CORRUPT_BKPT;

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

      /* "hash" field - SQLITE_HCT_JOURNAL_HASHSIZE byte BLOB */
      hctJrnlReadHash(&rc, &rdr, pRec->aHash);

      /* "schema_version" field - SQLITE_HCT_JOURNAL_HASHSIZE byte BLOB */
      hctJrnlReadHash(&rc, &rdr, pRec->aSchemaVersion);
    }
  }
  sqlite3HctDbCsrClose(pCsr);

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
  HctJrnlSchema *pSchema = 0;
  HctFile *pFile = sqlite3HctDbFile(pDb);
  int rc = SQLITE_OK;
  HctDbCsr *pCsr = 0;

  i64 iMaxCid = 0;                
  u8 aSchema[SQLITE_HCT_JOURNAL_HASHSIZE];
  memset(aSchema, 0, sizeof(aSchema));

  /* Read the contents of the sqlite_hct_baseline table. */
  rc = hctJrnlReadBaseline(pJrnl, &base);

  /* Allocate the new HctJrnlServer structure */
  pServer = (HctJrnlServer*)sqlite3HctMalloc(&rc, sizeof(HctJrnlServer));

  /* Read the last record of the sqlite_hct_journal table. Specifically,
  ** the value of fields "cid" and "schema_version". Store these values
  ** in stack variables iCid and aSchema, respectively.  Or, if the
  ** sqlite_hct_journal table is empty, populate iCid and aSchema[] with
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
        memcpy(aSchema, rec.pSchemaVersion, SQLITE_HCT_JOURNAL_HASHSIZE);
      }
    }else{
      iMaxCid = base.iCid;
      memcpy(aSchema, base.aSchemaVersion, SQLITE_HCT_JOURNAL_HASHSIZE);
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

  pSchema = (HctJrnlSchema*)sqlite3HctMalloc(&rc, sizeof(HctJrnlSchema));
  if( rc==SQLITE_OK ){
    memcpy(pSchema->aVersion, aSchema, SQLITE_HCT_JOURNAL_HASHSIZE);
    pServer->pSchema = pSchema;
    sqlite3HctFileSetJrnlPtr(pFile, (void*)pServer, hctJrnlDelServer);
    pJrnl->pServer = pServer;
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
      pNew->pServer = (HctJrnlServer*)sqlite3HctFileGetJrnlPtr(pFile);
      *pp = pNew;
    }
  }

  return rc;
}

void sqlite3HctJournalClose(HctJournal *pJrnl){
  sqlite3_free(pJrnl);
}

int sqlite3HctJournalIsTable(HctJournal *pJrnl, u64 iTable){
  return (pJrnl && (pJrnl->iJrnlRoot==iTable || pJrnl->iBaseRoot==iTable));
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
    for(rc=sqlite3HctDbCsrLast(pCsr);
        iDel==0 && rc==SQLITE_OK && sqlite3HctDbCsrEof(pCsr)==0;
        rc=sqlite3HctDbCsrPrev(pCsr)
    ){
      hctJrnlReadJournalRecord(pCsr, &rec);
      if( rec.iTid==iTid ) iDel = rec.iCid;
    }

    if( iDel!=0 ){
      const void *pSV = 0;
      if( rec.nSchema==0 ){
        pSV = rec.pSchemaVersion;
      }else{
        rc = sqlite3HctDbCsrPrev(pCsr);
        if( rc==SQLITE_OK ){
          if( 0==sqlite3HctDbCsrEof(pCsr) ){
            hctJrnlReadJournalRecord(pCsr, &rec);
            pSV = rec.pSchemaVersion;
          }else{
            assert( !"todo" );
            /* read schema version from sqlite_hct_baseline */
          }
        }
      }
      if( rc==SQLITE_OK ){
        rc = hctJrnlWriteRecord(pJrnl, iDel, "", 0, 0, pSV, iTid);
      }
    }

    sqlite3HctDbCsrClose(pCsr);
  }

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
