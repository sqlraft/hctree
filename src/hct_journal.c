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
    sqlite3HctDetectJournals(db);
  }

  return rc;
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
  u64 iTid,
  int *pnRec
){
  u8 *pRec = 0;
  int nRec = 0;
  int nHdr = 0;
  int nBody = 0;
  int nSchema = 0;                /* Length of zSchema, in bytes */
  int nTidByte = 0;

  nSchema = sqlite3Strlen30(zSchema);
  nTidByte = hctJrnlIntSize(iTid);

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
    memcpy(pBody, zSchema, nSchema);
    pBody += nSchema;

    /* "data" field - BLOB */
    pHdr += sqlite3PutVarint(pHdr, (nData*2) + 12);
    memcpy(pBody, pData, nData);
    pBody += nData;

    /* "schema_version" field - SQLITE_HCT_JOURNAL_HASHSIZE byte BLOB */
    *pHdr++ = (u8)((SQLITE_HCT_JOURNAL_HASHSIZE * 2) + 12);
    memset(pBody, 0, SQLITE_HCT_JOURNAL_HASHSIZE);
    pBody += SQLITE_HCT_JOURNAL_HASHSIZE;

    /* "hash" field - SQLITE_HCT_JOURNAL_HASHSIZE byte BLOB */
    *pHdr++ = (u8)((SQLITE_HCT_JOURNAL_HASHSIZE * 2) + 12);
    memset(pBody, 0, SQLITE_HCT_JOURNAL_HASHSIZE);
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

int hctJrnlLogTree(void *pCtx, u32 iRoot, KeyInfo *pKeyInfo){
  int rc = SQLITE_OK;
  JrnlCtx *pJrnl = (JrnlCtx*)pCtx;
  HctBuffer *pBuf = &pJrnl->buf;
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

  return rc;
}

int sqlite3HctJrnlLog(
  sqlite3 *db,
  Schema *pSchema,
  u64 iCid,
  u64 iTid,
  HctTree *pTree,
  u64 iJrnlRoot,
  HctDatabase *pDb
){
  u8 *pRec = 0;
  int nRec = 0;
  int rc = SQLITE_OK;
  JrnlCtx jrnlctx;

  memset(&jrnlctx, 0, sizeof(jrnlctx));
  jrnlctx.pSchema = pSchema;
  jrnlctx.pTree = pTree;

  rc = sqlite3HctTreeForeach(pTree, (void*)&jrnlctx, hctJrnlLogTree);
  if( rc==SQLITE_OK ){
    pRec = hctJrnlComposeRecord(
        iCid, "", jrnlctx.buf.aBuf, jrnlctx.buf.nBuf, iTid, &nRec
    );
    if( !pRec ) rc = SQLITE_NOMEM_BKPT;
  }

  if( rc==SQLITE_OK ){
    int nRetry = 0;
    do {
      nRetry = 0;
      rc = sqlite3HctDbInsert(pDb, iJrnlRoot, 0, iCid, 0, nRec, pRec, &nRetry);
      assert( nRetry==0 );
      if( rc!=SQLITE_OK ) break;
      rc = sqlite3HctDbInsertFlush(pDb, &nRetry);
      if( rc!=SQLITE_OK ) break;
    }while( nRetry );
  }

  sqlite3HctBufferFree(&jrnlctx.buf);
  return rc;
}

int sqlite3HctJrnlRecovery(HctDatabase *pDb, u64 iJrnlRoot){
  int rc = SQLITE_OK;
  HctDbCsr *pCsr = 0;

  rc = sqlite3HctDbCsrOpen(pDb, 0, (u32)iJrnlRoot, &pCsr);
  if( rc==SQLITE_OK ){
    rc = sqlite3HctDbCsrLast(pCsr);
    if( rc==SQLITE_OK && sqlite3HctDbCsrEof(pCsr)==0 ){
      HctFile *pFile = sqlite3HctDbFile(pDb);
      u64 iCid = 0;
      sqlite3HctDbCsrKey(pCsr, (i64*)&iCid);
      sqlite3HctFileSetCID(pFile, iCid);
    }
    sqlite3HctDbCsrClose(pCsr);
  }

  return rc;
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

static int hctBufferAppendIf(HctBuffer *pBuf, const char *zSep){
  if( pBuf->nBuf>0 ){
    hctBufferAppend(pBuf, "%s", zSep);
  }
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

