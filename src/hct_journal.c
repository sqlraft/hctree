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

int sqlite3HctJrnlLog(
  sqlite3 *db,
  u64 iCid,
  u64 iTid,
  HctTree *pTree,
  u64 iJrnlRoot,
  HctDatabase *pDb
){
  u8 *pRec = 0;
  int nRec = 0;
  int rc = SQLITE_OK;

  pRec = hctJrnlComposeRecord("schema!", 0, 0, iTid, &nRec);
  if( !pRec ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
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

