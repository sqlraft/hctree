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
#include <string.h>
#include <assert.h>

typedef struct HctDatabase HctDatabase;
typedef struct HctDatabasePage HctDatabasePage;

struct HctDatabase {
  HctFile *pFile;
  int pgsz;                       /* Page size in bytes */
  u64 iTid;                       /* Current transaction id (or zero) */
};

/* 
** Structure used for all database pages.
*/
struct HctDatabasePage {
  /* Recovery header fields */
  u64 iCksum;
  u64 iTid;
  u64 iLargestTid;
  u32 iLogicId;
  u32 iPrevId;

  /* Page header fields */
  u8 ePagetype;
  u8 unused;
  u16 nEntry;
  u32 iPeerPg;
};

int sqlite3HctDbOpen(const char *zFile, HctDatabase **ppDb){
  int rc = SQLITE_OK;
  HctDatabase *pNew;

  pNew = (HctDatabase*)sqlite3MallocZero(sizeof(*pNew));
  if( pNew ){
    rc = sqlite3HctFileOpen(zFile, &pNew->pFile);
  }else{
    rc = SQLITE_NOMEM_BKPT;
  }

  if( rc!=SQLITE_OK ){
    sqlite3HctDbClose(pNew);
    pNew = 0;
  }

  *ppDb = pNew;
  return rc;
}

void sqlite3HctDbClose(HctDatabase *p){
  if( p ){
    sqlite3HctFileClose(p->pFile);
    p->pFile = 0;
    sqlite3_free(p);
  }
}

int sqlite3HctDbRootNew(HctDatabase *p, u32 *piRoot){
  return sqlite3HctFileRootNew(p->pFile, piRoot);
}

int sqlite3HctDbRootFree(HctDatabase *p, u32 iRoot){
  return sqlite3HctFileRootFree(p->pFile, iRoot);
}

static void hctDbStartTrans(HctDatabase *p){
  if( p->iTid==0 ){
    p->iTid = sqlite3HctFileStartTrans(p->pFile);
  }
}

int sqlite3HctDbRootInit(HctDatabase *p, int bIndex, u32 iRoot){
  HctFilePage pg;
  int rc;

  hctDbStartTrans(p);
  rc = sqlite3HctFilePageNew(p->pFile, iRoot, &pg);
  if( rc==SQLITE_OK ){
    HctDatabasePage *pPg = (HctDatabasePage*)pg.aNew;
    assert( pg.aOld==0 && pg.aNew!=0 );
    memset(pg.aNew, 0, p->pgsz);
    pPg->ePagetype = bIndex?HCT_PAGETYPE_INDEX_LEAF:HCT_PAGETYPE_INTKEY_LEAF;
    rc = sqlite3HctFilePageRelease(&pg);
  }
  return rc;
}

int sqlite3HctDbInsert(
  HctDatabase *p, 
  u32 iRoot, 
  UnpackedRecord *pUnpacked, 
  i64 iKey, 
  int nData, const u8 *aData
){
  hctDbStartTrans(p);
}

int sqlite3HctDbDelete(
  HctDatabase *p, 
  u32 iRoot, 
  UnpackedRecord *pUnpacked, 
  i64 iKey
){
  hctDbStartTrans(p);
}

int sqlite3HctDbCommit(HctDatabase *p){
  p->iTid = 0;
  return SQLITE_OK;
}

