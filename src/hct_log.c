/*
** 2025 March 19
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

typedef struct HctLogFile HctLogFile;

struct HctLogFile {
  int iId;
  char *zPath;
  int fd;
  u64 iLastTid;
};

struct HctLog {
  HctFile *pFile;
  HctJournal *pJrnl;

  HctLogFile aFile[2];

  int iFile;                      /* Current file to write to */
  int iFileOff;                   /* Current offset of current file */
  int iFileBeginOff;              /* Offset at Begin() */

  u8 *aBuf;                       /* malloc'd buffer for writing log file */
  int nBuf;                       /* Size of aBuf[] in bytes */
  int iBufOff;
};

#define JRNL_HDR_SIZE 12

#define JRNL_BUFFER_SIZE 4096

/*
** Allocate a new HctLog object.
*/
int sqlite3HctLogNew(
  HctFile *pFile, 
  HctJournal *pJrnl,
  HctLog **ppLog
){
  HctLog *pNew = 0;
  int rc = SQLITE_OK;

  pNew = (HctLog*)sqlite3HctMalloc(&rc, sizeof(HctLog));
  if( pNew ){
    pNew->aFile[0].fd = -1;
    pNew->aFile[1].fd = -1;

    pNew->pFile = pFile;
    pNew->pJrnl = pJrnl;

    pNew->aBuf = (u8*)sqlite3HctMalloc(&rc, JRNL_BUFFER_SIZE);
    pNew->nBuf = JRNL_BUFFER_SIZE;
    if( rc!=SQLITE_OK ){
      sqlite3_free(pNew);
      pNew = 0;
    }
  }

  *ppLog = pNew;
  return rc;
}

int sqlite3HctLogBegin(HctLog *pLog){
  int iFile = pLog->iFile;
  int iFileOff = pLog->iFileOff;

  /* Determine where and in which log file to write this transaction */
  if( pLog->iFileOff==0 
   || sqlite3HctJrnlIsSafe(pLog->pJrnl, pLog->aFile[iFile].iLastTid)
  ){
    iFileOff = JRNL_HDR_SIZE;
  }else if( sqlite3HctJrnlIsSafe(pLog->pJrnl, pLog->aFile[!iFile].iLastTid) ){
    iFile = !iFile;
    iFileOff = JRNL_HDR_SIZE;
  }

  /* Check the selected log file is already open. Open it if not. */
  if( pLog->aFile[iFile].zPath==0 ){
    HctLogFile *p = &pLog->aFile[iFile];
    p->iId = sqlite3HctFileLogFileId(pLog->pFile, iFile);
    p->zPath = sqlite3HctFileLogFileName(pLog->pFile, p->iId);
    if( p->zPath==0 ){
      rc = SQLITE_NOMEM_BKPT;
    }else{
      p->fd = open(p->zPath, O_CREAT|O_RDWR, 0644);
      if( p->fd<0 ){
        sqlite3_free(p->zPath);
        p->zPath = 0;
        rc = sqlite3HctIoerr(SQLITE_IOERR);
      }
    }
  }

  if( rc==SQLITE_OK ){
    pLog->iFile = iFile;
    pLog->iFileBeginOff = pLog->iFileOff = iFileOff;
  }

  return rc;
}

static int hctLogFlushBuffer(HctLog *pLog){
  int rc = SQLITE_OK;
  if( pLog->iBufOff>0 ){
    HctLogFile *p = &pLog->aFile[pLog->iFile];
    lseek(p->fd, pLog->iFileOff, SEEK_SET);
    if( write(p->fd, pLog->aBuf, pLog->iBufOff)!=pLog->iBufOff ){
      rc = sqlite3HctIoerr(SQLITE_IOERR_WRITE);
    }else{
      pLog->iFileOff += pLog->iBufOff;
      pLog->iBufOff = 0;
    }
  }
  return rc;
}

static int hctLogPutU64(u8 *aBuf, u64 val){
  memcpy(aBuf, &val, sizeof(val));
  return sizeof(val);
}
static int hctLogPutU32(u8 *aBuf, u32 val){
  memcpy(aBuf, &val, sizeof(val));
  return sizeof(val);
}

static int hctLogWriteBuffer(HctLog *pLog, int nData, const u8 *aData){
  int nRem = nData;
  do {
    int nCopy = MIN(nRem, (pLog->nBuf - pLog->iBufOff));
    if( nCopy>0 ){
      memcpy(&pLog->aBuf[pLog->iBufOff], &aData[nData - nRem], nCopy);
      pLog->iBufOff += nCopy;
      nRem -= nCopy;
    }
    if( nRem==0 ) break;
    rc = hctLogFlushBuffer(pLog);
  }while( rc==SQLITE_OK );
  return rc;
}

int sqlite3HctLogRecord(HctLog *pLog, i64 iRoot, i64 nData, const u8 *aData){
  if( (pLog->nBuf-pLog->iBufOff)<(8 + 4 + 8) ){
    rc = hctLogFlushBuffer(pLog);
    if( rc!=SQLITE_OK ) return rc;
  }

  pLog->iBufOff += hctLogPutU64(&pLog->aBuf[pLog->iBufOff], (u64)iRoot);
  if( aData ){
    pLog->iBufOff += hctLogPutU32(&pLog->aBuf[pLog->iBufOff], (u32)nData);
    rc = hctLogWriteBuffer(pLog, (int)nData, aData);
  }else{
    pLog->iBufOff += hctLogPutU32(&pLog->aBuf[pLog->iBufOff], 0xFFFFFFFF);
    pLog->iBufOff += hctLogPutU64(&pLog->aBuf[pLog->iBufOff], (u64)nData);
  }

  return rc;
}

int sqlite3HctLogFinish(HctLog *pLog, u64 iTid){
  const u8 aEnd[9] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  HctLogFile *p = &pLog->aFile[pLog->iFile];
  int rc = SQLITE_OK;

  /* Write the 8-byte 0 value to end the transaction */
  rc = hctLogWriteBuffer(pLog, sizeof(aEnd), aEnd);

  if( rc==SQLITE_OK ){
    rc = hctLogFlushBuffer(pLog);
  }

  /* Write the log file header to make the transaction live */
  if( rc==SQLITE_OK ){
    u8 aBuf[12];
    hctLogPutU64(aBuf, iTid);
    hctLogPutU32(&aBuf[8], (u32)pLog->iFileBeginOff);
    lseek(p->fd, 0, SEEK_SET);
    if( write(p->fd, aBuf, sizeof(aBuf))!=sizeof(aBuf) ){
      rc = sqlite3HctIoerr(SQLITE_IOERR_WRITE);
    }else{
      p->iLastTid = iTid;
    }
  }

  return rc;
}

int sqlite3HctLogZero(HctLog *pLog){
  const u8 aZero[9] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  HctLogFile *p = &pLog->aFile[pLog->iFile];

  lseek(p->fd, 0, SEEK_SET);
  if( write(p->fd, aZero, sizeof(aZero))!=sizeof(aZero) ){
    rc = sqlite3HctIoerr(SQLITE_IOERR_WRITE);
  }
  p->iLastTid = 0;
  return rc;
}

void sqlite3HctLogClose(HctLog *pLog){
  int ii;
  for(ii=0; ii<2; ii++){
    HctLogFile *p = &pLog->aFile[ii];
    if( p->fd>=0 ) close(p->fd);
    if( p->zPath ){
      unlink(p->zPath);
    }
  }
  sqlite3_free(pLog->aBuf);
  sqlite3_free(pLog);
}


