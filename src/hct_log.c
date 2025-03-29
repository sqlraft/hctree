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

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

typedef struct HctLogFile HctLogFile;

struct HctLogFile {
  int iId;
  char *zPath;
  int fd;
  u64 iLastCid;
};

/*
** pCloseNext:
**   This is used in FOLLOWER mode only. In FOLLOWER mode, a connection may
**   be closed immediately after writing a non-contiguous transaction to
**   the database and journal. In this case it is not safe to unlink() the 
**   log file until all all prior transactions (those with smaller CID values)
**   have been written to the journal. This variable - pCloseNext - is part
**   of the mechanism that allows the tmap module to do that.
*/
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

  u8 aLogptr[16];
  HctLog *pCloseNext;
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
  u64 iSafeCid;
  int rc = SQLITE_OK;
  int iFile = pLog->iFile;
  int iFileOff = pLog->iFileOff;

  if( sqlite3HctJrnlMode(pLog->pJrnl)==SQLITE_HCT_FOLLOWER ){
    iSafeCid = sqlite3HctJrnlSnapshot(pLog->pJrnl);
  }else{
    iSafeCid = LARGEST_UINT64;
  }

  /* Determine where and in which log file to write this transaction */
  if( pLog->iFileOff==0 || pLog->aFile[iFile].iLastCid<=iSafeCid ){
    iFileOff = JRNL_HDR_SIZE;
  }else if( pLog->aFile[!iFile].iLastCid<=iSafeCid ){
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
  int rc = SQLITE_OK;
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
  int rc = SQLITE_OK;

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
    }
  }

  return rc;
}

void sqlite3HctLogSetCid(HctLog *pLog, u64 iCid){
  HctLogFile *p = &pLog->aFile[pLog->iFile];
  p->iLastCid = MAX(p->iLastCid, iCid);
}

int sqlite3HctLogZero(HctLog *pLog){
  const u8 aZero[9] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  int rc = SQLITE_OK;
  HctLogFile *p = &pLog->aFile[pLog->iFile];

  lseek(p->fd, 0, SEEK_SET);
  if( write(p->fd, aZero, sizeof(aZero))!=sizeof(aZero) ){
    rc = sqlite3HctIoerr(SQLITE_IOERR_WRITE);
  }
  return rc;
}

/*
** Close the HctLog object pLog. In LEADER or NORMAL mode, the log file may
** be deleted immediately. But in FOLLOWER mode, it may contain entries
** that may yet be rolled back. So in this case, pass the HctLog object to
** the journal module to be closed when it is safe to do so.
*/
void sqlite3HctLogClose(HctLog *pLog){
  if( pLog ){
    int ii;
    int eMode = sqlite3HctJrnlMode(pLog->pJrnl);
    int bDefer = 0;

    /* Check if these log files can be unlinked immediately. They can be
    ** unlinked immediately unless (a) the journal is in FOLLOWER mode, 
    ** and (b) the log files contain one or more transactions from the
    ** non-contiguous region of the journal. */
    if( eMode==SQLITE_HCT_FOLLOWER ){
      u64 iSafeCid = sqlite3HctJrnlSnapshot(pLog->pJrnl);
      if( iSafeCid<pLog->aFile[0].iLastCid
       || iSafeCid<pLog->aFile[1].iLastCid
      ){
        bDefer = 1;
      }
    }

    for(ii=0; ii<2; ii++){
      HctLogFile *p = &pLog->aFile[ii];
      if( p->fd>=0 ) close(p->fd);
      if( p->zPath && bDefer==0 ){
        // unlink(p->zPath);
        sqlite3_free(p->zPath);
      }
    }
    sqlite3_free(pLog->aBuf);
    pLog->aBuf = 0;

    if( bDefer ){
      void **pLogPtrPtr = sqlite3HctJrnlLogPtrPtr(pLog->pJrnl);
      HctLog **ppList = (HctLog**)pLogPtrPtr;
      pLog->pCloseNext = *ppList;
      *ppList = pLog;
      sqlite3HctJrnlLogPtrPtrRelease(pLog->pJrnl);
    }else{
      sqlite3_free(pLog);
    }
  }
}

int sqlite3HctLogPointer(HctLog *pLog, u64 iTid, int *pnRef, const u8 **paRef){
  int eMode = sqlite3HctJrnlMode(pLog->pJrnl);

  if( eMode==SQLITE_HCT_NORMAL ){
    *pnRef = 0;
    *paRef = 0;
  }else{
    memcpy(pLog->aLogptr, &iTid, sizeof(u64));
    *paRef = pLog->aLogptr;
    if( eMode==SQLITE_HCT_FOLLOWER ){
      HctLogFile *p = &pLog->aFile[pLog->iFile];
      memcpy(&pLog->aLogptr[8], &p->iId, sizeof(u32));
      memcpy(&pLog->aLogptr[12], &pLog->iFileBeginOff, sizeof(u32));
      *pnRef = 16;
    }else{
      *pnRef = 8;
    }
  }

  return SQLITE_OK;
}

/*
** Argument fd is an open file-descriptor. This function attempts to 
** determine the current size of the open file in bytes. 
*/
static int hctLogFileSize(int *pRc, int fd){
  int ret = 0;
  if( *pRc==SQLITE_OK ){
    struct stat sStat;
    memset(&sStat, 0, sizeof(sStat));
    fstat(fd, &sStat);
    ret = (int)sStat.st_size;
  }
  return ret;
}

/*
**
*/
static int hctLogIsComplete(const u8 *aData, int *pnData){
  int iOff = 0;
  i64 iRoot = 0;
  int sz = 0;
  int nRead = *pnData;

  while( 1 ){
    if( (nRead-iOff)<8 ) return 0;

    memcpy(&iRoot, (const void*)&aData[iOff], sizeof(iRoot));
    iOff += sizeof(iRoot);
    if( iRoot==0 ){
      *pnData = iOff;
      return 1;
    }
    if( (nRead-iOff)<4 ) return 0;

    memcpy(&sz, (const void*)&aData[iOff], sizeof(sz));
    iOff += sizeof(sz);
    if( sz==0xFFFFFFFF ){
      sz = 8;
    }
    iOff += sz;
  }
}

/*
** 
*/
int sqlite3HctLogLoadData(
  HctLog *pLog,                   /* Log object */
  int nPtr, const u8 *aPtr,       /* Pointer from sqlite3HctLogPointer() */
  i64 *piTid,                     /* OUT: TID value extracted from pointer */
  int *pnData, u8 **paData        /* OUT: Contents of requested log */
){
  int rc = SQLITE_OK;
  i64 iTid = 0;
  int iId = 0;
  int iOff = 0;
  char *zFile = 0;
  int fd = -1;

  assert( nPtr==(sizeof(iTid) + sizeof(iId) + sizeof(iOff)) );

  memcpy(&iTid, aPtr, sizeof(iTid));
  memcpy(&iId, &aPtr[8], sizeof(iId));
  memcpy(&iOff, &aPtr[12], sizeof(iOff));

  zFile = sqlite3HctFileLogFileName(pLog->pFile, iId);
  if( !zFile ) return SQLITE_NOMEM_BKPT;

  fd = open(zFile, O_RDONLY);
  sqlite3_free(zFile);
  if( fd<0 ){
    rc = SQLITE_CANTOPEN_BKPT;
  }else{
    int sz = hctLogFileSize(&rc, fd);

    if( rc==SQLITE_OK ){
      int nRead = 0;
      u8 *aData = 0;
      lseek(fd, iOff, SEEK_SET);
      do {
        int nExtra = MIN((nRead ? nRead : 1024), sz-nRead);
        int n;
        aData = sqlite3Realloc(aData, nRead+nExtra);
        n = read(fd, &aData[nRead], nExtra);
        if( n<=0 ){
          rc = SQLITE_IOERR_READ;
          break;
        }
        nRead += n;
      }while( hctLogIsComplete(aData, &nRead)==0 );

      if( rc==SQLITE_OK ){
        *paData = aData;
        *pnData = nRead;
        *piTid = iTid;
      }else{
        sqlite3_free(aData);
      }
    }

    close(fd);
  }
  return rc;
}

void *sqlite3HctLogFindLogToFree(void **pLogPtrPtr, u64 iSafeCid){
  HctLog **ppLog = (HctLog**)pLogPtrPtr;
  HctLog *pRet = 0;

  while( *ppLog ){
    HctLog *pLog = *ppLog;
    if( pLog->aFile[0].iLastCid<=iSafeCid
     && pLog->aFile[1].iLastCid<=iSafeCid
    ){
      *ppLog = pLog->pCloseNext;
      pLog->pCloseNext = pRet;
      pRet = pLog;
    }else{
      ppLog = &pLog->pCloseNext;
    }
  }

  return (void*)pRet;
}

void sqlite3HctLogFree(void *pLogList, int bUnlink){
  HctLog *p = pLogList;
  while( p ){
    int ii;
    HctLog *pFree = p;
    p = p->pCloseNext;
    for(ii=0; ii<2; ii++){
      HctLogFile *pFile = &pFree->aFile[ii];
      if( pFile->zPath ){
        if( bUnlink && 0 ) unlink(pFile->zPath);
        sqlite3_free(pFile->zPath);
      }
    }
    sqlite3_free(pFree);
  }
}

