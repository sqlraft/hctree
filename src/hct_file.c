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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/unistd.h>
#include <fcntl.h>

#define HCT_DEFAULT_PAGESIZE 4096

typedef struct HctFileServer HctFileServer;

/*
** Global variables for this module. Access is protected by
** SQLITE_MUTEX_STATIC_MAIN.
*/
static struct HctFileGlobal {
  HctFileServer *pServerList;
} g;

struct HctFileServer {
  sqlite3_mutex *pMutex;          /* Mutex to protect this object */
  HctFile *pFileList;
  int fd;                         /* Read/write file descriptor */

  i64 st_dev;                     /* File identification 1 */
  i64 st_ino;                     /* File identification 2 */
  HctFileServer *pServerNext;
};

struct HctFile {
  HctFileServer *pServer;         /* Connection to global db object */
  HctFile *pFileNext;             /* Next handle opened on same file */
};

int sqlite3HctFileOpen(const char *zFile, HctFile **ppFile){
  int rc = SQLITE_OK;
  HctFile *pNew;

  pNew = (HctFile*)sqlite3_malloc(sizeof(*pNew));
  if( pNew==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    HctFileServer *pServer = 0;
    struct stat sStat;
    memset(pNew, 0, sizeof(*pNew));
    memset(&sStat, 0, sizeof(sStat));

    sqlite3_mutex_enter(sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MAIN));
    if( 0==stat(zFile, &sStat) ){
      for(pServer=g.pServerList; pServer; pServer=pServer->pServerNext){
        if( pServer->st_ino==(i64)sStat.st_ino
         && pServer->st_dev==(i64)sStat.st_dev
        ){
          break;
        }
      }
    }

    if( pServer==0 ){
      int fd = open(zFile, O_CREAT|O_RDWR, 0644);
      if( fd<0 ){
        rc = SQLITE_IOERR;
      }else{
        fstat(fd, &sStat);
        pServer = (HctFileServer*)sqlite3_malloc(sizeof(*pServer));
        if( pServer==0 ){
          rc = SQLITE_NOMEM_BKPT;
        }else{
          memset(pServer, 0, sizeof(*pServer));
          pServer->st_dev = (i64)sStat.st_dev;
          pServer->st_ino = (i64)sStat.st_ino;
          pServer->pServerNext = g.pServerList;
          g.pServerList = pServer;
        }
      }
    }

    if( rc==SQLITE_OK ){
      pNew->pServer = pServer;
      pNew->pFileNext = pServer->pFileList;
      pServer->pFileList = pNew;
    }
    sqlite3_mutex_leave(sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MAIN));

    if( rc!=SQLITE_OK ){
      sqlite3_free(pNew);
      pNew = 0;
    }
    *ppFile = pNew;
  }

  return rc;
}

void sqlite3HctFileClose(HctFile *pFile){
  assert( 0 );
}

int sqlite3HctFilePagesize(HctFile *pFile){
  return HCT_DEFAULT_PAGESIZE;
}

int sqlite3HctFileRootNew(HctFile *pFile, u32 *piRoot){
  assert( 0 );
  return SQLITE_OK;
}

int sqlite3HctFileRootFree(HctFile *pFile, u32 iRoot){
  assert( 0 );
  return SQLITE_OK;
}

int sqlite3HctFilePageGet(HctFile *pFile, u32 iPg, HctFilePage *pPg){
  assert( 0 );
  return SQLITE_OK;
}

int sqlite3HctFilePageNew(HctFile *pFile, HctFilePage *pPg){
  assert( 0 );
  return SQLITE_OK;
}

int sqlite3HctFilePageWrite(HctFilePage *pPg){
  assert( 0 );
  return SQLITE_OK;
}

int sqlite3HctFilePageDelete(HctFilePage *pPg){
  assert( 0 );
  return SQLITE_OK;
}

int sqlite3HctFilePageRelease(HctFilePage *pPg){
  assert( 0 );
  return SQLITE_OK;
}

