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
#include <sys/mman.h>

#define HCT_DEFAULT_PAGESIZE     4096
// #define HCT_DEFAULT_PAGEPERCHUNK (128*1024)
#define HCT_DEFAULT_PAGEPERCHUNK 512

#define HCT_HEADER_PAGESIZE      4096

typedef struct HctFileServer HctFileServer;

/*
** Global variables for this module. Access is protected by
** SQLITE_MUTEX_STATIC_MAIN.
*/
static struct HctFileGlobal {
  HctFileServer *pServerList;
} g;

typedef struct HctChunkArray HctChunkArray;
struct HctChunkArray {
  int nAlloc;                     /* Allocated size of ap[] */
  int n;                          /* Valid entries in ap[] array */
  void **ap;                      /* Array of mapped database chunks */
};

struct HctFileServer {
  sqlite3_mutex *pMutex;          /* Mutex to protect this object */
  HctFile *pFileList;
  int fd;                         /* Read/write file descriptor */

  int szPage;                     /* Page size for database */
  void *pHdr;                     /* Pointer to mapping of db header pages */
  HctChunkArray chunks;

  i64 st_dev;                     /* File identification 1 */
  i64 st_ino;                     /* File identification 2 */
  HctFileServer *pServerNext;     /* Next object in g.pServerList list */
};

struct HctFile {
  HctFileServer *pServer;         /* Connection to global db object */
  HctFile *pFileNext;             /* Next handle opened on same file */

  /* Copies of HctFileServer variables */
  int szPage;
  HctChunkArray chunks;
};

static int hctFileGrowArray(int nReq, HctChunkArray *p){
  int rc = SQLITE_OK;
  if( nReq>p->nAlloc ){
    int nNew = p->nAlloc ? p->nAlloc : 4;
    void **apNew;

    while( nNew<nReq ) nNew = nNew*2;
    apNew = (void**)sqlite3_realloc(p->ap, sizeof(void*)*nNew);
    if( apNew ){
      memset(&apNew[p->nAlloc], 0, sizeof(void*)*(nNew-p->nAlloc));
      p->ap = apNew;
      p->nAlloc = nNew;
    }else{
      rc = SQLITE_NOMEM;
    }
  }
  return rc;
}

static void hctFileFreeArray(HctChunkArray *p){
  sqlite3_free(p->ap);
  p->n = 0;
  p->nAlloc = 0;
  p->ap = 0;
}

/*
** Return the number of bytes in a chunk of the file opened by the object
** passed as the only argument. This is calculated as:
**
**     (nPagePerChunk * 8) + (nPagePerChunk * szPage)
*/   
static i64 hctFileChunksize(HctFileServer *p){
  return (8 + p->szPage) * HCT_DEFAULT_PAGEPERCHUNK;
}

static int hctFileServerInit(HctFileServer *p){
  int rc = SQLITE_OK;
  int bNew = 0;
  i64 szFile;
  i64 szChunk;
  int nChunk;
  int i;
  struct stat sStat;

  assert( sqlite3_mutex_held(p->pMutex) );
  assert( p->pHdr==0 );

  if( fstat(p->fd, &sStat) ){
    return SQLITE_IOERR_FSTAT;
  }
  szFile = (i64)sStat.st_size;

  if( szFile==0 ){
    p->szPage = HCT_DEFAULT_PAGESIZE;
    szFile = HCT_HEADER_PAGESIZE*2 + hctFileChunksize(p);
    bNew = 1;
    if( ftruncate(p->fd, szFile) ){
      return SQLITE_IOERR_TRUNCATE;
    }
    /* TODO - write header pages */
  }else if( szFile<=HCT_HEADER_PAGESIZE*2 ){
    return SQLITE_NOTADB;
  }

  /* TODO - map and read stuff from the the header pages. */
  p->szPage = HCT_DEFAULT_PAGESIZE;

  /* Check that the file-size is as expected - must be a non-zero multiple
  ** of the chunk-size plus two header pages. If not, bail out here. */
  szChunk = hctFileChunksize(p);
  if( (szFile-HCT_HEADER_PAGESIZE*2) % szChunk ){
    return SQLITE_NOTADB;
  }

  nChunk = (szFile - HCT_HEADER_PAGESIZE*2) / szChunk;
  rc = hctFileGrowArray(nChunk, &p->chunks);
  for(i=0; i<nChunk && rc==SQLITE_OK; i++){
    void *pMap;
    off_t o = (off_t)(2*HCT_HEADER_PAGESIZE + i*szChunk);
    pMap = mmap(0, szChunk, PROT_READ|PROT_WRITE, MAP_SHARED, p->fd,o);
    if( pMap==MAP_FAILED ){
      rc = SQLITE_IOERR_MMAP;
    }
  }

  return rc;
}

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
        rc = SQLITE_CANTOPEN_BKPT;
      }else{
        fstat(fd, &sStat);
        pServer = (HctFileServer*)sqlite3_malloc(sizeof(*pServer));
        if( pServer==0 ){
          close(fd);
          rc = SQLITE_NOMEM_BKPT;
        }else{
          memset(pServer, 0, sizeof(*pServer));
          pServer->st_dev = (i64)sStat.st_dev;
          pServer->st_ino = (i64)sStat.st_ino;
          pServer->pServerNext = g.pServerList;
          pServer->fd = fd;
          pServer->pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);
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

    if( rc==SQLITE_OK ){
      sqlite3_mutex_enter(pServer->pMutex);
      if( pServer->chunks.n==0 ){
        rc = hctFileServerInit(pServer);
      }
      pNew->szPage = pServer->szPage;
      sqlite3_mutex_leave(pServer->pMutex);
    }

    if( rc!=SQLITE_OK ){
      sqlite3_free(pNew);
      pNew = 0;
    }
    *ppFile = pNew;
  }

  return rc;
}

void sqlite3HctFileClose(HctFile *pFile){
  if( pFile ){
    HctFileServer *pDel = 0;
    HctFile **pp;
    HctFileServer *pServer = pFile->pServer;
    hctFileFreeArray(&pFile->chunks);

    sqlite3_mutex_enter( sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MAIN) );
    for(pp=&pServer->pFileList; *pp!=pFile; pp=&(*pp)->pFileNext);
    *pp = pFile->pFileNext;
    if( pServer->pFileList==0 ){
      HctFileServer **ppS;
      pDel = pServer;
      for(ppS=&g.pServerList; *ppS!=pServer; ppS=&(*ppS)->pServerNext);
      *ppS = pServer->pServerNext;
    }
    sqlite3_mutex_leave( sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MAIN) );

    if( pDel ){
      int i;
      for(i=0; i<pDel->chunks.n; i++){
        if( pDel->chunks.ap[i] ){
          munmap(pDel->chunks.ap[i], hctFileChunksize(pDel));
        }
      }
      hctFileFreeArray(&pDel->chunks);
      if( pDel->pHdr ){
        munmap(pDel->pHdr, HCT_HEADER_PAGESIZE*2);
      }
      close(pDel->fd);
      sqlite3_mutex_free(pDel->pMutex);
      sqlite3_free(pDel);
    }

    sqlite3_free(pFile);
  }
}

int sqlite3HctFilePagesize(HctFile *pFile){
  return pFile->szPage;
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

