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


/*
** Pagemap slots used for special purposes.
*/
#define HCT_PAGEMAP_LOGICAL_EOF      2
#define HCT_PAGEMAP_PHYSICAL_EOF     3

#define HCT_PGMAPFLAG_PHYSICAL_USED  (((u64)0x00000001)<<32)


/*
** Page types. These are the values that may appear in the page-type
** field of a page header.
*/
#define HCT_PAGETYPE_INTKEY_LEAF 0x01
#define HCT_PAGETYPE_INTKEY_NODE 0x02
#define HCT_PAGETYPE_INDEX_LEAF  0x03
#define HCT_PAGETYPE_INDEX_NODE  0x04
#define HCT_PAGETYPE_OVERFLOW    0x05


typedef struct HctFileServer HctFileServer;
typedef struct HctMapping HctMapping;
typedef struct HctDatabasePage HctDatabasePage;

/* 
** Structure used to create root page of sqlite_schema.
*/
typedef struct HctDatabasePage {
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

/*
** Global variables for this module. Access is protected by
** SQLITE_MUTEX_STATIC_MAIN.
*/
static struct HctFileGlobal {
  HctFileServer *pServerList;
} g;


/*
** nRef:
**   Number of references to this object held by the system. The
**   HctFileServer object may hold one reference, each HctFile may
**   also hold one.
**
** iLogPPC:
**   Log2 of number of pages-per-chunk. e.g. if there are 512 pages
**   on each mapping chunk, this value is set to 9.
*/
struct HctMapping {
  int nRef;                       /* Number of pointers to this array */
  int szPage;                     /* Size of pages in bytes */
  int iLogPPC;                    /* Log2 of pages-per-chunk */
  int n;                          /* Valid entries in ap[] array */
  void **ap;                      /* Array of mapped database chunks */
};

struct HctFileServer {
  sqlite3_mutex *pMutex;          /* Mutex to protect this object */
  HctFile *pFileList;
  int fd;                         /* Read/write file descriptor */

  int szPage;                     /* Page size for database */
  void *pHdr;                     /* Pointer to mapping of db header pages */
  HctMapping *pMapping;

  i64 st_dev;                     /* File identification 1 */
  i64 st_ino;                     /* File identification 2 */
  HctFileServer *pServerNext;     /* Next object in g.pServerList list */
};

struct HctFile {
  HctFileServer *pServer;         /* Connection to global db object */
  HctFile *pFileNext;             /* Next handle opened on same file */

  /* Copies of HctFileServer variables */
  int szPage;
  HctMapping *pMapping;
};

static int hctLog2(int n){
  int i;
  assert( (n & (n-1))==0 );
  for(i=0; (1<<i)<n; i++);
  return i;
}

/*
** Allocate and return a new HctMapping object with enough space for
** nChunk chunks.
*/
static HctMapping *hctMappingNew(HctMapping *pOld, int nChunk){
  HctMapping *pNew;
  int nByte = sizeof(HctMapping) + nChunk*sizeof(void*);

  pNew = (HctMapping*)sqlite3MallocZero(nByte);
  if( pNew ){
    pNew->ap = (void**)&pNew[1];
    pNew->nRef = 1;
    pNew->n = nChunk;
    if( pOld ){
      assert( nChunk>pOld->n );
      pNew->iLogPPC = pOld->iLogPPC;
      pNew->szPage = pOld->szPage;
      memcpy(pNew->ap, pOld->ap, pOld->n * sizeof(void*));
    }
  }
  return pNew;
}

static void hctMappingUnref(HctMapping *p){
  if( p ){
    p->nRef--;
    if( p->nRef==0 ){
      sqlite3_free(p);
    }
  }
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

static u64 *hctPagemapPtr(HctMapping *p, u32 iSlot){
  return &((u64*)(p->ap[(iSlot-1)>>p->iLogPPC]))[(iSlot-1)&((1<<p->iLogPPC)-1)];
}

static void *hctPagePtr(HctMapping *p, u32 iPhys){
  return (void*)&((u8*)(p->ap[(iPhys-1)>>p->iLogPPC]))[
    (1<<p->iLogPPC)*8 + ((iPhys-1) & ((1<<p->iLogPPC)-1)) * p->szPage
  ];
}

static void hctFileInitRootpage(HctMapping *p, u32 iPg, u8 eType){
  void *pPage;
  HctDatabasePage *pDbPage;
  assert( eType==HCT_PAGETYPE_INTKEY_LEAF || eType==HCT_PAGETYPE_INDEX_LEAF );

  pPage = hctPagePtr(p, iPg);
  memset(pPage, 0, p->szPage);
  pDbPage = (HctDatabasePage*)pPage;
  /* TODO: Initialize recovery header fields! */
  pDbPage->ePagetype = eType;
}

/*
** Use a CAS instruction to the value of page-map slot iSlot. Return true
** if the slot is successfully set to value iNew, or false otherwise.
*/
static int hctFilePagemapSet(HctMapping *p, u32 iSlot, u64 iOld, u64 iNew){
  u64 *pPtr = hctPagemapPtr(p, iSlot);
  return (int)(__sync_bool_compare_and_swap(pPtr, iOld, iNew));
}

static u64 hctFilePagemapGet(HctMapping *p, u32 iSlot){
  return *(hctPagemapPtr(p, iSlot));
}

static int hctFileServerInit(HctFileServer *p){
  int rc = SQLITE_OK;
  int bNew = 0;
  HctMapping *pMapping = 0;
  i64 szFile;
  i64 szChunk;
  int nChunk;
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

  /* Create the initial HctMapping object and map all chunks of the file. */
  nChunk = (szFile - HCT_HEADER_PAGESIZE*2) / szChunk;
  p->pMapping = pMapping = hctMappingNew(0, nChunk);
  if( pMapping==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    int i;
    pMapping->iLogPPC = hctLog2(HCT_DEFAULT_PAGEPERCHUNK);
    pMapping->szPage = p->szPage;
    for(i=0; i<nChunk && rc==SQLITE_OK; i++){
      void *pMap;
      off_t o = (off_t)(2*HCT_HEADER_PAGESIZE + i*szChunk);
      pMap = mmap(0, szChunk, PROT_READ|PROT_WRITE, MAP_SHARED, p->fd,o);
      if( pMap==MAP_FAILED ){
        rc = SQLITE_IOERR_MMAP;
      }
      pMapping->ap[i] = pMap;
    }
  }

  /* If this is a new database: 
  **
  **   1. Make logical page 1 an empty intkey root page (SQLite uses this
  **      as the root of sqlite_schema).
  **
  **   2. Set the initial values of the largest logical and physical page 
  **      ids allocated fields (page-map slots 2 and 3).
  **
  ** TODO: Initialize contents of header pages.
  */
  if( rc==SQLITE_OK && bNew ){
    hctFilePagemapSet(pMapping, HCT_PAGEMAP_LOGICAL_EOF, 0, 32);
    hctFilePagemapSet(pMapping, HCT_PAGEMAP_PHYSICAL_EOF, 0, 1);
    hctFilePagemapSet(pMapping, 1, 0, 1|HCT_PGMAPFLAG_PHYSICAL_USED);
    hctFileInitRootpage(pMapping, 1, HCT_PAGETYPE_INTKEY_LEAF);
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
      if( pServer->pMapping==0 ){
        rc = hctFileServerInit(pServer);
      }
      pNew->szPage = pServer->szPage;
      pNew->pMapping = pServer->pMapping;
      pNew->pMapping->nRef++;
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

    /* Release the reference to the HctMapping object, if any */
    hctMappingUnref(pFile->pMapping);
    pFile->pMapping = 0;

    /* Remove this object from the HctFileServer.pFileList list. If this
    ** means there are no longer any connections to this server object,
    ** remove the HctFileServer object itself from the global list. In
    ** this case leave stack variable pDel set to point to the 
    ** HctFileServer.  */
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

    /* It if was removed from the global list, clean up the HctFileServer
    ** object.  */
    if( pDel ){
      int i;
      HctMapping *pMapping = pDel->pMapping;
      pDel->pMapping = 0;
      for(i=0; i<pMapping->n; i++){
        if( pMapping->ap[i] ){
          munmap(pMapping->ap[i], hctFileChunksize(pDel));
        }
      }
      hctMappingUnref(pMapping);
      if( pDel->pHdr ){
        munmap(pDel->pHdr, HCT_HEADER_PAGESIZE*2);
      }
      close(pDel->fd);
      sqlite3_mutex_free(pDel->pMutex);
      sqlite3_free(pDel);
    }

    /* Finally, free the HctFile object */
    sqlite3_free(pFile);
  }
}

int sqlite3HctFilePagesize(HctFile *pFile){
  return pFile->szPage;
}

/*
** Allocate a new logical or physical page id by incrementing the page-map
** field. This is a stop-gap only - we already know allocating pages this 
** way creates too much contention.
*/
static int hctFileTmpAllocate(HctFile *pFile, int eType, u32 *piNew){
  HctMapping *pMapping = pFile->pMapping;
  u64 iVal;
  assert( eType==HCT_PAGEMAP_LOGICAL_EOF || eType==HCT_PAGEMAP_PHYSICAL_EOF );
  while( 1 ){
    iVal = hctFilePagemapGet(pMapping, eType);
    if( (iVal & 0xFFFFFFFF)==0xFFFFFFFF ){
      return SQLITE_FULL;
    }
    if( hctFilePagemapSet(pMapping, eType, iVal, iVal+1) ){
      break;
    }
  }
  *piNew = (iVal & 0xFFFFFFFF)+1;
  return SQLITE_OK;
}


int sqlite3HctFileRootNew(HctFile *pFile, u32 *piRoot){
  return hctFileTmpAllocate(pFile, HCT_PAGEMAP_LOGICAL_EOF, piRoot);
}

int sqlite3HctFileRootFree(HctFile *pFile, u32 iRoot){
  /* TODO - do something with freed root-page */
  return SQLITE_OK;
}

/*
** Argument iRoot is a logical page id allocated by an earlier call to
** sqlite3HctFileRootNew(). This function allocates and populates a 
** physical page to use to store it.
*/
int sqlite3HctFileRootInit(HctFile *pFile, u32 iRoot){
  u32 iPg;
  int rc;

  rc = hctFileTmpAllocate(pFile, HCT_PAGEMAP_PHYSICAL_EOF, &iPg);
  if( rc==SQLITE_OK ){
    u64 iVal;
    HctDatabasePage *pDbPage = (HctDatabasePage*)hctPagePtr(p, iPg);
    memset(pDbPage, 0, p->szPage);
    pDbPage->ePagetype = HCT_PAGETYPE_INTKEY_LEAF;

    while( 1 ){
      u64 iVal = hctFilePagemapGet(pFile->pMapping, iRoot);
      u64 iNew = (iVal&HCT_PGMAPFLAG_PHYSICAL_USED) | iPg;
      if( hctFilePagemapSet(pFile->pMapping, iRoot, iVal, iNew) ) break;
    }
  }
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

