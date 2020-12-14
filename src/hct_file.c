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

// #define HCT_DEFAULT_PAGESIZE     512
#define HCT_DEFAULT_PAGESIZE     4096

// #define HCT_DEFAULT_PAGEPERCHUNK (128*1024)
#define HCT_DEFAULT_PAGEPERCHUNK 512

#define HCT_HEADER_PAGESIZE      4096


#ifndef HCT_TID_MASK
# define HCT_TID_MASK     ((((u64)0x00FFFFFF)<<32)|0xFFFFFFFF)
#endif

#define HCT_PGNO_MASK     (u64)0xFFFFFFFF

/*
** Pagemap slots used for special purposes.
*/
#define HCT_PAGEMAP_LOGICAL_EOF      2
#define HCT_PAGEMAP_PHYSICAL_EOF     3
#define HCT_PAGEMAP_TRANSID_EOF      4
#define HCT_PAGEMAP_COMMITID         5

#define HCT_PGMAPFLAG_PHYSINUSE  (((u64)0x00000001)<<56)


typedef struct HctFileServer HctFileServer;
typedef struct HctMapping HctMapping;
typedef struct HctMappingChunk HctMappingChunk;

/*
** Global variables for this module. Access is protected by
** SQLITE_MUTEX_STATIC_VFS1.
*/
static struct HctFileGlobalVars {
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
**
** aPagemap/nPagemap:
**   Mapping of the current page-map file.
*/
struct HctMappingChunk {
  void *pData;                    /* Mapping of chunk in data file */
  u64  *aMap;                     /* Mapping of chunk in map file */
};
struct HctMapping {
  int nRef;                       /* Number of pointers to this array */
  int szPage;                     /* Size of pages in bytes */
  int nChunk;                     /* Size of aChunk[] array */
  u32 mapShift;
  u32 mapMask;
  HctMappingChunk *aChunk;        /* Array of database chunk mappings */
};

struct HctFileServer {
  sqlite3_mutex *pMutex;          /* Mutex to protect this object */
  HctFile *pFileList;

  HctFileGlobal *pGlobal;
  void (*xGFree)(HctFileGlobal*);

  int fdDb;                       /* Read/write file descriptor */
  int fdMap;                      /* Read/write file descriptor for page-map */
  int fdHdr;                      /* Read/write file descriptor for hdr file */

  int szPage;                     /* Page size for database */
  int nPagePerChunk;
  void *pHdr;                     /* Pointer to mapping of db header pages */
  HctMapping *pMapping;           /* Mapping of pagemap and db pages */

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
static HctMapping *hctMappingNew(int *pRc, HctMapping *pOld, int nChunk){
  HctMapping *pNew = 0;
  if( *pRc==SQLITE_OK ){
    int nByte = sizeof(HctMapping) + nChunk*sizeof(HctMappingChunk);
    pNew = (HctMapping*)sqlite3MallocZero(nByte);
    if( pNew ){
      pNew->aChunk = (HctMappingChunk*)&pNew[1];
      pNew->nRef = 1;
      pNew->nChunk = nChunk;
      if( pOld ){
        assert( nChunk>pOld->nChunk );
        pNew->mapShift = pOld->mapShift;
        pNew->mapMask = pOld->mapMask;
        pNew->szPage = pOld->szPage;
        memcpy(pNew->aChunk,pOld->aChunk,pOld->nChunk*sizeof(HctMappingChunk));
      }
    }else{
      *pRc = SQLITE_NOMEM_BKPT;
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


static u64 *hctPagemapPtr(HctMapping *p, u32 iSlot){
  return &(p->aChunk[(iSlot-1) >> p->mapShift].aMap[(iSlot-1) & p->mapMask]);
}

static void *hctPagePtr(HctMapping *p, u32 iPhys){
  return &((u8*)(p->aChunk[(iPhys-1) >> p->mapShift].pData))[
    ((iPhys-1) & p->mapMask) * p->szPage
  ];
}

static void hctFileInitRootpage(HctMapping *p, u32 iPg, u8 bIndex){
  sqlite3HctDbRootPageInit(bIndex, hctPagePtr(p, iPg), p->szPage);
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

/*
** Increment the value in slot iSlot by nIncr. Return the new value.
*/
static u64 hctFilePagemapIncr(HctMapping *p, u32 iSlot, int nIncr){
  u64 *pPtr = hctPagemapPtr(p, iSlot);
  u64 iOld;
  do {
    iOld = *pPtr;
  }while( 0==(__sync_bool_compare_and_swap(pPtr, iOld, iOld+nIncr)) );
  return iOld+nIncr;
}

static int hctFilePagemapSetLogical(
  HctMapping *p, 
  u32 iSlot, 
  u64 iOld, 
  u64 iNew
){
  while( 1 ){
    u64 i1 = hctFilePagemapGet(p, iSlot);
    u64 iOld1 = (iOld&~HCT_PGMAPFLAG_PHYSINUSE) | (i1&HCT_PGMAPFLAG_PHYSINUSE);
    u64 iNew1 = (iNew&~HCT_PGMAPFLAG_PHYSINUSE) | (i1&HCT_PGMAPFLAG_PHYSINUSE);

    if( hctFilePagemapSet(p, iSlot, iOld1, iNew1) ) return 1;
    if( i1!=iOld1 ) return 0;
  }

  assert( !"unreachable" );
  return 0;
}

static int hctFileOpen(int *pRc, const char *zFile, const char *zPost){
  int fd = 0;
  if( *pRc==SQLITE_OK ){
    char *zPath = sqlite3_mprintf("%s%s", zFile, zPost);
    if( zPath==0 ){
      *pRc = SQLITE_NOMEM_BKPT;
    }else{
      fd = open(zPath, O_CREAT|O_RDWR, 0644);
      if( fd<0 ){
        *pRc = SQLITE_CANTOPEN_BKPT;
      }
      sqlite3_free(zPath);
    }
  }
  return fd;
}

/*
** Argument fd is an open file-handle. Return the size of the file in bytes.
**
** This function is a no-op (returns 0) if *pRc is other than SQLITE_OK 
** when it is called. If an error occurs, *pRc is set to an SQLite error
** code before returning.
*/
static i64 hctFileSize(int *pRc, int fd){
  i64 szRet = 0;
  if( *pRc==SQLITE_OK ){
    struct stat sStat;
    if( fstat(fd, &sStat) ){
      *pRc = SQLITE_IOERR_FSTAT;
    }else{
      szRet = (i64)(sStat.st_size);
    }
  }
  return szRet;
}

static int hctFileTruncate(int *pRc, int fd, i64 sz){
  if( *pRc==SQLITE_OK ){
    int res = ftruncate(fd, (off_t)sz);
    if( res ) *pRc = SQLITE_IOERR_TRUNCATE;
  }
  return *pRc;
}

static void *hctFileMmap(int *pRc, int fd, i64 nByte, int iChunk){
  void *pRet = 0;
  if( *pRc==SQLITE_OK ){
    pRet = mmap(0, nByte, PROT_READ|PROT_WRITE, MAP_SHARED, fd, iChunk*nByte);
    if( pRet==MAP_FAILED ){
      pRet = 0;
      *pRc = SQLITE_IOERR_MMAP;
    }
  }
  return pRet;
}

static int hctFileServerInit(HctFileServer *p, const char *zFile){
  int rc = SQLITE_OK;
  assert( sqlite3_mutex_held(p->pMutex) );
  if( p->pMapping==0 ){
    HctMapping *pMapping = 0;
    i64 szHdr;                    /* Size of header file */
    i64 szData;                   /* Size of data file */
    i64 szMap;                    /* Size of pagemap file */
    int nChunk = 0;               /* Number of chunks in database */
    int i;

    i64 szChunkData;
    i64 szChunkPagemap;

    /* Open the data and page-map files */
    p->fdDb = hctFileOpen(&rc, zFile, "-data");
    p->fdMap = hctFileOpen(&rc, zFile, "-pagemap");

    /* If the header file is zero bytes in size, the database is empty -
    ** regardless of the contents of the *-data or *-pagemap file. Truncate
    ** the pagemap to zero bytes in size to make sure of this. Also,
    ** initialize a new, empty, header file. */
    szHdr = hctFileSize(&rc, p->fdHdr);
    if( rc==SQLITE_OK && szHdr==0 ){
      hctFileTruncate(&rc, p->fdMap, 0);
      hctFileTruncate(&rc, p->fdDb, 0);
      hctFileTruncate(&rc, p->fdHdr, HCT_HEADER_PAGESIZE*2);
      /* TODO - initialize header pages */
    }

    /* TODO - map and read stuff from the the header pages. */
    p->szPage = HCT_DEFAULT_PAGESIZE;
    p->nPagePerChunk = HCT_DEFAULT_PAGESIZE;
    szChunkData = p->nPagePerChunk * p->szPage;
    szChunkPagemap = p->nPagePerChunk * sizeof(u64);

    szData = hctFileSize(&rc, p->fdDb);
    szMap = hctFileSize(&rc, p->fdMap);
    if( rc==SQLITE_OK ){
      if( (szData % szChunkData)
       || (szMap % szChunkPagemap)
       || (szData != p->szPage*(szMap/sizeof(u64)))
      ){
        rc = SQLITE_CANTOPEN_BKPT;
      }else{
        nChunk = szMap / szChunkPagemap;
      }
    }

    /* Create the initial mapping. For a new database, this will be zero
    ** chunks.  */
    if( nChunk==0 ){ 
      hctFileTruncate(&rc, p->fdMap, szChunkPagemap);
      hctFileTruncate(&rc, p->fdDb, szChunkData);
      nChunk = 1;
    }
    p->pMapping = pMapping = hctMappingNew(&rc, 0, nChunk);
    if( rc==SQLITE_OK ){
      pMapping->mapShift = hctLog2(HCT_DEFAULT_PAGEPERCHUNK);
      pMapping->mapMask = (1<<pMapping->mapShift)-1;
      pMapping->szPage = p->szPage;
    }
    for(i=0; rc==SQLITE_OK && i<nChunk; i++){
      pMapping->aChunk[i].pData = hctFileMmap(&rc, p->fdDb, szChunkData, i);
      pMapping->aChunk[i].aMap = hctFileMmap(&rc, p->fdMap, szChunkPagemap, i);
    }

    /* If this is a new database: 
    **
    **   1. Make logical page 1 an empty intkey root page (SQLite uses this
    **      as the root of sqlite_schema).
    **
    **   2. Set the initial values of the largest logical and physical page 
    **      ids allocated fields (page-map slots 2 and 3).
    */
    if( rc==SQLITE_OK && hctFilePagemapGet(pMapping, 1)==0 ){
      hctFilePagemapSet(pMapping, HCT_PAGEMAP_LOGICAL_EOF, 0, 32);
      hctFilePagemapSet(pMapping, HCT_PAGEMAP_PHYSICAL_EOF, 0, 1);
      hctFilePagemapSet(pMapping, 1, 0, 1|HCT_PGMAPFLAG_PHYSINUSE);
      hctFileInitRootpage(pMapping, 1, 0);
    }
  }
  return rc;
}

/*
** This is called to ensure that the mapping currently held by client
** pFile contains at least nChunk chunks.
*/
static int hctFileGrowMapping(HctFile *pFile, int nChunk){
  int rc = SQLITE_OK;
  if( pFile->pMapping->nChunk<nChunk ){
    HctFileServer *p = pFile->pServer;
    HctMapping *pOld;
    sqlite3_mutex_enter(p->pMutex);
    hctMappingUnref(pFile->pMapping);
    pFile->pMapping = 0;
    pOld = p->pMapping;
    if( pOld->nChunk<nChunk ){
      HctMapping *pNew = hctMappingNew(&rc, pOld, nChunk);
      if( pNew ){
        i64 szChunkData = p->nPagePerChunk*p->szPage;
        i64 szChunkMap = p->nPagePerChunk*sizeof(u64);
        int i;

        /* Grow the data and mapping files */
        hctFileTruncate(&rc, p->fdDb, nChunk*szChunkData);
        hctFileTruncate(&rc, p->fdMap, nChunk*szChunkMap);
        for(i=pOld->nChunk; i<nChunk; i++){
          pNew->aChunk[i].aMap = hctFileMmap(&rc, p->fdMap, szChunkMap, i);
          pNew->aChunk[i].pData = hctFileMmap(&rc, p->fdDb, szChunkData, i);
        }

        if( rc==SQLITE_OK ){
          p->pMapping = pNew;
          hctMappingUnref(pOld);
        }else{
          hctMappingUnref(pNew);
        }
      }
    }
    pFile->pMapping = p->pMapping;
    pFile->pMapping->nRef++;
    sqlite3_mutex_leave(p->pMutex);
  }
  return rc;
}

static int hctFileServerFind(HctFile *pFile, const char *zFile){
  int rc = SQLITE_OK;
  struct stat sStat;
  HctFileServer *pServer = 0;
  sqlite3_mutex *pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_VFS1);

  memset(&sStat, 0, sizeof(sStat));

  /* Take the VFS1 mutex that protects the globals in this file */
  sqlite3_mutex_enter(pMutex);

  /* Search for an existing HctFileServer already open on this database */
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
    int fd = hctFileOpen(&rc, zFile, "");
    if( rc==SQLITE_OK ){
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
        pServer->fdHdr = fd;
        pServer->pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);
        g.pServerList = pServer;
      }
    }
  }

  if( rc==SQLITE_OK ){
    pFile->pServer = pServer;
    pFile->pFileNext = pServer->pFileList;
    pServer->pFileList = pFile;
  }

  /* Release the global mutex */
  sqlite3_mutex_leave(pMutex);

  return rc;
}

/*
** Open a connection to the database zFile.
*/
int sqlite3HctFileOpen(const char *zFile, HctFile **ppFile){
  int rc = SQLITE_OK;
  HctFile *pNew;

  pNew = (HctFile*)sqlite3_malloc(sizeof(*pNew));
  if( pNew==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    memset(pNew, 0, sizeof(*pNew));

    rc = hctFileServerFind(pNew, zFile);
    if( rc==SQLITE_OK ){
      HctFileServer *pServer = pNew->pServer;
      sqlite3_mutex_enter(pServer->pMutex);
      rc = hctFileServerInit(pServer, zFile);
      if( rc==SQLITE_OK ){
        pNew->szPage = pServer->szPage;
        pNew->pMapping = pServer->pMapping;
        pNew->pMapping->nRef++;
      }
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

HctFileGlobal *sqlite3HctFileGlobal(
  HctFile *pFile,
  int nGlobal,
  void (*xDelete)(HctFileGlobal*)
){
  HctFileServer *pServer = pFile->pServer;
  HctFileGlobal *pRet = 0;
  sqlite3_mutex_enter(pServer->pMutex);
  pRet = pServer->pGlobal;
  if( pRet==0 ){
    pRet = sqlite3_malloc(sizeof(HctFileGlobal) + nGlobal);
    if( pRet ){
      memset(pRet, 0, sizeof(HctFileGlobal) + nGlobal);
      pRet->pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);
      if( pRet->pMutex==0 ){
        sqlite3_free(pRet);
        pRet = 0;
      }else{
        pServer->pGlobal = pRet;
        pServer->xGFree = xDelete;
      }
    }
  }
  sqlite3_mutex_leave(pServer->pMutex);
  return pRet;
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
    sqlite3_mutex_enter( sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_VFS1) );
    for(pp=&pServer->pFileList; *pp!=pFile; pp=&(*pp)->pFileNext);
    *pp = pFile->pFileNext;
    if( pServer->pFileList==0 ){
      HctFileServer **ppS;
      pDel = pServer;
      for(ppS=&g.pServerList; *ppS!=pServer; ppS=&(*ppS)->pServerNext);
      *ppS = pServer->pServerNext;
    }
    sqlite3_mutex_leave( sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_VFS1) );

    /* It if was removed from the global list, clean up the HctFileServer
    ** object.  */
    if( pDel ){
      int szChunkData = pDel->nPagePerChunk*pDel->szPage;
      int szChunkMap = pDel->nPagePerChunk*sizeof(u64);
      int i;
      HctMapping *pMapping = pDel->pMapping;
      pDel->pMapping = 0;
      for(i=0; i<pMapping->nChunk; i++){
        HctMappingChunk *pChunk = &pMapping->aChunk[i];
        if( pChunk->aMap ) munmap(pChunk->aMap, szChunkMap);
        if( pChunk->pData ) munmap(pChunk->pData, szChunkData);
      }
      hctMappingUnref(pMapping);

      if( pDel->pGlobal ){
        pDel->xGFree(pDel->pGlobal);
        sqlite3_mutex_free(pDel->pGlobal->pMutex);
        sqlite3_free(pDel->pGlobal);
        pDel->pGlobal = 0;
      }

      if( pDel->pHdr ){
        munmap(pDel->pHdr, HCT_HEADER_PAGESIZE*2);
      }
      if( pDel->fdHdr ) close(pDel->fdHdr);
      if( pDel->fdMap ) close(pDel->fdMap);
      if( pDel->fdDb ) close(pDel->fdDb);
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

u32 sqlite3HctFileMaxpage(HctFile *pFile){
  u64 iVal = hctFilePagemapGet(pFile->pMapping, HCT_PAGEMAP_LOGICAL_EOF);
  return (iVal & 0xFFFFFFFF);
}

/*
** Allocate a new logical or physical page id by incrementing the page-map
** field. This is a stop-gap only - we already know allocating pages this 
** way creates too much contention.
*/
static int hctFileTmpAllocate(HctFile *pFile, int eType, u32 *piNew){
  HctMapping *pMapping = pFile->pMapping;
  u64 iVal;
  int rc = SQLITE_OK;
  assert( eType==HCT_PAGEMAP_LOGICAL_EOF 
       || eType==HCT_PAGEMAP_PHYSICAL_EOF 
       || eType==HCT_PAGEMAP_TRANSID_EOF 
  );
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
  if( eType==HCT_PAGEMAP_LOGICAL_EOF || eType==HCT_PAGEMAP_PHYSICAL_EOF ){
    rc = hctFileGrowMapping(pFile, 1 + ((*piNew-1) / HCT_DEFAULT_PAGEPERCHUNK));
  }
  return rc;
}

static int hctFileSetFlag(HctFile *pFile, u32 iSlot, u64 mask){
  int rc;

  assert( iSlot>0 );
  rc = hctFileGrowMapping(pFile, 1 + ((iSlot-1) / HCT_DEFAULT_PAGEPERCHUNK));
  if( rc==SQLITE_OK ){
    HctMapping *pMapping = pFile->pMapping;
    while( 1 ){
      u64 iVal = hctFilePagemapGet(pMapping, iSlot);
      if( hctFilePagemapSet(pMapping, iSlot, iVal, iVal | mask) ) break;
    }
  }
  return rc;
}

int sqlite3HctFileRootNew(HctFile *pFile, u32 *piRoot){
  return hctFileTmpAllocate(pFile, HCT_PAGEMAP_LOGICAL_EOF, piRoot);
}

int sqlite3HctFileRootFree(HctFile *pFile, u32 iRoot){
  /* TODO - do something with freed root-page */
  return SQLITE_OK;
}

int sqlite3HctFilePageGet(HctFile *pFile, u32 iPg, HctFilePage *pPg){
  assert( iPg!=0 );
  memset(pPg, 0, sizeof(*pPg));
  pPg->pFile = pFile;
  pPg->iPg = iPg;
  pPg->iPagemap = hctFilePagemapGet(pFile->pMapping, iPg);
  pPg->aOld = (u8*)hctPagePtr(pFile->pMapping, (pPg->iPagemap & 0xFFFFFFFF));
  return SQLITE_OK;
}

/*
** Obtain a reference to physical page iPg. This is presently used by
** the virtual table interfaces only.
*/
int sqlite3HctFilePageGetPhysical(HctFile *pFile, u32 iPg, HctFilePage *pPg){
  u64 iVal;
  assert( iPg!=0 );
  memset(pPg, 0, sizeof(*pPg));
  iVal = hctFilePagemapGet(pFile->pMapping, iPg);
  if( iVal & HCT_PGMAPFLAG_PHYSINUSE ){
    pPg->iPagemap = iVal;
    pPg->aOld = (u8*)hctPagePtr(pFile->pMapping, iPg);
  }
  return SQLITE_OK;
}

/*
** Allocate a new logical page. If parameter iPg is zero, then a new
** logical page number is allocated. Otherwise, it must be a logical page
** number obtained by an earlier call to sqlite3HctFileRootNew().
*/
int sqlite3HctFilePageNew(HctFile *pFile, u32 iPg, HctFilePage *pPg){
  int rc = SQLITE_OK;             /* Return code */
  u32 iNewPg;                     /* New physical page id */
  u32 iLPg = iPg;

  if( iPg==0 ){
    rc = hctFileTmpAllocate(pFile, HCT_PAGEMAP_LOGICAL_EOF, &iLPg);
  }
  if( rc==SQLITE_OK ){
    memset(pPg, 0, sizeof(*pPg));
    rc = hctFileTmpAllocate(pFile, HCT_PAGEMAP_PHYSICAL_EOF, &iNewPg);
    if( rc==SQLITE_OK ){
      rc = hctFileSetFlag(pFile, iNewPg, HCT_PGMAPFLAG_PHYSINUSE);
      pPg->iPg = iLPg;
      pPg->iNewPg = iNewPg;
      pPg->aNew = (u8*)hctPagePtr(pFile->pMapping, iNewPg);
      pPg->iPagemap = hctFilePagemapGet(pFile->pMapping, iLPg);
      pPg->pFile = pFile;
    }
  }

  return rc;
}

int sqlite3HctFilePageWrite(HctFilePage *pPg){
  int rc = SQLITE_OK;             /* Return code */

  if( pPg->aNew==0 ){
    u32 iNewPg;                     /* New physical page id */
    rc = hctFileTmpAllocate(pPg->pFile, HCT_PAGEMAP_PHYSICAL_EOF, &iNewPg);
    if( rc==SQLITE_OK ){
      hctFileSetFlag(pPg->pFile, iNewPg, HCT_PGMAPFLAG_PHYSINUSE);
      pPg->iNewPg = iNewPg;
      pPg->aNew = (u8*)hctPagePtr(pPg->pFile->pMapping, iNewPg);
    }
  }

  return rc;
}

int sqlite3HctFilePageDelete(HctFilePage *pPg){
  assert( 0 );
  return SQLITE_OK;
}

int sqlite3HctFilePageRelease(HctFilePage *pPg){
  int rc = SQLITE_OK;
  if( pPg->aNew ){
    HctMapping *pMap = pPg->pFile->pMapping;
    if( !hctFilePagemapSetLogical(pMap, pPg->iPg, pPg->iPagemap, pPg->iNewPg) ){
      rc = SQLITE_BUSY;
    }
  }
  memset(pPg, 0, sizeof(*pPg));
  return rc;
}

u64 sqlite3HctFileStartTrans(HctFile *pFile){
  u32 iRet = 0;
  /* TODO - support 56-bit tids */
  hctFileTmpAllocate(pFile, HCT_PAGEMAP_TRANSID_EOF, &iRet);
  assert( iRet>0 );
  return (u64)iRet;
}

u64 sqlite3HctFileAllocateTransid(HctFile *pFile){
  u64 iVal = hctFilePagemapIncr(pFile->pMapping, HCT_PAGEMAP_TRANSID_EOF, 1);
  return iVal & HCT_TID_MASK;
}
u64 sqlite3HctFileAllocateSnapshotid(HctFile *pFile){
  u64 iVal = hctFilePagemapIncr(pFile->pMapping, HCT_PAGEMAP_COMMITID, 1);
  return iVal & HCT_TID_MASK;
}

u64 sqlite3HctFileGetTransid(HctFile *pFile){
  u64 iVal = hctFilePagemapGet(pFile->pMapping, HCT_PAGEMAP_TRANSID_EOF);
  return iVal & HCT_TID_MASK;
}
u64 sqlite3HctFileGetSnapshotid(HctFile *pFile){
  u64 iVal = hctFilePagemapGet(pFile->pMapping, HCT_PAGEMAP_COMMITID);
  return iVal & HCT_TID_MASK;
}

int sqlite3HctFileFinishTrans(HctFile *pFile){
  return SQLITE_OK;
}

int sqlite3HctFilePgsz(HctFile *pFile){
  return pFile->szPage;
}


/* 
** Virtual table type for "hctpgmap".
*/
typedef struct pgmap_vtab pgmap_vtab;
struct pgmap_vtab {
  sqlite3_vtab base;              /* Base class - must be first */
  sqlite3 *db;
};

/* 
** Virtual cursor type for "hctpgmap".
*/
typedef struct pgmap_cursor pgmap_cursor;
struct pgmap_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  HctFile *pFile;            /* Database to report on */
  u64 iMaxSlotno;            /* Maximum page number for this scan */
  u64 slotno;                /* The page-number/rowid value */
  u64 iVal;                  /* Value read from pagemap */
};

/*
** The pgmapConnect() method is invoked to create a new
** template virtual table.
**
** Think of this routine as the constructor for pgmap_vtab objects.
**
** All this routine needs to do is:
**
**    (1) Allocate the pgmap_vtab object and initialize all fields.
**
**    (2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
**        result set of queries against the virtual table will look like.
*/
static int pgmapConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  pgmap_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db,
      "CREATE TABLE x("
        "slotno INTEGER, pgno INTEGER, physical_in_use BOOLEAN"
      ")"
  );

  if( rc==SQLITE_OK ){
    pNew = sqlite3MallocZero( sizeof(*pNew) );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ) return SQLITE_NOMEM;
    pNew->db = db;
  }
  return rc;
}

/*
** This method is the destructor for pgmap_vtab objects.
*/
static int pgmapDisconnect(sqlite3_vtab *pVtab){
  pgmap_vtab *p = (pgmap_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new pgmap_cursor object.
*/
static int pgmapOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  pgmap_cursor *pCur;
  pCur = sqlite3MallocZero(sizeof(*pCur));
  if( pCur==0 ) return SQLITE_NOMEM;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a pgmap_cursor.
*/
static int pgmapClose(sqlite3_vtab_cursor *cur){
  pgmap_cursor *pCur = (pgmap_cursor*)cur;
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int pgmapEof(sqlite3_vtab_cursor *cur){
  pgmap_cursor *pCur = (pgmap_cursor*)cur;
  return pCur->slotno>pCur->iMaxSlotno;
}

static int pgmapLoadSlot(pgmap_cursor *pCur){
  pCur->iVal = hctFilePagemapGet(pCur->pFile->pMapping, pCur->slotno);
  return SQLITE_OK;
}

/*
** Advance a hctdb_cursor to its next row of output.
*/
static int pgmapNext(sqlite3_vtab_cursor *cur){
  pgmap_cursor *pCur = (pgmap_cursor*)cur;
  pCur->slotno++;
  return pgmapEof(cur) ? SQLITE_OK : pgmapLoadSlot(pCur);
}

/*
** Return values of columns for the row at which the pgmap_cursor
** is currently pointing.
*/
static int pgmapColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  pgmap_cursor *pCur = (pgmap_cursor*)cur;
  switch( i ){
    case 0: {  /* slotno */
      sqlite3_result_int64(ctx, pCur->slotno);
      break;
    }
    case 1: {  /* pgno */
      sqlite3_result_int64(ctx, (pCur->iVal & 0xFFFFFFFF));
      break;
    }
    case 2: {  /* physical_in_use */
      sqlite3_result_int(ctx, (pCur->iVal & HCT_PGMAPFLAG_PHYSINUSE)?1:0);
      break;
    }
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the slotno value.
*/
static int pgmapRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  pgmap_cursor *pCur = (pgmap_cursor*)cur;
  *pRowid = pCur->slotno;
  return SQLITE_OK;
}

/*
** This method is called to "rewind" the pgmap_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to pgmapColumn() or pgmapRowid() or 
** pgmapEof().
*/
static int pgmapFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  pgmap_cursor *pCur = (pgmap_cursor*)pVtabCursor;
  pgmap_vtab *pTab = (pgmap_vtab*)(pCur->base.pVtab);
  int rc;
  u64 max1;
  u64 max2;
 
  pCur->pFile = sqlite3HctDbFile(sqlite3HctDbFind(pTab->db, 0));
  pCur->slotno = 1;
  max1 = hctFilePagemapGet(pCur->pFile->pMapping, HCT_PAGEMAP_PHYSICAL_EOF);
  max2 = hctFilePagemapGet(pCur->pFile->pMapping, HCT_PAGEMAP_LOGICAL_EOF);
  max1 &= HCT_PGNO_MASK;
  max2 &= HCT_PGNO_MASK;
  pCur->iMaxSlotno = max1>max2 ? max1 : max2;
  rc = pgmapLoadSlot(pCur);
  return rc;
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int pgmapBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  pIdxInfo->estimatedCost = (double)10;
  pIdxInfo->estimatedRows = 10;
  return SQLITE_OK;
}

int sqlite3HctFileVtabInit(sqlite3 *db){
  static sqlite3_module pgmapModule = {
    /* iVersion    */ 0,
    /* xCreate     */ 0,
    /* xConnect    */ pgmapConnect,
    /* xBestIndex  */ pgmapBestIndex,
    /* xDisconnect */ pgmapDisconnect,
    /* xDestroy    */ 0,
    /* xOpen       */ pgmapOpen,
    /* xClose      */ pgmapClose,
    /* xFilter     */ pgmapFilter,
    /* xNext       */ pgmapNext,
    /* xEof        */ pgmapEof,
    /* xColumn     */ pgmapColumn,
    /* xRowid      */ pgmapRowid,
    /* xUpdate     */ 0,
    /* xBegin      */ 0,
    /* xSync       */ 0,
    /* xCommit     */ 0,
    /* xRollback   */ 0,
    /* xFindMethod */ 0,
    /* xRename     */ 0,
    /* xSavepoint  */ 0,
    /* xRelease    */ 0,
    /* xRollbackTo */ 0,
    /* xShadowName */ 0
  };

  return sqlite3_create_module(db, "hctpgmap", &pgmapModule, 0);
}


