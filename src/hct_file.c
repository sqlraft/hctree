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
#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include <dirent.h>
#include <errno.h>

#define HCT_DEFAULT_PAGESIZE     4096

/*
** The database file is extended and managed in chunks of
** HCT_DEFAULT_PAGEPERCHUNK pages. Since pages are normally 4096 bytes, this
** is 2MiB by default. But, if the file is mmap()ed 2MiB at a time, we 
** quickly read the system limit for number of mappings (on Linux, this is
** kernel parameter vm.max_map_count - 65530 by default). So, each mapping 
** is made for HCT_MMAP_QUANTA times this amount. Since we need mappings for
** both the database file and page-map, this means we can mmap() a database of:
**
**    32765 * 1024*512*4096 bytes
**
** or around 64TiB.
*/
#define HCT_DEFAULT_PAGEPERCHUNK  512
#define HCT_MMAP_QUANTA          1024

#define HCT_HEADER_PAGESIZE      4096

#define HCT_LOCK_OFFSET (1024*1024)
#define HCT_LOCK_SIZE   1


/*
** Pagemap slots used for special purposes.
*/
#define HCT_ROOTPAGE_SCHEMA          1
#define HCT_ROOTPAGE_META            2

#define HCT_PAGEMAP_LOGICAL_EOF      3
#define HCT_PAGEMAP_PHYSICAL_EOF     4

#define HCT_PAGEMAP_TRANSID_EOF      16

#define HCT_PMF_LOGICAL_EVICTED    (((u64)0x00000001)<<56)
#define HCT_PMF_LOGICAL_IRREVICTED (((u64)0x00000002)<<56)
#define HCT_PMF_PHYSICAL_IN_USE    (((u64)0x00000004)<<56)
#define HCT_PMF_LOGICAL_IN_USE     (((u64)0x00000008)<<56)
#define HCT_PMF_LOGICAL_IS_ROOT    (((u64)0x00000010)<<56)

#define HCT_FIRST_LOGICAL  33

/*
** Masks for use with pagemap values.
*/
#define HCT_PAGEMAP_FMASK  (((u64)0xFF) << 56)        /* Flags MASK */
#define HCT_PAGEMAP_VMASK  (~HCT_PAGEMAP_FMASK)       /* Value MASK */

#define assert_pgno_ok(iPg) assert( ((u64)iPg)<((u64)1<<48) && iPg>0 )

typedef struct HctFileServer HctFileServer;
typedef struct HctMapping HctMapping;
typedef struct HctMappingChunk HctMappingChunk;

/*
** Global variables for this module. 
**
** pServerList:
**   Linked list of distinct files opened by this process. Access to this
**   variable is protected by SQLITE_MUTEX_STATIC_VFS1.
**
** nCASFailCnt/nCASFailReset:
**   These are used to inject CAS instruction failures for testing purposes.
**   Set by the sqlite3_hct_cas_failure() API. They are not threadsafe.
**
** nProcFailCnt
**   These are used to inject process failures (i.e. abort() calls) for
**   testing purposes. Set by the sqlite3_hct_proc_failure() API. Not 
**   threadsafe.
*/
static struct HctFileGlobalVars {
  HctFileServer *pServerList;

  int nCASFailCnt;
  int nCASFailReset;

  int nProcFailCnt;
} g;

void sqlite3_hct_cas_failure(int nCASFailCnt, int nCASFailReset){
  g.nCASFailCnt = nCASFailCnt;
  g.nCASFailReset = nCASFailReset;
}
void sqlite3_hct_proc_failure(int nProcFailCnt){
  g.nProcFailCnt = nProcFailCnt;
}

/*
** This is called to check if a CAS fault should be injected. It returns
** true if a fault should be injected, or false otherwise.
*/
static int inject_cas_failure(void){
  if( g.nCASFailCnt>0 ){
    if( (--g.nCASFailCnt)==0 ){
      g.nCASFailCnt = g.nCASFailReset;
      return 1;
    }
  }
  if( g.nProcFailCnt>0 ){
    if( (--g.nProcFailCnt)==0 ){
      abort();
    }
  }
  return 0;
}

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

/*
** eInitState:
**   Set to one of the HCT_INIT_XXX constants defined below. See comments
**   above those constants for details.
**
** iNextFileId:
**   Used to allocate unique ids to each HctFile associated with this
**   HctFileServer object. These ids are used for debugging, and also
**   to generate log file names.
*/
struct HctFileServer {
  sqlite3_mutex *pMutex;          /* Mutex to protect this object */
  HctFile *pFileList;

  u64 iCommitId;                  /* CID value */
  u64 nWriteCount;                /* Write count */

  int iNextFileId;
  char *zPath;                    /* Path to database (aFdDb[0]) */
  char *zDir;                     /* Directory component of zPath */
  int fdMap;                      /* Read/write file descriptor for page-map */
  int nFdDb;                      /* Number of valid entries in aFdDb[] */
  int aFdDb[HCT_MAX_NDBFILE];

  int szPage;                     /* Page size for database */
  int nPagePerChunk;
  HctMapping *pMapping;           /* Mapping of pagemap and db pages */

  int bReadOnlyMap;               /* True for a read-only mapping of db file */

  HctTMapServer *pTMapServer;     /* Transaction map server */
  HctPManServer *pPManServer;     /* Page manager server */
  int eInitState;

  void *pJrnlPtr;
  void(*xJrnlDel)(void*);

  i64 st_dev;                     /* File identification 1 */
  i64 st_ino;                     /* File identification 2 */
  HctFileServer *pServerNext;     /* Next object in g.pServerList list */
};

/*
** System initialization state:
**
** HCT_INIT_NONE:
**   No initialization has been done.
**
** HCT_INIT_RECOVER1:
**   The sqlite_schema table (root page 1) has been recovered. And the
**   page-map scanned to initialize the page-manager.
**
** HCT_INIT_RECOVER2:
**   Other tables (apart from sqlite_schema) have been recovered.
**   Initialization has finished.
*/
#define HCT_INIT_NONE     0
#define HCT_INIT_RECOVER1 1
#define HCT_INIT_RECOVER2 2

/*
** Event counters used by the hctstats virtual table.
*/
typedef struct HctFileStats HctFileStats;
struct HctFileStats {
  i64 nCasAttempt;
  i64 nCasFail;
  i64 nIncrAttempt;
  i64 nIncrFail;
  i64 nMutex;
  i64 nMutexBlock;
};

/*
** iCurrentTid:
**   Most recent value returned by sqlite3HctFileAllocateTransid(). This
**   is the current TID while the upper layer is writing the database, and
**   meaningless at other times. Used by this object as the "current TID"
**   when freeing a page.
**
** nPageAlloc:
**   The total number of physical page allocations requested by the upper
**   layer in the lifetime of this object.
*/
struct HctFile {
  HctConfig *pConfig;             /* Connection configuration object */
  HctFileServer *pServer;         /* Connection to global db object */
  HctFile *pFileNext;             /* Next handle opened on same file */
  int iFileId;                    /* Id used for debugging output */
  int eInitState;

  HctTMapClient *pTMapClient;     /* Transaction map client object */
  HctPManClient *pPManClient;     /* Transaction map client object */

  u64 iCurrentTid;
  u64 nPageAlloc;

  /* Copies of HctFileServer variables */
  int szPage;
  HctMapping *pMapping;

  /* Event counters used by the hctstats virtual table */
  HctFileStats stats;
};

static int hctLog2(int n){
  int i;
  assert( (n & (n-1))==0 );
  for(i=0; (1<<i)<n; i++);
  return i;
}

/*
** Atomic version of:
**
**   if( *pPtr==iOld ){
**     *pPtr = iNew;
**     return 1;
**   }else{
**     return 0;
**   }
*/
static int hctBoolCAS64(u64 *pPtr, u64 iOld, u64 iNew){
  return HctCASBool(pPtr, iOld, iNew);
}

static int hctBoolCompareAndSwap64(u64 *pPtr, u64 iOld, u64 iNew){
#if 0
  if( g.nCASFailCnt>0 ){
    if( (--g.nCASFailCnt)==0 ){
      g.nCASFailCnt = g.nCASFailReset;
      return 0;
    }
  }
#endif
  return HctCASBool(pPtr, iOld, iNew);
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

/*
** Buffer aBuf[] is p->szPage bytes in size. This function writes the
** contents of said buffer to physical database page iPhys.
*/
static int hctPageWriteToDisk(HctFileServer *p, u64 iPhys, u8 *aBuf){
  i64 iChunk = ((iPhys-1) / p->nPagePerChunk);
  int iFd = iChunk % p->nFdDb;
  i64 iOff = p->szPage * (
      ((iChunk / p->nFdDb) * p->nPagePerChunk) 
    + (iPhys-1) %  p->nPagePerChunk
  );
  ssize_t res;
  assert_pgno_ok( iPhys );
  res = pwrite(p->aFdDb[iFd], aBuf, p->szPage, iOff);
  return (res==p->szPage ? SQLITE_OK : SQLITE_ERROR);
}

static void hctFilePagemapSetDirect(HctMapping *p, u32 iSlot, u64 iNew){
  u64 *pPtr = hctPagemapPtr(p, iSlot);
  *pPtr = iNew;
}

static void hctFilePagemapSetFlag(HctMapping *p, u32 iSlot, u64 mask){
  u64 *pPtr = hctPagemapPtr(p, iSlot);
  *pPtr = *pPtr | mask;
}

/*
** Use a CAS instruction to the value of page-map slot iSlot. Return true
** if the slot is successfully set to value iNew, or false otherwise.
*/
static int hctFilePagemapSet(HctFile *pFile, u32 iSlot, u64 iOld, u64 iNew){
  u64 *pPtr = hctPagemapPtr(pFile->pMapping, iSlot);
  pFile->stats.nCasAttempt++;
  if(  hctBoolCompareAndSwap64(pPtr, iOld, iNew) ) return 1;
  pFile->stats.nCasFail++;
  return 0;
}


static u64 hctFilePagemapGet(HctMapping *p, u32 iSlot){
  return HctAtomicLoad( hctPagemapPtr(p, iSlot) );
}

static u64 hctFilePagemapGetSafe(HctMapping *p, u32 iSlot){
  if( ((iSlot-1)>>p->mapShift)>=p->nChunk ){
    return 0;
  }
  return hctFilePagemapGet(p, iSlot);
}

static u64 hctFileAtomicIncr(HctFile *pFile, u64 *pPtr, int nIncr){
  u64 iOld;
  while( 1 ){
    iOld = HctAtomicLoad(pPtr);
    pFile->stats.nIncrAttempt++;
    if( hctBoolCompareAndSwap64(pPtr, iOld, iOld+nIncr) ) return iOld+nIncr;
    pFile->stats.nIncrFail++;
  }
}

/*
** Increment the value in slot iSlot by nIncr. Return the new value.
*/
static u64 hctFilePagemapIncr(HctFile *pFile, u32 iSlot, int nIncr){
  u64 *pPtr = hctPagemapPtr(pFile->pMapping, iSlot);
  u64 iOld;
  while( 1 ){
    iOld = HctAtomicLoad(pPtr);
    pFile->stats.nIncrAttempt++;
    if( hctBoolCompareAndSwap64(pPtr, iOld, iOld+nIncr) ) return iOld+nIncr;
    pFile->stats.nIncrFail++;
  }
}

/*
** Set the physical page id mapped from logical page iLogical to physical
** page id iNew. Return 1 if successful, or 0 if the operation fails. The
** operation fails if either:
**
**   * the LOGICAL_EVICTED flag is already set for the logical page, or 
**   * the current physical page id to which the logical page is mapped
**     is not equal to parameter iOld.
*/
static int hctFilePagemapSetLogical(
  HctFile *pFile,                 /* Use mapping object of this file */
  u32 iLogical,                   /* Logical page to set the physical id for */
  u64 iOld,                       /* Old physical page id */
  u64 iNew                        /* New physical page id */
){
  HctMapping *p = pFile->pMapping;
  while( 1 ){
    u64 i1 = hctFilePagemapGet(p, iLogical);
    u64 iOld1 = (iOld & HCT_PAGEMAP_VMASK) | (i1 & HCT_PAGEMAP_FMASK);
    u64 iNew1 = (iNew & HCT_PAGEMAP_VMASK) | (i1 & HCT_PAGEMAP_FMASK);

    iNew1 |= HCT_PMF_LOGICAL_IN_USE;

    /* If a CAS instruction failure injection is scheduled, return 0
    ** to the caller.  */
    if( inject_cas_failure() ) return 0;

    /* This operation fails if LOGICAL_EVICTED has been set. */
    iOld1 &= ~HCT_PMF_LOGICAL_EVICTED;
    iNew1 &= ~HCT_PMF_LOGICAL_EVICTED;

    if( hctFilePagemapSet(pFile, iLogical, iOld1, iNew1) ){
      return 1;
    }
    if( i1!=iOld1 ) return 0;
  }

  assert( !"unreachable" );
  return 0;
}

/*
** Set the EVICTED or IRREVICTED flag on page iLogical.
*/
static int hctFileSetEvicted(
  HctFile *pFile,
  u32 iLogical,
  u32 iOldPg,
  int bIrrevocable
){
  u64 *pPtr = hctPagemapPtr(pFile->pMapping, iLogical);
  while( 1 ){
    u64 iOld = HctAtomicLoad(pPtr);
    u64 iNew = iOld | (
        bIrrevocable ? HCT_PMF_LOGICAL_IRREVICTED : HCT_PMF_LOGICAL_EVICTED
    );

    /* Fail if either the current physical page mapped to logical page iLogical
    ** is not iOldPg, or if the LOGICAL_EVICTED flag has already been set. */
    if( (iOld & HCT_PAGEMAP_VMASK)!=iOldPg 
     || ((iOld & HCT_PMF_LOGICAL_EVICTED) && !bIrrevocable)
     || ((iOld & HCT_PMF_LOGICAL_EVICTED)==0 && bIrrevocable)
     || ((iOld & HCT_PMF_LOGICAL_IN_USE)==0)
    ){
      return 0;
    }
    if( inject_cas_failure() ) return 0;

    pFile->stats.nCasAttempt++;
    if( hctBoolCAS64(pPtr, iOld, iNew) ) return 1;
    pFile->stats.nCasFail++;
  }

  assert( !"unreachable" );
  return 0;
}

/*
** Clear the LOGICAL_EVICTED flag from page-map entry iLogical. This will
** fail if the LOGICAL_IRREVICTED flag is already set. Return 1 if the
** flag is successfully cleared, or 0 otherwise.
*/ 
static int hctFileClearEvicted(HctFile *pFile, u32 iLogical){
  u64 *pPtr = hctPagemapPtr(pFile->pMapping, iLogical);
  while( 1 ){
    u64 iOld = HctAtomicLoad(pPtr);
    u64 iNew = iOld & ~HCT_PMF_LOGICAL_EVICTED;

    if( (iOld & HCT_PMF_LOGICAL_IRREVICTED) ) return 0;
    if( inject_cas_failure() ) return 0;

    pFile->stats.nCasAttempt++;
    if( hctBoolCAS64(pPtr, iOld, iNew) ) return 1;
    pFile->stats.nCasFail++;
  }

  assert( !"unreachable" );
  return 0;
}

static void hctFilePagemapZeroValue(HctFile *pFile, u32 iSlot){
  while( 1 ){
    u64 i1 = hctFilePagemapGet(pFile->pMapping, iSlot);
    u64 i2 = (i1 & HCT_PMF_PHYSICAL_IN_USE);
    if( hctFilePagemapSet(pFile, iSlot, i1, i2) ) return;
  }
}

/*
** Open a file descriptor for read/write access on the filename formed by
** concatenating arguments zFile and zPost (e.g. "test.db" and "-pagemap").
** Return the file descriptor if successful.
*/
static int hctFileOpen(int *pRc, const char *zFile, const char *zPost){
  int fd = -1;
  if( *pRc==SQLITE_OK ){
    char *zPath = sqlite3_mprintf("%s%s", zFile, zPost);
    if( zPath==0 ){
      *pRc = SQLITE_NOMEM_BKPT;
    }else{
      while( fd<0 ){
        fd = open(zPath, O_CREAT|O_RDWR, 0644);
        if( fd<0 ){
          *pRc = SQLITE_CANTOPEN_BKPT;
          break;
        }
        if( fd<3 ){
          /* Do not use any file-descriptor with values 0, 1 or 2. Using 
          ** these means that stray calls to printf() etc. may corrupt the
          ** database.  */
          close(fd);
          fd = open("/dev/null", O_RDONLY, 0644);
          if( fd<0 ){
            *pRc = SQLITE_CANTOPEN_BKPT;
            break;
          }
          fd = -1;
        }
      }
      sqlite3_free(zPath);
    }
  }
  return fd;
}

/*
** Take an exclusive POSIX lock on the file-descriptor passed as the 
** second argument.
*/
static void hctFileLock(int *pRc, int fd, const char *zFile){
  if( *pRc==SQLITE_OK ){
    int res;
    struct flock l;
    memset(&l, 0, sizeof(l));
    l.l_type = F_WRLCK;
    l.l_whence = SEEK_SET;
    l.l_start = HCT_LOCK_OFFSET;
    l.l_len = HCT_LOCK_SIZE;
    res = fcntl(fd, F_SETLK, &l);
    if( res!=0 ){
      fcntl(fd, F_GETLK, &l);
      sqlite3_log(SQLITE_BUSY, "hct file \"%s\" locked by process %lld",
          zFile, (i64)l.l_pid
      );
      *pRc = SQLITE_BUSY;
    }
  }
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
      *pRc = sqlite3HctIoerr(SQLITE_IOERR_FSTAT);
    }else{
      szRet = (i64)(sStat.st_size);
    }
  }
  return szRet;
}

static int hctFileTruncate(int *pRc, int fd, i64 sz){
  if( *pRc==SQLITE_OK ){
    int res = ftruncate(fd, (off_t)sz);
    if( res ){
      *pRc = sqlite3HctIoerr(SQLITE_IOERR_TRUNCATE);
    }
  }
  return *pRc;
}

static void hctFileUnlink(int *pRc, const char *zFile){
  if( *pRc==SQLITE_OK ) unlink(zFile);
}


/*
** This function is a no-op if (*pRc) is set to other than SQLITE_OK
** when it is called.
**
** Otherwise, argument fd is assumed to be an open file-descriptor. This
** function attempts to map and return a pointer to a region nByte bytes in
** size at offset iOff of the open file. The mapping is read-only if parameter
** bRO is non-zero, or read/write if it is zero.
**
** If an error occurs, NULL is returned and (*pRc) set to an SQLite error
** code.
*/
static void *hctFileMmap(int *pRc, int fd, i64 nByte, i64 iOff, int bRO){
  void *pRet = 0;
  if( *pRc==SQLITE_OK ){
    const int flags = PROT_READ | (bRO ? 0 : PROT_WRITE);
    pRet = mmap(0, nByte, flags, MAP_SHARED, fd, iOff);
    if( pRet==MAP_FAILED ){
      pRet = 0;
      *pRc = sqlite3HctIoerr(SQLITE_IOERR_MMAP);
    }
  }
  return pRet;
}

static void hctFileMunmap(void *pMap, i64 nByte){
  if( pMap ) munmap(pMap, nByte);
}

static char *hctStrdup(int *pRc, const char *zIn){
  char *zRet = 0;
  if( *pRc==SQLITE_OK ){
    zRet = sqlite3_mprintf("%s", zIn);
    if( zRet==0 ) *pRc = SQLITE_NOMEM_BKPT;
  }
  return zRet;
}


/*
** Given local path zFile, return the associated canonical path in a buffer
** obtained from sqlite3_malloc(). It is the responsibility of the caller
** to eventually free this buffer using sqlite3_free().
*/
static char *fileGetFullPath(int *pRc, const char *zFile){
  char *zRet = 0;
  if( *pRc==SQLITE_OK ){
    char *zFree = realpath(zFile, 0);
    if( zFree==0 ){
      *pRc = SQLITE_CANTOPEN_BKPT;
    }else{
      zRet = hctStrdup(pRc, zFree);
      free(zFree);
    }
  }
  return zRet;
}

static int hctFileFindLogs(
  HctFileServer *pServer, 
  void *pCtx,
  int (*xLog)(void*, const char*)
){
  DIR *d;
  const char *zName = &pServer->zPath[strlen(pServer->zDir)];
  int nName = strlen(zName);
  int rc = SQLITE_OK;

  d = opendir(pServer->zDir);
  if (d) {
    struct dirent *dir;
    while( rc==SQLITE_OK && (dir = readdir(d))!=NULL ){
      const char *zFile = (const char*)dir->d_name;
      int nFile = strlen(zFile);
      if( nFile>(nName+5) 
       && memcmp(zFile, zName, nName)==0
       && memcmp(&zFile[nName], "-log-", 5)==0
      ){
        char *zFull = sqlite3_mprintf("%s/%s", pServer->zDir, zFile);
        rc = xLog(pCtx, zFull);
        sqlite3_free(zFull);
      }
    }
    closedir(d);
  }

  return rc;
}

static int hctFileServerInitUnlinkLog(void *pDummy, const char *zFile){
  int rc = SQLITE_OK;
  hctFileUnlink(&rc, zFile); 
  return rc;
}

static void hctFileReadHdr(
  int *pRc, 
  void *pHdr,
  int *pszPage,
  int *pnDbFile
){
  *pszPage = 0;
  if( *pRc==SQLITE_OK ){
    /*            12345678901234567890123456789012 */
    int szPage = 0;
    int nDbFile = 0;
    char *zHdr = "Hctree database version 00000001";
    u8 aEmpty[32] = {0};

    assert( strlen(zHdr)==32 );
    if( memcmp(zHdr, pHdr, 32)==0 ){
      memcpy(&szPage, &((u8*)pHdr)[32], sizeof(int));
      if( szPage<512 || szPage>32768 || (szPage & (szPage-1))!=0 ){
        *pRc = SQLITE_CANTOPEN_BKPT;
        return;
      }

      memcpy(&nDbFile, &((u8*)pHdr)[36], sizeof(int));
      if( nDbFile<1 || nDbFile>HCT_MAX_NDBFILE ){
        *pRc = SQLITE_CANTOPEN_BKPT;
        return;
      }
    }else if( memcmp(aEmpty, pHdr, 32)==0 ){
      /* no-op */
    }else{
      *pRc = SQLITE_CANTOPEN_BKPT;
    }

    *pszPage = szPage;
    *pnDbFile = nDbFile;
  }
}

static void *hctFileMmapDbChunk(
  int *pRc,
  HctFileServer *p,
  HctMapping *pMap,
  int iChunk
){
  void *pRet = 0;
  i64 szChunk = p->nPagePerChunk * p->szPage;
  int iFd = iChunk % p->nFdDb;
  int iChunkOfFile = (iChunk / p->nFdDb);

  if( (iChunkOfFile % HCT_MMAP_QUANTA)==0 ){
    i64 iOff = szChunk * iChunkOfFile;
    pRet = hctFileMmap(
        pRc, p->aFdDb[iFd], szChunk*HCT_MMAP_QUANTA, iOff, p->bReadOnlyMap
    );
  }else{
    pRet = (void*)(((u8*)pMap->aChunk[iChunk - p->nFdDb].pData) + szChunk);
  }

  return pRet;
}

static void *hctFileMmapPagemapChunk(
  int *pRc,
  HctFileServer *p,
  HctMapping *pMap,
  int iChunk
){
  void *pRet = 0;
  i64 szChunk = p->nPagePerChunk * sizeof(u64);

  if( (iChunk % HCT_MMAP_QUANTA)==0 ){
    pRet = hctFileMmap(
        pRc, p->fdMap, szChunk*HCT_MMAP_QUANTA, (szChunk*iChunk), 0
    );
  }else{
    pRet = (void*)(((u8*)(pMap->aChunk[iChunk-1].aMap)) + szChunk);
  }

  return pRet;
}

static void hctFileOpenDataFiles(
  int *pRc,
  HctFileServer *p,
  int nDbFile
){
  int ii;
  int rc = *pRc;
  assert( p->nFdDb==1 );
  for(ii=1; ii<nDbFile && rc==SQLITE_OK; ii++){
    char *z = sqlite3_mprintf("-%d", ii);
    p->aFdDb[ii] = hctFileOpen(&rc, p->zPath, z);
    sqlite3_free(z);
    if( rc==SQLITE_OK ) p->nFdDb = ii+1;
  }

  if( rc!=SQLITE_OK ){
    for(ii=1; ii<p->nFdDb; ii++){
      if( p->aFdDb[ii]>0 ) close(p->aFdDb[ii]);
      p->aFdDb[ii] = -1;
    }
    p->nFdDb = 1;
  }
  *pRc = rc;
}

static i64 round_up(i64 iVal, i64 nQuanta){
  return ((iVal + nQuanta - 1) / nQuanta) * nQuanta;
}

static void hctFileAllocateMapping(
  int *pRc,
  HctFileServer *p, 
  int nChunk
){
  i64 szChunkPagemap = p->nPagePerChunk * sizeof(u64);
  i64 szChunkData = p->nPagePerChunk * p->szPage;
  int rc = *pRc;
  HctMapping *pMapping = 0;
  int iFd = 0;
  int i = 0;

  p->pMapping = pMapping = hctMappingNew(&rc, 0, nChunk);
  if( rc==SQLITE_OK ){
    pMapping->mapShift = hctLog2(p->nPagePerChunk);
    pMapping->mapMask = (1<<pMapping->mapShift)-1;
    pMapping->szPage = p->szPage;
  }

  /* Map all chunks of the pagemap file using a single call to mmap() */
  {
    int nAll = round_up(nChunk, HCT_MMAP_QUANTA);
    u8 *pMap = (u8*)hctFileMmap(&rc, p->fdMap, nAll*szChunkPagemap,0,0);
    for(i=0; rc==SQLITE_OK && i<nChunk; i++){
      pMapping->aChunk[i].aMap = (u64*)&pMap[i * szChunkPagemap];
    }
  }

  /* Map all chunks of the data files. One call to mmap() for each file. */
  for(iFd=0; iFd<p->nFdDb && rc==SQLITE_OK; iFd++){
    int nFileChunk = (nChunk / p->nFdDb) + (iFd < (nChunk % p->nFdDb));
    i64 n = round_up(nFileChunk, HCT_MMAP_QUANTA) * szChunkData;
    u8 *pMap = (u8*)hctFileMmap(&rc, p->aFdDb[iFd], n, 0, p->bReadOnlyMap);
    for(i=0; i<nFileChunk && rc==SQLITE_OK; i++){
      int iChunk = iFd + i*p->nFdDb;
      pMapping->aChunk[iChunk].pData = &pMap[i*szChunkData];
    }
  }

  *pRc = rc;
}

typedef struct Uncommitted Uncommitted;
struct Uncommitted {
  int nAlloc;
  int nTid;
  i64 *aTid;
};

static int hctFileServerInitUncommitted(void *pCtx, const char *zFile){
  int fd;
  Uncommitted *p = (Uncommitted*)pCtx;
  
  fd = open(zFile, O_RDONLY);
  if( fd>=0 ){
    i64 iTid = 0;
    read(fd, &iTid, sizeof(iTid));
    close(fd);
    if( iTid>0 ){
      if( p->nTid==p->nAlloc ){
        int nNew = p->nTid ? p->nTid*4 : 64;
        i64 *aNew = sqlite3_realloc(p->aTid, nNew*sizeof(i64));
        if( aNew==0 ){
          return SQLITE_NOMEM;
        }else{
          p->aTid = aNew;
          p->nAlloc = nNew;
        }
      }
      p->aTid[p->nTid++] = iTid;
    }
  }
  return SQLITE_OK;
}

static int hctFileServerInit(
  HctFileServer *p, 
  HctConfig *pConfig, 
  const char *zFile
){
  int rc = SQLITE_OK;
  assert( sqlite3_mutex_held(p->pMutex) );
  if( p->zPath==0 ){
    i64 szHdr;                    /* Size of header file */
    i64 szMap;                    /* Size of pagemap file */
    int nChunk = 0;               /* Number of chunks in database */
    int szPage = 0;
    int nDbFile = 0;

    Uncommitted unc;
    memset(&unc, 0, sizeof(unc));

    /* Open the data and page-map files */
    p->fdMap = hctFileOpen(&rc, zFile, "-pagemap");
    p->zPath = fileGetFullPath(&rc, zFile);

    if( rc==SQLITE_OK ){
      int n = strlen(p->zPath);
      while( p->zPath[n-1]!='/' && n>1 ) n--;
      p->zDir = sqlite3_mprintf("%.*s", n, p->zPath);
      if( p->zDir==0 ) rc = SQLITE_NOMEM_BKPT;
    }

    /* Initialize the page-manager */
    p->pPManServer = sqlite3HctPManServerNew(&rc, p);

    /* If the header file is zero bytes in size, or is not yet populated, 
    ** then the database is empty, regardless of the contents of the 
    ** *-data or *-pagemap file. Truncate the pagemap and data files to 
    ** zero bytes in size to make sure of this. 
    **
    ** Alternatively, if the header file is the right size, try to read it. 
    */
    szHdr = hctFileSize(&rc, p->aFdDb[0]);
    if( rc==SQLITE_OK ){
      void *pHdr = 0;
      if( szHdr==0 ){
        szHdr = HCT_HEADER_PAGESIZE*2;
        hctFileTruncate(&rc, p->aFdDb[0], szHdr);
      }else if( szHdr<(HCT_HEADER_PAGESIZE*2) ){
        rc = SQLITE_CANTOPEN_BKPT;
      }

      pHdr = hctFileMmap(&rc, p->aFdDb[0], HCT_HEADER_PAGESIZE*2, 0, 1);
      hctFileReadHdr(&rc, pHdr, &szPage, &nDbFile);
      if( rc==SQLITE_OK && szPage==0 ){
        hctFileTruncate(&rc, p->fdMap, 0);
        hctFileTruncate(&rc, p->fdMap, HCT_DEFAULT_PAGEPERCHUNK*sizeof(i64));
        szHdr = HCT_HEADER_PAGESIZE*2;
        hctFileTruncate(&rc, p->aFdDb[0], szHdr);
        if( rc==SQLITE_OK ){
          rc = hctFileFindLogs(p, 0, hctFileServerInitUnlinkLog);
        }
      }else{
        if( rc==SQLITE_OK ){
          rc = hctFileFindLogs(p, (void*)&unc, hctFileServerInitUncommitted);
        }
        hctFileOpenDataFiles(&rc, p, nDbFile);
      }
      hctFileMunmap(pHdr, HCT_HEADER_PAGESIZE*2);
    }
    p->nPagePerChunk = HCT_DEFAULT_PAGEPERCHUNK;

    assert( szPage==0 || rc==SQLITE_OK );
    if( szPage>0 ){
      i64 szChunkPagemap = p->nPagePerChunk * sizeof(u64);

      p->szPage = szPage;
      szMap = hctFileSize(&rc, p->fdMap);
      if( rc==SQLITE_OK ){
        if( szMap<szChunkPagemap
         || (szMap % szChunkPagemap)!=0
         || (p->nFdDb==1 && szHdr!=p->szPage*(szMap/sizeof(u64)))
        ){
          rc = SQLITE_CANTOPEN_BKPT;
        }else{
          nChunk = szMap / szChunkPagemap;
        }
      }

      hctFileAllocateMapping(&rc, p, nChunk);
    }

    /* Initialize CID value */
    p->iCommitId = 5;

    /* Allocate a transaction map server */
    if( rc==SQLITE_OK && p->pTMapServer==0 ){
      u64 iFirst = 0;             /* First tid that will be written in tmap */
      u64 iLast = 0;              /* Last such tid */
      int ii;                     /* To iterate through unc.aTid[] */

      if( p->pMapping ){
        iFirst = hctFilePagemapGet(p->pMapping, HCT_PAGEMAP_TRANSID_EOF);
        iFirst = (iFirst & HCT_TID_MASK) + 1;
      }else{
        iFirst = 1;
      }

      iLast = iFirst;
      for(ii=0; ii<unc.nTid; ii++){
        i64 iThis = unc.aTid[ii];
        if( iThis<iFirst ) iFirst = iThis;
        if( iThis>=iLast ) iLast = iThis+1;
      }

      /* Allocate the tmap-server object. Set all entries between iFirst and
      ** iLast to (HCT_TMAP_COMMITTED, cid=1). Ensuring that the contents of
      ** these transactions are visible to all readers. 
      **
      ** Then go back and set the entry for all tid values in unc.aTid[] to
      ** (HCT_TMAP_ROLLBACK, 0) - not visible to any readers.  */
      rc = sqlite3HctTMapServerNew(iFirst, iLast, &p->pTMapServer);
      for(ii=0; ii<unc.nTid && rc==SQLITE_OK; ii++){
        i64 iThis = unc.aTid[ii];
        rc = sqlite3HctTMapServerSet(p->pTMapServer, iThis, HCT_TMAP_ROLLBACK);
      }
    }
    sqlite3_free(unc.aTid);

    if( rc==SQLITE_OK ){
      rc = sqlite3HctJournalServerNew(&p->pJrnlPtr);
    }
  }
  return rc;
}

/*
** This is called as part of initializing a new database on disk. Mutex
** HctFileServer.mutex must be held to call this function. It writes a
** new, empty, root page to physical page iPhys, to be used for either
** HCT_ROOTPAGE_SCHEMA or HCT_ROOTPAGE_META.
**
** SQLITE_OK is returned if successful, or an SQLite error code otherwise.
*/
static int hctFileInitSystemRoot(HctFileServer *p, u64 iPhys){
  int rc = SQLITE_OK;
  u8 *aBuf = sqlite3_malloc(p->szPage);

  assert( sqlite3_mutex_held(p->pMutex) );
  if( aBuf==0 ){
    rc = SQLITE_NOMEM;
  }else{
    sqlite3HctDbRootPageInit(0, aBuf, p->szPage);

    if( p->bReadOnlyMap ){
      rc = hctPageWriteToDisk(p, iPhys, aBuf);
    }else{
      u8 *a = (u8*)hctPagePtr(p->pMapping, iPhys);
      memcpy(a, aBuf, p->szPage);
    }
    sqlite3_free(aBuf);
  }
  return rc;
}

static int hctFileInitHdr(HctFileServer *p){
  int rc = SQLITE_OK;
  u8 *aBuf = sqlite3_malloc(HCT_HEADER_PAGESIZE);
  assert( sqlite3_mutex_held(p->pMutex) );
  if( aBuf==0 ){
    rc = SQLITE_NOMEM;
  }else{
    char *zHdr = "Hctree database version 00000001";
    assert( strlen(zHdr)==32 );
    memset(aBuf, 0, HCT_HEADER_PAGESIZE);
    memcpy(aBuf, zHdr, 32);
    memcpy(&aBuf[32], &p->szPage, sizeof(int));
    memcpy(&aBuf[36], &p->nFdDb, sizeof(int));
    if( p->bReadOnlyMap ){
      ssize_t res = pwrite(p->aFdDb[0], aBuf, HCT_HEADER_PAGESIZE, 0);
      rc = (res==HCT_HEADER_PAGESIZE ? SQLITE_OK : SQLITE_ERROR);
    }else{
      memcpy(p->pMapping->aChunk[0].pData, aBuf, HCT_HEADER_PAGESIZE);
    }
  }
  sqlite3_free(aBuf);
  return rc;
}


/*
** This is called each time a new snapshot is opened. If HctFile.szPage is
** still set to 0, then:
**
**   a) this is the first snapshot opened by connection pFile, and 
**   b) the database had not been created when pFile was opened.
**
** In this case the server-mutex is taken, and if the db has still not been
** created (HctFileServer.szPage==0), then it is created on disk under the
** cover of the mutex.
*/
int sqlite3HctFileNewDb(HctFile *pFile){
  int rc = SQLITE_OK;
  if( pFile->szPage==0 ){
    HctFileServer *p = pFile->pServer;
    sqlite3_mutex_enter(p->pMutex);
    if( p->szPage==0 ){
      HctConfig *pConfig = pFile->pConfig;
      HctMapping *pMapping = 0;
      int szPage = pConfig->pgsz;
      int nDbFile = pConfig->nDbFile;

      p->szPage = szPage;
      hctFileTruncate(&rc, p->fdMap, p->nPagePerChunk * sizeof(u64));
      hctFileTruncate(&rc, p->aFdDb[0], p->nPagePerChunk * szPage);

      hctFileAllocateMapping(&rc, p, 1);
      pMapping = p->pMapping;

      assert( nDbFile>=1 && nDbFile<=HCT_MAX_NDBFILE );
      hctFileOpenDataFiles(&rc, p, nDbFile);

      /* 1. Make logical page 1 an empty intkey root page (SQLite uses this
      **    as the root of sqlite_schema).
      **
      ** 2. Set the initial values of the largest logical and physical page 
      **    ids allocated fields.
      */

      /* Set the initial values of the largest logical and physical page 
      ** ids allocated fields. These will be used when the set of free pages
      ** is recovered in sqlite3HctFileRecoverFreelists(). */
      if( rc==SQLITE_OK ){
        const int nPageSet = pConfig->nPageSet;
        hctFilePagemapSetDirect(pMapping, HCT_PAGEMAP_LOGICAL_EOF, nPageSet);
        hctFilePagemapSetDirect(pMapping, HCT_PAGEMAP_PHYSICAL_EOF, nPageSet);
      }

      if( rc==SQLITE_OK ){
        const u64 f =  HCT_PMF_LOGICAL_IN_USE | HCT_PMF_LOGICAL_IS_ROOT;
        u64 aRoot[] = {
          HCT_ROOTPAGE_SCHEMA,
          HCT_ROOTPAGE_META,
        };
        int ii = 0;
        u64 iPhys1 = 1 + (((HCT_HEADER_PAGESIZE*2)+szPage-1) / szPage);

        for(ii=0; ii<ArraySize(aRoot) && rc==SQLITE_OK; ii++){
          u64 iRoot = aRoot[ii];
          u64 iPhys = iPhys1 + ii;
          hctFilePagemapSetDirect(pMapping, iRoot, iPhys | f);
          hctFilePagemapSetFlag(pMapping, iPhys, HCT_PMF_PHYSICAL_IN_USE);
          rc = hctFileInitSystemRoot(p, iPhys);
        }
      }

      if( rc==SQLITE_OK ){
        rc = hctFileInitHdr(p);
      }

      if( rc!=SQLITE_OK ){
        hctMappingUnref(p->pMapping);
        p->pMapping = 0;
      }
    }

    if( rc==SQLITE_OK ){
      pFile->szPage = p->szPage;
      pFile->pMapping = p->pMapping;
      pFile->pMapping->nRef++;
      pFile->eInitState = p->eInitState;
    }
    sqlite3_mutex_leave(p->pMutex);
  }
  return rc;
}


/*
** Return true if the db has not yet been created on disk. Or false 
** if it already has.
*/
int sqlite3HctFileIsNewDb(HctFile *pFile){
  int bRet = 0;
  if( pFile->szPage==0 ){
    HctFileServer *p = pFile->pServer;
    sqlite3_mutex_enter(p->pMutex);
    if( p->szPage==0 ){
      bRet = 1;
    }
    sqlite3_mutex_leave(p->pMutex);
  }
  return bRet;
}

#if 0
#include <sys/time.h>
static sqlite3_int64 current_time(){
  struct timeval sNow;
  gettimeofday(&sNow, 0);
  return (sqlite3_int64)sNow.tv_sec*1000 + sNow.tv_usec/1000;
}
#endif

static void hctFileEnterServerMutex(HctFile *pFile){
  sqlite3_mutex *pMutex = pFile->pServer->pMutex;
  pFile->stats.nMutex++;
  if( sqlite3_mutex_try(pMutex)!=SQLITE_OK ){
    pFile->stats.nMutexBlock++;
    sqlite3_mutex_enter(pMutex);
  }
}

/*
** This is called to ensure that the mapping currently held by client
** pFile contains at least nChunk chunks.
*/
static int hctFileGrowMapping(HctFile *pFile, int nChunk){
  int rc = SQLITE_OK;
  if( pFile->pMapping->nChunk<nChunk ){
    i64 nOld;                     /* Old number of chunks */
    HctFileServer *p = pFile->pServer;
    HctMapping *pOld;
    hctFileEnterServerMutex(pFile);
    hctMappingUnref(pFile->pMapping);
    pFile->pMapping = 0;
    pOld = p->pMapping;
    nOld = pOld->nChunk;
    if( nOld<nChunk ){
      HctMapping *pNew = hctMappingNew(&rc, pOld, nChunk);
      if( pNew ){
        i64 szChunkData = p->nPagePerChunk*p->szPage;
        i64 szChunkMap = p->nPagePerChunk*sizeof(u64);
        int i;

        /* Grow the mapping file */
        hctFileTruncate(&rc, p->fdMap, nChunk*szChunkMap);

        for(i=nOld; i<nChunk; i++){
          HctMappingChunk *pChunk = &pNew->aChunk[i];

          /* Grow the data file */
          int iFd = (i % p->nFdDb);
          i64 sz = ((i / p->nFdDb) + 1) * szChunkData;
          hctFileTruncate(&rc, p->aFdDb[iFd], sz);

          /* Map the new chunks of both the data and mapping files. */
          pChunk->aMap = hctFileMmapPagemapChunk(&rc, p, pNew, i);
          pChunk->pData = hctFileMmapDbChunk(&rc, p, pNew, i);
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

/*
** Grow the mapping so that it is at least large enough to have an entry
** for slot iSlot. Return SQLITE_OK if successful (or if the mapping does
** not need to grow), or an SQLite error code otherwise.
*/
static int hctFileGrowMappingForSlot(HctFile *pFile, u32 iSlot){
  assert( iSlot>0 );
  return hctFileGrowMapping(pFile, 1 + ((iSlot-1) / HCT_DEFAULT_PAGEPERCHUNK));
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
      assert( fd>0 );
      hctFileLock(&rc, fd, zFile);
      pServer = (HctFileServer*)sqlite3HctMallocRc(&rc, sizeof(*pServer));
      if( pServer==0 ){
        close(fd);
      }else{
        int ii;
        for(ii=0; ii<HCT_MAX_NDBFILE; ii++){
          pServer->aFdDb[ii] = -1;
        }
        fstat(fd, &sStat);
        pServer->st_dev = (i64)sStat.st_dev;
        pServer->st_ino = (i64)sStat.st_ino;
        pServer->pServerNext = g.pServerList;
        pServer->aFdDb[0] = fd;
        pServer->nFdDb = 1;
        pServer->pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);
        /* pServer->bReadOnlyMap = 1; */
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
HctFile *sqlite3HctFileOpen(int *pRc, const char *zFile, HctConfig *pConfig){
  int rc = *pRc;
  HctFile *pNew;

  pNew = (HctFile*)sqlite3HctMallocRc(&rc, sizeof(*pNew));
  if( pNew ){
    pNew->pConfig = pConfig;
    rc = hctFileServerFind(pNew, zFile);
    if( rc==SQLITE_OK ){
      HctFileServer *pServer = pNew->pServer;

      sqlite3_mutex_enter(pServer->pMutex);
      rc = hctFileServerInit(pServer, pConfig, zFile);
      if( rc==SQLITE_OK && pServer->szPage>0 ){
        pNew->szPage = pServer->szPage;
        pNew->pMapping = pServer->pMapping;
        pNew->pMapping->nRef++;
      }
      pNew->eInitState = pServer->eInitState;
      pNew->iFileId = pServer->iNextFileId++;
      sqlite3_mutex_leave(pServer->pMutex);

      if( rc==SQLITE_OK ){
        sqlite3HctTMapClientNew(
            pServer->pTMapServer, pConfig, &pNew->pTMapClient
        );
      }
      if( rc==SQLITE_OK ){
        pNew->pPManClient = sqlite3HctPManClientNew(
            &rc, pConfig, pServer->pPManServer, pNew
        );
      }
    }else{
      sqlite3_free(pNew);
      pNew = 0;
    }

    if( rc!=SQLITE_OK ){
      sqlite3HctFileClose(pNew);
      pNew = 0;
    }
  }
  assert( (rc==SQLITE_OK)==(pNew!=0) );
  *pRc = rc;
  return pNew;
}

HctTMapClient *sqlite3HctFileTMapClient(HctFile *pFile){
  return pFile->pTMapClient;
}
HctPManClient *sqlite3HctFilePManClient(HctFile *pFile){
  return pFile->pPManClient;
}

void sqlite3HctFileClose(HctFile *pFile){
  if( pFile ){
    HctFileServer *pDel = 0;
    HctFile **pp;
    HctFileServer *pServer = pFile->pServer;

    /* Release the page-manager client */
    sqlite3HctPManClientFree(pFile->pPManClient);
    pFile->pPManClient = 0;

    /* Release the transaction map client */
    sqlite3HctTMapClientFree(pFile->pTMapClient);
    pFile->pTMapClient = 0;

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

      sqlite3HctTMapServerFree(pDel->pTMapServer);
      pDel->pTMapServer = 0;

      sqlite3HctPManServerFree(pDel->pPManServer);
      pDel->pPManServer = 0;

      if( pMapping ){
        pDel->pMapping = 0;
        for(i=0; i<pMapping->nChunk; i++){
          HctMappingChunk *pChunk = &pMapping->aChunk[i];
          if( pChunk->aMap ) hctFileMunmap(pChunk->aMap, szChunkMap);
          if( pChunk->pData ) hctFileMunmap(pChunk->pData, szChunkData);
        }
        hctMappingUnref(pMapping);
      }

      /* Close the data files and the mapping file. */
      for(i=0; i<pDel->nFdDb; i++){ 
        if( pDel->aFdDb[i]>0 ) close(pDel->aFdDb[i]); 
      }
      if( pDel->fdMap ) close(pDel->fdMap);

      sqlite3HctJournalServerFree(pDel->pJrnlPtr);

      sqlite3_free(pDel->zDir);
      sqlite3_free(pDel->zPath);
      sqlite3_mutex_free(pDel->pMutex);
      sqlite3_free(pDel);
    }

    /* Finally, free the HctFile object */
    sqlite3_free(pFile);
  }
}

u32 sqlite3HctFileMaxpage(HctFile *pFile){
  u64 iVal = hctFilePagemapGet(pFile->pMapping, HCT_PAGEMAP_PHYSICAL_EOF);
  return (iVal & 0xFFFFFFFF);
}

/*
** Set the flags in mask within page-map slot iSlot.
*/
static int hctFileSetFlag(HctFile *pFile, u32 iSlot, u64 mask){
  int rc = hctFileGrowMappingForSlot(pFile, iSlot);
  if( rc==SQLITE_OK ){
    HctMapping *pMapping = pFile->pMapping;
    while( 1 ){
      u64 iVal = hctFilePagemapGet(pMapping, iSlot);
      if( hctFilePagemapSet(pFile, iSlot, iVal, iVal | mask) ) break;
    }
  }
  return rc;
}

/*
** Clear the flags in mask within page-map slot iSlot.
*/
static int hctFileClearFlag(HctFile *pFile, u32 iSlot, u64 mask){
  int rc = hctFileGrowMappingForSlot(pFile, iSlot);
  if( rc==SQLITE_OK ){
    HctMapping *pMapping = pFile->pMapping;
    while( 1 ){
      u64 iVal = hctFilePagemapGet(pMapping, iSlot);
      if( hctFilePagemapSet(pFile, iSlot, iVal, iVal & ~mask) ) break;
    }
  }
  return rc;
}


int sqlite3HctFileRootFree(HctFile *pFile, u32 iRoot){
  /* TODO - do something with freed root-page */
  return SQLITE_OK;
}

int sqlite3HctFilePageClearIsRoot(HctFile *pFile, u32 iRoot){
  return hctFileClearFlag(pFile, iRoot, HCT_PMF_LOGICAL_IS_ROOT);
}
int sqlite3HctFilePageClearInUse(HctFile *pFile, u32 iPg, int bLogic){
  u64 flag = bLogic ? HCT_PMF_LOGICAL_IN_USE : HCT_PMF_PHYSICAL_IN_USE;
  return hctFileClearFlag(pFile, iPg, flag);
}

int sqlite3HctFileTreeFree(HctFile *pFile, u32 iRoot, int bImmediate){
  u64 iTid = bImmediate ? 0 : pFile->iCurrentTid;
  return sqlite3HctPManFreeTree(pFile->pPManClient, pFile, iRoot, iTid, 0);
}

int sqlite3HctFileTreeClear(HctFile *pFile, u32 iRoot){
  return sqlite3HctPManFreeTree(pFile->pPManClient, pFile, iRoot, 0, 1);
}

static int hctFilePagemapGetGrow(HctFile *pFile, u32 iPg, u64 *piVal){
  int rc = hctFileGrowMapping(pFile, 1+(iPg>>pFile->pMapping->mapShift));
  if( rc==SQLITE_OK ){
    *piVal = hctFilePagemapGet(pFile->pMapping, iPg);
  }
  return rc;
}

/*
** Obtain the lower 32-bits of the value currently stored in slot iSlot.
*/
static int hctFilePagemapGetGrow32(HctFile *pFile, u32 iSlot, u32 *piVal){
  int rc;
  u64 val = 0;
  rc = hctFilePagemapGetGrow(pFile, iSlot, &val);
  *piVal = (u32)(val & 0xFFFFFFFF);
  return rc;
}


static int hctFilePagemapPtr(HctFile *pFile, u32 iPg, u8 **paData){
  int rc = hctFileGrowMapping(pFile, 1+(iPg>>pFile->pMapping->mapShift));
  if( rc==SQLITE_OK ){
    *paData = hctPagePtr(pFile->pMapping, iPg);
  }
  return rc;
}

int sqlite3HctFilePageGet(HctFile *pFile, u32 iPg, HctFilePage *pPg){
  int rc;
  assert( iPg!=0 );
  memset(pPg, 0, sizeof(*pPg));
  pPg->pFile = pFile;
  pPg->iPg = iPg;
  rc = hctFilePagemapGetGrow32(pFile, iPg, &pPg->iOldPg);
  if( rc==SQLITE_OK ){
    u32 iPhys = pPg->iOldPg;
    assert( iPhys!=0 );
    rc = hctFilePagemapPtr(pFile, iPhys, &pPg->aOld);
  }
  return rc;
}

u32 sqlite3HctFilePageMapping(HctFile *pFile, u32 iLogical, int *pbEvicted){
  u64 val = hctFilePagemapGet(pFile->pMapping, iLogical);
  *pbEvicted = (val & HCT_PMF_LOGICAL_EVICTED) ? 1 : 0;
  return (u32)(val & 0xFFFFFFFF);
}

/*
** Obtain a reference to physical page iPg.
*/
int sqlite3HctFilePageGetPhysical(HctFile *pFile, u32 iPg, HctFilePage *pPg){
  u32 iVal;
  int rc;
  assert( iPg!=0 );
  memset(pPg, 0, sizeof(*pPg));
  rc = hctFilePagemapGetGrow32(pFile, iPg, &iVal);
  if( rc==SQLITE_OK ){
    pPg->iOldPg = iPg;
    pPg->aOld = (u8*)hctPagePtr(pFile->pMapping, iPg);
  }
  return rc;
}

static u32 hctFileAllocPg(int *pRc, HctFile *pFile, int bLogical){
  int rc = *pRc;
  u32 iRet = 0;

  if( bLogical==0 ) pFile->nPageAlloc++;
  iRet = sqlite3HctPManAllocPg(&rc, pFile->pPManClient, pFile, bLogical);
  if( rc==SQLITE_OK ){
    rc = hctFileGrowMappingForSlot(pFile, iRet);
    if( rc!=SQLITE_OK ){
      /* TODO: Something about this resource leak */
      iRet = 0;
    }
  }

  *pRc = rc;
  return iRet;
}

/*
** This function makes the page object pPg writable if it is not already
** so. Specifically, it allocates a new physical page and sets the
** following variables accordingly:
**
**   HctFilePage.iNewPg
**   HctFilePage.aNew
**
** The PHYSICAL_IN_USE flag is set on the new physical page allocated
** here.
*/
static void hctFilePageMakeWritable(int *pRc, HctFilePage *pPg){
  if( pPg->aNew==0 ){
    HctFile *pFile = pPg->pFile;
    u32 iNewPg = hctFileAllocPg(pRc, pFile, 0);
    if( iNewPg ){
      hctFileSetFlag(pPg->pFile, iNewPg, HCT_PMF_PHYSICAL_IN_USE);
      pPg->iNewPg = iNewPg;

      if( pFile->pServer->bReadOnlyMap ){
        pPg->aNew = (u8*)sqlite3_malloc(pFile->szPage);
        /* todo: handle oom here */
      }else{
        pPg->aNew = (u8*)hctPagePtr(pPg->pFile->pMapping, iNewPg);
      }
    }
  }
}


#if 0 
static void debug_printf(const char *zFmt, ...){
  va_list ap;
  va_start(ap, zFmt);
  vprintf(zFmt, ap);
  va_end(ap);
}

static void debug_slot_value(HctFile *pFile, u32 iSlot){
  u64 iVal = hctFilePagemapGet(pFile->pMapping, iSlot);
  printf("[flags=%02x val=%lld]", (u32)(iVal>>56), iVal & HCT_PAGEMAP_VMASK);
}

#define DEBUG_PAGE_MUTEX_ENTER(pPg) \
    sqlite3_mutex_enter(pPg->pFile->pServer->pMutex)

#define DEBUG_PAGE_MUTEX_LEAVE(pPg) \
    fflush(stdout); sqlite3_mutex_leave(pPg->pFile->pServer->pMutex)

#define DEBUG_PRINTF(...) debug_printf(__VA_ARGS__)
#define DEBUG_SLOT_VALUE(pFile, iSlot) debug_slot_value(pFile, iSlot)

void sqlite3HctFileDebugPrint(HctFile *pFile, const char *zFmt, ...){
  va_list ap;
  sqlite3_mutex_enter(pFile->pServer->pMutex);
  printf("f=%d: ", pFile->iFileId);
  va_start(ap, zFmt);
  vprintf(zFmt, ap);
  va_end(ap);
  sqlite3_mutex_leave(pFile->pServer->pMutex);
}

#else
# define DEBUG_PAGE_MUTEX_ENTER(x)
# define DEBUG_PAGE_MUTEX_LEAVE(x)
# define DEBUG_PRINTF(...)
# define DEBUG_SLOT_VALUE(x,y)
void sqlite3HctFileDebugPrint(HctFile *pFile, const char *zFmt, ...){ }
#endif

void hctFileFreePg(
  int *pRc, 
  HctFile *pFile, 
  i64 iTid,                       /* Associated TID value */
  u32 iPg,                        /* Page number */
  int bLogical                    /* True for logical, false for physical */
){
  if( pFile->eInitState>=HCT_INIT_RECOVER1 ){
    sqlite3HctPManFreePg(pRc, pFile->pPManClient, iTid, iPg, bLogical);
  }
}


static int hctFilePageFlush(HctFilePage *pPg){
  int rc = SQLITE_OK;
  if( pPg->aNew ){
    u32 iOld = pPg->iOldPg;

    DEBUG_PAGE_MUTEX_ENTER(pPg);
    DEBUG_PRINTF("f=%d: Flushing page %d orig=", pPg->pFile->iFileId, pPg->iPg);
    DEBUG_SLOT_VALUE(pPg->pFile, pPg->iPg);
    DEBUG_PRINTF(" (ioldpg=%d) (inewpg=%d)", pPg->iOldPg, pPg->iNewPg);

    DEBUG_PRINTF("\n");
    DEBUG_PAGE_MUTEX_LEAVE(pPg);

    if( pPg->pFile->pServer->bReadOnlyMap ){
      rc = hctPageWriteToDisk(pPg->pFile->pServer, pPg->iNewPg, pPg->aNew);
    }

    if( rc==SQLITE_OK ){
      if( !hctFilePagemapSetLogical(pPg->pFile, pPg->iPg, iOld, pPg->iNewPg) ){
        rc = SQLITE_LOCKED_ERR(pPg->iPg, "flush");
      }else{
        if( iOld ){
          u64 iTid = pPg->pFile->iCurrentTid;
          hctFileFreePg(&rc, pPg->pFile, iTid, iOld, 0);
          hctFileClearFlag(pPg->pFile, iOld, HCT_PMF_PHYSICAL_IN_USE);
        }
        pPg->iOldPg = pPg->iNewPg;
        if( pPg->pFile->pServer->bReadOnlyMap ){
          sqlite3_free(pPg->aNew);
          pPg->aOld = hctPagePtr(pPg->pFile->pMapping, pPg->iOldPg);
        }else{
          pPg->aOld = pPg->aNew;
        }
        pPg->aNew = 0;
        pPg->iNewPg = 0;
      }
    }

    DEBUG_PAGE_MUTEX_ENTER(pPg);
    DEBUG_PRINTF("f=%d:", pPg->pFile->iFileId);

    DEBUG_PRINTF(" rc=%d final=", rc);
    DEBUG_SLOT_VALUE(pPg->pFile, pPg->iPg);
    DEBUG_PRINTF("%s\n", rc==SQLITE_LOCKED ? "  SQLITE_LOCKED" : "");
    DEBUG_PAGE_MUTEX_LEAVE(pPg);
  }
  return rc;
}

int sqlite3HctFilePageCommit(HctFilePage *pPg){
  assert( pPg->iPg );
  return hctFilePageFlush(pPg);
}

int sqlite3HctFilePageEvict(HctFilePage *pPg, int bIrrevocable){
  int ret;

  DEBUG_PAGE_MUTEX_ENTER(pPg);
  DEBUG_PRINTF("f=%d: Evicting page %d (irrecocable=%d) orig=", 
      pPg->pFile->iFileId, pPg->iPg, bIrrevocable
  );
  DEBUG_SLOT_VALUE(pPg->pFile, pPg->iPg);

  ret = hctFileSetEvicted(pPg->pFile, pPg->iPg, pPg->iOldPg, bIrrevocable);
  ret = (ret ? SQLITE_OK : SQLITE_LOCKED_ERR(pPg->iPg, "evict"));

  DEBUG_PRINTF(" rc=%d final=", ret);
  DEBUG_SLOT_VALUE(pPg->pFile, pPg->iPg);
  DEBUG_PRINTF("%s\n", ret==SQLITE_LOCKED ? "  SQLITE_LOCKED" : "");
  DEBUG_PAGE_MUTEX_LEAVE(pPg);
  return ret;
}

void sqlite3HctFilePageUnevict(HctFilePage *pPg){
  DEBUG_PAGE_MUTEX_ENTER(pPg);
  DEBUG_PRINTF("f=%d: Unevicting page %d orig=", pPg->pFile->iFileId, pPg->iPg);
  DEBUG_SLOT_VALUE(pPg->pFile, pPg->iPg);

  hctFileClearEvicted(pPg->pFile, pPg->iPg);

  DEBUG_PRINTF(" final=");
  DEBUG_SLOT_VALUE(pPg->pFile, pPg->iPg);
  DEBUG_PRINTF("\n");
  DEBUG_PAGE_MUTEX_LEAVE(pPg);
}

int sqlite3HctFilePageIsEvicted(HctFile *pFile, u32 iPgno){
  u64 val;
  int rc = hctFilePagemapGetGrow(pFile, iPgno, &val);
  return (rc || (val & HCT_PMF_LOGICAL_EVICTED)!=0);
}

int sqlite3HctFilePageIsFree(HctFile *pFile, u32 iPgno, int bLogical){
  u64 iVal = hctFilePagemapGet(pFile->pMapping, iPgno);
  u64 mask = (bLogical ? HCT_PMF_LOGICAL_IN_USE : HCT_PMF_PHYSICAL_IN_USE);
  return (iVal & mask) ? 0 : 1;
}

int sqlite3HctFilePageRelease(HctFilePage *pPg){
  int rc = SQLITE_OK;
  if( pPg->iPg ){
    if( pPg->aNew!=pPg->aOld ){
      rc = hctFilePageFlush(pPg);
    }
  }else if( pPg->aNew && pPg->pFile->pServer->bReadOnlyMap ){
    rc = hctPageWriteToDisk(pPg->pFile->pServer, pPg->iNewPg, pPg->aNew);
    sqlite3_free(pPg->aNew);
  }
  memset(pPg, 0, sizeof(*pPg));
  return rc;
}



/*
** Allocate a new physical page and set (*pPg) to refer to it. The new
** physical page number is available in HctFilePage.iNewPg.
*/
int sqlite3HctFilePageNewPhysical(HctFile *pFile, HctFilePage *pPg){
  int rc = SQLITE_OK;
  memset(pPg, 0, sizeof(*pPg));
  pPg->pFile = pFile;
  hctFilePageMakeWritable(&rc, pPg);
  return rc;
}

/*
** Allocate a new logical page. If parameter iPg is zero, then a new
** logical page number is allocated. Otherwise, it must be a logical page
** number obtained by an earlier call to sqlite3HctFileRootPgno().
*/
int sqlite3HctFilePageNew(HctFile *pFile, HctFilePage *pPg){
  int rc = SQLITE_OK;             /* Return code */
  u32 iLPg = hctFileAllocPg(&rc, pFile, 1);
  if( rc==SQLITE_OK ){
    memset(pPg, 0, sizeof(*pPg));
    pPg->pFile = pFile;
    pPg->iPg = iLPg;
    hctFilePagemapZeroValue(pFile, iLPg);
    hctFilePageMakeWritable(&rc, pPg);
  }

  return rc;
}

/*
** Allocate a new logical root page number.
*/
int sqlite3HctFileRootPgno(HctFile *pFile, u32 *piRoot){
  int rc = SQLITE_OK;
  u32 iRoot = hctFileAllocPg(&rc, pFile, 1);
  if( rc==SQLITE_OK ){
    hctFilePagemapZeroValue(pFile, iRoot);
    *piRoot = iRoot;
  }
  return rc;
}

/*
** Parameter iRoot is a root page number previously obtained from
** sqlite3HctFileRootPgno(). This function allocates a physical
** page to go with the logical one.
*/
int sqlite3HctFileRootNew(HctFile *pFile, u32 iRoot, HctFilePage *pPg){
  int rc = SQLITE_OK;             /* Return code */

  memset(pPg, 0, sizeof(*pPg));
  pPg->pFile = pFile;
  pPg->iPg = iRoot;
  hctFilePageMakeWritable(&rc, pPg);

  /* Set the LOGICAL_IN_USE and LOGICAL_IS_ROOT flags on page iRoot. At
  ** the same time, set the mapping to 0. Take care not to clear the
  ** PHYSICAL_IN_USE flag while doing so, in case there is a physical
  ** page with page number iRoot currently in use somewhere.  */
  while( rc==SQLITE_OK ){
    u64 i1 = hctFilePagemapGet(pFile->pMapping, iRoot);
    u64 i2 = (i1 & HCT_PMF_PHYSICAL_IN_USE);
    i2 |= (HCT_PMF_LOGICAL_IS_ROOT|HCT_PMF_LOGICAL_IN_USE);
    if( hctFilePagemapSet(pFile, iRoot, i1, i2) ) break;
  }

  return rc;
}

void sqlite3HctFilePageUnwrite(HctFilePage *pPg){
  int rc = SQLITE_OK;
  if( pPg->aNew ){
    hctFileClearFlag(pPg->pFile, pPg->iNewPg, HCT_PMF_PHYSICAL_IN_USE);
    hctFileFreePg(&rc, pPg->pFile, 0, pPg->iNewPg, 0);
    if( pPg->pFile->pServer->bReadOnlyMap ){
      sqlite3_free(pPg->aNew);
    }
    pPg->iNewPg = 0;
    pPg->aNew = 0;
    if( pPg->iOldPg==0 ){
      assert( pPg->aOld==0 );
      hctFileFreePg(&rc, pPg->pFile, 0, pPg->iPg, 1);
      pPg->iPg = 0;
    }
  }
}

int sqlite3HctFilePageWrite(HctFilePage *pPg){
  int rc = SQLITE_OK;             /* Return code */
  hctFilePageMakeWritable(&rc, pPg);
  return rc;
}

int sqlite3HctFilePageDirectWrite(HctFilePage *pPg){
  if( pPg->aNew==0 ){
    assert( pPg->pFile->pServer->bReadOnlyMap==0 );
    pPg->aNew = pPg->aOld;
  }
  return SQLITE_OK;
}

u64 sqlite3HctFilePeekTransid(HctFile *pFile){
  return hctFilePagemapGet(pFile->pMapping, HCT_PAGEMAP_TRANSID_EOF);
}

u64 sqlite3HctFileAllocateTransid(HctFile *pFile){
  u64 iVal = hctFilePagemapIncr(pFile, HCT_PAGEMAP_TRANSID_EOF, 1);
  pFile->iCurrentTid = (iVal & HCT_TID_MASK);
  return pFile->iCurrentTid;
}
u64 sqlite3HctFileAllocateCID(HctFile *pFile, int nWrite){
  assert( nWrite>0 );
  return hctFileAtomicIncr(pFile, &pFile->pServer->iCommitId, nWrite);
}

void sqlite3HctFileSetCID(HctFile *pFile, u64 iVal){
  HctAtomicStore(&pFile->pServer->iCommitId, iVal);
}

u64 sqlite3HctFileIncrWriteCount(HctFile *pFile, int nIncr){
  return hctFileAtomicIncr(pFile, &pFile->pServer->nWriteCount, nIncr);
}

u64 sqlite3HctFileGetSnapshotid(HctFile *pFile){
  return HctAtomicLoad( &pFile->pServer->iCommitId );
}

int sqlite3HctFilePgsz(HctFile *pFile){
  return pFile->szPage;
}

void sqlite3HctFileSetJrnlPtr(
  HctFile *pFile, 
  void *pPtr, 
  void(*xDel)(void*)
){
  assert( pFile->pServer->pJrnlPtr==0 );
  assert( pFile->pServer->xJrnlDel==0 );
  pFile->pServer->pJrnlPtr = pPtr;
  pFile->pServer->xJrnlDel = xDel;
}

void *sqlite3HctFileGetJrnlPtr(HctFile *pFile){
  return pFile->pServer->pJrnlPtr;
}

/*
** Return the current "safe" TID value.
*/
u64 sqlite3HctFileSafeTID(HctFile *pFile){
  return sqlite3HctTMapSafeTID(pFile->pTMapClient);
}

/*
** Allocate a block of nPg physical or logical page ids from the 
** end of the current range.
*/
u32 sqlite3HctFilePageRangeAlloc(HctFile *pFile, int bLogical, int nPg){
  u32 iSlot = HCT_PAGEMAP_PHYSICAL_EOF - bLogical;
  u64 iNew = 0;

  assert( bLogical==0 || iSlot==HCT_PAGEMAP_LOGICAL_EOF );
  assert( bLogical!=0 || iSlot==HCT_PAGEMAP_PHYSICAL_EOF );

  /* Increment the selected slot by nPg. The returned value, iNew, is the 
  ** new value of the slot - the last page in the range allocated. */
  iNew = hctFilePagemapIncr(pFile, iSlot, nPg);

  /* Return the first page number in the range of nPg allocated */
  return (iNew+1 - nPg);
}

/*
** This function is called by the upper layer to clear the:
**
**   * LOGICAL_IN_USE flag on the specified page id, and the
**   * PHYSICAL_IN_USE flag on currently mapped physical page id.
**
** If parameter bReuseNow is true, then the page was never properly linked
** into a list, and so the logical and physical page ids can be reused 
** immediately. Otherwise, they are handled as if freed by the current
** transaction.
*/
int sqlite3HctFileClearInUse(HctFilePage *pPg, int bReuseNow){
  int rc = SQLITE_OK;
  if( pPg->pFile ){
    u64 iTid = pPg->pFile->iCurrentTid;
    u32 iPhysPg = pPg->iOldPg;

    assert( pPg->iPg>0 );
    assert( pPg->iOldPg>0 );

#ifdef SQLITE_DEBUG
    if( bReuseNow==0 ){
      u64 iVal = hctFilePagemapGet(pPg->pFile->pMapping, pPg->iPg);
      assert( iVal & HCT_PMF_LOGICAL_EVICTED );
    }
#endif

    hctFileClearFlag(pPg->pFile, pPg->iPg, HCT_PMF_LOGICAL_IN_USE);
    hctFileClearFlag(pPg->pFile, iPhysPg, HCT_PMF_PHYSICAL_IN_USE);
    hctFileFreePg(&rc, pPg->pFile, iTid, pPg->iPg, 1);
    hctFileFreePg(&rc, pPg->pFile, iTid, iPhysPg, 0);
  }

  return rc;
}

int sqlite3HctFileClearPhysInUse(HctFile *pFile, u32 pgno, int bReuseNow){
  u64 iTid = pFile->iCurrentTid;
  int rc = SQLITE_OK;
  
  hctFileClearFlag(pFile, pgno, HCT_PMF_PHYSICAL_IN_USE);
  hctFileFreePg(&rc, pFile, iTid, pgno, 0);
  return rc;
}

int sqlite3HctFileLogFileId(HctFile *pFile, int iFile){
  assert( iFile==0 || iFile==1 );
  return pFile->iFileId*2 + iFile;
}
char *sqlite3HctFileLogFileName(HctFile *pFile, int iId){
  HctFileServer *pServer = pFile->pServer;
  return sqlite3_mprintf("%s-log-%d", pServer->zPath, iId);
}

char *sqlite3HctFileLogFile(HctFile *pFile, int iFile){
  char *zRet = 0;
  HctFileServer *pServer = pFile->pServer;
  assert( iFile==0 || iFile==1 );
  zRet = sqlite3_mprintf("%s-log-%d", pServer->zPath, pFile->iFileId*2+iFile);
  return zRet;
}

int sqlite3HctFileStartRecovery(HctFile *pFile, int iStage){
  int bRet = 0;
  if( pFile->eInitState==iStage ){
    HctFileServer *pServer = pFile->pServer;
    sqlite3_mutex_enter(pServer->pMutex);
    if( pServer->eInitState==iStage ){
      bRet = 1;
    }else{
      pFile->eInitState = pServer->eInitState;
      sqlite3_mutex_leave(pServer->pMutex);
    }
  }
  return bRet;
}

int sqlite3HctFileFinishRecovery(HctFile *pFile, int iStage, int rc){
  HctFileServer *pServer = pFile->pServer;
  if( rc==SQLITE_OK ){
    pFile->eInitState = iStage+1;
    pServer->eInitState = iStage+1;
  }
  sqlite3HctPManClientHandoff(pFile->pPManClient);
  sqlite3_mutex_leave(pFile->pServer->pMutex);
  return rc;
}

int sqlite3HctFileRecoverFreelists(
  HctFile *pFile,                 /* File to recover freelists for */
  int nRoot, i64 *aRoot,          /* Array of root page numbers */
  int nPhys, i64 *aPhys           /* Sorted array of phys. pages to preserve */
){
  int rc = SQLITE_OK;
  HctFileServer *pServer = pFile->pServer;
  HctPManServer *pPManServer = pServer->pPManServer;
  HctMapping *pMapping = pServer->pMapping;
  u64 iSafeTid = hctFilePagemapGet(pMapping, HCT_PAGEMAP_TRANSID_EOF);
  u64 nPg1 = hctFilePagemapGet(pMapping, HCT_PAGEMAP_PHYSICAL_EOF);
  u64 nPg2 = hctFilePagemapGet(pMapping, HCT_PAGEMAP_LOGICAL_EOF);
  u32 iPg;
  u32 nPg;
  u32 iPhysOff = ((HCT_HEADER_PAGESIZE*2)+pServer->szPage-1)/pServer->szPage;

  int iPhys = 0;

  nPg1 = nPg1 & HCT_PAGEMAP_VMASK;
  nPg2 = nPg2 & HCT_PAGEMAP_VMASK;

  /* TODO: Really - page-manager must be empty at this point. Should assert()
  ** that instead of making this call. */
  sqlite3HctPManServerReset(pPManServer);

  nPg = MAX((nPg1 & 0xFFFFFFFF), (nPg2 & 0xFFFFFFFF));
  for(iPg=1; iPg<=nPg; iPg++){
    u64 iVal = hctFilePagemapGetSafe(pMapping, iPg);

    if( (iVal & HCT_PMF_LOGICAL_IS_ROOT) && iPg>=3  ){
      int ii;
      for(ii=0; ii<nRoot && aRoot[ii]!=iPg; ii++);
      if( ii==nRoot ){
        /* A free root page! */
        sqlite3HctPManServerInitRoot(&rc, pPManServer, iSafeTid, pFile, iPg);
      }
    }

    if( (iVal & HCT_PMF_PHYSICAL_IN_USE)==0 
     && (iPg<=nPg1) 
     && (iPg>iPhysOff) 
    ){
      /* Check if page iPg is one that must be preserved. */
      u64 iTid = iSafeTid;
      while( iPhys<nPhys && aPhys[iPhys]<iPg ){
        iPhys++;
      }
      if( iPhys<nPhys && aPhys[iPhys]==iPg ){
        iTid = iSafeTid+1;
      }
      sqlite3HctPManServerInit(&rc, pPManServer, iTid, iPg, 0);
    }

    if( (iVal & HCT_PMF_LOGICAL_IN_USE)==0 
        && iPg<=nPg2 
        && iPg>=HCT_FIRST_LOGICAL
    ){
      sqlite3HctPManServerInit(&rc, pPManServer, iSafeTid, iPg, 1);
    }
  }

  return rc;
}

int sqlite3HctFileFindLogs(
  HctFile *pFile, 
  void *pCtx,
  int (*xLog)(void*, const char*)
){
  return hctFileFindLogs(pFile->pServer, pCtx, xLog);
}

int sqlite3HctFileRootArray(
  HctFile *pFile, 
  u32 **paiRoot, 
  int *pnRoot
){
  int nAlloc = 0;
  int nRoot = 0;
  u32 *aRoot = 0;
  u32 nLogic = 0;
  int ii;
  int rc;

  rc = hctFilePagemapGetGrow32(pFile, HCT_PAGEMAP_LOGICAL_EOF, &nLogic);
  for(ii=1; rc==SQLITE_OK && ii<=nLogic; ii++){
    u64 val;
    rc = hctFilePagemapGetGrow(pFile, ii, &val);
    if( rc==SQLITE_OK && (val & HCT_PMF_LOGICAL_IS_ROOT) ){
      if( nRoot>=nAlloc ){
        int nNew = (nAlloc ? nAlloc*2 : 16);
        u32 *aNew = (u32*)sqlite3_realloc(aRoot, nNew*sizeof(u32));
        if( aNew==0 ){
          rc = SQLITE_NOMEM_BKPT;
        }else{
          nAlloc = nNew;
          aRoot = aNew;
        }
      }

      if( rc==SQLITE_OK ){
        aRoot[nRoot++] = ii;
      }
    }
  }

  if( rc!=SQLITE_OK ){
    sqlite3_free(aRoot);
    aRoot = 0;
    nRoot = 0;
  }
  *paiRoot = aRoot;
  *pnRoot = nRoot;
  return rc;
}

u64 sqlite3HctFileWriteCount(HctFile *pFile){
  return pFile->nPageAlloc;
}

void sqlite3HctFileICArrays(
  HctFile *pFile, 
  u8 **paLogic, u32 *pnLogic, 
  u8 **paPhys, u32 *pnPhys
){
  int rc = SQLITE_OK;
  u32 nLogic = 0;
  u32 nPhys = 0;
  u8 *aLogic = 0;
  u8 *aPhys = 0;
  u32 ii;

  rc = hctFilePagemapGetGrow32(pFile, HCT_PAGEMAP_LOGICAL_EOF, &nLogic);
  if( rc==SQLITE_OK ){
    rc = hctFilePagemapGetGrow32(pFile, HCT_PAGEMAP_PHYSICAL_EOF, &nPhys);
  }

  if( rc==SQLITE_OK ){
    aLogic = (u8*)sqlite3HctMallocRc(&rc, (nLogic + nPhys) * sizeof(u8));
    if( aLogic ){
      aPhys = &aLogic[nLogic];
    }
  }

  for(ii=1; ii<=nLogic && rc==SQLITE_OK; ii++){
    u64 val;
    rc = hctFilePagemapGetGrow(pFile, ii, &val);
    if( rc==SQLITE_OK && (val & HCT_PMF_LOGICAL_IN_USE)==0 ){
      aLogic[ii-1] = 1;
    }
  }
  for(ii=1; ii<=nPhys && rc==SQLITE_OK; ii++){
    u64 val;
    rc = hctFilePagemapGetGrow(pFile, ii, &val);
    if( rc==SQLITE_OK && (val & HCT_PMF_PHYSICAL_IN_USE)==0 ){
      aPhys[ii-1] = 1;
    }
  }

  if( rc!=SQLITE_OK ){
    sqlite3_free(aLogic);
    aLogic = aPhys = 0;
    nLogic = nPhys = 0;
  }

  *paLogic = aLogic;
  *paPhys = aPhys;
  *pnLogic = nLogic;
  *pnPhys = nPhys;
}

i64 sqlite3HctFileStats(sqlite3 *db, int iStat, const char **pzStat){
  i64 iVal = -1;
  HctFile *pFile = sqlite3HctDbFile(sqlite3HctDbFind(db, 0));

  switch( iStat ){
    case 0:
      *pzStat = "cas_attempt";
      iVal = pFile->stats.nCasAttempt;
      break;
    case 1:
      *pzStat = "cas_fail";
      iVal = pFile->stats.nCasFail;
      break;
    case 2:
      *pzStat = "incr_attempt";
      iVal = pFile->stats.nIncrAttempt;
      break;
    case 3:
      *pzStat = "incr_fail";
      iVal = pFile->stats.nIncrFail;
      break;
    case 4:
      *pzStat = "mutex_attempt";
      iVal = pFile->stats.nMutex;
      break;
    case 5:
      *pzStat = "mutex_block";
      iVal = pFile->stats.nMutexBlock;
      break;
    default:
      break;
  }

  return iVal;
}

int sqlite3HctFileNFile(HctFile *pFile, int *pbFixed){
  int iRet = 0;
  HctFileServer *p = pFile->pServer;
  sqlite3_mutex_enter(p->pMutex);
  iRet = p->nFdDb;
  *pbFixed = (p->szPage>0);
  sqlite3_mutex_leave(p->pMutex);
  return iRet;
}

/*************************************************************************
** Beginning of vtab implemetation.
*************************************************************************/

#define HCT_PGMAP_SCHEMA         \
"  CREATE TABLE hct_pgmap("      \
"    slot INTEGER,"              \
"    value INTEGER,"             \
"    comment TEXT,"              \
"    physical_in_use BOOLEAN,"   \
"    logical_in_use BOOLEAN,"    \
"    logical_evicted BOOLEAN,"   \
"    logical_irrevicted BOOLEAN,"\
"    logical_is_root BOOLEAN"    \
"  );"

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

  rc = sqlite3_declare_vtab(db, HCT_PGMAP_SCHEMA);
  pNew = (pgmap_vtab*)sqlite3HctMallocRc(&rc, sizeof(*pNew));
  if( pNew ){
    pNew->db = db;
  }

  *ppVtab = (sqlite3_vtab*)pNew;
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
  return hctFilePagemapGetGrow(
      pCur->pFile, pCur->slotno, &pCur->iVal
  );
}

/*
** Advance a hctdb_cursor to its next row of output.
*/
static int pgmapNext(sqlite3_vtab_cursor *cur){
  pgmap_cursor *pCur = (pgmap_cursor*)cur;
  pCur->slotno++;
  return pgmapEof(cur) ? SQLITE_OK : pgmapLoadSlot(pCur);
}

static void pgmapGetComment(sqlite3_context *ctx, i64 iSlot){
  const char *zText = 0;

  switch( iSlot ){
    case HCT_ROOTPAGE_SCHEMA:
      zText = "ROOTPAGE_SCHEMA";
      break;
    case HCT_ROOTPAGE_META:
      zText = "ROOTPAGE_META";
      break;
    case HCT_PAGEMAP_LOGICAL_EOF:
      zText = "LOGICAL_EOF";
      break;
    case HCT_PAGEMAP_PHYSICAL_EOF:
      zText = "PHYSICAL_EOF";
      break;
    case HCT_PAGEMAP_TRANSID_EOF:
      zText = "TRANSID_EOF";
      break;
  }

  if( zText ){
    sqlite3_result_text(ctx, zText, -1, SQLITE_TRANSIENT);
  }
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
    case 2: {  /* pgno */
      pgmapGetComment(ctx, pCur->slotno);
      break;
    }
    case 3: {  /* physical_in_use */
      sqlite3_result_int64(ctx, (pCur->iVal & HCT_PMF_PHYSICAL_IN_USE)?1:0);
      break;
    }
    case 4: {  /* logical_in_use */
      sqlite3_result_int64(ctx, (pCur->iVal & HCT_PMF_LOGICAL_IN_USE)?1:0);
      break;
    }
    case 5: {  /* logical_evicted */
      sqlite3_result_int64(ctx, (pCur->iVal & HCT_PMF_LOGICAL_EVICTED)?1:0);
      break;
    }
    case 6: {  /* logical_irrevicted */
      sqlite3_result_int64(ctx, (pCur->iVal & HCT_PMF_LOGICAL_IRREVICTED)?1:0);
      break;
    }
    case 7: {  /* logical_is_root */
      sqlite3_result_int64(ctx, (pCur->iVal & HCT_PMF_LOGICAL_IS_ROOT)?1:0);
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
  if( idxNum ){
    i64 iVal = sqlite3_value_int64(argv[0]);
    assert( argc==1 );
    pCur->iMaxSlotno = MIN(pCur->iMaxSlotno, iVal);
    pCur->slotno = MAX(pCur->slotno, iVal);
  }
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
  int iPgnoEq = -1;
  int i;

  /* Search for a "slot = ?" constraint. */
  for(i=0; i<pIdxInfo->nConstraint; i++){
    struct sqlite3_index_constraint *p = &pIdxInfo->aConstraint[i];
    if( p->usable && p->iColumn==0 && p->op==SQLITE_INDEX_CONSTRAINT_EQ ){
      iPgnoEq = i;
    }
  }

  if( iPgnoEq>=0 ){
    pIdxInfo->idxNum = 1;
    pIdxInfo->aConstraintUsage[iPgnoEq].argvIndex = 1;
    pIdxInfo->estimatedCost = (double)10;
    pIdxInfo->estimatedRows = 10;
  }else{
    pIdxInfo->idxNum = 0;
    pIdxInfo->estimatedCost = (double)100;
    pIdxInfo->estimatedRows = 100;
  }

  return SQLITE_OK;
}

/* 
** This function is the implementation of the xUpdate callback used by 
** hctpgmap virtual tables. It is invoked by SQLite each time a row is 
** to be inserted, updated or deleted.
**
** A delete specifies a single argument - the rowid of the row to remove.
** 
** Update and insert operations pass:
**
**   1. The "old" rowid (for an UPDATE), or NULL (for an INSERT).
**   2. The "new" rowid.
**   3. Values for each of the 6 columns.
**
** Specifically:
**
**   apVal[2]: slot
**   apVal[3]: value
**   apVal[4]: comment
**   apVal[5]: physical_in_use
**   apVal[6]: logical_in_use
**   apVal[7]: logical_evicted
**   apVal[8]: logical_irrevicted
**   apVal[9]: logical_is_root
*/
static int pgmapUpdate(
  sqlite3_vtab *pVtab, 
  int nVal, 
  sqlite3_value **apVal, 
  sqlite3_int64 *piRowid
){
  pgmap_vtab *p = (pgmap_vtab*)pVtab;
  HctFile *pFile = sqlite3HctDbFile(sqlite3HctDbFind(p->db, 0));
  u32 iSlot = 0;
  u64 val = 0;
  u64 *pPtr = 0;

  i64 iValue = 0;
  int bPhysicalInUse = 0;
  int bLogicalInUse = 0;
  int bLogicalEvicted = 0;
  int bLogicalIrrevicted = 0;
  int bLogicalIsRoot = 0;

  if( nVal==1 || sqlite3_value_type(apVal[0])!=SQLITE_INTEGER ){
    return SQLITE_CONSTRAINT;
  }
  iSlot = sqlite3_value_int64(apVal[0]);

  iValue = sqlite3_value_int64(apVal[3]);
  bPhysicalInUse = sqlite3_value_int(apVal[5]);
  bLogicalInUse = sqlite3_value_int(apVal[6]);
  bLogicalEvicted = sqlite3_value_int(apVal[7]);
  bLogicalIrrevicted = sqlite3_value_int(apVal[8]);
  bLogicalIsRoot = sqlite3_value_int(apVal[9]);

  val = iValue & HCT_PAGEMAP_VMASK;
  val |= (bPhysicalInUse ? HCT_PMF_PHYSICAL_IN_USE : 0);
  val |= (bLogicalInUse ? HCT_PMF_LOGICAL_IN_USE : 0);
  val |= (bLogicalEvicted ? HCT_PMF_LOGICAL_EVICTED : 0);
  val |= (bLogicalIrrevicted ? HCT_PMF_LOGICAL_IRREVICTED : 0);
  val |= (bLogicalIsRoot ? HCT_PMF_LOGICAL_IS_ROOT : 0);

  pPtr = hctPagemapPtr(pFile->pMapping, iSlot);
  AtomicStore(pPtr, val);

  *piRowid = iSlot;
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
    /* xUpdate     */ pgmapUpdate,
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

int sqlite3HctIoerr(int rc){
  sqlite3_log(rc, "sqlite3HctIoerr() - rc=%d errno=%d\n", rc, (int)errno);
  assert( 0 );
  abort();
  return rc;
}

