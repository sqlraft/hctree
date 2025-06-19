/*
** 2023 January 6
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
*/

typedef struct HctFileServer HctFileServer;
typedef struct HctFile HctFile;

HctFile *sqlite3HctFileOpen(
  int *pRc,
  const char *zFile, 
  HctConfig *pConfig
);
void sqlite3HctFileClose(HctFile *pFile);

/*
** If the database has not yet been created on disk, create it. Or, if
** the db has already been created, then this function is a no-op.
*/
int sqlite3HctFileNewDb(HctFile *pFile);

u32 sqlite3HctFileMaxpage(HctFile *pFile);

typedef struct HctFilePage HctFilePage;
struct HctFilePage {
  u8 *aOld;                       /* Current buffer, or NULL */
  u8 *aNew;                       /* New buffer (to be populated) */

  /* Used internally by hct_file.c. Mostly... */
  u32 iPg;                        /* logical page number */
  u32 iNewPg;                     /* New physical page number */
  u32 iOldPg;                     /* Original physical page number */
  u8 bCommitPhaseOne;             /* True if this has been CommitPhaseOne()d */
  HctFile *pFile;
};

/*
** Allocate logical root page numbers. And free the same (required if the
** transaction is rolled back).
*/
int sqlite3HctFileRootPgno(HctFile *pFile, u32 *piRoot);
int sqlite3HctFileRootNew(HctFile *pFile, u32 iRoot, HctFilePage*);


int sqlite3HctFilePageNew(HctFile *pFile, HctFilePage *pPg);
int sqlite3HctFilePageNewTransfer(
  HctFile *pFile, 
  HctFilePage *pPg, 
  HctFilePage *pFrom
);

int sqlite3HctFilePageNewLogical(HctFile *pFile, HctFilePage *pPg);

/*
** Obtain a read-only reference to logical page iPg.
*/
int sqlite3HctFilePageGet(HctFile *pFile, u32 iPg, HctFilePage *pPg);

/*
** If the page is not already writable (if pPg->aNew==0), make it writable.
** This involves allocating a new physical page and setting pPg->aNew
** to point to the buffer.
*/
int sqlite3HctFilePageWrite(HctFilePage *pPg);

int sqlite3HctFilePageDirectWrite(HctFilePage *pPg);

/*
** This is a no-op if the page is not writable.
**
** If the page is already writable, reverse this so that will not be
** written out when PageRelease() or PageCommit() is called. This reclaims 
** the physical page that was allocated by the earlier PageWrite() call
** and sets pPg->aNew to NULL.
*/
void sqlite3HctFilePageUnwrite(HctFilePage *pPg);

/*
** This is a no-op if the page is not writable.
**
** Commit the new version of the page to disk (i.e. set the page-map entry 
** so that the logical page number now maps to the new version of the page
** in pPg->aNew). Then make pPg a non-writable reference to the logical
** page (so that pPg->aOld points to the new version of the page and
** pPg->aNew is NULL).
*/
int sqlite3HctFilePageCommit(HctFilePage *pPg);

int sqlite3HctFilePageCommitPhaseOne(HctFilePage *pPg);
void sqlite3HctFilePageCommitPhaseTwo(HctFilePage *pPg);
void sqlite3HctFilePageRollback(HctFilePage *pPg);

/*
** Evict the page from the data structure - i.e. set the LOGICAL_EVICTED
** flag for it. This operation fails if the LOGICAL_EVICTED flag has 
** already been set, or if the page has been written since it was read.
*/
int sqlite3HctFilePageEvict(HctFilePage *pPg, int bIrrevocable);

void sqlite3HctFilePageUnevict(HctFilePage *pPg);

int sqlite3HctFilePageIsEvicted(HctFile *pFile, u32 iPgno);
int sqlite3HctFilePageIsFree(HctFile *pFile, u32 iPgno, int bLogical);

/*
** Release a page reference obtained via an earlier call to 
** sqlite3HctFilePageGet() or sqlite3HctFilePageNew(). After this call
** pPg->aOld is NULL.
**
** If the page is writable, it is committed (see sqlite3HctFilePageCommit)
** before the reference is released.
*/
int sqlite3HctFilePageRelease(HctFilePage *pPg);


int sqlite3HctFilePageGetPhysical(HctFile *pFile, u32 iPg, HctFilePage *pPg);
int sqlite3HctFilePageNewPhysical(HctFile *pFile, HctFilePage *pPg);

u64 sqlite3HctFileAllocateTransid(HctFile *pFile);
u64 sqlite3HctFileAllocateCID(HctFile *pFile, int);
u64 sqlite3HctFileGetSnapshotid(HctFile *pFile);

u64 sqlite3HctFilePeekTransid(HctFile *pFile);

void sqlite3HctFileSetCID(HctFile *pFile, u64);

/*
** Increment the global write-count by nIncr, and return the final value. 
*/
u64 sqlite3HctFileIncrWriteCount(HctFile *pFile, int nIncr);

HctTMapClient *sqlite3HctFileTMapClient(HctFile*);

int sqlite3HctFilePgsz(HctFile *pFile);
int sqlite3HctFileVtabInit(sqlite3 *db);

u64 sqlite3HctFileSafeTID(HctFile*);
u32 sqlite3HctFilePageRangeAlloc(HctFile*, int bLogical, int nPg);

int sqlite3HctFileClearInUse(HctFilePage *pPg, int bReuseNow);
int sqlite3HctFileClearPhysInUse(HctFile *pFile, u32 pgno, int bReuseNow);

void sqlite3HctFileDebugPrint(HctFile *pFile, const char *zFmt, ...);

int sqlite3HctFileStartRecovery(HctFile *pFile, int iStage);
int sqlite3HctFileFinishRecovery(HctFile *pFile, int iStage, int rc);
int sqlite3HctFileRecoverFreelists(
  HctFile *pFile,                 /* File to recover freelists for */
  int nRoot, i64 *aRoot,          /* Array of root page numbers */
  int nPhys, i64 *aPhys           /* Sorted array of phys. pages to preserve */
);

int sqlite3HctFileFindLogs(HctFile*, void*, int(*)(void*, const char*));

u32 sqlite3HctFilePageMapping(HctFile *pFile, u32 iLogical, int *pbEvicted);

void sqlite3HctFileICArrays(HctFile*, u8**, u32*, u8**, u32*);
int sqlite3HctFileTreeFree(HctFile *, u32, int);
int sqlite3HctFileTreeClear(HctFile *, u32);
int sqlite3HctFilePageClearIsRoot(HctFile*, u32);
int sqlite3HctFilePageClearInUse(HctFile *pFile, u32 iPg, int bLogic);

#include <hctPManInt.h>
HctPManClient *sqlite3HctFilePManClient(HctFile*);

int sqlite3HctFileRootArray(HctFile*, u32**, int*);

/* Interface used by hct_stats virtual table */
i64 sqlite3HctFileStats(sqlite3*, int, const char**);

/*
** Return the total number of physical page allocations made during 
** the entire lifetime of this object.
*/
u64 sqlite3HctFileWriteCount(HctFile *pFile);

/*
** Return the number of files used to store data within the database (the
** value to return for "PRAGMA hct_ndbfile"). Before returning, set output
** parameter *pbFixed if the database has been created and the number
** of files is therefore fixed, or clear it if the db has yet to be created.
*/
int sqlite3HctFileNFile(HctFile *pFile, int *pbFixed);

void *sqlite3HctFileGetJrnlPtr(HctFile *pFile);

int sqlite3HctIoerr(int rc);

int sqlite3HctFileLogFileId(HctFile *pFile, int iFile);
char *sqlite3HctFileLogFileName(HctFile *pFile, int iId);

#define HCT_MAX_NPREFAULT 256
void sqlite3HctFilePrefault(HctFile *pFile, int nThread, int bMinorOnly, i64*);


