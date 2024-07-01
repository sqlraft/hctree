/*
** 2022 March 20
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

/*
** There is a single object of this type for each distinct database opened
** by the process. Allocated and later freed using the following functions.
**
**     sqlite3HctPManServerNew()
**     sqlite3HctPManServerFree()
**
** Once an HctPManServer object has been created, it is configured with 
** the set of free logical and physical pages, which the caller presumably
** discovers by scanning the page-map.
*/
typedef struct HctPManServer HctPManServer;
typedef struct HctFile HctFile;

HctPManServer *sqlite3HctPManServerNew(
  int *pRc,                       /* IN/OUT: Error code */
  HctFileServer *pFileServer      /* Associated file-server object */
);
void sqlite3HctPManServerFree(HctPManServer*);

/*
** This function is called multiple times while scanning the page-map
** during initialization. To load the initial set of free physical and
** logical pages.
*/
void sqlite3HctPManServerInit(
    int *pRc, HctPManServer*, u64 iTid, u32 iPg, int bLogical
);

/*
** Each separate database connection holds a handle of this type for
** the lifetime of the connection.
*/
typedef struct HctPManClient HctPManClient;

HctPManClient *sqlite3HctPManClientNew(
  int *pRc, 
  HctConfig*, 
  HctPManServer*, 
  HctFile*
);
void sqlite3HctPManClientFree(HctPManClient*);

/*
** Allocate a new logical or physical page.
*/
u32 sqlite3HctPManAllocPg(
  int *pRc,                       /* IN/OUT: Error code */
  HctPManClient *p,               /* page-manager client handle */
  HctFile *pFile,
  int bLogical
);

/*
** Mark a logical or physical page as no longer in use. Parameter iTid
** is the transaction-id associated with the transaction that freed the
** page. The page may be reused once all clients are accessing a 
** snapshot that includes this transaction. In other words, once the
** snapshot id of all readers is greater than or equal to the commit id
** that maps to transaction id iTid.
**
** Sometimes this function is called with iTid==0, to indicate that the
** page in question may be reused immediately.
*/
void sqlite3HctPManFreePg(
  int *pRc,                       /* IN/OUT: Error code */
  HctPManClient *p,               /* page-manager client handle */
  i64 iTid,                       /* Associated TID value */
  u32 iPg,                        /* Page number */
  int bLogical                    /* True for logical, false for physical */
);

void sqlite3HctPManClientHandoff(HctPManClient *p);

void sqlite3HctPManServerReset(HctPManServer *pServer);

int sqlite3HctPManVtabInit(sqlite3 *db);

/*
** Mark an entire tree of logical and physical pages as free. The iTid
** parameter works just as it does for sqlite3HctPManFreePg().
**
** SQLITE_OK is returned if successful, or an error code (e.g. SQLITE_NOMEM)
** otherwise.
*/
int sqlite3HctPManFreeTree(HctPManClient *p, HctFile*, u32 iRoot, u64 iTid);

int sqlite3HctPManServerInitRoot(int *pRc, HctPManServer*, u64, HctFile*, u32);
i64 sqlite3HctPManStats(sqlite3 *db, int iStat, const char **pzStat);
