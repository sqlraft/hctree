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

HctPManServer *sqlite3HctPManServerNew(int *pRc);
void sqlite3HctPManServerFree(HctPManServer*);

/*
** Each separate database connection holds a handle of this type for
** the lifetime of the connection.
*/
typedef struct HctPManClient HctPManClient;

HctPManClient *sqlite3HctPManClientNew(int *pRc, HctPManServer*, HctFile*);
void sqlite3HctPManClientFree(HctPManClient*);

/*
** Allocate a new logical or physical page.
**
** Parameter iCid is the smallest CID value that any current or future
** may read the snapshot of. In other words, the caller is guaranteeing that
** no reader, current or future, will ever read from a snapshot with an
** id smaller than iCid.
*/
u32 sqlite3HctPManAllocPg(
  int *pRc,                       /* IN/OUT: Error code */
  HctPManClient *p,               /* page-manager client handle */
  i64 iCid,
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








