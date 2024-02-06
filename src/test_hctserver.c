/*
** 2023 December 20
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

#include <tcl.h>
#include <assert.h>
#include <sqlite3.h>
#include <sqlite3hct.h>
#include <string.h>
#include <pthread.h>

#include <arpa/inet.h>
#include <poll.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>

#ifndef MIN
# define MIN(A,B) ((A)<(B)?(A):(B))
#endif
#ifndef MAX
# define MAX(A,B) ((A)>(B)?(A):(B))
#endif

typedef sqlite3_int64 i64;
typedef unsigned char u8;

/*
** Default port for leader to listen for connections on. This can be
** overidden with -port.
*/
#define TESTSERVER_DEFAULT_PORT 21212

/*
** Maximum jobs and followers for a single leader process.
*/
#define TESTSERVER_MAX_JOB 64          /* Max number of jobs per process */
#define TESTSERVER_MAX_FOLLOWER 16     /* Max number of follower nodes */

/*
** The protocol used to communicate between follower and leader nodes is
** very simple. The follower connects to the leader and sends either a
** SYNC or SUB message as described below. The leader replies with a 
** SYNCREPLY, SUBREPLY or ERROR message, and then closes the socket 
** connection.
** 
** Message Formats (follower to leader)
** ------------------------------------
**
**  Each message begins with a 32-bit message type - one
**  of the TESTSERVER_MESSAGE_* values defined below. The remainder of
**  the message depends on the message type:
**
**  TESTSERVER_MESSAGE_SYNC:
**   * A 64-bit CID value, and
**   * A SQLITE_HCT_JOURNAL_HASHSIZE byte xor/hash value.
**
**  TESTSERVER_MESSAGE_SUB:
**   * A 32-bit job number (jobs are numbered from 0 to (nJob-1)).
**
** Message Formats (leader to follower)
** ------------------------------------
**
**  TESTSERVER_MESSAGE_SYNCREPLY:
**   * A 32-bit integer containing the number of jobs.
**   * Zero or more journal entries, each formatted as:
**     * A 32-bit size field - the size of the following 4 fields combined.
**     * A 64-bit "cid" value,
**     * A 64-bit "schemacid" value,
**     * A nul-terminated utf-8 "schema" string,
**     * A blob of "data" for the entry.
**   * A 32-bit 0 - "0x00 0x00 0x00 0x00".
**
**  TESTSERVER_MESSAGE_SUBREPLY:
**   * Zero or more journal entries, formatted as for SYNCREPLY.
**
**  TESTSERVER_MESSAGE_ERROR:
**   * A 32-bit size field (nSz),
**   * Error string (nSz bytes, including nul-term).
**
** Protocol
** --------
**
**  The leader node listens on a well-known tcp/ip port for connections.
**  To connect a follower to a leader:
**
**    * The follower connects and sends a TESTSERVER_MESSAGE_SYNC message.
**
**    * The leader replies with either a TESTSERVER_MESSAGE_ERROR, or
**      a TESTSERVER_MESSAGE_SYNCREPLY. If the latter, it includes all 
**      transactions it currently has that the follower node does not.
**      Either way, the leader then closes the socket connection.
**
**    * The follower makes N connections to the leader, where N is the
**      number of jobs the leader has running. It sends a
**      TESTSERVER_MESSAGE_SUB message on each connection. 
**
**    * The follower then makes another connection to the leader, sending
**      a TESTSERVER_MESSAGE_SYNC message.
**
**    * The follower applies all journal entries received.
*/
#define TESTSERVER_MESSAGE_SYNC        1
#define TESTSERVER_MESSAGE_SUB         2
#define TESTSERVER_MESSAGE_ERROR       3
#define TESTSERVER_MESSAGE_SYNCREPLY   4
#define TESTSERVER_MESSAGE_SUBREPLY    5

/*
** Atomic read/write operators for the various integer types.
*/
#define TestAtomicStore(PTR,VAL) __atomic_store_n((PTR),(VAL), __ATOMIC_SEQ_CST)
#define TestAtomicLoad(PTR)      __atomic_load_n((PTR), __ATOMIC_SEQ_CST)

/* 
** Parameter zCmd should be the name of a Tcl database handle command 
** created by the [sqlite3] command. This function attempts to extract the 
** database handle and return it via output variable (*ppDb). If succesful,
** TCL_OK is returned. Otherwise, (*ppDb) is set to NULL, an error message
** left in interp, and TCL_ERROR returned.
*/
int getDbPointer(Tcl_Interp *interp, const char *zCmd, sqlite3 **ppDb);

/*
** From tclsqlite.c and test_hct.c - these add the SQLite + hctree interfaces 
** to the Tcl interpreter passed as the only argument.
*/
int Sqlite3_Init(Tcl_Interp *);
int SqliteHctTest_Init(Tcl_Interp *);

typedef struct TestServerJob TestServerJob;
typedef struct TestServer TestServer;

/* 
** General buffer type. Used by testBufferAppend(), recvDataBuf() and others.
*/
typedef struct TestBuffer TestBuffer;
struct TestBuffer {
  u8 *aBuf;
  int nBuf;
  int nAlloc;
};

/*
** Wrapper around a socket fd that buffers writes.
*/
typedef struct TestSocket TestSocket;
struct TestSocket {
  int fd;
  TestBuffer buf;
};

/*
** Each Tcl job (configured with the [testserver job] method) is represented
** by an instance of the following structure. Stored in the 
** TestServerJob.aJob[] array.
*/
struct TestServerJob {
  TestServer *pServer;            /* Server object */
  Tcl_Obj *pScript;               /* Tcl script to run */
  pthread_t tid;                  /* Thread-id of job thread */
  int bDone;                      /* Set to true once job is finished */
  char *zErr;                     /* ckalloc()'d error message (if any) */

  pthread_mutex_t mutex;
  int nFollower;
  TestSocket aFollower[TESTSERVER_MAX_FOLLOWER];
};

/*
** nSyncThread:
**  The number of threads that work on synchronization in follower
**  nodes, including the main thread. So a value of 1 here means no
**  extra threads are used for synchronization - just the main thread
**  that also calls recv() to read from the socket.
**
**  Set using the -syncthreads configuration option.
*/
struct TestServer {
  Tcl_Interp *interp;             /* If error, leave error message here */
  sqlite3 *db;
  char *zPath;                    /* Copy of DBFILE argument */
  int fdListen;                   /* Listening socket */
  i64 iTimeout;

  int nJob;
  TestServerJob aJob[TESTSERVER_MAX_JOB];

  /* Fields set by the [configure] method */
  int iPort;                      /* Tcp port to listen for connections on */
  char *zHost;                    /* Tcp host to connect to */
  int nSyncThread;                /* Number of threads helping with sync */
  int nSecond;                    /* Number of seconds to run for */
  int bFollower;                  /* True for follower, false for leader */
  int nSyncBytes;                 /* Bytes of data to quit syncing on */
};

/*
** Like strdup(). Result must be freed by ckfree().
*/
static char *test_strdup(const char *zIn){
  char *zRet = 0;
  if( zIn ){
    int nByte = strlen(zIn) + 1;
    zRet = (char*)ckalloc(nByte);
    memcpy(zRet, zIn, nByte);
  }
  return zRet;
}

/*
** Write 32 and 64 bit integer values to the supplied buffer, respectively
*/
static void putInt32(u8 *aBuf, int val){
  memcpy(aBuf, &val, 4);
}
static void putInt64(u8 *aBuf, i64 val){
  memcpy(aBuf, &val, 8);
}

/*
** Read 32 and 64 bit integer values from the supplied buffer, respectively
*/
static int getInt32(u8 *aBuf){
  int val;
  memcpy(&val, aBuf, 4);
  return val;
}
static i64 getInt64(const u8 *aBuf){
  i64 val;
  memcpy(&val, aBuf, 8);
  return val;
}

/*
** Free the allocation managed by buffer pBuf. The structure pointed to by
** pBuf itself is not freed.
*/
static void testBufferFree(TestBuffer *pBuf){
  ckfree(pBuf->aBuf);
  memset(pBuf, 0, sizeof(TestBuffer));
}

/*
** Append nData bytes of data from buffer aData[] to buffer pBuf.
*/
static void testBufferAppend(TestBuffer *pBuf, const u8 *aData, int nData){
  if( (pBuf->nBuf+nData)>pBuf->nAlloc ){
    int nNew = (pBuf->nBuf + nData) * 2;
    pBuf->aBuf = (u8*)ckrealloc(pBuf->aBuf, nNew);
    pBuf->nAlloc = nNew;
  }

  memcpy(&pBuf->aBuf[pBuf->nBuf], aData, nData);
  pBuf->nBuf += nData;
}

/*
** If (*pRc) is other than TCL_OK when this is called, it is a no-op. If
** it is TCL_OK, then an attempt to send nData bytes of data from buffer
** aData[] on socket file-descriptor fd. If an error occurs, (*pRc)
** is set to TCL_ERROR before returning.
*/
static void sendData(int *pRc, int fd, const u8 *aData, int nData){
  int nTotal = 0;
  while( *pRc==TCL_OK && nTotal<nData ){
    ssize_t res = 0;
    res = send(fd, &aData[nTotal], nData-nTotal, 0);
    if( res<0 ){
      *pRc = TCL_ERROR;
    }else{
      nTotal += res;
    }
  }
}

/*
** TestSocket API:
**
**    socketSendData()
**    socketSendInt32()
**    socketSendInt64()
**    socketFlush()
**    socketFlushAndFree()
*/
static void socketFlush(int *pRc, TestSocket *p){
  if( *pRc==TCL_OK ){
    sendData(pRc, p->fd, p->buf.aBuf, p->buf.nBuf);
  }
  p->buf.nBuf = 0;
}
static void socketSendData(int *pRc, TestSocket *p, const u8 *aData, int nData){
  if( p->buf.nBuf+nData>p->buf.nAlloc ){
    if( p->buf.nBuf==0 ){
      p->buf.nAlloc = 1<<20;
      p->buf.aBuf = ckalloc(p->buf.nAlloc);
    }else{
      socketFlush(pRc, p);
    }
  }
  testBufferAppend(&p->buf, aData, nData);
}
static void socketSendInt32(int *pRc, TestSocket *p, int val){
  u8 aBuf[4];
  putInt32(aBuf, val);
  socketSendData(pRc, p, aBuf, sizeof(aBuf));
}
static void socketSendInt64(int *pRc, TestSocket *p, i64 val){
  u8 aBuf[8];
  putInt64(aBuf, val);
  socketSendData(pRc, p, aBuf, sizeof(aBuf));
}
static void socketFlushAndFree(int *pRc, TestSocket *p){
  socketFlush(pRc, p);
  testBufferFree(&p->buf);
}

/*
** Return the current time in ms since julian day 0.0. 
*/
static i64 test_gettime(){
  i64 ret = 0;
  sqlite3_vfs *pVfs = sqlite3_vfs_find(0);
  pVfs->xCurrentTimeInt64(pVfs, &ret);
  return ret;
}

/*
** Debugging output functions.
*/
static i64 hct_test_time_zero = 0;
static void hct_test_debug_start(){
  hct_test_time_zero = test_gettime();
}
static void hct_test_debug(const char *zFmt, ...){
  i64 tm = test_gettime() - hct_test_time_zero;
  va_list ap;
  char *z;

  va_start(ap, zFmt);
  z = sqlite3_vmprintf(zFmt, ap);
  va_end(ap);

  printf("hct_testserver: tm=%lldms %s\n", tm, z);
  sqlite3_free(z);
}


static void sendInt32(int *pRc, int fd, int val){
  u8 aBuf[4];
  putInt32(aBuf, val);
  sendData(pRc, fd, aBuf, sizeof(aBuf));
}
static void sendInt64(int *pRc, int fd, i64 val){
  u8 aBuf[8];
  putInt64(aBuf, val);
  sendData(pRc, fd, aBuf, sizeof(aBuf));
}



/*
** sqlite3_hct_journal_hook() callback for connections used by job threads.
*/
static int hctServerJournalHook(
  void *pCtx,
  i64 iCid,
  const char *zSchema,
  const void *pData, int nData,
  i64 iSchemaCid
){
  int rc = SQLITE_OK;
  TestServerJob *pJob = (TestServerJob*)pCtx;
  int ii;
  int nSchema = strlen(zSchema) + 1;
  for(ii=0; ii<pJob->nFollower; ii++){
    TestSocket *p = &pJob->aFollower[ii];
    socketSendInt32(&rc, p, sizeof(i64)*2 + nSchema + nData);
    socketSendInt64(&rc, p, iCid);
    socketSendInt64(&rc, p, iSchemaCid);
    socketSendData(&rc, p, (const u8*)zSchema, nSchema);
    socketSendData(&rc, p, (const u8*)pData, nData);
    socketFlush(&rc, p);
  }
  return rc;
}

/*
** tclcmd: hct_testserver_timeout
*/
static int testTimeoutCmd(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  TestServer *p = (TestServer*)clientData;
  int bRet = 0;
  i64 iTimeout = TestAtomicLoad(&p->iTimeout);

  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }

  if( iTimeout>0 && test_gettime()>=iTimeout ) bRet = 1;
  Tcl_SetObjResult(interp, Tcl_NewIntObj(bRet));
  return TCL_OK;
}

/*
** The main() routine for a Tcl job thread. Within either a leader or 
** follower node.
*/
static void *hctServerJobThread(void *pCtx){
  TestServerJob *pJob = (TestServerJob*)pCtx;
  TestServer *p = pJob->pServer;
  Tcl_Interp *interp = 0;
  int rc = TCL_OK;
  Tcl_Obj *pOpenDb = 0;
  sqlite3 *db = 0;

  /* Create Tcl interpreter for this job */
  interp = Tcl_CreateInterp();
  Sqlite3_Init(interp);
  SqliteHctTest_Init(interp);

  /* Add the [hct_testserver_timeout] command. Returns true after the timer
  ** configured by the -seconds option expires.  */
  Tcl_CreateObjCommand(
      interp, "hct_testserver_timeout", testTimeoutCmd, (void*)p, 0
  );

  /* Open the database handle */
  pOpenDb = Tcl_NewObj();
  Tcl_IncrRefCount(pOpenDb);
  Tcl_ListObjAppendElement(interp, pOpenDb, Tcl_NewStringObj("sqlite3", -1));
  Tcl_ListObjAppendElement(interp, pOpenDb, Tcl_NewStringObj("db", -1));
  Tcl_ListObjAppendElement(interp, pOpenDb, Tcl_NewStringObj(p->zPath, -1));
  rc = Tcl_EvalObjEx(interp, pOpenDb, 0);
  Tcl_DecrRefCount(pOpenDb);

  /* Register the journal hook with the database handle just created. Do
  ** this for both leader and follower nodes, even though the callback will
  ** never be invoked for the read-only connections used on follower nodes. */
  if( rc==TCL_OK ){
    getDbPointer(interp, "db", &db);
    sqlite3_hct_journal_hook(db, pCtx, hctServerJournalHook);
  }

  /* Evaluate the job script and delete the intepreter */
  if( rc==TCL_OK ){
    rc = Tcl_EvalObjEx(interp, pJob->pScript, 0);
  }
  if( rc!=SQLITE_OK ){
    pJob->zErr = test_strdup(Tcl_GetStringResult(interp));
  }
  Tcl_DeleteInterp(interp);

  /* Set "job done" flag before returning. */
  TestAtomicStore(&pJob->bDone, 1);
  return 0;
}

/*
** Open a listening socket on port p->iPort, interface p->zHost. If 
** successful, leave the file-descriptor in p->fdListen and return TCL_OK.
** Otherwise, if an error occurs, leave an error message in p->interp
** and return TCL_ERROR.
*/
static int hctServerListen(TestServer *p){
  int fd = -1;
  struct sockaddr_in addr;
  int rc = TCL_OK;
  int reuse = 1;

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if( fd<0 ){
    Tcl_AppendResult(p->interp, "socket() failed", (char*)0);
    rc = TCL_ERROR;
  }

  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(int));

  if( rc==TCL_OK ){
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)p->iPort);
    addr.sin_addr.s_addr = inet_addr(p->zHost);
  }

  if( rc==TCL_OK && bind(fd, (struct sockaddr*)&addr, sizeof(addr))<0 ){
    Tcl_AppendResult(p->interp, "bind() failed", (char*)0);
    rc = TCL_ERROR;
  }

  if( rc==TCL_OK && listen(fd, 128)<0 ){
    Tcl_AppendResult(p->interp, "listen() failed", (char*)0);
    rc = TCL_ERROR;
  }

  if( rc==TCL_OK && fcntl(fd, F_SETFL, O_NONBLOCK)<0 ){
    Tcl_AppendResult(p->interp, "fcntl() failed", (char*)0);
    rc = TCL_ERROR;
  }

  if( rc==TCL_OK ){
    p->fdListen = fd;
  }else{
    if( fd>=0 ) close(fd);
  }

  return rc;
}

/*
** This function is a no-op if (*pRc) is not TCL_OK when it is called.
** Otherwise, an attempt is made to read nBuf bytes of data from 
** socket file-descriptor fd into buffer aBuf. If an error occurs,
** (*pRc) is set to TCL_ERROR before returning.
*/
static int recvData(int *pRc, int fd, u8 *aBuf, int nBuf){
  if( *pRc==TCL_OK ){
    int nRead = 0;
    while( nRead<nBuf ){
      int n = recv(fd, &aBuf[nRead], nBuf-nRead, 0);
      if( n<=0 ){
        *pRc = TCL_ERROR;
        return (n==0);
      }else{
        nRead += n;
      }
    }
  }
  return 0;
}

/*
** These functions are no-ops if (*pRc) is not TCL_OK when it is called.
** Otherwise, an attempt is made to read a 32 or 64-bit integer from socket
** file-descriptor fd into output variable (*piVal). If an error occurs,
** (*pRc) is set to TCL_ERROR before returning.
*/
static int recvInt32(int *pRc, int fd, int *piVal){
  u8 aBuf[4] = {0, 0, 0, 0};
  assert( sizeof(*piVal)==sizeof(aBuf) );
  if( recvData(pRc, fd, aBuf, sizeof(aBuf)) ) return 1;
  *piVal = getInt32(aBuf);
  return 0;
}
static void recvInt64(int *pRc, int fd, i64 *piVal){
  u8 aBuf[8] = {0, 0, 0, 0, 0, 0, 0, 0};
  assert( sizeof(*piVal)==sizeof(aBuf) );
  recvData(pRc, fd, aBuf, sizeof(aBuf));
  *piVal = getInt64(aBuf);
}

/*
** This function is a no-op if (*pRc) is not TCL_OK when it is called.
** Otherwise, an attempt is made to read nBuf bytes of data from 
** socket file-descriptor fd into resizable buffer pBuf. If an error
** occurs, (*pRc) is set to TCL_ERROR before returning.
*/
static void recvDataBuf(int *pRc, int fd, int nData, TestBuffer *pBuf){
  if( pBuf->nAlloc<nData ){
    pBuf->aBuf = (u8*)ckrealloc(pBuf->aBuf, nData*2);
    pBuf->nAlloc = nData*2;
  }
  recvData(pRc, fd, pBuf->aBuf, nData);
  pBuf->nBuf = nData;
}


/*
** Finalize the statement handle passed as the second argument. If (*pRc)
** is set to SQLITE_OK, then set (*pRc) to the return value of
** sqlite3_finalize() before returning.
*/
static void testServerFinalize(int *pRc, sqlite3_stmt *pStmt){
  int rc = sqlite3_finalize(pStmt);
  if( *pRc==SQLITE_OK ) *pRc = rc;
}

/*
** This function is a no-op if (*pRc) is not set to TCL_OK when this function
** is called. Assuming that it is, this function attempts to prepare SQL
** statement zSql against database handle TestServer.db. If successful,
** the statement handle is returned. Otherwise, (*pRc) is set to TCL_ERROR,
** an error message left in TestServer.interp and NULL returned.
*/
static sqlite3_stmt *testServerPrepare(
  int *pRc, 
  TestServer *p, 
  const char *zSql
){
  sqlite3_stmt *pRet = 0;
  if( *pRc==TCL_OK ){
    int rc = sqlite3_prepare_v2(p->db, zSql, -1, &pRet, 0);
    if( rc!=SQLITE_OK ){
      const char *zErr = sqlite3_errmsg(p->db);
      Tcl_AppendResult(p->interp, "sqlite3_prepare_v2(): ", zErr, (char*)0);
      *pRc = TCL_ERROR;
    }
  }
  return pRet;
}

/*
** A new follower has connected and requested a sync.
**
** This function first verifies that the follower is compatible with
** the leader database (using the iCid and aXor values). If not, then
** it sends a TESTSERVER_MESSAGE_ERROR message to the follower. If
** the follower is compatible with the leader database, it sends a
** TESTSERVER_MESSAGE_DATA message.
**
** Return SQLITE_OK if successful, or an SQLite error code if an error
** occurs. It is not an error if the follower db is incompatible with
** the local leader db. If an error code is returned, then an English
** language error message may be left in TestServer.interp.
*/
static int hctServerDoSync(
  TestServer *p,                  /* Server (leader) object */
  int newfd,                      /* File descriptor of new follower */
  i64 iCid,                       /* Last CID in follower db */
  const u8 *aXor                  /* XOR of all entries in follower db */
){
  const char *z1 = "SELECT hash FROM sqlite_hct_baseline";
  const char *z2 = "SELECT hash FROM sqlite_hct_journal WHERE cid<=?";
  const char *z3 = 
    "SELECT cid, schema, data, schemacid FROM sqlite_hct_journal WHERE cid>?";

  sqlite3_stmt *pStmt = 0;
  int rc = SQLITE_OK;
  u8 aHash[SQLITE_HCT_JOURNAL_HASHSIZE];

  TestSocket sock;
  memset(&sock, 0, sizeof(sock));
  sock.fd = newfd;

  memset(aHash, 0, sizeof(aHash));
  pStmt = testServerPrepare(&rc, p, z1);
  if( rc==SQLITE_OK && sqlite3_step(pStmt)==SQLITE_ROW ){
    const u8 *a = sqlite3_column_blob(pStmt, 0);
    memcpy(aHash, a, SQLITE_HCT_JOURNAL_HASHSIZE);
  }
  testServerFinalize(&rc, pStmt);
  pStmt = testServerPrepare(&rc, p, z2);
  if( rc==SQLITE_OK ) sqlite3_bind_int64(pStmt, 1, iCid);
  while( rc==SQLITE_OK && sqlite3_step(pStmt)==SQLITE_ROW ){
    const u8 *a = sqlite3_column_blob(pStmt, 0);
    sqlite3_hct_journal_hash(aHash, a);
  }
  testServerFinalize(&rc, pStmt);

  if( memcmp(aHash, aXor, SQLITE_HCT_JOURNAL_HASHSIZE) ){
    const char *zErr = "incompatible database version";
    int nErr = strlen(zErr);
    socketSendInt32(&rc, &sock, TESTSERVER_MESSAGE_ERROR);
    socketSendInt32(&rc, &sock, nErr+1);
    socketSendData(&rc, &sock, (const u8*)zErr, nErr+1);
  }else{
    i64 iThisCid = 0;
    int bLogged = 0;
    pStmt = testServerPrepare(&rc, p, z3);
    if( rc==SQLITE_OK ) sqlite3_bind_int64(pStmt, 1, iCid);
    socketSendInt32(&rc, &sock, TESTSERVER_MESSAGE_SYNCREPLY);
    socketSendInt32(&rc, &sock, p->nJob);
    while( rc==SQLITE_OK && sqlite3_step(pStmt)==SQLITE_ROW ){
      int nSchema = sqlite3_column_bytes(pStmt, 1);
      int nData = sqlite3_column_bytes(pStmt, 2);
      int nSz = sizeof(i64) + sizeof(i64) + nSchema+1 + nData;

      iThisCid = sqlite3_column_int64(pStmt, 0);
      if( bLogged==0 ){
        hct_test_debug("SYNC: first CID = %lld", iThisCid);
        bLogged = 1;
      }
      socketSendInt32(&rc, &sock, nSz);
      socketSendInt64(&rc, &sock, iThisCid);
      socketSendInt64(&rc, &sock, sqlite3_column_int64(pStmt, 3));
      socketSendData(&rc, &sock, sqlite3_column_text(pStmt, 1), nSchema+1);
      socketSendData(&rc, &sock, sqlite3_column_blob(pStmt, 2), nData);
    }
    hct_test_debug("SYNC: last CID = %lld (rc=%d)", iThisCid, rc);
    socketSendInt32(&rc, &sock, 0);
    testServerFinalize(&rc, pStmt);
  }

  socketFlushAndFree(&rc, &sock);
  close(newfd);
  return rc;
}

/*
** Accept a new connection on the listening socket TestServer.fdListen and
** handle the message sent on it.
*/
static int hctServerAccept(TestServer *p){
  int rc = TCL_OK;
  struct sockaddr_in address;
  socklen_t addrlen = sizeof(address);
  int newfd = accept(p->fdListen, (struct sockaddr*)&address, &addrlen);
  if( newfd>=0 ){
    int eType;
    int flags = fcntl(newfd, F_GETFL, 0);
    fcntl(newfd, F_SETFL, flags & ~O_NONBLOCK);

    /* Read message */
    recvInt32(&rc, newfd, &eType);

    if( eType==TESTSERVER_MESSAGE_SYNC ){
      /* SYNC message. Read the CID and xor value. */
      i64 iCid = 0;
      u8 aXor[SQLITE_HCT_JOURNAL_HASHSIZE];
      recvInt64(&rc, newfd, &iCid);
      recvData(&rc, newfd, aXor, sizeof(aXor));
      hctServerDoSync(p, newfd, iCid, aXor);
    }
    else if( eType==TESTSERVER_MESSAGE_SUB ){
      /* SUB message */
      int iJob = 0;
      recvInt32(&rc, newfd, &iJob);
      if( iJob<p->nJob ){
        TestServerJob *pJob = &p->aJob[iJob];
        pthread_mutex_lock(&pJob->mutex);
        if( pJob->nFollower<TESTSERVER_MAX_FOLLOWER ){
          sendInt32(&rc, newfd, TESTSERVER_MESSAGE_SUBREPLY);
          pJob->aFollower[pJob->nFollower++].fd = newfd;
          newfd = -1;
        }
        pthread_mutex_unlock(&pJob->mutex);
      }else{
        assert( 0 );
      }
    }else{
      /* An error - do nothing. */
      assert( 0 );
    }

    /* Unless this was a SUB, close the file-descriptor */
    if( newfd>=0 ) close(newfd);
  }else{
    assert( 0 );
  }

  return 0;
}

/*
** Implementation of testserver method [run] for leader nodes.
*/
static int hctLeaderRun(TestServer *p){
  int ii;
  int rc = TCL_OK;
  struct pollfd fds[1];
  int res = 0;

  rc = sqlite3_hct_journal_setmode(p->db, SQLITE_HCT_JOURNAL_MODE_LEADER);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(p->interp, 
        "sqlite3_hct_journal_setmode():", sqlite3_errmsg(p->db), (char*)0
    );
    return TCL_ERROR;
  }

  /* Listen for connections on the configured host and port. */
  rc = hctServerListen(p);
  if( rc!=SQLITE_OK ) return rc;
  memset(fds, 0, sizeof(fds));
  fds[0].fd = p->fdListen;
  fds[0].events = POLLIN;

  /* Start the tcl jobs */
  for(ii=0; ii<p->nJob; ii++){
    TestServerJob *pJob = &p->aJob[ii];
    pthread_mutex_init(&pJob->mutex, 0);
    pthread_create(&pJob->tid, NULL, hctServerJobThread, (void*)pJob);
  }

  while( 1 ){
    res = poll(fds, 1, 100);
    if( res<0 ){
      Tcl_AppendResult(p->interp, "poll() failed", (char*)0);
      return TCL_ERROR;
    }
    assert( res==0 || res==1 );
    if( res ){
      rc = hctServerAccept(p);
    }else{
      /* Check if all the user jobs are finished. If so, break out of
      ** the while( 1 ) loop.  */
      for(ii=0; ii<p->nJob; ii++){
        if( TestAtomicLoad(&p->aJob[ii].bDone)==0 ) break;
      }
      if( ii==p->nJob ){
        break;
      }
    }
  }

  /* Wait on the user jobs. */
  for(ii=0; ii<p->nJob; ii++){
    void *pVal = 0;
    TestServerJob *pJob = &p->aJob[ii];
    pthread_join(pJob->tid, &pVal);
    if( pJob->zErr ){
      hct_test_debug("Error in job %d: %s\n", ii, pJob->zErr);
    }
  }

  return TCL_OK;
}

/*
** Argument zFmt is a printf() style formatting string. Process it along
** with any trailing arguments and set the result of interpreter 
** TestServer.interp to the results. e.g.
**
**     testServerResult(p, "error in function: %d", rc);
*/
static void testServerResult(TestServer *p, const char *zFmt, ...){
  va_list ap;
  char *z;
  va_start(ap, zFmt);
  z = sqlite3_vmprintf(zFmt, ap);
  va_end(ap);
  Tcl_ResetResult(p->interp);
  Tcl_SetObjResult(p->interp, Tcl_NewStringObj(z, -1));
  sqlite3_free(z);
}

/*
** This function is called in follower nodes to establish a socket
** connection to the leader node - port TestServer.iPort of host 
** TestServer.zHost. 
**
** This function is a no-op if (*pRc) is other than TCL_OK when it is 
** called. Otherwise, an attempt is made to establish the connection.
** If successful, the file descriptor is returned. Otherwise, (*pRc)
** is set to TCL_ERROR, and error message is left in TestServer.interp
** and a negative value returned.
*/
static int testServerConnect(int *pRc, TestServer *p){
  int rc = *pRc;
  int fd = -1;
  if( rc==TCL_OK ){
    struct sockaddr_in addr;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(p->iPort);
    if( inet_pton(AF_INET, p->zHost, &addr.sin_addr)<=0 ){
      testServerResult(p, "error in inet_pton()");
      rc = TCL_ERROR;
    }

    if( rc==TCL_OK ){
      fd = socket(AF_INET, SOCK_STREAM, 0);
      if( fd<0 ){
        testServerResult(p, "error in socket()");
        rc = TCL_ERROR;
      }
    }

    if( rc==SQLITE_OK && connect(fd, (struct sockaddr*)&addr, sizeof(addr))<0 ){
      testServerResult(p, "error in connect()");
      rc = TCL_ERROR;
    }

    if( rc!=TCL_OK && fd>=0 ){
      close(fd);
      fd = -1;
    }
  }
  *pRc = rc;
  assert( rc!=TCL_OK || fd>=0 );
  return fd;
}

/*
** Read the body of a TESTSERVER_MESSAGE_ERROR message from file-descriptor
** fd. The body of such a message consists of:
**
**   * A 32-bit integer, N, and
**   * N bytes of data containing a nul-terminated error message string.
*/
static char *recvErrorMsg(int *pRc, int fd){
  char *aRet = 0;
  int nByte = 0;
  recvInt32(pRc, fd, &nByte);
  if( *pRc==TCL_OK ){
    aRet = (char*)ckalloc(nByte);
    recvData(pRc, fd, (u8*)aRet, nByte);
    if( *pRc!=TCL_OK ){
      ckfree(aRet);
      aRet = 0;
    }
  }
  return aRet;
}

/*
** This function is a no-op if (*pRc) is not TCL_OK when it is called. If
** it is, open a database connection to TestServer.zPath. If successful, 
** return the new database handle. Otherwise, (*pRc) is set to TCL_ERROR,
** an error message is left in TestServer.interp and NULL returned.
*/
static sqlite3 *testServerOpenDb(int *pRc, TestServer *p){
  int rc = *pRc;
  sqlite3 *db = 0;
  if( rc==SQLITE_OK ){
    rc = sqlite3_open_v2(p->zPath, &db, SQLITE_OPEN_READWRITE, 0);
    if( rc!=SQLITE_OK ){
      Tcl_AppendResult(p->interp, sqlite3_errmsg(db), (char*)0);
      sqlite3_close(db);
      db = 0;
      rc = TCL_ERROR;
    }
    *pRc = rc;
  }
  return db;
}

/*
** This is a wrapper around sqlite3_hct_journal_write(). It retries any 
** attempted _write() that fails with SQLITE_BUSY.
*/
static int hctWriteWithRetry(
  sqlite3 *db,                    /* Database handle */
  i64 iCid,                       /* CID of journal entry */
  const char *zSchema,            /* Schema modifications */
  const u8 *aData, int nData,     /* Data changes */
  i64 iSchemaCid                  /* CID of required schema */
){
  int rc = SQLITE_OK;
  int nBusy = 0;
  while( 1 ){
    rc = sqlite3_hct_journal_write(db, iCid, zSchema, aData, nData, iSchemaCid);
    if( rc!=SQLITE_BUSY ) break;
    nBusy++;
    if( nBusy>=10 ){ 
      int nDelay = (nBusy-9)*(nBusy-9)*39;
      usleep( MIN(nDelay, 50000) );
    }
    if( (nBusy % 200)==0 ){
      hct_test_debug("warning - CID %lld failed %d times (%s)\n", 
          iCid, nBusy, sqlite3_errmsg(db)
      );
    }
  }
  assert( rc==SQLITE_OK );
  return rc;
}

/*
** The following data structures are used while syncing.
*/
typedef struct TestFollowerChunk TestFollowerChunk;
typedef struct TestFollower TestFollower;
typedef struct TestFollowerThread TestFollowerThread;

#define SYNC_BYTES_PER_CHUNK ((1<<20) - 64)

/*
** When a SYNCREPLY message is being received, the data is read into a
** linked list of the following buffers.
*/
struct TestFollowerChunk {
  TestFollowerChunk *pNext;       /* Next chunk in SYNCREPLY message */
  int nRef;                       /* Current number of users of this link */
  int nChunk;                     /* Valid size of aChunk[] in bytes */
  u8 aChunk[SYNC_BYTES_PER_CHUNK];/* Payload */
};

/*
** Each follower thread - one that exists only to process the SUBREPLY
** data of a single connection to the leader - is represented by an
** instance of this structure.
*/
struct TestFollowerThread {
  TestFollower *pFollower;
  pthread_t tid;                  /* Thread id of this thread */
  sqlite3 *db;                    /* Database handle for journal_write() */
  int fd;                         /* Socket connection to leader node */
};

/*
** Object allocated on the stack of the main follower thread.
**
** pFirst, pLast:
**   Linked list of chunks of SYNCREPLY data. pFirst is the first (oldest),
**   and pLast points to the last (newest). Protected by TestFollower.mutex.
**
** nChunkTotal:
**   Total data stored on all chunks in pFirst/pLast linked list. In bytes.
**
**   This variable is only accessed by the main follower thread, so it
**   is not protected by any mutex.
**
** iNext:
**   Offset of the next serialized journal entry within the linked list
**   of SYNCREPLY data that no thread has yet claimed to work on. The
**   offset is a byte offset from the start of the data (i.e. offset 0
**   is the first byte on chunk TestFollower.pFirst).
**
** iEof:
**   This variable is set only when the last of the SYNCREPLY data for
**   the last synchronization request has been added to the pFirst/pLast
**   list. At that point it is set to the offset of the first non-contiguous
**   entry in the received data (i.e. the entry following the first hole).
**   All threads but the main follower thread (the same one that calls recv()
**   to receive the data) stop working on the synchronization data once 
**   this point is reached. Such threads either go on to handle SUBREPLY data
**   on a socket of their own, or else exit altogether.
*/
struct TestFollower {
  TestServer *pServer;            /* Pointer back to testserver object */
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  TestFollowerChunk *pFirst;
  TestFollowerChunk *pLast;
  i64 nChunkTotal;
  i64 iNext;
  i64 iEof;
  TestFollowerThread aThread[TESTSERVER_MAX_JOB];
};

/*
** Equivalent to an atomic version of:
**
**     if( *pPtr==iOld ){
**       *pPtr = iNew;
**       return 1;
**     }
**     return 0;
*/
static int testBoolCAS64(i64 *pPtr, i64 iOld, i64 iNew){
  return (int)__sync_bool_compare_and_swap(pPtr, iOld, iNew);
}

/*
** Buffer aChange[], size nChange bytes, contains a journal entry serialized
** in the manner of a SYNCREPLY or SUBREPLY message. i.e.
**
**     * A 64-bit "cid" value,
**     * A 64-bit "schemacid" value,
**     * A nul-terminated utf-8 "schema" string,
**     * A blob of "data" for the entry.
**
** This function decodes the journal entry and attempts to _write() it to
** database handle db. SQLITE_OK is returned if successful or an SQLite
** error code if not.
*/
static int hctWriteSerialWithRetry(sqlite3 *db, const u8 *aChange, int nChange){
  i64 iCid = getInt64(&aChange[0]);
  i64 iSchemaCid = getInt64(&aChange[8]);
  const char *zSchema = (const char*)&aChange[16];
  const u8 *aData = (const u8*)&zSchema[strlen(zSchema)+1];
  int nData = &aChange[nChange] - aData;
  return hctWriteWithRetry(db, iCid, zSchema, aData, nData, iSchemaCid);
}

/*
** Signal the condition variable to wake up any sleeping follower threads.
*/
static void hctFollowerSignal(TestFollower *pFollower){
  pthread_mutex_lock(&pFollower->mutex);
  pthread_cond_broadcast(&pFollower->cond);
  pthread_mutex_unlock(&pFollower->mutex);
}

/*
** Called by a thread to help with applying journal entries received
** as part of a SYNCREPLY message.
**
** If bReturn is true, then this function returns as soon as there is
** no more SYNCREPLY data to process. Or, if bReturn is false, the thread
** blocks, waiting for more data. In this case such threads are woken
** up by signalling condition variable TestFollower.cond.
*/
static int hctFollowerHelpWithSync(
  TestFollower *pFollower,        /* Follower context */
  sqlite3 *db,                    /* Db handle to apply changes to */
  int bReturn                     /* True to return (not block) when no data */
){
  int rc = TCL_OK;
  i64 iOff = 0;
  i64 iStart = 0;
  TestFollowerChunk *pChunk = 0;
  TestBuffer buf = {0, 0, 0};

  while( rc==TCL_OK ){
    TestFollowerChunk *pNext = 0;
    i64 iEof = 0;

    /* Grab the next TestFollowerChunk to work on. */
    pthread_mutex_lock(&pFollower->mutex);
    while( 1 ){
      iEof = pFollower->iEof;

      if( bReturn==0 && iEof>=0 && iOff>=pFollower->iEof ) break;
      pNext = (pChunk ? pChunk->pNext : pFollower->pFirst);
      if( pNext || bReturn ) break;

      /* Need to wait for the next chunk. Block on the condition variable. */
      pthread_cond_wait(&pFollower->cond, &pFollower->mutex);
    }
    if( pNext ) pNext->nRef++;
    if( pChunk ) pChunk->nRef--;
    pthread_mutex_unlock(&pFollower->mutex);
    pChunk = pNext;

    assert( iOff>=iStart || (buf.nBuf==0 && pChunk==0) );
    assert( rc==TCL_OK );

    if( buf.nBuf>0 ){
      int nCopy = MIN(pChunk->nChunk, iOff - iStart);
      assert( pChunk );
      assert( iOff>iStart );
      testBufferAppend(&buf, pChunk->aChunk, nCopy);
      if( iStart+nCopy==iOff ){
        rc = hctWriteSerialWithRetry(db, buf.aBuf, buf.nBuf);
        buf.nBuf = 0;
      }
    }

    /* If there is no next chunk, exit the outer loop here. */
    if( pChunk==0 || rc!=TCL_OK ) break;

    while( rc==TCL_OK && iOff<(iStart+pChunk->nChunk) ){
      /* Read the next nByte value. The 12 bytes that make up this and
      ** the CID field are guaranteed to be stored contiguously within
      ** a single chunk.  */
      int nByte = getInt32(&pChunk->aChunk[iOff - iStart]);

      /* See if this thread can apply the current change */
      if( iOff==pFollower->iNext
       && testBoolCAS64(&pFollower->iNext, iOff, iOff+4+nByte) 
      ){
        /* This thread is responsible for applying the current change. 
        ** There are two possibilies - either the entire change fits
        ** on this chunk, or it does not. If it does, apply the change
        ** right now. Otherwise, copy the part of the change on this
        ** chunk into a buffer and apply the change on the next iteration
        ** of the outer loop - after grabbing the next chunk.  */
        const u8 *aChange = &pChunk->aChunk[iOff - iStart + 4];
        if( (iOff+4+nByte)<=(iStart+pChunk->nChunk) ){
          /* Case 1 - entire change is right here. */
          rc = hctWriteSerialWithRetry(db, aChange, nByte);
        }else{
          /* Case 2 - have to buffer the start of this one. */
          int nHave = &pChunk->aChunk[pChunk->nChunk] - aChange;
          assert( buf.nBuf==0 );
          testBufferAppend(&buf, aChange, nHave);
        }

        /* If this change was the last before EOF, kick the condition variable
        ** to start any mirror threads that were not part of the sync */
        if( (iOff + 4 + nByte)==iEof ){
          hctFollowerSignal(pFollower);
        }
      }

      iOff += (4 + nByte);
      if( bReturn==0 && iEof>=0 && iOff>=iEof ) break;
    }

    iStart += pChunk->nChunk;
    assert( iOff>=iStart || (bReturn==0 && iEof>=0 && iOff>=iEof) );
  }

  hctFollowerSignal(pFollower);
  testBufferFree(&buf);
  return rc;
}

/*
** Called by the main follower thread to process the body of a SYNCREPLY
** message.
*/
static void hctFollowerSyncReply(
  int *pRc,                       /* IN/OUT: error code */
  TestFollower *pFollower, 
  int fd,
  int bFinalSync,                 /* True if this is final SYNCREPLY */
  i64 iSentCid                    /* CID sent in SYNC message */
){
  TestServer *p = pFollower->pServer;
  int rc = *pRc;
  int bRecvDone = 0;              /* True to break of out recv() loop */
  i64 iLocalOff = pFollower->nChunkTotal;
  i64 iPrevCid = iSentCid;
  i64 iFirstNonContiguous = -1;
  int nCarry = 0;

  hct_test_debug("syncreply %d start", bFinalSync);

  while( rc==TCL_OK && bRecvDone==0 ){
    /* Allocate a new TestFollowerChunk link */
    TestFollowerChunk *pNew = (TestFollowerChunk*)ckalloc(sizeof(*pNew));
    memset(pNew, 0, sizeof(TestFollowerChunk));

    if( nCarry>0 ){
      TestFollowerChunk *pLast = pFollower->pLast;
      assert( pLast );
      memcpy(pNew->aChunk, &pLast->aChunk[pLast->nChunk], nCarry);
      pNew->nChunk = nCarry;
      nCarry = 0;
    }

    while( pNew->nChunk<SYNC_BYTES_PER_CHUNK ){
      int nMaxRead = SYNC_BYTES_PER_CHUNK - pNew->nChunk;
      ssize_t res = recv(fd, &pNew->aChunk[pNew->nChunk], nMaxRead, 0);
      if( res<=0 ){
        if( res<0 ){
          testServerResult(p, "error in recv()");
          rc = TCL_ERROR;
        }
        bRecvDone = 1;
        break;
      }
      pNew->nChunk += res;
    }

    if( rc==SQLITE_OK && pNew->nChunk>0 ){
      /* Check if this chunk contains the first non-contiguous CID value. */
      i64 iOff = 0;
      assert( iLocalOff>=pFollower->nChunkTotal );
      iOff = iLocalOff - pFollower->nChunkTotal;
      while( iOff<pNew->nChunk ){
        if( (pNew->nChunk-iOff)<12 ){
          nCarry = pNew->nChunk - iOff;
          pNew->nChunk = iOff;
        }else{
          int nThis = getInt32(&pNew->aChunk[iOff]);
          i64 iCid = getInt64(&pNew->aChunk[iOff+4]);
          if( iCid!=iPrevCid+1 && iFirstNonContiguous<0 ){
            iFirstNonContiguous = iOff + pFollower->nChunkTotal;
          }
          iOff += (4 + nThis);
          iPrevCid = iCid;
        }
      }
      iLocalOff = pFollower->nChunkTotal + iOff;
    }

    if( iFirstNonContiguous>=0 && bFinalSync==0 ){
      bRecvDone = 1;
      pNew->nChunk = iFirstNonContiguous - pFollower->nChunkTotal;
    }

    /* If all went well and data was received, link the new chunk into the
    ** list and hit the condition variable to restart paused sync threads. 
    ** Or, if an error occurred or there was no data left to receive, free
    ** the chunk allocated above.  */
    if( rc==TCL_OK && pNew->nChunk>0 ){
      pthread_mutex_lock(&pFollower->mutex);
      pNew->nRef = 1;
      assert( (pFollower->pLast==0)==(pFollower->pFirst==0) );
      if( pFollower->pLast==0 ){
        pFollower->pFirst = pNew;
      }else{
        pFollower->pLast->pNext = pNew;
      }
      pFollower->pLast = pNew;
      pFollower->nChunkTotal += pNew->nChunk;
      if( bFinalSync && iFirstNonContiguous>=0 ){
        pFollower->iEof = iFirstNonContiguous;
      }

      pthread_cond_broadcast(&pFollower->cond);
      pthread_mutex_unlock(&pFollower->mutex);
    }else{
      ckfree(pNew);
    }
  }

  hct_test_debug(
      "syncreply %d received (%lld bytes total, %lld processed)", 
      bFinalSync, pFollower->nChunkTotal, TestAtomicLoad(&pFollower->iNext)
  );

  pthread_mutex_lock(&pFollower->mutex);
  if( bFinalSync && pFollower->iEof<0 ){
    pFollower->iEof = iLocalOff;
  }
  pthread_mutex_unlock(&pFollower->mutex);

  if( rc==SQLITE_OK ){
    hctFollowerSignal(pFollower);
    rc = hctFollowerHelpWithSync(pFollower, p->db, 1);
  }
  hct_test_debug("syncreply %d applied", bFinalSync);

  *pRc = rc;
}

/*
** This function is used in follower nodes to determine the current state
** of the database. Specifically, to discover:
**
**   *  The last CID before the first hole in the journal, and
**   *  The xor/hash value corresponding to that CID.
*/
static void hctFollowerGetVersion(
  int *pRc,                       /* IN/OUT: Error code */
  TestServer *p,                  /* Follower testserver object */
  i64 *piCid,                     /* OUT: Last contiguous CID value */
  u8 *aHash                       /* OUT: Xor/hash value for (*piCid) */
){
  const char *z1 = "SELECT cid, hash FROM sqlite_hct_baseline";
  const char *z2 = "SELECT cid, hash FROM sqlite_hct_journal ORDER BY cid ASC";
  int rc = *pRc;
  sqlite3_stmt *pStmt = 0;
  i64 iCid = 0;

  /* Determine the current CID and hash values for the follower database.
  ** This is easy, as sqlite3_hct_journal_rollback(MAXIMUM) has already
  ** been called on the database. */
  memset(aHash, 0, SQLITE_HCT_JOURNAL_HASHSIZE);
  pStmt = testServerPrepare(&rc, p, z1);
  if( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pStmt) ){
    int nBaseHash = sqlite3_column_bytes(pStmt, 1);
    if( nBaseHash!=SQLITE_HCT_JOURNAL_HASHSIZE ){
      testServerResult(p, "sqlite_hct_baseline.hash is wrong size");
      rc = TCL_ERROR;
    }else{
      const u8 *aBaseHash = sqlite3_column_blob(pStmt, 1);
      memcpy(aHash, aBaseHash, SQLITE_HCT_JOURNAL_MODE_FOLLOWER);
      iCid = sqlite3_column_int64(pStmt, 0);
    }
  }
  testServerFinalize(&rc, pStmt);

  pStmt = testServerPrepare(&rc, p, z2);
  while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pStmt) ){
    int nJrnlHash = sqlite3_column_bytes(pStmt, 1);
    if( nJrnlHash!=SQLITE_HCT_JOURNAL_HASHSIZE ){
      testServerResult(p, "sqlite_hct_journal.hash is wrong size");
      rc = TCL_ERROR;
    }else{
      const u8 *aJrnlHash = sqlite3_column_blob(pStmt, 1);
      i64 iThis = sqlite3_column_int64(pStmt, 0);
      if( iThis!=iCid+1 && iCid!=0 ) break;
      sqlite3_hct_journal_hash(aHash, aJrnlHash);
      iCid = iThis;
    }
  }
  testServerFinalize(&rc, pStmt);

  *piCid = iCid;
  *pRc = rc;
}

/*
** Sync with the leader node. Return the number of jobs running on the 
** leader. Specifically, this function:
**
**   1) Establishes a socket connection to the leader.
**   2) Sends a SYNC message, specifying the values iCid and aHash.
**   3) Processes the SYNCREPLY message. Waiting follower threads are
**      signaled to help with this.
**   4) Closes the socket and returns.
**    
** This function is a no-op if (*pRc) is other than TCL_OK when it is 
** called. Or, if an error occurs within this function, an error message is 
** left in pFollower->interp and (*pRc) set to TCL_ERROR.
*/
static int hctFollowerDoSync(
  int *pRc, 
  TestFollower *pFollower, 
  int bFinalSync,
  i64 iCid, 
  const u8 *aHash
){
  TestServer *p = pFollower->pServer;
  int rc = *pRc;
  int fd = -1;
  int eReply = -1;
  int nJob = 0;

  fd = testServerConnect(&rc, p);
  sendInt32(&rc, fd, TESTSERVER_MESSAGE_SYNC);
  sendInt64(&rc, fd, iCid);
  sendData(&rc, fd, aHash, SQLITE_HCT_JOURNAL_HASHSIZE);

  recvInt32(&rc, fd, &eReply);
  if( rc==TCL_OK ){
    if( eReply==TESTSERVER_MESSAGE_SYNCREPLY ){
      recvInt32(&rc, fd, &nJob);
      hctFollowerSyncReply(&rc, pFollower, fd, bFinalSync, iCid);
    }else if( eReply==TESTSERVER_MESSAGE_ERROR ){
      char *zErr = recvErrorMsg(&rc, fd);
      testServerResult(p, "error from leader: %s", zErr);
      ckfree(zErr);
      rc = TCL_ERROR;
    }else{
      testServerResult(p, 
          "unexpected reply type to SYNC - %d (expected %d or %d)",
          eReply, TESTSERVER_MESSAGE_SYNCREPLY, TESTSERVER_MESSAGE_ERROR
      );
      rc = TCL_ERROR;
    }
  }

  if( fd>=0 ) close(fd);
  *pRc = rc;
  return nJob;
}


/*
** This is the main() routine for follower threads. A follower thread
** may do two things:
**
**  1) help with synchronization until the last contiguous journal entry
**     received from the leader has been applied, then
**  2) process entries that are part of a SUBREPLY received from a job 
**     thread on a dedicated socket connection. 
*/
static void *hctFollowerThread(void *pCtx){
  TestFollowerThread *pThread = (TestFollowerThread*)pCtx;
  TestFollower *pFollower = pThread->pFollower;
  int iThread = pThread - pFollower->aThread;

  /* If this thread should help with synchronization, invoke the
  ** hctFollowerHelpWithSync() function now. It will not return until
  ** it is time to start processing messages on the SUB sockets. */
  if( iThread<(pFollower->pServer->nSyncThread-1) ){
    int rc = hctFollowerHelpWithSync(pFollower, pThread->db, 0);
    assert( rc==SQLITE_OK );
  }

  /* Wait until it is time to start processing messages on the SUB
  ** sockets. Really, this is only required for threads that did not
  ** participate in synchronization, but it doesn't hurt to check
  ** for all threads.  */
  pthread_mutex_lock(&pFollower->mutex);
  while( pFollower->iEof<0 || pFollower->iNext<pFollower->iEof ){
    pthread_cond_wait(&pFollower->cond, &pFollower->mutex);
  }
  pthread_mutex_unlock(&pFollower->mutex);

  /* If a SUB socket has been assigned to this thread, start reading
  ** and applying changes from it now. Otherwise, the thread exits. */
  if( pThread->fd>=0 ){
    int rc = SQLITE_OK;
    int fd = pThread->fd;
    sqlite3 *db = pThread->db;
    TestBuffer buf = {0, 0, 0};

    while( rc==SQLITE_OK ){

      /* Read the next change from the socket. If the socket is at EOF,
      ** break out of the loop.  */
      int nThis = 0;
      if( recvInt32(&rc, fd, &nThis) ){
        rc = SQLITE_OK;
        break;
      }
      recvDataBuf(&rc, fd, nThis, &buf);

      /* Assuming no error occurred, write the change into the db. */
      if( rc==SQLITE_OK ){
        i64 iCid = getInt64(&buf.aBuf[0]);
        i64 iSchemaCid = getInt64(&buf.aBuf[8]);
        const char *zSchema = (const char*)&buf.aBuf[16];
        const u8 *aData = (const u8*)&zSchema[strlen(zSchema)+1];
        int nData = nThis - (aData - buf.aBuf);
        rc = hctWriteWithRetry(db, iCid, zSchema, aData, nData, iSchemaCid);
      }
    }
    hct_test_debug("thread fd=%d finished, rc=%d\n", fd, rc);
    testBufferFree(&buf);
  }

  return 0;
}

/*
** Launch follower thread iThread. Threads are numbered starting from 0.
*/
static int hctFollowerThreadInit(TestFollower *p, int iThread){
  int rc = SQLITE_OK;
  TestFollowerThread *pT = &p->aThread[iThread];
  pT->pFollower = p;
  pT->db = testServerOpenDb(&rc, p->pServer);
  pT->fd = -1;
  if( rc==SQLITE_OK ){
    pthread_create(&pT->tid, NULL, hctFollowerThread, (void*)pT);
  }
  return rc;
}

/*
** Implementation of [run] command for follower nodes.
*/
static int hctFollowerRun(TestServer *p){
  int rc = SQLITE_OK;
  i64 iCid = 0;
  u8 aHash[SQLITE_HCT_JOURNAL_HASHSIZE];
  int nJob = 0;
  int ii;
  i64 nPrevSyncData = ((i64)1 << 60);

  TestFollower fol;
  hctFollowerInit(&fol, p);
  memset(&fol, 0, sizeof(fol));
  pthread_mutex_init(&fol.mutex, 0);
  pthread_cond_init(&fol.cond, 0);
  fol.pServer = pServer;
  fol.iEof = -1;

  /* If extra sync threads are configured, launch them now. They will wait
  ** on condition variable "fol.cond" until there is data to process */
  for(ii=0; ii<(p->nSyncThread-1); ii++){
    hctFollowerThreadInit(&fol, ii);
  }

  while( rc==TCL_OK ){
    i64 nTotal = fol.nChunkTotal;
    hct_test_debug("start GetVersion()");
    hctFollowerGetVersion(&rc, p, &iCid, aHash); 
    hct_test_debug("end GetVersion()");
    nJob = hctFollowerDoSync(&rc, &fol, 0, iCid, aHash);
    nTotal = fol.nChunkTotal - nTotal;
    hct_test_debug("recv %lld bytes of SYNCREPLY data", nTotal);
    if( nTotal>nPrevSyncData || nTotal<p->nSyncBytes || p->nSyncBytes==0 ){
      break;
    }
    nPrevSyncData = nTotal;
  }

  /* If there are more jobs on the leader than there are sync threads, 
  ** launch some more threads now. */
  for(/* no-op */; ii<nJob; ii++){
    hctFollowerThreadInit(&fol, ii);
  }

  /* Start the tcl jobs */
  for(ii=0; ii<p->nJob; ii++){
    TestServerJob *pJob = &p->aJob[ii];
    pthread_mutex_init(&pJob->mutex, 0);
    pthread_create(&pJob->tid, NULL, hctServerJobThread, (void*)pJob);
  }

  /* For each TestFollowerThread that will handle mirroring a leader
  ** thread, establish a connection to the leader. */
  for(ii=0; ii<nJob && rc==TCL_OK; ii++){
    int fd = -1;
    int eReply = 0;

    fd = testServerConnect(&rc, p);
    sendInt32(&rc, fd, TESTSERVER_MESSAGE_SUB);
    sendInt32(&rc, fd, ii);
    recvInt32(&rc, fd, &eReply);
    if( eReply==TESTSERVER_MESSAGE_SUBREPLY ) {
      fol.aThread[ii].fd = fd;
      fd = -1;
    }else if( eReply==TESTSERVER_MESSAGE_ERROR ){
      char *zErr = recvErrorMsg(&rc, fd);
      testServerResult(p, "error from leader: %s", zErr);
      ckfree(zErr);
      rc = TCL_ERROR;
    }else{
      testServerResult(p, 
          "unexpected reply type to SUB - %d (expected %d or %d)",
          eReply, TESTSERVER_MESSAGE_SUBREPLY, TESTSERVER_MESSAGE_ERROR
      );
      rc = TCL_ERROR;
    }

    if( fd>=0 ) close(fd);
  }

  hctFollowerGetVersion(&rc, p, &iCid, aHash); 
  nJob = hctFollowerDoSync(&rc, &fol, 1, iCid, aHash);

  /* Wait on the threads handling sockets linked to jobs on the leader. */
  for(ii=0; ii<MAX(p->nSyncThread-1, nJob); ii++){
    void *pDummy = 0;
    TestFollowerThread *pT = &fol.aThread[ii];
    pthread_join(pT->tid, &pDummy);
  }

  if( p->iTimeout==0 ){
    TestAtomicStore(&p->iTimeout, 1);
  }

  /* Wait on local job threads. */
  for(ii=0; ii<p->nJob; ii++){
    void *pVal = 0;
    TestServerJob *pJob = &p->aJob[ii];
    pthread_join(pJob->tid, &pVal);
  }

  return rc;
}

/*
** Configure the TestServer object according to the objc arguments in
** the objv[] array. The array may be extracted from the arguments passed
** to either the testserver [configure] method, or from a [hct_testserver]
** constructor.
*/
static int testServerConfigure(
  TestServer *p,                  /* Test-server object */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  struct CfgOpt {
    const char *zName;
    int iType;                    /* 0==text, 1==integer, 2==boolean */
  } aCfg[] = {
    { "-port",        1 },        /* 0 */
    { "-host",        0 },        /* 1 */
    { "-syncthreads", 1 },        /* 2 */
    { "-seconds",     1 },        /* 3 */
    { "-follower",    2 },        /* 4 */
    { "-syncbytes",   1 },        /* 5 */
    { 0, 0 }
  };
  int rc = TCL_OK;
  int ii;

  for(ii=0; ii<objc; ii+=2){
    int iCfg = 0;
    int d = 0;
    rc = Tcl_GetIndexFromObjStruct(
        p->interp, objv[ii], aCfg, sizeof(aCfg[0]), "OPTION", 0, &iCfg
    );
    if( rc!=TCL_OK ) return rc;
    switch( aCfg[iCfg].iType ){
      case 1:
        if( Tcl_GetIntFromObj(p->interp, objv[ii+1], &d) ) return TCL_ERROR;
        break;
      case 2:
        if( Tcl_GetBooleanFromObj(p->interp, objv[ii+1], &d) ) return TCL_ERROR;
        break;
      default:
        assert( iCfg==0 );
        break;
    }
  }

  if( (objc%2)!=0 ){
    Tcl_AppendResult(p->interp, "option requires an argument: ", 
        Tcl_GetString(objv[objc-1]), (char*)0
    );
    return TCL_ERROR;
  }

  for(ii=0; ii<objc; ii+=2){
    int iCfg = 0;
    rc = Tcl_GetIndexFromObjStruct(
        p->interp, objv[ii], aCfg, sizeof(aCfg[0]), "OPTION", 0, &iCfg
    );
    switch( iCfg ){
      case 0: assert( 0==strcmp(aCfg[iCfg].zName, "-port") ); {
        Tcl_GetIntFromObj(p->interp, objv[ii+1], &p->iPort);
        break;
      }
      case 1: assert( 0==strcmp(aCfg[iCfg].zName, "-host") ); {
        ckfree(p->zHost);
        p->zHost = test_strdup(Tcl_GetString(objv[ii]));
        break;
      }
      case 2: assert( 0==strcmp(aCfg[iCfg].zName, "-syncthreads") ); {
        int nThread = 0;
        Tcl_GetIntFromObj(p->interp, objv[ii+1], &nThread);
        nThread = MAX(nThread, 1);
        nThread = MIN(nThread, TESTSERVER_MAX_FOLLOWER);
        p->nSyncThread = nThread;
        break;
      }
      case 3: assert( 0==strcmp(aCfg[iCfg].zName, "-seconds") ); {
        Tcl_GetIntFromObj(p->interp, objv[ii+1], &p->nSecond);
        break;
      }
      case 4: assert( 0==strcmp(aCfg[iCfg].zName, "-follower") ); {
        Tcl_GetBooleanFromObj(p->interp, objv[ii+1], &p->bFollower);
        break;
      }
      case 5: assert( 0==strcmp(aCfg[iCfg].zName, "-syncbytes") ); {
        Tcl_GetIntFromObj(p->interp, objv[ii+1], &p->nSyncBytes);
        break;
      }
    }
  }

  return TCL_OK;
}


/*
** tclcmd: TESTSERVERCMD SUBCMD ...ARGS...
**
** The implementation of the commands returned by [hct_testserver].
*/
static int hctServerCmd(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  const char *azSub[] = {
    "configure",   /* 0 */
    "job",         /* 1 */
    "run",         /* 2 */
    "delete",      /* 3 */
    0,
  };

  TestServer *p = (TestServer*)clientData;
  int rc = TCL_OK;
  int iSub = 0;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUBCMD ...");
    return TCL_ERROR;
  }

  rc = Tcl_GetIndexFromObj(interp, objv[1], azSub, "SUBCMD", 0, &iSub);
  if( rc!=TCL_OK ) return rc;

  switch( iSub ){
    case 0: assert( 0==strcmp(azSub[iSub], "configure") ); {
      rc = testServerConfigure(p, objc-2, &objv[2]);
      break;
    };
    case 1: assert( 0==strcmp(azSub[iSub], "job") ); {
      TestServerJob *pJob = &p->aJob[p->nJob];
      if( objc!=3 ){
        Tcl_WrongNumArgs(interp, 2, objv, "SCRIPT");
        return TCL_ERROR;
      }
      pJob->pScript = Tcl_DuplicateObj(objv[2]);
      pJob->pServer = p;
      p->nJob++;
      break;
    };
    case 2: assert( 0==strcmp(azSub[iSub], "run") ); {
      hct_test_debug_start();
      if( p->nSecond ){
        TestAtomicStore(&p->iTimeout, test_gettime() + 1000*(p->nSecond));
      }
      if( p->bFollower ){
        rc = hctFollowerRun(p);
      }else{
        rc = hctLeaderRun(p);
      }
      break;
    };
    case 3: assert( 0==strcmp(azSub[iSub], "delete") ); {
      Tcl_DeleteCommand(interp, Tcl_GetStringFromObj(objv[0], 0));
      break;
    };
    default:
      assert( 0 );
      break;
  }

  return rc;
}

/*
** Destructor for an object created by [hct_testserver].
*/
static void hctServerDel(void *pCtx){
  TestServer *p = (TestServer*)pCtx;
  if( p->fdListen>=0 ) close(p->fdListen);
  ckfree(p->zPath);
  ckfree(p->zHost);
  ckfree(p);
}

/*
** tclcmd: hct_testserver NAME DBFILE ?OPTIONS?
**
** Create a new testserver object.
*/
static int test_hct_testserver(
  ClientData clientData,          /* Unused */
  Tcl_Interp *interp,             /* The TCL interpreter */
  int objc,                       /* Number of arguments */
  Tcl_Obj *CONST objv[]           /* Command arguments */
){
  const char *zCmd = 0;
  TestServer *p = 0;
  int rc = TCL_OK;

  if( objc<3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "NAME DBFILE ?OPTIONS?");
    return TCL_ERROR;
  }
  zCmd = Tcl_GetString(objv[1]);

  p = (TestServer*)ckalloc(sizeof(TestServer));
  memset(p, 0, sizeof(TestServer));
  p->zPath = test_strdup(Tcl_GetString(objv[2]));
  p->iPort = TESTSERVER_DEFAULT_PORT;
  p->zHost = test_strdup("127.0.0.1");
  p->interp = interp;
  p->fdListen = -1;

  p->db = testServerOpenDb(&rc, p);
  if( rc==SQLITE_OK ){
    rc = sqlite3_hct_journal_setmode(p->db, SQLITE_HCT_JOURNAL_MODE_FOLLOWER);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_hct_journal_rollback(p->db, SQLITE_HCT_ROLLBACK_MAXIMUM);
  }
  if( rc==SQLITE_OK ){
    rc = testServerConfigure(p, objc-3, &objv[3]);
  }
  if( rc==TCL_OK ){
    Tcl_CreateObjCommand(interp, zCmd, hctServerCmd, (void*)p, hctServerDel);
    Tcl_SetObjResult(interp, objv[1]);
  }else{
    hctServerDel((void*)p);
  }

  return rc;
}

/*
** Register commands with the TCL interpreter.
*/
int SqliteHctServerTest_Init(Tcl_Interp *interp){
  struct TestCmd {
    const char *zName;
    Tcl_ObjCmdProc *x;
  } aCmd[] = {
    { "hct_testserver", test_hct_testserver },
  };
  int ii = 0;

  for(ii=0; ii<sizeof(aCmd)/sizeof(aCmd[0]); ii++){
    Tcl_CreateObjCommand(interp, aCmd[ii].zName, aCmd[ii].x, 0, 0);
  }

  return TCL_OK;
}


