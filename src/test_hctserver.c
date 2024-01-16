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

#define TESTSERVER_DEFAULT_PORT 21212

#define TESTSERVER_MAX_JOB 64          /* Max number of jobs per process */
#define TESTSERVER_MAX_FOLLOWER 16     /* Max number of follower nodes */

/*
** Message types 
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

#define TestAtomicStore(PTR,VAL) __atomic_store_n((PTR),(VAL), __ATOMIC_SEQ_CST)
#define TestAtomicLoad(PTR)      __atomic_load_n((PTR), __ATOMIC_SEQ_CST)

int getDbPointer(Tcl_Interp *interp, const char *zA, sqlite3 **ppDb);
const char *sqlite3ErrName(int);

typedef struct TestServerJob TestServerJob;
typedef struct TestServerMirror TestServerMirror;
typedef struct TestServer TestServer;
typedef struct TestBuffer TestBuffer;


struct TestBuffer {
  u8 *aBuf;
  int nBuf;
  int nAlloc;
};

struct TestServerMirror {
  TestServer *pServer;
  pthread_t tid;
  sqlite3 *db;
  int fd;
};
struct TestServerJob {
  TestServer *pServer;
  Tcl_Obj *pScript;
  pthread_t tid;
  pthread_mutex_t mutex;
  int nFollower;
  int aFollower[TESTSERVER_MAX_FOLLOWER];
  int bDone;                      /* Set to true once job is finished */
  char *zErr;                     /* ckalloc()'d error message (if any) */

  int bFirstLogged;
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
  int iPort;                      /* Tcp port to listen for connections on */
  char *zHost;                    /* Tcp host to connect to */
  int nSyncThread;                /* Number of threads helping with sync */
  int fdListen;                   /* Listening socket */
  int nJob;
  TestServerJob aJob[TESTSERVER_MAX_JOB];
  int bFollower;                  /* True for follower, false for leader */
  int nMirror;
  TestServerMirror aMirror[TESTSERVER_MAX_JOB];
};

static i64 test_gettime(){
  i64 ret = 0;
  sqlite3_vfs *pVfs = sqlite3_vfs_find(0);
  pVfs->xCurrentTimeInt64(pVfs, &ret);
  return ret;
}

static char *test_strdup(const char *zIn){
  char *zRet = 0;
  if( zIn ){
    int nByte = strlen(zIn) + 1;
    zRet = (char*)ckalloc(nByte);
    memcpy(zRet, zIn, nByte);
  }
  return zRet;
}

int Sqlite3_Init(Tcl_Interp *);
int SqliteHctTest_Init(Tcl_Interp *);

/*
** Write 32 and 64 bit integer values to the supplied buffer, respectively
*/
static void putInt32(u8 *aBuf, int val){
  memcpy(aBuf, &val, 4);
}
static void putInt64(u8 *aBuf, i64 val){
  memcpy(aBuf, &val, 8);
}

static void sendData(int *pRc, int fd, const u8 *aData, int nData){
  int nTotal = 0;
  while( *pRc==SQLITE_OK && nTotal<nData ){
    ssize_t res = 0;
    res = send(fd, &aData[nTotal], nData-nTotal, 0);
    if( res<0 ){
      *pRc = SQLITE_IOERR;
    }else{
      nTotal += res;
    }
  }
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


static int hctServerValidateHook(
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
    int fd = pJob->aFollower[ii];
    sendInt32(&rc, fd, sizeof(i64)*2 + nSchema + nData);
    sendInt64(&rc, fd, iCid);
    sendInt64(&rc, fd, iSchemaCid);
    sendData(&rc, fd, (const u8*)zSchema, nSchema);
    sendData(&rc, fd, (const u8*)pData, nData);
    if( pJob->bFirstLogged==0){
      printf("SUB: first cid = %lld (job=%d)\n", iCid,
          (int)(pJob - pJob->pServer->aJob)
      );
      pJob->bFirstLogged = 1;
    }
  }
  return rc;
}

static void *hctLeaderJobThread(void *pCtx){
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

  /* Open the database handle */
  pOpenDb = Tcl_NewObj();
  Tcl_IncrRefCount(pOpenDb);
  Tcl_ListObjAppendElement(interp, pOpenDb, Tcl_NewStringObj("sqlite3", -1));
  Tcl_ListObjAppendElement(interp, pOpenDb, Tcl_NewStringObj("db", -1));
  Tcl_ListObjAppendElement(interp, pOpenDb, Tcl_NewStringObj(p->zPath, -1));
  rc = Tcl_EvalObjEx(interp, pOpenDb, 0);
  Tcl_DecrRefCount(pOpenDb);

  if( rc==TCL_OK ){
    getDbPointer(interp, "db", &db);
    sqlite3_hct_journal_validation_hook(db, pCtx, hctServerValidateHook);
  }

  if( rc==TCL_OK ){
    rc = Tcl_EvalObjEx(interp, pJob->pScript, 0);
  }
  if( rc!=SQLITE_OK ){
    pJob->zErr = test_strdup(Tcl_GetStringResult(interp));
  }
  Tcl_DeleteInterp(interp);
  TestAtomicStore(&pJob->bDone, 1);
  
  return 0;
}

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
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
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
** Write 32 and 64 bit integer values from the supplied buffer, respectively
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

static void recvData(int *pRc, int fd, u8 *aBuf, int nBuf){
  if( *pRc==TCL_OK ){
    int nRead = 0;
    while( nRead<nBuf ){
      int n = recv(fd, &aBuf[nRead], nBuf-nRead, 0);
      if( n<=0 ){
        *pRc = TCL_ERROR;
        break;
      }else{
        nRead += n;
      }
    }
  }
}

static void recvInt32(int *pRc, int fd, int *piVal){
  u8 aBuf[4] = {0, 0, 0, 0};
  assert( sizeof(*piVal)==sizeof(aBuf) );
  recvData(pRc, fd, aBuf, sizeof(aBuf));
  *piVal = getInt32(aBuf);
}

static int recvInt64(int fd, i64 *piVal){
  int rc = SQLITE_OK;
  u8 aBuf[8] = {0, 0, 0, 0, 0, 0, 0, 0};
  assert( sizeof(*piVal)==sizeof(aBuf) );
  recvData(&rc, fd, aBuf, sizeof(aBuf));
  *piVal = getInt64(aBuf);
  return rc;
}

static void recvDataBuf(int *pRc, int fd, int nData, TestBuffer *pBuf){
  if( pBuf->nAlloc<nData ){
    pBuf->aBuf = (u8*)ckrealloc(pBuf->aBuf, nData*2);
    pBuf->nAlloc = nData*2;
  }
  recvData(pRc, fd, pBuf->aBuf, nData);
  pBuf->nBuf = nData;
}


static void testServerFinalize(int *pRc, sqlite3_stmt *pStmt){
  int rc = sqlite3_finalize(pStmt);
  if( *pRc==SQLITE_OK ) *pRc = rc;
}

static sqlite3_stmt *testServerPrepare(
  int *pRc, 
  TestServer *p, 
  const char *zSql
){
  sqlite3_stmt *pRet = 0;
  int rc = *pRc;
  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(p->db, zSql, -1, &pRet, 0);
    if( rc!=SQLITE_OK ){
      const char *zErr = sqlite3_errmsg(p->db);
      Tcl_AppendResult(p->interp, "sqlite3_prepare_v2(): ", zErr, (char*)0);
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
    sendInt32(&rc, newfd, TESTSERVER_MESSAGE_ERROR);
    sendInt32(&rc, newfd, nErr+1);
    sendData(&rc, newfd, (const u8*)zErr, nErr+1);
  }else{
    i64 iThisCid = 0;
    int bLogged = 0;
    pStmt = testServerPrepare(&rc, p, z3);
    if( rc==SQLITE_OK ) sqlite3_bind_int64(pStmt, 1, iCid);
    sendInt32(&rc, newfd, TESTSERVER_MESSAGE_SYNCREPLY);
    sendInt32(&rc, newfd, p->nJob);
    while( rc==SQLITE_OK && sqlite3_step(pStmt)==SQLITE_ROW ){
      int nSchema = sqlite3_column_bytes(pStmt, 1);
      int nData = sqlite3_column_bytes(pStmt, 2);
      int nSz = sizeof(i64) + sizeof(i64) + nSchema+1 + nData;

      iThisCid = sqlite3_column_int64(pStmt, 0);
      if( bLogged==0 ){
        printf("SYNC: first CID = %lld\n", iThisCid);
        bLogged = 1;
      }
      sendInt32(&rc, newfd, nSz);
      sendInt64(&rc, newfd, iThisCid);
      sendInt64(&rc, newfd, sqlite3_column_int64(pStmt, 3));
      sendData(&rc, newfd, sqlite3_column_text(pStmt, 1), nSchema+1);
      sendData(&rc, newfd, sqlite3_column_blob(pStmt, 2), nData);
    }
printf("SYNC: last CID = %lld (rc=%d)\n", iThisCid, rc);
    sendInt32(&rc, newfd, 0);
    testServerFinalize(&rc, pStmt);
  }

  close(newfd);
  return rc;
}

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
      recvInt64(newfd, &iCid);
      recv(newfd, aXor, sizeof(aXor), 0);
      hctServerDoSync(p, newfd, iCid, aXor);
    }
    else if( eType==TESTSERVER_MESSAGE_SUB ){
      /* SUB message */
      int iJob = 0;
      recvInt32(&rc, newfd, &iJob);
printf("SUB to job %d\n", iJob);
      if( iJob<p->nJob ){
        TestServerJob *pJob = &p->aJob[iJob];
        pthread_mutex_lock(&pJob->mutex);
        if( pJob->nFollower<TESTSERVER_MAX_FOLLOWER ){
          sendInt32(&rc, newfd, TESTSERVER_MESSAGE_SUBREPLY);
          pJob->aFollower[pJob->nFollower++] = newfd;
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

  /* Listen on the proscribed socket */
  rc = hctServerListen(p);
  if( rc!=SQLITE_OK ) return rc;
  memset(fds, 0, sizeof(fds));
  fds[0].fd = p->fdListen;
  fds[0].events = POLLIN;

  /* Start the tcl jobs */
  for(ii=0; ii<p->nJob; ii++){
    TestServerJob *pJob = &p->aJob[ii];
    pthread_mutex_init(&pJob->mutex, 0);
    pthread_create(&pJob->tid, NULL, hctLeaderJobThread, (void*)pJob);
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
      printf("Error in job %d: %s\n", ii, pJob->zErr);
    }
  }

  return TCL_OK;
}

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
    }
  }
  *pRc = rc;
  return fd;
}

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
** Open database connection TestServer.db. If successful, TCL_OK is returned.
** Otherwise, TCL_ERROR is returned and an error message left in
** TestServer.interp.
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
  sqlite3 *db, 
  i64 iCid, 
  const char *zSchema, 
  const u8 *aData, int nData, 
  i64 iSchemaCid
){
  int rc = SQLITE_OK;
  int nBusy = 0;
  while( 1 ){
    rc = sqlite3_hct_journal_write(db, iCid, zSchema, aData, nData, iSchemaCid);
    if( rc!=SQLITE_BUSY ) break;
    nBusy++;
    if( (nBusy % 10000)==0 ){
      printf("warning - CID %lld failed %d times (%s)\n", iCid, nBusy, sqlite3_errmsg(db));
    }
    usleep(1);
  }
  assert( rc==SQLITE_OK );
  return rc;
}

static void *hctMirrorThread(void *pCtx){
  TestServerMirror *p = (TestServerMirror*)pCtx;
  u8 *aBuf = 0;
  int nAlloc = 0;
  int rc = 0;

  while( rc==SQLITE_OK ){
    int nThis = 0;
    recvInt32(&rc, p->fd, &nThis);
    if( rc==SQLITE_OK && nThis>nAlloc ){
      int nNew = (nAlloc + nThis)*2;
      aBuf = (u8*)ckrealloc(aBuf, nNew);
      nAlloc = nNew;
    }
    recvData(&rc, p->fd, aBuf, nThis);

    if( rc==SQLITE_OK ){
      i64 iCid = getInt64(&aBuf[0]);
      i64 iSchemaCid = getInt64(&aBuf[8]);
      const char *zSchema = (const char*)&aBuf[16];
      const u8 *aData = (const u8*)&zSchema[strlen(zSchema)+1];
      int nData = nThis - (aData - aBuf);
      rc = hctWriteWithRetry(p->db, iCid, zSchema, aData, nData, iSchemaCid);
    }
  }

  ckfree(aBuf);
  return 0;
}

/*
** The following data structures are used while syncing.
*/
typedef struct SyncChunk SyncChunk;
typedef struct TestFollower TestFollower;
typedef struct TestFollowerThread TestFollowerThread;

#define SYNC_BYTES_PER_CHUNK ((1<<20) - 64)

struct SyncChunk {
  SyncChunk *pNext;               /* Next chunk in SYNCREPLY message */
  int nRef;                       /* Current number of users of this link */
  int nChunk;                     /* Valid size of aChunk[] in bytes */
  u8 aChunk[SYNC_BYTES_PER_CHUNK];/* Payload */
};

struct TestFollowerThread {
  TestFollower *pFollower;
  pthread_t tid;
  sqlite3 *db;
  int fd;
};

/*
** Object allocated on the stack of the main follower thread.
*/
struct TestFollower {
  TestServer *pServer;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  SyncChunk *pFirst;
  SyncChunk *pLast;
  i64 nChunkTotal;
  i64 iNext;
  i64 iEof;
  int nSyncDone;
  TestFollowerThread aThread[TESTSERVER_MAX_JOB];
};

static void testBufferFree(TestBuffer *pBuf){
  ckfree(pBuf->aBuf);
}

static void testBufferAppend(TestBuffer *pBuf, const u8 *aData, int nData){
  if( (pBuf->nBuf+nData)>pBuf->nAlloc ){
    int nNew = (pBuf->nBuf + nData) * 2;
    pBuf->aBuf = (u8*)ckrealloc(pBuf->aBuf, nNew);
    pBuf->nAlloc = nNew;
  }

  memcpy(&pBuf->aBuf[pBuf->nBuf], aData, nData);
  pBuf->nBuf += nData;
}


#define TestAtomicStore(PTR,VAL) __atomic_store_n((PTR),(VAL), __ATOMIC_SEQ_CST)
#define TestAtomicLoad(PTR)      __atomic_load_n((PTR), __ATOMIC_SEQ_CST)

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

static int hctWriteSerialWithRetry(sqlite3 *db, const u8 *aChange, int nChange){
  i64 iCid = getInt64(&aChange[0]);
  i64 iSchemaCid = getInt64(&aChange[8]);
  const char *zSchema = (const char*)&aChange[16];
  const u8 *aData = (const u8*)&zSchema[strlen(zSchema)+1];
  int nData = &aChange[nChange] - aData;
  return hctWriteWithRetry(db, iCid, zSchema, aData, nData, iSchemaCid);
}

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
  sqlite3 *db,
  int bReturn                     /* True to return (not block) when no data */
){
  int rc = TCL_OK;
  i64 iOff = 0;
  i64 iStart = 0;
  SyncChunk *pChunk = 0;
  TestBuffer buf = {0, 0, 0};

  while( rc==TCL_OK ){
    SyncChunk *pNext = 0;
    i64 iEof = 0;

    /* Grab the next SyncChunk to work on. */
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
assert( rc==SQLITE_OK );
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

assert( nByte>0 && nByte<100000000 );
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

static void hctFollowerCheck(TestFollower *p){
  i64 iOff = 0;
  i64 iStart = 0;
  SyncChunk *pChunk = 0;

  for(pChunk=p->pFirst; pChunk; pChunk=pChunk->pNext ){
    while( iOff<(iStart+pChunk->nChunk) ){
      int nByte = getInt32(&pChunk->aChunk[(iOff-iStart)]);
      assert( nByte>0 );
      iOff += (nByte+4);
    }
    iStart += pChunk->nChunk;
  }

  assert( iOff==iStart );
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

  i64 tm = test_gettime();
  printf("start recv(SYNCREPLY)\n");

  while( rc==TCL_OK && bRecvDone==0 ){
    /* Allocate a new SyncChunk link */
    SyncChunk *pNew = (SyncChunk*)ckalloc(sizeof(SyncChunk));
    memset(pNew, 0, sizeof(SyncChunk));

    if( nCarry>0 ){
      SyncChunk *pLast = pFollower->pLast;
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
  printf("finished recv(SYNCREPLY) (%lld ms)\n", test_gettime() - tm);

  pthread_mutex_lock(&pFollower->mutex);
  if( bFinalSync && pFollower->iEof<0 ){
    pFollower->iEof = iLocalOff;
  }
  pthread_mutex_unlock(&pFollower->mutex);

  hctFollowerCheck(pFollower);

  if( rc==SQLITE_OK ){
  printf("iEof is at %lld\n", pFollower->iEof);
    hctFollowerSignal(pFollower);
    rc = hctFollowerHelpWithSync(pFollower, p->db, 1);
  }
  printf("finished apply(SYNCREPLY) (%lld ms)\n", test_gettime() - tm);

  *pRc = rc;
}

static void hctFollowerGetVersion(
  int *pRc, 
  TestServer *p, 
  i64 *piCid, 
  u8 *aHash
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
** leader.
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
    switch( eReply ){
      case TESTSERVER_MESSAGE_SYNCREPLY: {
        recvInt32(&rc, fd, &nJob);
        hctFollowerSyncReply(&rc, pFollower, fd, bFinalSync, iCid);
        break;
      }

      case TESTSERVER_MESSAGE_ERROR: {
        char *zErr = recvErrorMsg(&rc, fd);
        testServerResult(p, "error from leader: %s", zErr);
        ckfree(zErr);
        rc = TCL_ERROR;
        break;
      }

      default: {
        testServerResult(p, 
            "unexpected reply type to SYNC - %d (expected %d or %d)",
            eReply, TESTSERVER_MESSAGE_SYNCREPLY, TESTSERVER_MESSAGE_ERROR
        );
        rc = TCL_ERROR;
        break;
      }
    }
  }

  if( fd>=0 ) close(fd);
  *pRc = rc;
  return nJob;
}


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

      /* Read the next change from the socket. */
      int nThis = 0;
      recvInt32(&rc, fd, &nThis);
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
    printf("thread fd=%d finished, rc=%d\n", fd, rc);
    testBufferFree(&buf);
  }

  return 0;
}

/*
** Initialize the contents of the TestFollower object.
*/
static void hctFollowerInit(TestFollower *pSync, TestServer *pServer){
  memset(pSync, 0, sizeof(*pSync));
  pthread_mutex_init(&pSync->mutex, 0);
  pthread_cond_init(&pSync->cond, 0);
  pSync->pServer = pServer;
  pSync->iEof = -1;
}

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

static int hctFollowerRun(TestServer *p){
  int rc = SQLITE_OK;
  i64 iCid = 0;
  u8 aHash[SQLITE_HCT_JOURNAL_HASHSIZE];
  int nJob = 0;
  int ii;

  TestFollower fol;
  hctFollowerInit(&fol, p);

  /* If extra sync threads are configured, launch them now. They will wait
  ** on condition variable "sd.cond" until there is data to process */
  for(ii=0; ii<(p->nSyncThread-1); ii++){
    hctFollowerThreadInit(&fol, ii);
  }

  hctFollowerGetVersion(&rc, p, &iCid, aHash); 
  nJob = hctFollowerDoSync(&rc, &fol, 0, iCid, aHash);

  /* If there are more jobs on the leader than there are sync threads, 
  ** launch some more threads now. */
  for(/* no-op */; ii<nJob; ii++){
    hctFollowerThreadInit(&fol, ii);
  }

  /* Start the tcl jobs */
  for(ii=0; ii<p->nJob; ii++){
    TestServerJob *pJob = &p->aJob[ii];
    pthread_mutex_init(&pJob->mutex, 0);
    pthread_create(&pJob->tid, NULL, hctLeaderJobThread, (void*)pJob);
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

  /* Wait on the follower threads. */
  for(ii=0; ii<MAX(p->nSyncThread-1, nJob); ii++){
    void *pDummy = 0;
    TestFollowerThread *pT = &fol.aThread[ii];
    pthread_join(pT->tid, &pDummy);
  }

  /* Wait on local job threads. */
#if 0
  for(ii=0; p->nJob; ii++){
    void *pVal = 0;
    TestServerJob *pJob = &p->aJob[ii];
    pthread_join(pJob->tid, &pVal);
  }
#endif

  return rc;
}

static char *testServerStrdup(const char *zIn){
  char *zRet = 0;
  if( zIn ){
    int nIn = strlen(zIn) + 1;
    zRet = ckalloc(nIn);
    memcpy(zRet, zIn, nIn);
  }
  return zRet;
}

/*
** tclcmd: CMD SUBCMD ...ARGS...
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
    0,
  };
  const char *azCfg[] = {
    "-port",            /* 0 */
    "-host",            /* 1 */
    "-syncthreads",     /* 2 */
    0,
  };
  const char *azRun[] = {
    "-follower",   /* 0 */
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
      int ii;
      for(ii=2; ii<objc; ii++){
        int iCfg = -1;
        rc = Tcl_GetIndexFromObj(interp, objv[ii], azCfg, "OPTION", 0, &iCfg);
        if( rc!=TCL_OK ) return rc;
        if( ii==objc-1 ){
          Tcl_AppendResult(
              interp, "option requires an argument:", 
              Tcl_GetString(objv[ii]), (char*)0
          );
          return TCL_ERROR;
        }
        ii++;
        switch( iCfg ){
          case 0: assert( 0==strcmp(azCfg[iCfg], "-port") ); {
            if( Tcl_GetIntFromObj(interp, objv[ii], &p->iPort) ){
              return TCL_ERROR;
            }
            break;
          }
          case 1: assert( 0==strcmp(azCfg[iCfg], "-host") ); {
            ckfree(p->zHost);
            p->zHost = testServerStrdup(Tcl_GetString(objv[ii]));
            break;
          }
          case 2: assert( 0==strcmp(azCfg[iCfg], "-syncthreads") ); {
            int nThread = 0;
            if( Tcl_GetIntFromObj(interp, objv[ii], &nThread) ){
              return TCL_ERROR;
            }
            nThread = MAX(nThread, 1);
            nThread = MIN(nThread, TESTSERVER_MAX_FOLLOWER);
            p->nSyncThread = nThread;
            break;
          }
        }
      }
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
      int ii;
      for(ii=2; ii<objc; ii++){
        int iOpt = -1;
        rc = Tcl_GetIndexFromObj(interp, objv[ii], azRun, "OPTION", 0, &iOpt);
        if( rc!=TCL_OK ) return rc;
        p->bFollower = 1;
      }
      if( p->bFollower ){
        rc = hctFollowerRun(p);
      }else{
        rc = hctLeaderRun(p);
      }
      break;
    };
    default:
      assert( 0 );
      break;
  }

  return rc;
}

static void hctServerDel(void *pCtx){
  TestServer *p = (TestServer*)pCtx;
  if( p->fdListen>=0 ) close(p->fdListen);
  ckfree(p->zPath);
  ckfree(p->zHost);
  ckfree(p);
}

/*
** tclcmd: hct_testserver NAME DBFILE
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

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "NAME DBFILE");
    return TCL_ERROR;
  }
  zCmd = Tcl_GetString(objv[1]);

  p = (TestServer*)ckalloc(sizeof(TestServer));
  memset(p, 0, sizeof(TestServer));
  p->zPath = testServerStrdup(Tcl_GetString(objv[2]));
  p->iPort = TESTSERVER_DEFAULT_PORT;
  p->zHost = testServerStrdup("127.0.0.1");
  p->interp = interp;
  p->fdListen = -1;

  p->db = testServerOpenDb(&rc, p);
  if( rc==SQLITE_OK ){
    rc = sqlite3_hct_journal_setmode(p->db, SQLITE_HCT_JOURNAL_MODE_FOLLOWER);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_hct_journal_rollback(p->db, SQLITE_HCT_ROLLBACK_MAXIMUM);
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


