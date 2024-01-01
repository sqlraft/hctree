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


typedef sqlite3_int64 i64;
typedef unsigned char u8;

#define TESTSERVER_DEFAULT_PORT 21212

#define TESTSERVER_MAX_JOB 64          /* Max number of jobs per process */
#define TESTSERVER_MAX_FOLLOWER 64     /* Max number of follower nodes */

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
};

struct TestServer {
  Tcl_Interp *interp;             /* If error, leave error message here */
  sqlite3 *db;
  char *zPath;                    /* Copy of DBFILE argument */
  int iPort;                      /* Tcp port to listen for connections on */
  char *zHost;                    /* Tcp host to connect to */
  int fdListen;                   /* Listening socket */
  int nJob;
  TestServerJob aJob[TESTSERVER_MAX_JOB];

  int nMirror;
  TestServerMirror aMirror[TESTSERVER_MAX_JOB];
};

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

/*
** Write 32 and 64 bit integer values to the supplied buffer, respectively
*/
static void putInt32(u8 *aBuf, int val){
  memcpy(aBuf, &val, 4);
}
static void putInt64(u8 *aBuf, i64 val){
  memcpy(aBuf, &val, 8);
}

static void sendInt32(int *pRc, int fd, int val){
  if( *pRc==SQLITE_OK ){
    u8 aBuf[4];
    putInt32(aBuf, val);
    if( send(fd, aBuf, sizeof(aBuf), 0)!=sizeof(aBuf) ){
      *pRc = SQLITE_IOERR;
    }
  }
}
static void sendInt64(int *pRc, int fd, i64 val){
  if( *pRc==SQLITE_OK ){
    u8 aBuf[8];
    putInt64(aBuf, val);
    if( send(fd, aBuf, sizeof(aBuf), 0)!=sizeof(aBuf) ){
      *pRc = SQLITE_IOERR;
    }
  }
}
static void sendData(int *pRc, int fd, const u8 *aData, int nData){
  if( *pRc==SQLITE_OK ){
    if( send(fd, aData, nData, 0)!=nData ){
      *pRc = SQLITE_IOERR;
    }
  }
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
  }
  printf("transaction cid=%lld\n", iCid);
  return rc;
}

static void *hctServerThread(void *pCtx){
  TestServerJob *pJob = (TestServerJob*)pCtx;
  TestServer *p = pJob->pServer;
  Tcl_Interp *interp = 0;
  int rc = TCL_OK;
  Tcl_Obj *pOpenDb = 0;
  sqlite3 *db = 0;

  /* Create Tcl interpreter for this job */
  interp = Tcl_CreateInterp();
  Sqlite3_Init(interp);

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
static i64 getInt64(u8 *aBuf){
  i64 val;
  memcpy(&val, aBuf, 8);
  return val;
}

static void recvInt32(int *pRc, int fd, int *piVal){
  if( *pRc==TCL_OK ){
    u8 aBuf[4] = {0, 0, 0, 0};
    assert( sizeof(*piVal)==sizeof(aBuf) );
    if( recv(fd, aBuf, sizeof(aBuf), 0)!=sizeof(aBuf) ){
      *pRc = TCL_ERROR;
    }
    *piVal = getInt32(aBuf);
  }
}

static int recvInt64(int fd, i64 *piVal){
  u8 aBuf[8] = {0, 0, 0, 0, 0, 0, 0, 0};
  assert( sizeof(*piVal)==sizeof(aBuf) );
  if( recv(fd, aBuf, sizeof(aBuf), 0)!=sizeof(aBuf) ) return 1;
  *piVal = getInt64(aBuf);
  return 0;
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
    pStmt = testServerPrepare(&rc, p, z3);
    if( rc==SQLITE_OK ) sqlite3_bind_int64(pStmt, 1, iCid);
    sendInt32(&rc, newfd, TESTSERVER_MESSAGE_SYNCREPLY);
    sendInt32(&rc, newfd, p->nJob);
    while( rc==SQLITE_OK && sqlite3_step(pStmt)==SQLITE_ROW ){
      int nSchema = sqlite3_column_bytes(pStmt, 1);
      int nData = sqlite3_column_bytes(pStmt, 2);
      int nSz = sizeof(i64) + sizeof(i64) + nSchema+1 + nData;

      sendInt32(&rc, newfd, nSz);
      sendInt64(&rc, newfd, sqlite3_column_int64(pStmt, 0));
      sendInt64(&rc, newfd, sqlite3_column_int64(pStmt, 3));
      sendData(&rc, newfd, sqlite3_column_text(pStmt, 1), nSchema+1);
      sendData(&rc, newfd, sqlite3_column_blob(pStmt, 2), nData);
    }
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

static int hctServerRun(TestServer *p){
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
    pthread_create(&pJob->tid, NULL, hctServerThread, (void*)pJob);
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
      i64 iSCid = getInt64(&aBuf[8]);
      const char *zSchema = (const char*)&aBuf[16];
      const u8 *aData = (const u8*)&zSchema[strlen(zSchema)+1];
      int nData = nThis - (aData - aBuf);

      while( 1 ){
        rc = sqlite3_hct_journal_write(p->db, iCid, zSchema, aData,nData,iSCid);
        printf("applying %lld (mirror) rc=%d\n", iCid, rc);
        if( rc!=SQLITE_BUSY ) break;
      }
    }

  }

  ckfree(aBuf);
  return 0;
}

static void hctFollowerSyncReply(int *pRc, TestServer *p, int fd){
  int rc = *pRc;
  u8 *aBuf = 0;
  int nBuf = 0;
  int iOff = 0;

  while( rc==TCL_OK ){
    int nNew = nBuf ? (nBuf * 4) : (1024*1024);
    aBuf = ckrealloc(aBuf, nNew);
    while( nBuf<nNew ){
      ssize_t res = recv(fd, &aBuf[nBuf], nNew-nBuf, 0);
      if( res<=0 ){
        if( res<0 ){
          testServerResult(p, "error in recv()");
          rc = TCL_ERROR;
        }
        break;
      }
      nBuf += res;
    }
    if( nBuf<nNew ) break;
  }

  while( rc==TCL_OK && iOff<nBuf ){
    int nThis = getInt32(&aBuf[iOff]);
    if( nThis>0 ){
      if( iOff+4+nThis>nBuf ){
        testServerResult(p, "malformed SYNCREPLY message");
        rc = TCL_ERROR;
      }else{
        i64 iCid = getInt64(&aBuf[iOff+4]);
        i64 iSCid = getInt64(&aBuf[iOff+12]);
        const char *zSchema = (const char*)&aBuf[iOff+20];
        const u8 *aData = (const u8*)&zSchema[strlen(zSchema)+1];
        int nData = nThis+4 - (aData - &aBuf[iOff]);
        rc = sqlite3_hct_journal_write(p->db, iCid, zSchema, aData,nData,iSCid);
        if( rc!=SQLITE_OK ){
          testServerResult(p, "error in sqlite3_hct_journal_write() - %d", rc);
          rc = TCL_ERROR;
        }
      }
    }
    iOff += nThis+4;
  }

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
      sqlite3_hct_journal_hash(aHash, aJrnlHash);
      iCid = sqlite3_column_int64(pStmt, 0);
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
  TestServer *p, 
  i64 iCid, 
  const u8 *aHash
){
  int rc = TCL_OK;
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
        hctFollowerSyncReply(&rc, p, fd);
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
  return nJob;
}

static int hctFollowerRun(TestServer *p){
  int rc = SQLITE_OK;
  i64 iCid = 0;
  u8 aHash[SQLITE_HCT_JOURNAL_HASHSIZE];
  int nJob = 0;
  int ii;

  hctFollowerGetVersion(&rc, p, &iCid, aHash); 
  nJob = hctFollowerDoSync(&rc, p, iCid, aHash);

  /* Launch a thread for each job on the leader node. Connect a socket
  ** and send a SUB message for each. */
  for(ii=0; ii<nJob && rc==TCL_OK; ii++){
    sqlite3 *db = 0;
    int fd = -1;
    int eReply = 0;

    db = testServerOpenDb(&rc, p);
    fd = testServerConnect(&rc, p);

    sendInt32(&rc, fd, TESTSERVER_MESSAGE_SUB);
    sendInt32(&rc, fd, ii);
    recvInt32(&rc, fd, &eReply);
    if( eReply==TESTSERVER_MESSAGE_SUBREPLY ) {
      TestServerMirror *pMirror = &p->aMirror[ii];
      pMirror->db = db;
      pMirror->fd = fd;
      pMirror->pServer = p;
      pthread_create(&pMirror->tid, NULL, hctMirrorThread, (void*)pMirror);
      p->nMirror++;
      db = 0;
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

    if( db ) sqlite3_close(db);
    if( fd>=0 ) close(fd);
  }

  hctFollowerGetVersion(&rc, p, &iCid, aHash); 
  nJob = hctFollowerDoSync(&rc, p, iCid, aHash);

  /* Wait on the mirrors and user jobs. */
  for(ii=0; ii<p->nMirror; ii++){
    void *pVal = 0;
    TestServerMirror *pMirror = &p->aMirror[ii];
    pthread_join(pMirror->tid, &pVal);
  }

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
    "-port",       /* 0 */
    "-host",       /* 1 */
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
          case 0: assert( 0==strcmp(azCfg[iCfg], "port") ); {
            if( Tcl_GetIntFromObj(interp, objv[ii], &p->iPort) ){
              return TCL_ERROR;
            }
            break;
          }
          case 1: assert( 0==strcmp(azCfg[iCfg], "host") ); {
            ckfree(p->zHost);
            p->zHost = testServerStrdup(Tcl_GetString(objv[ii]));
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
      int bFollower = 0;
      int ii;
      for(ii=2; ii<objc; ii++){
        int iOpt = -1;
        rc = Tcl_GetIndexFromObj(interp, objv[ii], azRun, "OPTION", 0, &iOpt);
        if( rc!=TCL_OK ) return rc;
        bFollower = 1;
      }
      if( bFollower ){
        rc = hctFollowerRun(p);
      }else{
        rc = hctServerRun(p);
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


