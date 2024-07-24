/*
** 2023 January 10
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
** This file contains a standalone program to test the speed of reading
** a page of data from one part of a file and immediately writing it
** to another, using multiple threads. Compile with:
**
**    gcc -O2 pagetest.c -o pagetest
**
** Then:
**
**    ./pagetest
**
** Try:
**
**    ./pagetest -help
**
** For more information.
**
** If you're reading this code, the part you are interested in is the 
** "do_test" function below and the "main" that launches it.
*/ 

#include <stdio.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <assert.h>

typedef unsigned char u8;
typedef long long i64;

/*
** State of a simple PRNG used for the per-connection and per-pager
** pseudo-random number generators.
*/
typedef struct FastPrng FastPrng;
struct FastPrng {
  unsigned int x, y;
};

/*
** Generate N bytes of pseudo-randomness using a FastPrng
*/
static void ttFastRandomness(FastPrng *pPrng, int N, void *P){
  unsigned char *pOut = (unsigned char*)P;
  while( N-->0 ){
    pPrng->x = ((pPrng->x)>>1) ^ ((1+~((pPrng->x)&1)) & 0xd0000001);
    pPrng->y = (pPrng->y)*1103515245 + 12345;
    *(pOut++) = (pPrng->x ^ pPrng->y) & 0xff;
  }
}

static int ttFastRandomInt(FastPrng *pPrng){
  unsigned int ret;
  ttFastRandomness(pPrng, sizeof(unsigned int), &ret);
  return (int)(ret & 0x7FFFFFFF);
}

static void error_out(const char *zFmt, ...){
  va_list ap;
  va_start(ap, zFmt);
  vfprintf(stderr, zFmt, ap);
  fprintf(stderr, "\n");
  va_end(ap);
  exit(1);
}

static i64 stime_now(){
  struct timeval tm;
  gettimeofday(&tm, 0);
  return ((i64)tm.tv_sec * 1000000 + (i64)tm.tv_usec);
}

typedef struct TestFile TestFile;
struct TestFile {
  int fd;
  u8 *pMap;
};

typedef struct TestCtx TestCtx;
struct TestCtx {
  pthread_t tid;
  i64 iEnd;                       /* Time to finish */
  i64 pgsz;                       /* Page size */
  int nPg;                        /* Number of pages */
  int nPgPerFile;                 /* Number of pages per file */
  int nFile;
  TestFile *aFile;
  FastPrng prng;
  u8 *pBuf;
  i64 nWrite;                     /* OUT: Number of pages written */
};


typedef struct TestOptions TestOptions;
struct TestOptions {
  int nMB;                        /* Size of test file in MB */
  int nSecond;                    /* Number of seconds to run for */
  int nThread;                    /* Number of threads to run */
  i64 pgsz;                       /* Page size to use */
  int pwrite;                     /* Page size to use */
  int nFile;                      /* Number of files nMB of data is split */
};

#define MAX_NFILE 32              /* Max value for -nfile */

static void print_count(int p){
  assert( p>=0 );
  if( p>1000000000 ){
    printf("%d,%03d,%03d,%03d", 
        (p / 1000000000),
        (p%1000000000) / 1000000,
        (p%1000000) / 1000,
        (p%1000)
    );
  }else if( p>1000000 ){
    printf("%d,%03d,%03d", (p / 1000000), (p%1000000) / 1000, (p%1000));
  }
  else if( p>1000 ){
    printf("%d,%03d", (p / 1000), (p%1000));
  }else{
    printf("%d", p);
  }
}

static void parse_options(int argc, char **argv, TestOptions *p){
  const char *azOpt[] = {
    "-nthread",            /* 0 */
    "-size",               /* 1 */
    "-nsecond",            /* 2 */
    "-pagesize",           /* 3 */
    "-pwrite",             /* 4 */
    "-nfile"               /* 5 */
  };
  int ii;

  for(ii=1; ii<argc; ii++){
    const char *zOpt = argv[ii];
    int nOpt = strlen(zOpt);
    int iOpt = -1;
    int iVal = 0;
    int jj = 0;

    for(jj=0; jj<sizeof(azOpt)/sizeof(azOpt[0]); jj++){
      if( strlen(azOpt[jj])>=nOpt && memcmp(azOpt[jj], zOpt, nOpt)==0 ){
        if( iOpt>=0 ){
          iOpt = -1;
          break;
        }
        iOpt = jj;
      }
    }
    if( iOpt<0 ){
      fprintf(stderr, "unknown or ambiguous option - \"%s\"\n", zOpt);
      error_out(
          "usage: %s ?-pwrite? ?-nthread N? ?-size MB?"
          " ?-nsecond N? ?-pagesize BYTES? ?-nfile N?", 
          argv[0]
      );
    }
    if( iOpt!=4 ){
      if( (ii+1)>=argc ){
        error_out("option %s requires a parameter", azOpt[iOpt]);
      }
      iVal = strtol(argv[++ii], 0, 0);
    }
    switch( iOpt ){
      case 0:
        if( iVal<1 || iVal>128 ){
          error_out("-nthread parameter must be between 1 and 128");
        }
        p->nThread = iVal;
        break;

      case 1:
        if( iVal<1 ){
          error_out("-size parameter must be greater than 0");
        }
        p->nMB = iVal;
        break;

      case 2:
        if( iVal<1 ){
          error_out("-nsecond parameter must be greater than 0");
        }
        p->nSecond = iVal;
        break;

      case 3:
        if( iVal<512 || iVal>65536 || (iVal & (iVal-1))!=0 ){
          error_out(
              "-pagesize parameter must be a power-of-two between 512 and 65536"
          );
        }
        p->pgsz = iVal;
        break;

      case 4:
        p->pwrite = 1;
        break;

      case 5:
        if( iVal<1 || iVal>MAX_NFILE ){
          error_out("-nfile parameter must be between 1 and 32");
        }
        p->nFile = iVal;
        break;
    }
  }
}

static void *do_test(void *pCtx){
  TestCtx *p = (TestCtx*)pCtx;
  

  while( stime_now()<p->iEnd ){
    int iPg1 = ttFastRandomInt(&p->prng) % p->nPg;
    int iPg2 = ttFastRandomInt(&p->prng) % p->nPg;
    int iFile1 = iPg1 / p->nPgPerFile;
    int iFile2 = iPg2 / p->nPgPerFile;
    int iFilePg1 = iPg1 % p->nPgPerFile;
    int iFilePg2 = iPg2 % p->nPgPerFile;
    const u8 *pFrom = &p->aFile[iFile1].pMap[iFilePg1*p->pgsz];

    if( p->pBuf ){
      /* This branch is taken if "-pwrite" was specified. */
      int fd = p->aFile[iFile2].fd;
      memcpy(p->pBuf, pFrom, p->pgsz);
      size_t res = pwrite(fd, p->pBuf, p->pgsz, iFilePg2*p->pgsz);
      if( res!=p->pgsz ) break;
    }else{
      i64 iOff = iFilePg2 * p->pgsz;
      u8 *pTo = &p->aFile[iFile2].pMap[iOff];
      memcpy(pTo, pFrom, p->pgsz);
    }

    p->nWrite++;
  }

  return 0;
}

int main(int argc, char **argv){
  TestOptions opt;
  TestCtx aCtx[128];
  TestFile aFile[MAX_NFILE];
  int rc = 0;
  int ii = 0;
  i64 iEnd = 0;
  i64 nWrite = 0;
  int iFile = 0;

  int nPg = 0;
  i64 nPgPerFile = 0;

  memset(aFile, 0, sizeof(aFile));
  memset(aCtx, 0, sizeof(aCtx));
  memset(&opt, 0, sizeof(opt));
  opt.nMB = 512;
  opt.nSecond = 10;
  opt.nThread = 8;
  opt.pgsz = 4096;
  opt.nFile = 1;
  parse_options(argc, argv, &opt);

  nPg = (i64)opt.nMB*(1024*1024)/opt.pgsz;
  nPgPerFile = (nPg+opt.nFile-1) / opt.nFile;

  for(iFile=0; iFile<opt.nFile; iFile++){
    struct stat sStat;
    i64 sz = nPgPerFile * opt.pgsz;
    TestFile *f = &aFile[iFile];
    char zFile[128];
    int bFileExists = 0;

    sprintf(zFile, "pagetest%d.db", iFile);

    /* Open the test file */
    f->fd = open(zFile, O_CREAT|O_RDWR, 0644);
    if( f->fd<0 ){
      error_out("open() failed...");
    }

    memset(&sStat, 0, sizeof(sStat));
    fstat(f->fd, &sStat);
    if( sStat.st_size==sz ){
      bFileExists = 1;
    }

    /* Populate the file to its required size */
    if( bFileExists==0 ){
      rc = ftruncate(f->fd, sz);
      if( rc<0 ){
        error_out("ftruncate() failed...");
      }
    }

    /* Memory map the file */
    f->pMap = mmap(0, sz, PROT_READ|PROT_WRITE, MAP_SHARED, f->fd, 0);
    if( f->pMap==MAP_FAILED ){
      error_out("mmap() failed...");
    }

    /* Populate the file */
    if( bFileExists==0 ){
      for(ii=0; ii<sz; ii+=8){
        *(i64*)&f->pMap[ii] = ii;
      }
    }
  }

  /* Populate the context objects */
  iEnd = stime_now() + opt.nSecond*1000000;
  for(ii=0; ii<opt.nThread; ii++){
    aCtx[ii].iEnd = iEnd;
    aCtx[ii].pgsz = opt.pgsz;
    aCtx[ii].nPg = nPg;
    aCtx[ii].nPgPerFile = nPgPerFile;
    aCtx[ii].prng.x = 13+ii;
    aCtx[ii].prng.y = 123456789+ii;
    aCtx[ii].aFile = aFile;
    aCtx[ii].nFile = opt.nFile;
    if( opt.pwrite ){
      aCtx[ii].pBuf = (u8*)malloc(opt.pgsz);
    }
  }

  /* Launch the threads */
  printf("%d threads for %d seconds. db=%dMB, pgsz=%lld, nfile=%d. "
      "%ssing pwrite().\n", 
      opt.nThread, opt.nSecond, (int)opt.nMB, opt.pgsz, opt.nFile,
      opt.pwrite ? "U" : "Not u"
  );
  for(ii=0; ii<opt.nThread; ii++){
    pthread_create(&aCtx[ii].tid, 0, do_test, (void*)&aCtx[ii]);
  }

  /* Join all the threads */
  for(ii=0; ii<opt.nThread; ii++){
    void *dummy = 0;
    rc = pthread_join(aCtx[ii].tid, &dummy);
    if( rc!=0 ){
      error_out("pthread_join() failed...");
    }
  }

  /* Print out the report */
  nWrite = 0;
  for(ii=0; ii<opt.nThread; ii++){
    nWrite += aCtx[ii].nWrite++;
  }
  printf("%d total pages written - ", (int)nWrite);
  print_count((int)nWrite/opt.nSecond);
  printf(" per second\n");

  return 0;
}


