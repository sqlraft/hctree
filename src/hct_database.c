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

typedef struct HctDatabase HctDatabase;

struct HctDatabase {

  int dummy;
};

int sqlite3HctDbOpen(const char *zFile, HctDatabase **ppDb){
  return SQLITE_OK;
}

void sqlite3HctDbClose(HctDatabase *pFile){
}


