/*
** 2022 May 19
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
#include "sqliteInt.h"
#include "vdbeInt.h"

#include <string.h>
#include <assert.h>

/*
** Write the serialized data blob for the value stored in pMem into 
** buf. It is assumed that the caller has allocated sufficient space.
** Return the number of bytes written.
**
** nBuf is the amount of space left in buf[].  The caller is responsible
** for allocating enough space to buf[] to hold the entire field, exclusive
** of the pMem->u.nZero bytes for a MEM_Zero value.
**
** Return the number of bytes actually written into buf[].  The number
** of bytes in the zero-filled tail is included in the return value only
** if those bytes were zeroed in buf[].
*/ 
static u32 hctRecordSerialPut(u8 *buf, Mem *pMem, u32 serial_type){
  u32 len;

  /* Integer and Real */
  if( serial_type<=7 && serial_type>0 ){
    u64 v;
    u32 i;
    if( serial_type==7 ){
      assert( sizeof(v)==sizeof(pMem->u.r) );
      memcpy(&v, &pMem->u.r, sizeof(v));
      swapMixedEndianFloat(v);
    }else{
      v = pMem->u.i;
    }
    len = i = sqlite3SmallTypeSizes[serial_type];
    assert( i>0 );
    do{
      buf[--i] = (u8)(v&0xFF);
      v >>= 8;
    }while( i );
    return len;
  }

  /* String or blob */
  if( serial_type>=12 ){
    assert( pMem->n + ((pMem->flags & MEM_Zero)?pMem->u.nZero:0)
             == (int)sqlite3VdbeSerialTypeLen(serial_type) );
    len = pMem->n;
    if( len>0 ) memcpy(buf, pMem->z, len);
    return len;
  }

  /* NULL or constants 0 or 1 */
  return 0;
}

/*
** Return the serial-type for the value stored in pMem.
**
** This routine might convert a large MEM_IntReal value into MEM_Real.
*/
static u32 hctRecordSerialType(Mem *pMem, u32 *pLen){
  int flags = pMem->flags;
  u32 n;

  assert( pLen!=0 );
  if( flags&MEM_Null ){
    *pLen = 0;
    return 0;
  }
  if( flags&(MEM_Int|MEM_IntReal) ){
    /* Figure out whether to use 1, 2, 4, 6 or 8 bytes. */
#   define MAX_6BYTE ((((i64)0x00008000)<<32)-1)
    i64 i = pMem->u.i;
    u64 u;
    testcase( flags & MEM_Int );
    testcase( flags & MEM_IntReal );
    if( i<0 ){
      u = ~i;
    }else{
      u = i;
    }
    if( u<=127 ){
      if( (i&1)==i ){
        *pLen = 0;
        return 8+(u32)u;
      }else{
        *pLen = 1;
        return 1;
      }
    }
    if( u<=32767 ){ *pLen = 2; return 2; }
    if( u<=8388607 ){ *pLen = 3; return 3; }
    if( u<=2147483647 ){ *pLen = 4; return 4; }
    if( u<=MAX_6BYTE ){ *pLen = 6; return 5; }
    *pLen = 8;
    if( flags&MEM_IntReal ){
      /* If the value is IntReal and is going to take up 8 bytes to store
      ** as an integer, then we might as well make it an 8-byte floating
      ** point value */
      pMem->u.r = (double)pMem->u.i;
      pMem->flags &= ~MEM_IntReal;
      pMem->flags |= MEM_Real;
      return 7;
    }
    return 6;
  }
  if( flags&MEM_Real ){
    *pLen = 8;
    return 7;
  }
  assert( pMem->db->mallocFailed || flags&(MEM_Str|MEM_Blob) );
  assert( pMem->n>=0 );
  n = (u32)pMem->n;
  if( flags & MEM_Zero ){
    n += pMem->u.nZero;
  }
  *pLen = n;
  return ((n*2) + 12 + ((flags&MEM_Str)!=0));
}


/*
**
*/
int sqlite3HctSerializeRecord(
  UnpackedRecord *pRec,           /* Record to serialize */
  u8 **ppRec,                     /* OUT: buffer containing serialization */
  int *pnRec                      /* OUT: size of (*ppRec) in bytes */
){
  int ii;
  int nData = 0;
  int nHdr = 0;
  u8 *pOut = 0;
  int iOffHdr = 0;
  int iOffData = 0;

  for(ii=0; ii<pRec->nField; ii++){
    u32 n;
    u32 stype = hctRecordSerialType(&pRec->aMem[ii], &n);
    nData += n;
    nHdr += sqlite3VarintLen(stype);
    pRec->aMem[ii].uTemp = stype;
  }

  if( nHdr<=126 ){
    /* The common case */
    nHdr += 1;
  }else{
    /* Rare case of a really large header */
    int nVarint = sqlite3VarintLen(nHdr);
    nHdr += nVarint;
    if( nVarint<sqlite3VarintLen(nHdr) ) nHdr++;
  }

  pOut = (u8*)sqlite3_malloc(nData+nHdr);
  if( pOut==0 ){
    return SQLITE_NOMEM_BKPT;
  }

  iOffData = nHdr;
  iOffHdr = putVarint32(pOut, nHdr);
  for(ii=0; ii<pRec->nField; ii++){
    u32 stype = pRec->aMem[ii].uTemp;
    iOffHdr += putVarint32(&pOut[iOffHdr], stype);
    iOffData += hctRecordSerialPut(&pOut[iOffData], &pRec->aMem[ii], stype);
  }
  assert( iOffData==(nHdr+nData) );

  *ppRec = pOut;
  *pnRec = iOffData;

  return SQLITE_OK;
}

int sqlite3HctNameFromSchemaRecord(
  const u8 *aData, 
  int nData, 
  const u8 **ppName
){
  int nRet = 0;
  if( nData>0 ){
    int iOff = 0;
    int nHdr = 0;
    int aType[5];
    int ii;

    iOff += getVarint32(&aData[iOff], nHdr);
    for(ii=0; ii<ArraySize(aType); ii++){
      iOff += getVarint32(&aData[iOff], aType[ii]);
    }

    if( aType[0]==27 || aType[0]==40 ){
      /* A trigger */
      *ppName = "";
      nRet = 0;
    }else{
      *ppName = &aData[iOff + sqlite3VdbeSerialTypeLen(aType[0])];
      nRet = sqlite3VdbeSerialTypeLen(aType[1]);
    }
  }
  return nRet;
}

