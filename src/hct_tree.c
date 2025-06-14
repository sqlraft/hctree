/*
** 2020 September 24
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

#define HCT_TREE_MAX_DEPTH 60

typedef struct HctTreeNode HctTreeNode;
typedef struct HctTreeRoot HctTreeRoot;

struct HctTree {
  int nRootHash;
  int nRootEntry;
  HctTreeRoot **apRootHash;
  HctTreeNode *pRollback;         /* List of rollback list items */
  HctTreeNode **apStmt;           /* Array of open statement transactions */
  int nStmt;                      /* Allocated size of apStmt[] */
  int iStmt;                      /* Current entry in apStmt (-1 == none) */
};

/*
** pReseek:
**   Set to non-NULL if the cursor was disrupted by a write. The cursor
**   should be seeked to the key in node pReseek.
*/
struct HctTreeCsr {
  HctTree *pTree;
  HctTreeRoot *pRoot;
  u8 bPin;                        /* True if cursor is pinned */
  u8 eIncrblob;                   /* Incrblob cursor state */
  i64 iSeekRowid;                 /* Last rowid value seeked to */
  int iSkip;                      /* -ve -> skip Prev(), +ve -> skip Next() */
  int iNode;                      /* Current depth */
  HctTreeNode *apNode[HCT_TREE_MAX_DEPTH];
  HctTreeNode *pReseek;
  HctTreeCsr *pCsrNext;           /* Next item in HctTreeRoot.pCsrList list */
};

#define TREE_INCRBLOB_NONE  0
#define TREE_INCRBLOB_READY 1
#define TREE_INCRBLOB_ABORT 2

struct HctTreeNode {
  i64 iKey;                       /* 64-bit key for this node */
  u8 bBlack;                      /* 1 for black node, 0 for red node */
  u8 nRef;                        /* Number of pointers to this node */
  u8 bDelete;                     /* True if this is a delete key */
  int nData;                      /* Size of aData[] in bytes */
  u8 *aData;                      /* Pointer to associated data (or NULL) */
  u32 iRoot;                      /* Root id of table this node belongs to */
  HctTreeNode *pLeft;             /* Left child in tree */
  HctTreeNode *pRight;            /* Right child in tree */

  /* Rollback list related variables */
  HctTreeNode *pPrev;             /* Previous entry in rollback list */
  HctTreeNode *pClobber;          /* If non-NULL, entry this one clobbered */
};

static HctTreeNode hctTreeGlobalEofNode;
#define TREE_RESEEK_EOF (&hctTreeGlobalEofNode)

/*
** pCsrCache:
**   List of unused cursor objects for this table/index.
*/
struct HctTreeRoot {
  u32 iRoot;                      /* Name of this tree structure */
  KeyInfo *pKeyInfo;
  HctTreeNode *pNode;             /* Root node of tree (or NULL) */
  HctTreeRoot *pHashNext;         /* Next entry in hash-chain */
  HctTreeCsr *pCsrList;           /* Cursors open on this tree */
  HctTreeCsr *pCsrCache;          /* Cache of unused cursor objects */
};

/*
** Allocate and return nByte bytes of zeroed memory.
*/
static void *hctMallocZero(int nByte){
  void *pNew = sqlite3_malloc(nByte);
  if( pNew ){
    memset(pNew, 0, nByte);
  }
  return pNew;
}

int sqlite3HctTreeNew(HctTree **ppTree){
  HctTree *pNew;
  int rc = SQLITE_OK;

  pNew = (HctTree*)hctMallocZero(sizeof(HctTree));
  if( pNew ){
    pNew->apRootHash = (HctTreeRoot**)hctMallocZero(sizeof(HctTreeRoot*)*16);
    pNew->nRootHash = 16;
  }
  if( pNew==0 || pNew->apRootHash==0 ){
    sqlite3_free(pNew);
    rc = SQLITE_NOMEM_BKPT;
  }

  *ppTree = pNew;
  return rc;
}

static void treeNodeUnref(HctTreeNode *pNode){
  if( pNode!=TREE_RESEEK_EOF ){
    assert( pNode->nRef>0 );
    pNode->nRef--;
    if( pNode->nRef==0 ){
      sqlite3_free(pNode);
    }
  }
}

static void hctTreeFreeNode(HctTreeNode *pNode){
  if( pNode ){
    hctTreeFreeNode(pNode->pLeft);
    hctTreeFreeNode(pNode->pRight);
    assert( pNode->nRef==1 );
    treeNodeUnref(pNode);
  }
}

void sqlite3HctTreeFree(HctTree *pTree){
  if( pTree ){
    int i;
    sqlite3HctTreeRelease(pTree, 0);
    assert( pTree->pRollback==0 );
    for(i=0; i<pTree->nRootHash; i++){
      while( pTree->apRootHash[i] ){
        HctTreeRoot *p = pTree->apRootHash[i];
        HctTreeCsr *pCsr = p->pCsrCache;
        sqlite3KeyInfoUnref(p->pKeyInfo);
        pTree->apRootHash[i] = p->pHashNext;
        while( pCsr ){
          HctTreeCsr *pNext = pCsr->pCsrNext;
          sqlite3_free(pCsr);
          pCsr = pNext;
        }
        hctTreeFreeNode(p->pNode);
        sqlite3_free(p);
      }
    }
    sqlite3_free(pTree->apRootHash);
    sqlite3_free(pTree->apStmt);
    sqlite3_free(pTree);
  }
}

#ifdef SQLITE_DEBUG
#include <stdio.h>
static void hct_print_subtree2(HctTreeNode *pNode, char *aPrefix){
  if( pNode ){
    int n = strlen(aPrefix);
    fprintf(stdout, "%-8s %s k=%lld\n", 
        aPrefix, pNode->bBlack ? "BLACK" : "RED  ", pNode->iKey
    );
    aPrefix[n] = 'L';
    hct_print_subtree2(pNode->pLeft, aPrefix);
    aPrefix[n] = 'R';
    hct_print_subtree2(pNode->pRight, aPrefix);
    aPrefix[n] = '\0';
  }
}
static void hct_print_subtree(HctTreeNode *pNode){
  if( pNode ){
    char aPrefix[64];
    memset(aPrefix, 0, sizeof(aPrefix));
    hct_print_subtree2(pNode, aPrefix);
    fflush(stdout);
  }
}

/*
** To be used as:
**
**     assert( hct_tree_check(pTree) )
**
** An assert() fails if any of the following tree properties are violated:
**
**     1. Root node must be black.
**     2. A red node may not have a red parent.
**     3. Every path from root to NULL passes through the same number 
**        of black nodes.
*/
static void hct_tree_check_subtree(HctTreeNode *pNode, int nDepth, int nExpect){
  if( pNode ){
    int nThisDepth = nDepth;
    if( pNode->bBlack ){
      nThisDepth++;
    }else{
      /* Property 2 - red parents have black children */
      assert( pNode->pLeft==0 || pNode->pLeft->bBlack );
      assert( pNode->pRight==0 || pNode->pRight->bBlack );
    }

    /* Property 3 - Every path from root to NULL has same black-depth */
    assert( (pNode->pLeft && pNode->pRight) || nThisDepth==nExpect );

    hct_tree_check_subtree(pNode->pLeft, nThisDepth, nExpect);
    hct_tree_check_subtree(pNode->pRight, nThisDepth, nExpect);
  }
  hct_print_subtree(0);           /* no-op - just to avoid a warning */
}
static int hct_tree_check(HctTreeRoot *pRoot){
  if( 0 && pRoot->pNode ){
    int nBlack = 0;
    HctTreeNode *pNode = 0;
    assert( pRoot->pNode->bBlack );      /* 1. Root is black */

    /* Calculate the expected number of black nodes between root and NULL. */
    for(pNode=pRoot->pNode; pNode; pNode=pNode->pLeft){
      if( pNode->bBlack ) nBlack++;
    }

    hct_tree_check_subtree(pRoot->pNode, 0, nBlack);
  }
  return 1;
}
#endif

static HctTreeRoot *hctTreeFindRoot(HctTree *pTree, u32 iRoot){
  HctTreeRoot *pNew = 0;

  /* Search the hash table for an existing root. Return immediately if 
  ** one is found.  */
  HctTreeRoot *pRoot;
  for(pRoot = pTree->apRootHash[iRoot % pTree->nRootHash];
      pRoot;
      pRoot=pRoot->pHashNext
  ){
    if( pRoot->iRoot==iRoot ) return pRoot;
  }

  /* If the hash table needs to grow, do that now */
  if( (pTree->nRootEntry+1)*2 > pTree->nRootHash ){
    int ii;
    int nOld = pTree->nRootHash;
    int nNew = nOld ? nOld*2 : 16;
    HctTreeRoot **apNew = (HctTreeRoot**)sqlite3_realloc(
        pTree->apRootHash, nNew*sizeof(HctTreeRoot*)
    );
    if( apNew==0 ) return 0;
    memset(&apNew[nOld], 0, (nNew-nOld)*sizeof(HctTreeRoot*));

    for(ii=0; ii<nOld; ii++){
      HctTreeRoot *p = apNew[ii];
      apNew[ii] = 0;
      while( p ){
        HctTreeRoot *pNext = p->pHashNext;
        int iHash = p->iRoot % nNew;
        p->pHashNext = apNew[iHash];
        apNew[iHash] = p;
        p = pNext;
      }
    }

    pTree->apRootHash = apNew;
    pTree->nRootHash = nNew;
  }

  /* Allocate a new root and add it to the hash table */
  pNew = hctMallocZero(sizeof(HctTreeRoot));
  if( pNew ){
    int iHash = iRoot % pTree->nRootHash;
    pNew->iRoot = iRoot;
    pNew->pHashNext = pTree->apRootHash[iHash];
    pTree->apRootHash[iHash] = pNew;
    pTree->nRootEntry++;
  }

  return pNew;
}

static void leftRotate(HctTreeNode **pp){
  HctTreeNode *pG = *pp;
  HctTreeNode *pRight = pG->pRight;

  pG->pRight = pRight->pLeft;
  pRight->pLeft = pG;
  *pp = pRight;
}

static void rightRotate(HctTreeNode **pp){
  HctTreeNode *pG = *pp;
  HctTreeNode *pLeft = pG->pLeft;

  pG->pLeft = pLeft->pRight;
  pLeft->pRight = pG;
  *pp = pLeft;
}

static HctTreeNode **hctTreeFindPointer(HctTreeCsr *pCsr, int iNode){
  HctTreeNode **pp;
  if( iNode==0 ){
    assert( pCsr->apNode[0]==pCsr->pRoot->pNode );
    pp = &pCsr->pRoot->pNode;
  }else{
    HctTreeNode *pParent = pCsr->apNode[iNode-1];
    if( pParent->pLeft==pCsr->apNode[iNode] ){
      pp = &pParent->pLeft;
    }else{
      assert( pParent->pRight==pCsr->apNode[iNode] );
      pp = &pParent->pRight;
    }
  }
  return pp;
}

static void hctTreeFixInsert(
  HctTree *pTree, 
  HctTreeCsr *pCsr, 
  HctTreeNode *pX
){
  HctTreeNode *pP = pCsr->apNode[pCsr->iNode];
  HctTreeNode *pG = pCsr->apNode[pCsr->iNode-1];
  HctTreeNode *pU;

  assert( pCsr->iNode>=1 );

  if( pG->pLeft==pP ){
    pU = pG->pRight;
  }else{
    pU = pG->pLeft;
  }

  if( pU && pU->bBlack==0 ){
    /* Uncle of X is red */
    pP->bBlack = 1;
    pU->bBlack = 1;
    if( pCsr->iNode>1 ){
      pG->bBlack = 0;
      if( pCsr->apNode[pCsr->iNode-2]->bBlack==0 ){
        pCsr->iNode -= 2;
        hctTreeFixInsert(pTree, pCsr, pG);
      }
    }
  }else{
    /* Uncle of X is black */
    int iCase = ((pG->pRight==pP) ? 2 : 0) + (pP->pRight==pX ? 1 : 0);
    HctTreeNode **ppG = hctTreeFindPointer(pCsr, pCsr->iNode-1);

    switch( iCase ){
      case 1: /* left/right */
        leftRotate(&pG->pLeft);
        pP = pX;
        /* fall-through */
      case 0: /* left/left */
        rightRotate(ppG);
        pP->bBlack = 1;
        pG->bBlack = 0;
        break;
      case 2: /* right/left */
        rightRotate(&pG->pRight);
        pP = pX;
        /* fall-through */
      case 3: /* right/right */
        leftRotate(ppG);
        pP->bBlack = 1;
        pG->bBlack = 0;
        break;
      default:
        assert( 0 );
    }
  }
}

static int hctSaveCursors(
  HctTreeRoot *pRoot, 
  HctTreeCsr *pExcept,
  int bAbortBlob,
  i64 iRowid
){
  int rc = SQLITE_OK;
  HctTreeCsr *pCsr;
  for(pCsr=pRoot->pCsrList; pCsr; pCsr=pCsr->pCsrNext){
    if( pCsr!=pExcept && pCsr->pReseek==0 ){
      if( pCsr->iNode>=0 ){
        if( pCsr->bPin ){
          return SQLITE_CONSTRAINT_PINNED;
        }
        pCsr->pReseek = pCsr->apNode[pCsr->iNode];
        pCsr->pReseek->nRef++;
      }else{
        pCsr->pReseek = TREE_RESEEK_EOF;
      }
    }
    if( bAbortBlob 
     && pCsr->eIncrblob==TREE_INCRBLOB_READY 
     && pCsr->iSeekRowid==iRowid 
    ){
      pCsr->eIncrblob = TREE_INCRBLOB_ABORT;
    }
  }
  return rc;
}


static int hctTreeCsrSeekInt(
  HctTreeCsr *pCsr, 
  i64 iKey, 
  int *pRes
){
  int rc = SQLITE_OK;             /* Return code */
  int res = -1;                   /* Value to return via *pRes */
  HctTreeNode *pNode = pCsr->pRoot->pNode;
  pCsr->iNode = -1;
  while( pNode ){
    i64 iNodeKey = pNode->iKey;
    pCsr->apNode[++pCsr->iNode] = pNode;
    if( iNodeKey==iKey ){
      res = 0;
      break;
    }
    if( iKey<iNodeKey ){
      res = +1;
      pNode = pNode->pLeft;
    }else{
      res = -1;
      pNode = pNode->pRight;
    }
    assert( pCsr->iNode<HCT_TREE_MAX_DEPTH );
  }
  if( pRes ) *pRes = res;
  return rc;
}

static int hctTreeCsrSeekUnpacked(
  HctTreeCsr *pCsr, 
  UnpackedRecord *pRec,
  int *pRes
){
  int rc = SQLITE_OK;             /* Return code */
  int res = -1;                   /* Value to return via *pRes */
  HctTreeNode *pNode = pCsr->pRoot->pNode;
  pCsr->iNode = -1;
  while( pNode ){
    pCsr->apNode[++pCsr->iNode] = pNode;
    res = sqlite3VdbeRecordCompare(pNode->nData, pNode->aData, pRec);
    if( res==0 ) break;
    if( res>0 ){
      /* pRec is smaller than this node's key. Go left. */
      pNode = pNode->pLeft;
    }else{
      /* pRec is larger than this node's key. Go left. */
      pNode = pNode->pRight;
    }
    assert( pCsr->iNode<HCT_TREE_MAX_DEPTH );
  }

  if( pCsr->pRoot->pKeyInfo==0 ){
    pCsr->pRoot->pKeyInfo = sqlite3KeyInfoRef(pRec->pKeyInfo);
  }

  if( pRes ) *pRes = res;
  return rc;
}

static int hctTreeCsrSeekPacked(
  HctTreeCsr *pCsr, 
  int nKey,
  const u8 *aKey,
  int *pRes
){
  int rc;
  KeyInfo *pKeyInfo = pCsr->pRoot->pKeyInfo;
  UnpackedRecord *pRec;

  assert( pKeyInfo );
  pRec = sqlite3VdbeAllocUnpackedRecord(pKeyInfo);
  if( pRec ){
    sqlite3VdbeRecordUnpack(nKey, aKey, pRec);
    rc = hctTreeCsrSeekUnpacked(pCsr, pRec, pRes);
    sqlite3DbFree(pKeyInfo->db, pRec);
  }else{
    rc = SQLITE_NOMEM;
  }
  return rc;
}


static int hctRestoreCursor(HctTreeCsr *pCsr, int *pRes){
  int rc = SQLITE_OK;
  HctTreeNode *pReseek = pCsr->pReseek;
  if( pReseek ){
    if( pReseek!=TREE_RESEEK_EOF ){
      if( pCsr->pRoot->pKeyInfo ){
        rc = hctTreeCsrSeekPacked(pCsr, pReseek->nData, pReseek->aData, pRes);
      }else{
        rc = hctTreeCsrSeekInt(pCsr, pReseek->iKey, pRes);
      }
      treeNodeUnref(pReseek);
    }
    pCsr->pReseek = 0;
  }else{
    *pRes = 0;
  }
  return rc;
}

static void hctRestoreDiscard(HctTreeCsr *pCsr){
  if( pCsr->pReseek ){
    treeNodeUnref(pCsr->pReseek);
    pCsr->pReseek = 0;
    pCsr->iNode = -1;
  }
  pCsr->iSkip = 0;
}

static int treeInsertNode(
  HctTree *pTree, 
  int bRollback, 
  UnpackedRecord *pKey,
  i64 iKey, 
  HctTreeNode *pNew
){
  HctTreeRoot *pRoot = hctTreeFindRoot(pTree, pNew->iRoot);
  UnpackedRecord *pFree = 0;
  int res = 0;
  HctTreeCsr csr;
  memset(&csr, 0, sizeof(csr));
  csr.pRoot = pRoot;
  csr.pTree = pTree;

  /* Special case. If this insert is to effect a rollback on an index
  ** tree, pKey will still be NULL. In this case construct a pKey value
  ** with which to do the seek.  */
  if( pRoot->pKeyInfo && pKey==0 ){
    assert( bRollback );
    pFree = sqlite3VdbeAllocUnpackedRecord(pRoot->pKeyInfo);
    if( pFree==0 ){
      return SQLITE_NOMEM;
    }
    sqlite3VdbeRecordUnpack(pNew->nData, pNew->aData, pFree);
    pKey = pFree;
  }

  sqlite3HctTreeCsrSeek(&csr, pKey, iKey, &res);
  if( csr.iNode<0 ){
    assert( pRoot->pNode==0 );
    pRoot->pNode = pNew;
  }else{
    HctTreeNode *pNode = csr.apNode[csr.iNode];
    if( res==0 ){
      pNew->pLeft = pNode->pLeft;
      pNew->pRight = pNode->pRight;
      pNew->bBlack = pNode->bBlack;
      *(hctTreeFindPointer(&csr, csr.iNode)) = pNew;
      if( bRollback==0 && pTree->iStmt>=0 ){
        pNew->pClobber = pNode;
        assert( pNew->iKey==pNode->iKey );
      }else{
        treeNodeUnref(pNode);
      }
    }else{
      if( res<0 ){
        assert( pNode->pRight==0 );
        pNode->pRight = pNew;
      }else{
        assert( pNode->pLeft==0 );
        pNode->pLeft = pNew;
      }
      if( pNode->bBlack==0 ){
        hctTreeFixInsert(pTree, &csr, pNew);
      }
    }
  }
  pNew->nRef++;

  /* Root node is always black */
  pRoot->pNode->bBlack = 1;
  assert( hct_tree_check(pRoot) );
  if( pFree ){
    sqlite3DbFree(pFree->pKeyInfo->db, pFree);
  }
  return SQLITE_OK;
}

static HctTreeNode *treeNewNode2(
  HctTree *pTree,
  HctTreeRoot *pRoot,
  i64 iKey, 
  int bDelete,
  int nData, 
  const u8 *aData,
  int nZero
){
  HctTreeNode *pNew;

  pNew = (HctTreeNode*)hctMallocZero(sizeof(HctTreeNode) + nData + nZero);
  if( pNew ){
    pNew->iKey = iKey;
    pNew->nData = nData + nZero;
    pNew->iRoot = pRoot->iRoot;
    pNew->bDelete = bDelete;
    if( (nData+nZero)>0 ){
      pNew->aData = (u8*)&pNew[1];
      memcpy(pNew->aData, aData, nData);
    }

    if( pTree->iStmt>0 ){
      pNew->pPrev = pTree->pRollback;
      pTree->pRollback = pNew;
      pNew->nRef = 1;
    }
  }

  return pNew;
}

/*
** Allocate a new tree node. Link it into the rollback list.
*/
static HctTreeNode *treeNewNode(
  HctTreeCsr *pCsr,
  i64 iKey, 
  int bDelete,
  int nData, 
  const u8 *aData,
  int nZero
){
  return treeNewNode2(
      pCsr->pTree, pCsr->pRoot, iKey, bDelete, nData, aData, nZero
  );
}

static int treeInsert(
  HctTreeCsr *pCsr,
  UnpackedRecord *pKey,
  i64 iKey, 
  int bDelete,
  int nData, 
  const u8 *aData,
  int nZero
){
  HctTree *pTree = pCsr->pTree;
  HctTreeNode *pNew;
  int rc = SQLITE_OK;

  assert( bDelete==0 || pKey || (aData==0 && nData==0 && nZero==0) );

  pNew = treeNewNode(pCsr, iKey, bDelete, nData, aData, nZero);
  if( pNew==0 ){
    rc = SQLITE_NOMEM;
  }else{
    int nSave = 0;
    int bPinSave = pCsr->bPin;
    if( pKey ){
      nSave = pKey->nField;
      sqlite3HctDbRecordTrim(pKey);
    }
    pCsr->bPin = 0;
    rc = hctSaveCursors(pCsr->pRoot, 0, (pCsr->eIncrblob==0), iKey);
    if( rc==SQLITE_OK && bPinSave ){
      int dummy;
      rc = hctRestoreCursor(pCsr, &dummy);
    }
    pCsr->bPin = bPinSave;
    if( rc==SQLITE_OK ){
      rc = treeInsertNode(pTree, pTree->iStmt<=0, pKey, iKey, pNew);
    }
    if( pKey ) pKey->nField = nSave;
  }

  return rc;
}

int sqlite3HctTreeUpdateMeta(
  HctTree *pTree,
  const u8 *aMeta,                /* Meta data */
  int nMeta                       /* Size of meta data in bytes */
){
  HctTreeRoot *pRoot = hctTreeFindRoot(pTree, 2);
  HctTreeNode *pNew = treeNewNode2(pTree, pRoot, 0, 0, nMeta, aMeta, 0);
  treeInsertNode(pTree, pTree->iStmt<=0, 0, 0, pNew);
  return SQLITE_OK;
}

/*
** This function is like sqlite3HctTreeInsert(), except that:
**
**   1) the new key is always larger than any existing key in the
**      tree, and
**
**   2) unless the tree is empty, cursor pCsr is guaranteed to point to the
**      largest record in it, and
**
**   3) before returning, this function leaves cursor pCsr pointing to the 
**      new entry.
*/
int sqlite3HctTreeAppend(
  HctTreeCsr *pCsr,
  KeyInfo *pKeyInfo,
  i64 iKey, 
  int nData, 
  const u8 *aData,
  int nZero
){
  HctTreeRoot *pRoot = pCsr->pRoot;
  int rc = SQLITE_OK;

  assert( pCsr->pTree->iStmt>0 );

  if( pKeyInfo && pRoot->pKeyInfo==0 ){
    pRoot->pKeyInfo = sqlite3KeyInfoRef(pKeyInfo);
  }

  rc = hctSaveCursors(pRoot, pCsr, pCsr->eIncrblob==0, iKey);
  if( rc==SQLITE_OK ){
    HctTreeNode *pNew = treeNewNode(pCsr, iKey, 0, nData, aData, nZero);
    if( pNew==0 ){
      rc = SQLITE_NOMEM_BKPT;
    }else{
      pNew->nRef++;
      if( pRoot->pNode==0 ){
        pRoot->pNode = pNew;
        pCsr->apNode[0] = pNew;
        pCsr->iNode = 0;
      }else{
        HctTreeNode *pParent = pCsr->apNode[pCsr->iNode];

        assert( pCsr->iNode>=0 );
        assert( pParent->pRight==0 );
        pParent->pRight = pNew;

        if( pParent->bBlack==0 ){
          hctTreeFixInsert(pCsr->pTree, pCsr, pNew);
          sqlite3HctTreeCsrLast(pCsr);
        }else{
          pCsr->apNode[++pCsr->iNode] = pNew;
        }
      }

      /* Root node is always black */
      pRoot->pNode->bBlack = 1;
      assert( hct_tree_check(pRoot) );
    }
  }
    
  return rc;
}

#if 0
static void debug_write_op(
  HctTreeCsr *pCsr, 
  const char *zOp, 
  UnpackedRecord *pKey,
  i64 iKey, 
  int nData, 
  const u8 *aData
){
  printf("%s(%d) ", zOp, (int)pCsr->pRoot->iRoot);
  if( pKey ){
    char *z = sqlite3HctDbRecordToText(0, aData, nData);
    printf("[%s]\n", z);
  }else{
    printf("%lld\n", iKey);
  }
  fflush(stdout);
}
#else
# define debug_write_op(r,s,w,x,y,z)
#endif

int sqlite3HctTreeInsert(
  HctTreeCsr *pCsr,
  UnpackedRecord *pKey,
  i64 iKey, 
  int nData, 
  const u8 *aData,
  int nZero
){
  assert( pKey==0 || iKey==0 );
  debug_write_op(pCsr, "INSERT", pKey, iKey, nData, aData);
  return treeInsert(pCsr, pKey, iKey, 0, nData, aData, nZero);
}

int sqlite3HctTreeDeleteKey(
  HctTreeCsr *pCsr,
  UnpackedRecord *pKey,
  i64 iKey,
  int nData, 
  const u8 *aData
){
  debug_write_op(pCsr, "DELETE", pKey, iKey, nData, aData);
  return treeInsert(pCsr, pKey, iKey, 1, nData, aData, 0);
}

/*
** Cursor pCsr currently points at a double-black node. Fix it.
*/
static void hctTreeFixDelete(HctTreeCsr *pCsr){
  assert( pCsr->iNode>0 || pCsr->pRoot->pNode->bBlack );
  if( pCsr->iNode>0 ){
    HctTreeNode *pDB;             /* The double-black */
    HctTreeNode *pP;              /* Parent of pDB */
    HctTreeNode *pS;              /* Sibling of pDB */

    pDB = pCsr->apNode[pCsr->iNode];
    pP = pCsr->apNode[pCsr->iNode-1];
    pS = pP->pLeft==pDB ? pP->pRight : pP->pLeft;

    if( pS->bBlack ){
      HctTreeNode *pR = 0;
      if( pS->pLeft && pS->pLeft->bBlack==0 ){
        pR = pS->pLeft;
      }else if( pS->pRight && pS->pRight->bBlack==0 ){
        pR = pS->pRight;
      }

      if( pR ){
        /* Sibling is black, pR is a red child */
        HctTreeNode **ppP = hctTreeFindPointer(pCsr, pCsr->iNode-1);
        int iCase = ((pP->pRight==pS) ? 2 : 0) + (pS->pRight==pR ? 1 : 0);
        switch( iCase ){
          case 0:       /* Left/Left */
            pR->bBlack = 1;
            pS->bBlack = pP->bBlack;
            rightRotate(ppP);
            pP->bBlack = 1;
            break;
          case 1:       /* Left/Right */
            leftRotate(&pP->pLeft);
            rightRotate(ppP);
            pR->bBlack = pP->bBlack;
            pP->bBlack = 1;
            break;
          case 2:       /* Right/Left */
            rightRotate(&pP->pRight);
            leftRotate(ppP);
            pR->bBlack = pP->bBlack;
            pP->bBlack = 1;
            break;
          case 3:       /* Right/Right */
            pR->bBlack = 1;
            pS->bBlack = pP->bBlack;
            leftRotate(ppP);
            pP->bBlack = 1;
            break;
        }
      }else{
        /* Sibling is black, with no red children. */
        pS->bBlack = 0;
        if( pP->bBlack ){
          pCsr->iNode--;
          hctTreeFixDelete(pCsr);
        }else{
          pP->bBlack = 1;
        }
      }
    }else{
      HctTreeNode **ppP = hctTreeFindPointer(pCsr, pCsr->iNode-1);

      /* Sibling is red. Because it is the red sibling of a double-black, it
      ** must have children on both sides. And because it is red, both those
      ** children must be black.  */
      assert( pS->pLeft->bBlack && pS->pRight->bBlack );

      if( pS==pP->pLeft ){
        rightRotate(ppP);
      }else{
        leftRotate(ppP);
      }
      pS->bBlack = 1;
      pP->bBlack = 0;
      pCsr->apNode[pCsr->iNode-1] = pS;
      pCsr->apNode[pCsr->iNode] = pP;
      pCsr->apNode[pCsr->iNode+1] = pDB;
      pCsr->iNode++;
      hctTreeFixDelete(pCsr);
    }
  }
}

static int treeDelete(HctTreeCsr *pCsr, int bRollback){
  HctTreeNode *pDel = pCsr->apNode[pCsr->iNode];
  HctTreeNode *pU = 0;
  HctTreeNode *pReseek = 0;
  int rc;

  /* Save the positions of all cursors on this table */
  rc = hctSaveCursors(pCsr->pRoot, pCsr, 0, 0);
  if( rc ) return rc;

  assert( pCsr->pReseek==0 );
  assert( pCsr->iNode>=0 );
#if 0
  fprintf(stdout, "deleting %lld\n", iKey);
  hct_print_subtree(pCsr->pRoot->pNode);
#endif

  if( bRollback==0 ){
    HctTreeNode *pEntry = hctMallocZero(sizeof(*pEntry));
    if( pEntry==0 ) return SQLITE_NOMEM;
    pEntry->iKey = pDel->iKey;
    pEntry->pClobber = pDel;
    pEntry->pPrev = pCsr->pTree->pRollback;
    pEntry->nRef = 1;
    pEntry->iRoot = pCsr->pRoot->iRoot;
    pDel->nRef++;
    pCsr->pTree->pRollback = pEntry;
    pReseek = pDel;
    pReseek->nRef++;
  }

  /* If node pDel has two children, swap it with its immediate successor
  ** in the tree. This node is guaranteed to have pNode->pLeft==0. */
  if( pDel->pLeft && pDel->pRight ){
    int iDel = pCsr->iNode;
    HctTreeNode *pSwap;
    sqlite3HctTreeCsrNext(pCsr);
    pSwap = pCsr->apNode[pCsr->iNode];
    SWAP(HctTreeNode*, pSwap->pLeft, pDel->pLeft);
    SWAP(HctTreeNode*, pSwap->pRight, pDel->pRight);
    SWAP(int, pSwap->bBlack, pDel->bBlack);
    *hctTreeFindPointer(pCsr, iDel) = pSwap;
    pCsr->apNode[iDel] = pSwap;
    *hctTreeFindPointer(pCsr, pCsr->iNode) = pDel;
    pCsr->apNode[pCsr->iNode] = pDel;

    assert( pDel->pLeft==0 );
    assert( hct_tree_check(pCsr->pRoot) );
  }

  assert( pCsr->apNode[pCsr->iNode]==pDel );
  assert( pDel->pLeft==0 || pDel->pRight==0 );

  pU = pDel->pLeft ? pDel->pLeft : pDel->pRight;
  *hctTreeFindPointer(pCsr, pCsr->iNode) = pU;
  if( pDel->bBlack==0 || (pU && pU->bBlack==0) || pCsr->pRoot->pNode==0 ){
    /* Simple case. If either pDel or its child pU are red, then 
    ** replacing the pDel with the child and ensuring the child is 
    ** colored black is enough. No change in black-height for the 
    ** children of pU. */
    if( pU ) pU->bBlack = 1;
  }else{
    pCsr->apNode[pCsr->iNode] = pU;
    hctTreeFixDelete(pCsr);
  }

  treeNodeUnref(pDel);
  assert( pCsr->pReseek==0 );
  pCsr->pReseek = pReseek;

#if 0
  fprintf(stdout, "finished deleting %lld\n", iKey);
  hct_print_subtree(pCsr->pRoot->pNode);
#endif
  assert( hct_tree_check(pCsr->pRoot) );
  return SQLITE_OK;
}

int sqlite3HctTreeDelete(HctTreeCsr *pCsr){
  int rc;
  assert( pCsr->pReseek==0 );
  rc = treeDelete(pCsr, 0);
  return rc;
}

int sqlite3HctTreeBegin(HctTree *pTree, int iStmt){
  if( iStmt>pTree->iStmt ){
    int ii;
    if( pTree->nStmt<=iStmt ){
      int nNew = iStmt+16;
      HctTreeNode **apNew = (HctTreeNode**)hctMallocZero(nNew*sizeof(*apNew));
      if( apNew==0 ) return SQLITE_NOMEM;
      if( pTree->apStmt ){
        memcpy(apNew, pTree->apStmt, pTree->nStmt*sizeof(*apNew));
        sqlite3_free(pTree->apStmt);
      }
      pTree->apStmt = apNew;
      pTree->nStmt = nNew;
    }
    for(ii=pTree->iStmt+1; ii<=iStmt; ii++){
      pTree->apStmt[ii] = pTree->pRollback;
    }
    pTree->iStmt = iStmt;
  }
  return SQLITE_OK;
}

int sqlite3HctTreeRelease(HctTree *pTree, int iStmt){
  if( iStmt<pTree->iStmt ){
    if( iStmt==0 ){
      HctTreeNode *pStop = pTree->apStmt[iStmt+1];
      HctTreeNode *pNode;
      HctTreeNode *pPrev;
      for(pNode=pTree->pRollback; pNode!=pStop; pNode=pPrev){
        pPrev = pNode->pPrev;
        if( pNode->pClobber ) treeNodeUnref(pNode->pClobber);
        treeNodeUnref(pNode);
      }
      pTree->pRollback = pStop;
    }
    pTree->iStmt = iStmt;
  }
  return SQLITE_OK;
}

int sqlite3HctTreeRollbackTo(HctTree *pTree, int iStmt){
  int rc = SQLITE_OK;
  if( iStmt<=pTree->iStmt ){
    HctTreeNode *pStop = pTree->apStmt[iStmt];
    HctTreeNode *pNode;
    HctTreeNode *pPrev;
    for(pNode=pTree->pRollback; pNode!=pStop; pNode=pPrev){
      KeyInfo *pKeyInfo = 0;
      UnpackedRecord *pRec = 0;
      HctTreeRoot *pRoot = hctTreeFindRoot(pTree, pNode->iRoot);

      pPrev = pNode->pPrev;

      if( (pKeyInfo = pRoot->pKeyInfo) ){
        pRec = sqlite3VdbeAllocUnpackedRecord(pKeyInfo);
        if( pRec==0 ){
          rc = SQLITE_NOMEM;
          pStop = pNode;
          break;
        }
        sqlite3VdbeRecordUnpack(pNode->nData, pNode->aData, pRec);
      }

      if( pNode->pClobber ){
        HctTreeNode *pClobber = pNode->pClobber;
        assert( pNode->iKey==pNode->pClobber->iKey );
        pClobber->pLeft = pClobber->pRight = 0;
        pClobber->bBlack = 0;
        if( (rc = hctSaveCursors(pRoot, 0, 0, 0))
         || (rc = treeInsertNode(pTree, 1, pRec, pNode->iKey, pClobber)) ){
          pStop = pNode;
          break;
        }
        treeNodeUnref(pClobber);
      }else{
        HctTreeCsr csr;
        int res;
        memset(&csr, 0, sizeof(csr));
        csr.pRoot = pRoot;
        csr.pTree = pTree;
        sqlite3HctTreeCsrSeek(&csr, pRec, pNode->iKey, &res);
        if( res==0 ) treeDelete(&csr, 1);
      }
      if( pRec ) sqlite3DbFree(pKeyInfo->db, pRec);
      treeNodeUnref(pNode);
    }
    pTree->pRollback = pStop;
    pTree->iStmt = iStmt;
  }
  return rc;
}

/*
** Clear the contents of the entire tree.
*/
void sqlite3HctTreeClear(HctTree *pTree){
  HctTreeRoot **pp;
  HctTreeRoot **pEnd = &pTree->apRootHash[pTree->nRootHash];
  for(pp=pTree->apRootHash; pp<pEnd; pp++){
    HctTreeRoot *p;
    for(p=*pp; p; p=p->pHashNext){
      hctSaveCursors(p, 0, 0, 0);
      hctTreeFreeNode(p->pNode);
      p->pNode = 0;
      sqlite3KeyInfoUnref(p->pKeyInfo);
      p->pKeyInfo = 0;
    }
  }
}

int sqlite3HctTreeClearOne(HctTree *pTree, u32 iRoot, i64 *pnRow){
  HctTreeCsr csr;
  int rc = SQLITE_OK;
  int nRow = 0;

  memset(&csr, 0, sizeof(csr));
  csr.pTree = pTree;
  csr.pRoot = hctTreeFindRoot(pTree, iRoot);
  csr.iNode = -1;
  rc = hctSaveCursors(csr.pRoot, 0, 0, 0);
  if( rc ) return rc;
  while( rc==SQLITE_OK && csr.pRoot->pNode ){
    sqlite3HctTreeCsrFirst(&csr);
    rc = sqlite3HctTreeDelete(&csr);
    nRow++;
    hctRestoreDiscard(&csr);
  }
  if( pnRow ) *pnRow = nRow;
  return rc;
}

int sqlite3HctTreeCsrOpen(HctTree *pTree, u32 iRoot, HctTreeCsr **ppCsr){
  int rc = SQLITE_OK;
  HctTreeCsr *pNew = 0;
  HctTreeRoot *pRoot = hctTreeFindRoot(pTree, iRoot);
  if( pRoot==0 ){
    rc = SQLITE_NOMEM;
  }else{
    if( pRoot->pCsrCache ){
      pNew = pRoot->pCsrCache;
      pRoot->pCsrCache = pNew->pCsrNext;
      pNew->pCsrNext = 0;
      assert( pNew->pTree==pTree );
      assert( pNew->pRoot==pRoot );
      assert( pNew->iNode==-1 );
      assert( pNew->eIncrblob==TREE_INCRBLOB_NONE );
    }else{
      pNew = (HctTreeCsr*)hctMallocZero(sizeof(HctTreeCsr));
      if( pNew==0 ){
        rc = SQLITE_NOMEM;
      }else{
        pNew->pTree = pTree;
        pNew->pRoot = pRoot;
        pNew->iNode = -1;
      }
    }
    if( pNew ){
      pNew->pCsrNext = pRoot->pCsrList;
      pRoot->pCsrList = pNew;
    }
  }
  *ppCsr = pNew;
  return rc;
}

int sqlite3HctTreeCsrClose(HctTreeCsr *pCsr){
  if( pCsr ){
    HctTreeCsr **pp;
    for(pp=&pCsr->pRoot->pCsrList; *pp!=pCsr; pp=&(*pp)->pCsrNext);
    *pp = pCsr->pCsrNext;
    if( pCsr->pReseek ){
      treeNodeUnref(pCsr->pReseek);
      pCsr->pReseek = 0;
    }
    pCsr->pCsrNext = pCsr->pRoot->pCsrCache;
    pCsr->pRoot->pCsrCache = pCsr;
    pCsr->iSkip = 0;
    pCsr->bPin = 0;
    pCsr->iNode = -1;
    pCsr->eIncrblob = TREE_INCRBLOB_NONE;
  }
  return SQLITE_OK;
}

/*
** An integer is written into *pRes which is the result of
** comparing the key with the entry to which the cursor is 
** pointing.  The meaning of the integer written into
** *pRes is as follows:
**
**     *pRes<0      The cursor is left pointing at an entry that
**                  is smaller than intKey/pIdxKey. Or, the table is empty
**                  and the cursor is therefore left point to nothing.
**
**     *pRes==0     The cursor is left pointing at an entry that
**                  exactly matches intKey/pIdxKey.
**
**     *pRes>0      The cursor is left pointing at an entry that
**                  is larger than intKey/pIdxKey.
*/
int sqlite3HctTreeCsrSeek(
  HctTreeCsr *pCsr, 
  UnpackedRecord *pRec, 
  i64 iKey, 
  int *pRes
){
  hctRestoreDiscard(pCsr);
  pCsr->iSeekRowid = iKey;
  if( pRec ){
    return hctTreeCsrSeekUnpacked(pCsr, pRec, pRes);
  }
  return hctTreeCsrSeekInt(pCsr, iKey, pRes);
}

/*
** Move the cursor to EOF.
*/
void sqlite3HctTreeCsrClear(HctTreeCsr *pCsr){
  hctRestoreDiscard(pCsr);
  pCsr->iNode = -1;
}

int sqlite3HctTreeCsrNext(HctTreeCsr *pCsr){
  int iNode;
  int res = 0;

  assert( pCsr->pReseek==0 || pCsr->iSkip==0 );
  if( pCsr->iSkip>0 ){
    pCsr->iSkip = 0;
    return SQLITE_OK;
  }
  if( hctRestoreCursor(pCsr, &res) ) return SQLITE_NOMEM;
  if( res>0 ) return SQLITE_OK;

  iNode = pCsr->iNode;
  if( iNode>=0 ){
    HctTreeNode *pNode = pCsr->apNode[iNode];
    assert( iNode>=0 );
    if( pNode->pRight ){
      pNode = pNode->pRight;
      while( pNode ){
        iNode++;
        pCsr->apNode[iNode] = pNode;
        pNode = pNode->pLeft;
      }
    }else{
      while( (--iNode)>=0 ){
        HctTreeNode *pParent = pCsr->apNode[iNode];
        assert( pNode==pParent->pLeft || pNode==pParent->pRight );
        if( pNode==pParent->pLeft ) break;
        pNode = pParent;
      }
    }
    pCsr->iNode = iNode;
  }
  return SQLITE_OK;
}

int sqlite3HctTreeCsrPrev(HctTreeCsr *pCsr){
  int iNode;
  int res = 0;

  assert( pCsr->pReseek==0 || pCsr->iSkip==0 );
  if( pCsr->iSkip<0 ){
    pCsr->iSkip = 0;
    return SQLITE_OK;
  }
  if( hctRestoreCursor(pCsr, &res) ) return SQLITE_NOMEM;
  if( res<0 ) return SQLITE_OK;

  iNode = pCsr->iNode;
  if( iNode>=0 ){
    HctTreeNode *pNode = pCsr->apNode[iNode];
    assert( iNode>=0 );
    if( pNode->pLeft ){
      pNode = pNode->pLeft;
      while( pNode ){
        iNode++;
        pCsr->apNode[iNode] = pNode;
        pNode = pNode->pRight;
      }
    }else{
      while( (--iNode)>=0 ){
        HctTreeNode *pParent = pCsr->apNode[iNode];
        assert( pNode==pParent->pLeft || pNode==pParent->pRight );
        if( pNode==pParent->pRight ) break;
        pNode = pParent;
      }
    }
    pCsr->iNode = iNode;
  }
  return SQLITE_OK;
}

/*
** Return false if cursor points to a valid entry, or true otherwise.
*/
int sqlite3HctTreeCsrEof(HctTreeCsr *pCsr){
  return (pCsr->iNode<0);
}

static void hctTreeCursorEnd(HctTreeCsr *pCsr, int bLast){
  int iNode = -1;
  HctTreeNode *pNode = pCsr->pRoot->pNode;

  hctRestoreDiscard(pCsr);
  while( pNode ){
    iNode++;
    assert( iNode<HCT_TREE_MAX_DEPTH );
    pCsr->apNode[iNode] = pNode;
    pNode = (bLast ? pNode->pRight : pNode->pLeft);
  }
  pCsr->iNode = iNode;
}

int sqlite3HctTreeCsrFirst(HctTreeCsr *pCsr){
  hctTreeCursorEnd(pCsr, 0);
  return SQLITE_OK;
}

int sqlite3HctTreeCsrLast(HctTreeCsr *pCsr){
  hctTreeCursorEnd(pCsr, 1);
  return SQLITE_OK;
}

int sqlite3HctTreeCsrKey(HctTreeCsr *pCsr, i64 *piKey){
  assert( pCsr->iNode>=0 );
  assert( pCsr->pReseek==0 );
  *piKey = pCsr->apNode[pCsr->iNode]->iKey;
  return SQLITE_OK;
}

int sqlite3HctTreeCsrData(HctTreeCsr *pCsr, int *pnData, const u8 **paData){
  HctTreeNode *pNode = pCsr->apNode[pCsr->iNode];
  assert( pCsr->pReseek==0 );
  assert( pCsr->iNode>=0 );
  *pnData = pNode->nData;
  if( paData ) *paData = pNode->aData;
  return SQLITE_OK;
}

/*
** Return non-zero if the cursor is pointing to a delete key. Return zero 
** if it is pointing to an insert or to EOF.
*/
int sqlite3HctTreeCsrIsDelete(HctTreeCsr *pCsr){
  assert( pCsr->pReseek==0 );
  return (pCsr->iNode>=0 && pCsr->apNode[pCsr->iNode]->bDelete);
}

void sqlite3HctTreeCsrPin(HctTreeCsr *pCsr){
  pCsr->bPin = 1;
}
void sqlite3HctTreeCsrUnpin(HctTreeCsr *pCsr){
  pCsr->bPin = 0;
}

void sqlite3HctTreeCsrIncrblob(HctTreeCsr *pCsr){
  if( pCsr->eIncrblob==TREE_INCRBLOB_NONE ){
    pCsr->eIncrblob = TREE_INCRBLOB_READY;
  }
}

int sqlite3HctTreeCsrHasMoved(HctTreeCsr *pCsr){
  return pCsr && pCsr->pReseek!=0;
}

int sqlite3HctTreeCsrReseek(HctTreeCsr *pCsr, int *pRes){
  assert( 
      pCsr->eIncrblob==TREE_INCRBLOB_READY 
   || pCsr->eIncrblob==TREE_INCRBLOB_ABORT
  );
  assert( pCsr->pReseek );
  if( pCsr->eIncrblob==TREE_INCRBLOB_ABORT ) return SQLITE_ABORT;
  return sqlite3HctTreeCsrSeek(pCsr, 0, pCsr->iSeekRowid, pRes);
}


int sqlite3HctTreeCsrRestore(HctTreeCsr *pCsr, int *pIsDifferent){
  int rc = SQLITE_OK;
  if( pCsr->pReseek ){
    assert( pCsr->iSkip==0 );
    rc = hctRestoreCursor(pCsr, &pCsr->iSkip);
  }
  *pIsDifferent = pCsr->iSkip;
  return rc;
}

u32 sqlite3HctTreeCsrRoot(HctTreeCsr *pCsr){
  return pCsr->pRoot->iRoot;
}

/*
** Return true if the tree that the cursor is open on is empty. Otherwise,
** if it contains one or more keys or delete-keys, return false.
*/
int sqlite3HctTreeCsrIsEmpty(HctTreeCsr *pCsr){
  return (pCsr->pRoot->pNode==0);
}

int sqlite3HctTreeForeach(
  HctTree *pTree,
  void *pCtx,
  int (*x)(void *, u32, KeyInfo*)
){
  int rc = SQLITE_OK;
  int iRound;

  /* Round 0 picks up IPK tables, and UNIQUE or PRIMARY indexes. Round 1 
  ** picks up other indexes. Round 0 objects are the ones that require
  ** write/write conflict detection. Write/write conflict detection is
  ** skipped for round 1 objects. */
  for(iRound=0; iRound<2; iRound++){
    int i;
    for(i=0; rc==SQLITE_OK && i<pTree->nRootHash; i++){
      HctTreeRoot *p;
      for(p=pTree->apRootHash[i]; rc==SQLITE_OK && p; p=p->pHashNext){
        /* bWWC is true for objects that need write/write conflict detection */
        int bWWC = (p->pKeyInfo==0 || p->pKeyInfo->nUniqField>0);
        if( p->pNode && bWWC==(iRound==0) ){
          rc = x(pCtx, p->iRoot, p->pKeyInfo);
        }
      }
    }
  }
  return rc;
}


