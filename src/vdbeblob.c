/*
** 2007 May 1
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
** This file contains code used to implement incremental BLOB I/O.
*/

#include "sqliteInt.h"
#include "vdbeInt.h"

#ifndef SQLITE_OMIT_INCRBLOB

/*
** Valid sqlite4_blob* handles point to Incrblob structures.
*/
typedef struct Incrblob Incrblob;
struct Incrblob {
  int nByte;              /* Size of open blob, in bytes */
  int iOffset;            /* Byte offset of blob in cursor data */
  u16 iCol;               /* Table column this handle is open on */
  KVCursor *pCsr;         /* Cursor pointing at blob row */ // [koreauniv] BtCursor -> KVCursor
  sqlite4_stmt *pStmt;    /* Statement holding cursor open */
  sqlite4 *db;            /* The associated database */
  char *zDb;              /* Database name */
  Table *pTab;            /* Table object */
};


/*
** This function is used by both blob_open() and blob_reopen(). It seeks
** the b-tree cursor associated with blob handle p to point to row iRow.
** If successful, SQLITE_OK is returned and subsequent calls to
** sqlite4_blob_read() or sqlite4_blob_write() access the specified row.
**
** If an error occurs, or if the specified row does not exist or does not
** contain a value of type TEXT or BLOB in the column nominated when the
** blob handle was opened, then an error code is returned and *pzErr may
** be set to point to a buffer containing an error message. It is the
** responsibility of the caller to free the error message buffer using
** sqlite4DbFree().
**
** If an error does occur, then the b-tree cursor is closed. All subsequent
** calls to sqlite4_blob_read(), blob_write() or blob_reopen() will 
** immediately return SQLITE_ABORT.
*/
static int blobSeekToRow(Incrblob *p, sqlite4_int64 iRow, char **pzErr){
  int rc;                         /* Error code */
  char *zErr = 0;                 /* Error message */
  Vdbe *v = (Vdbe *)p->pStmt;

  /* Set the value of register r[1] in the SQL statement to integer iRow. 
  ** This is done directly as a performance optimization
  */
  sqlite4VdbeMemSetInt64(&v->aMem[1], iRow);

  /* If the statement has been run before (and is paused at the OP_ResultRow)
  ** then back it up to the point where it does the OP_NotExists.  This could
  ** have been down with an extra OP_Goto, but simply setting the program
  ** counter is faster. */
  if( v->pc>4 ){
    v->pc = 4;
    assert( v->aOp[v->pc].opcode==OP_NotExists );
    rc = sqlite4VdbeExec(v);
  }else{
    rc = sqlite4_step(p->pStmt);
  }
  if( rc==SQLITE4_ROW ){
    VdbeCursor *pC = v->apCsr[0];
    u32 type;
    assert( pC!=0 );
    assert( pC->eCurType==CURTYPE_BTREE );
    type = pC->nHdrParsed>p->iCol ? pC->aType[p->iCol] : 0;
    testcase( pC->nHdrParsed==p->iCol );
    testcase( pC->nHdrParsed==p->iCol+1 );
    if( type<12 ){
      zErr = sqlite4MPrintf(p->db, "cannot open value of type %s",
          type==0?"null": type==7?"real": "integer"
      );
      rc = SQLITE4_ERROR;
      sqlite4_finalize(p->pStmt);
      p->pStmt = 0;
    }else{
      p->iOffset = pC->aType[p->iCol + pC->nField];
      p->nByte = sqlite4VdbeSerialTypeLen(type); // [koreauniv] 수정필요 (임시완료)
      p->pCsr =  pC->uc.pCursor;
      sqlite4BtreeIncrblobCursor(p->pCsr); // [koreauniv] 수정필요
    }
  }

  if( rc==SQLITE4_ROW ){
    rc = SQLITE4_OK;
  }else if( p->pStmt ){
    rc = sqlite4_finalize(p->pStmt);
    p->pStmt = 0;
    if( rc==SQLITE4_OK ){
      zErr = sqlite4MPrintf(p->db, "no such rowid: %lld", iRow);
      rc = SQLITE4_ERROR;
    }else{
      zErr = sqlite4MPrintf(p->db, "%s", sqlite4_errmsg(p->db));
    }
  }

  assert( rc!=SQLITE4_OK || zErr==0 );
  assert( rc!=SQLITE4_ROW && rc!=SQLITE4_DONE );

  *pzErr = zErr;
  return rc;
}

/*
** Open a blob handle.
*/
int sqlite4_blob_open(
  sqlite4* db,            /* The database connection */
  const char *zDb,        /* The attached database containing the blob */
  const char *zTable,     /* The table containing the blob */
  const char *zColumn,    /* The column containing the blob */
  sqlite4_int64 iRow,      /* The row containing the glob */
  int wrFlag,             /* True -> read/write access, false -> read-only */
  sqlite4_blob **ppBlob   /* Handle for accessing the blob returned here */
){
  int nAttempt = 0;
  int iCol;               /* Index of zColumn in row-record */
  int rc = SQLITE4_OK;
  char *zErr = 0;
  Table *pTab;
  Incrblob *pBlob = 0;
  Parse sParse;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( ppBlob==0 ){
    return SQLITE4_MISUSE_BKPT;
  }
#endif
  *ppBlob = 0;
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlite4SafetyCheckOk(db) || zTable==0 || zColumn==0 ){
    return SQLITE4_MISUSE_BKPT;
  }
#endif
  wrFlag = !!wrFlag;                /* wrFlag = (wrFlag ? 1 : 0); */

  sqlite4_mutex_enter(db->mutex);

  pBlob = (Incrblob *)sqlite4DbMallocZero(db, sizeof(Incrblob));
  while(1){
    sqlite4ParseObjectInit(&sParse,db); // [koreauniv] 수정필요
    if( !pBlob ) goto blob_open_out;
    sqlite4DbFree(db, zErr);
    zErr = 0;

    sqlite4BtreeEnterAll(db); // [koreauniv] 수정필요
    pTab = sqlite4LocateTable(&sParse, 0, zTable, zDb);
    if( pTab && IsVirtual(pTab) ){
      pTab = 0;
      sqlite4ErrorMsg(&sParse, "cannot open virtual table: %s", zTable);
    }
    if( pTab && !HasRowid(pTab) ){
      pTab = 0;
      sqlite4ErrorMsg(&sParse, "cannot open table without rowid: %s", zTable);
    }
#ifndef SQLITE_OMIT_VIEW
    if( pTab && IsView(pTab) ){
      pTab = 0;
      sqlite4ErrorMsg(&sParse, "cannot open view: %s", zTable);
    }
#endif
    if( !pTab ){
      if( sParse.zErrMsg ){
        sqlite4DbFree(db, zErr);
        zErr = sParse.zErrMsg;
        sParse.zErrMsg = 0;
      }
      rc = SQLITE4_ERROR;
      sqlite4BtreeLeaveAll(db); // [koreauniv] 수정필요
      goto blob_open_out;
    }
    pBlob->pTab = pTab;
    pBlob->zDb = db->aDb[sqlite4SchemaToIndex(db, pTab->pSchema)].zName;

    /* Now search pTab for the exact column. */
    for(iCol=0; iCol<pTab->nCol; iCol++) {
      if( sqlite4_stricmp(pTab->aCol[iCol].zName, zColumn)==0 ){
        break;
      }
    }
    if( iCol==pTab->nCol ){
      sqlite4DbFree(db, zErr);
      zErr = sqlite4MPrintf(db, "no such column: \"%s\"", zColumn);
      rc = SQLITE4_ERROR;
      sqlite4BtreeLeaveAll(db); // [koreauniv] 수정필요
      goto blob_open_out;
    }

    /* If the value is being opened for writing, check that the
    ** column is not indexed, and that it is not part of a foreign key. 
    */
    if( wrFlag ){
      const char *zFault = 0;
      Index *pIdx;
#ifndef SQLITE_OMIT_FOREIGN_KEY
      if( db->flags&SQLITE4_ForeignKeys ){
        /* Check that the column is not part of an FK child key definition. It
        ** is not necessary to check if it is part of a parent key, as parent
        ** key columns must be indexed. The check below will pick up this 
        ** case.  */
        FKey *pFKey;
        assert( IsOrdinaryTable(pTab) );
        for(pFKey=pTab->pFKey; pFKey; pFKey=pFKey->pNextFrom){
          int j;
          for(j=0; j<pFKey->nCol; j++){
            if( pFKey->aCol[j].iFrom==iCol ){
              zFault = "foreign key";
            }
          }
        }
      }
#endif
      for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
        int j;
        for(j=0; j<pIdx->nColumn; j++){
          /* FIXME: Be smarter about indexes that use expressions */
          if( pIdx->aiColumn[j]==iCol || pIdx->aiColumn[j]==XN_EXPR ){
            zFault = "indexed";
          }
        }
      }
      if( zFault ){
        sqlite4DbFree(db, zErr);
        zErr = sqlite4MPrintf(db, "cannot open %s column for writing", zFault);
        rc = SQLITE4_ERROR;
        sqlite4BtreeLeaveAll(db); // [koreauniv] 수정필요
        goto blob_open_out;
      }
    }

    pBlob->pStmt = (sqlite4_stmt *)sqlite4VdbeCreate(&sParse);
    assert( pBlob->pStmt || db->mallocFailed );
    if( pBlob->pStmt ){
      
      /* This VDBE program seeks a btree cursor to the identified 
      ** db/table/row entry. The reason for using a vdbe program instead
      ** of writing code to use the b-tree layer directly is that the
      ** vdbe program will take advantage of the various transaction,
      ** locking and error handling infrastructure built into the vdbe.
      **
      ** After seeking the cursor, the vdbe executes an OP_ResultRow.
      ** Code external to the Vdbe then "borrows" the b-tree cursor and
      ** uses it to implement the blob_read(), blob_write() and 
      ** blob_bytes() functions.
      **
      ** The sqlite4_blob_close() function finalizes the vdbe program,
      ** which closes the b-tree cursor and (possibly) commits the 
      ** transaction.
      */
      static const int iLn = VDBE_OFFSET_LINENO(2);
      static const VdbeOpList openBlob[] = {
        {OP_TableLock,      0, 0, 0},  /* 0: Acquire a read or write lock */
        {OP_OpenRead,       0, 0, 0},  /* 1: Open a cursor */
        /* blobSeekToRow() will initialize r[1] to the desired rowid */
        {OP_NotExists,      0, 5, 1},  /* 2: Seek the cursor to rowid=r[1] */
        {OP_Column,         0, 0, 1},  /* 3  */
        {OP_ResultRow,      1, 0, 0},  /* 4  */
        {OP_Halt,           0, 0, 0},  /* 5  */
      };
      Vdbe *v = (Vdbe *)pBlob->pStmt;
      int iDb = sqlite4SchemaToIndex(db, pTab->pSchema);
      VdbeOp *aOp;

      sqlite4VdbeAddOp4Int(v, OP_Transaction, iDb, wrFlag, 
                           pTab->pSchema->schema_cookie,
                           pTab->pSchema->iGeneration);
      sqlite4VdbeChangeP5(v, 1);
      assert( sqlite4VdbeCurrentAddr(v)==2 || db->mallocFailed );
      aOp = sqlite4VdbeAddOpList(v, ArraySize(openBlob), openBlob);

      /* Make sure a mutex is held on the table to be accessed */
      sqlite4VdbeUsesStorage(v, iDb); // [koreauniv] sqlite4VdbeUsesBtree -> sqlite4VdbeUsesStorage

      if( db->mallocFailed==0 ){
        assert( aOp!=0 );
        /* Configure the OP_TableLock instruction */
#ifdef SQLITE_OMIT_SHARED_CACHE
        aOp[0].opcode = OP_Noop;
#else
        aOp[0].p1 = iDb;
        aOp[0].p2 = pTab->tnum;
        aOp[0].p3 = wrFlag;
        sqlite4VdbeChangeP4(v, 2, pTab->zName, P4_TRANSIENT);
      }
      if( db->mallocFailed==0 ){
#endif

        /* Remove either the OP_OpenWrite or OpenRead. Set the P2 
        ** parameter of the other to pTab->tnum.  */
        if( wrFlag ) aOp[1].opcode = OP_OpenWrite;
        aOp[1].p2 = pTab->tnum;
        aOp[1].p3 = iDb;   

        /* Configure the number of columns. Configure the cursor to
        ** think that the table has one more column than it really
        ** does. An OP_Column to retrieve this imaginary column will
        ** always return an SQL NULL. This is useful because it means
        ** we can invoke OP_Column to fill in the vdbe cursors type 
        ** and offset cache without causing any IO.
        */
        aOp[1].p4type = P4_INT32;
        aOp[1].p4.i = pTab->nCol+1;
        aOp[3].p2 = pTab->nCol;

        sParse.nVar = 0;
        sParse.nMem = 1;
        sParse.nTab = 1;
        sqlite4VdbeMakeReady(v, &sParse);
      }
    }
   
    pBlob->iCol = iCol;
    pBlob->db = db;
    sqlite4BtreeLeaveAll(db); // [koreauniv] 수정필요
    if( db->mallocFailed ){
      goto blob_open_out;
    }
    rc = blobSeekToRow(pBlob, iRow, &zErr);
    if( (++nAttempt)>=SQLITE4_MAX_SCHEMA_RETRY || rc!=SQLITE4_SCHEMA ) break;
    sqlite4ParseObjectReset(&sParse); // [koreauniv] 수정필요
  }

blob_open_out:
  if( rc==SQLITE4_OK && db->mallocFailed==0 ){
    *ppBlob = (sqlite4_blob *)pBlob;
  }else{
    if( pBlob && pBlob->pStmt ) sqlite4VdbeFinalize((Vdbe *)pBlob->pStmt);
    sqlite4DbFree(db, pBlob);
  }
  // sqlite4ErrorwithMsg(db, rc, (zErr ? "%s" : (char*)0), zErr); // [koreauniv] 수정필요
  printf("[blob] open failed: %s with rc = %d\n", zErr ? zErr : "(no detail)", rc); // [koreauniv] 위 코드 대신 추가
  sqlite4DbFree(db, zErr);
  sqlite4ParseObjectReset(&sParse);
  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

/*
** Close a blob handle that was previously created using
** sqlite4_blob_open().
*/
int sqlite4_blob_close(sqlite4_blob *pBlob){
  Incrblob *p = (Incrblob *)pBlob;
  int rc;
  sqlite4 *db;

  if( p ){
    sqlite4_stmt *pStmt = p->pStmt;
    db = p->db;
    sqlite4_mutex_enter(db->mutex);
    sqlite4DbFree(db, p);
    sqlite4_mutex_leave(db->mutex);
    rc = sqlite4_finalize(pStmt);
  }else{
    rc = SQLITE4_OK;
  }
  return rc;
}

/*
** Perform a read or write operation on a blob
*/
static int blobReadWrite(
  sqlite4_blob *pBlob, 
  void *z, 
  int n, 
  int iOffset, 
  int (*xCall)(KVCursor*, u32, u32, void*)
){
  int rc;
  Incrblob *p = (Incrblob *)pBlob;
  Vdbe *v;
  sqlite4 *db;

  if( p==0 ) return SQLITE4_MISUSE_BKPT;
  db = p->db;
  sqlite4_mutex_enter(db->mutex);
  v = (Vdbe*)p->pStmt;

  if( n<0 || iOffset<0 || ((sqlite4_int64)iOffset+n)>p->nByte ){
    /* Request is out of range. Return a transient error. */
    rc = SQLITE4_ERROR;
  }else if( v==0 ){
    /* If there is no statement handle, then the blob-handle has
    ** already been invalidated. Return SQLITE_ABORT in this case.
    */
    rc = SQLITE4_ABORT;
  }else{
    /* Call either BtreeData() or BtreePutData(). If SQLITE_ABORT is
    ** returned, clean-up the statement handle.
    */
    assert( db == v->db );
    sqlite4VdbeSeekEnd(p->pCsr, +1); // [koreauniv] sqlite3BtreeEnterCursor -> sqlite4VdbeSeekEnd

#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
    if( xCall==sqlite3BtreePutData && db->xPreUpdateCallback ){
      /* If a pre-update hook is registered and this is a write cursor, 
      ** invoke it here. 
      ** 
      ** TODO: The preupdate-hook is passed SQLITE_DELETE, even though this
      ** operation should really be an SQLITE_UPDATE. This is probably
      ** incorrect, but is convenient because at this point the new.* values 
      ** are not easily obtainable. And for the sessions module, an 
      ** SQLITE_UPDATE where the PK columns do not change is handled in the 
      ** same way as an SQLITE_DELETE (the SQLITE_DELETE code is actually
      ** slightly more efficient). Since you cannot write to a PK column
      ** using the incremental-blob API, this works. For the sessions module
      ** anyhow.
      */
      sqlite3_int64 iKey;
      iKey = sqlite3BtreeIntegerKey(p->pCsr);
      assert( v->apCsr[0]!=0 );
      assert( v->apCsr[0]->eCurType==CURTYPE_BTREE );
      sqlite3VdbePreUpdateHook(
          v, v->apCsr[0], SQLITE4_DELETE, p->zDb, p->pTab, iKey, -1, p->iCol
      );
    }
#endif

    rc = xCall(p->pCsr, iOffset+p->iOffset, n, z);
    sqlite4VdbeFreeCursor(p->pCsr); // [koreauniv] sqlite3BtreeLeaveCursor -> sqlite4VdbeFreeCursor
    if( rc==SQLITE4_ABORT ){
      sqlite4VdbeFinalize(v);
      p->pStmt = 0;
    }else{
      v->rc = rc;
    }
  }
  sqlite4Error(db, rc, NULL); // [koreauniv] sqlite3Error -> sqlite4Error
  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

/*
** Read data from a blob handle.
*/
int sqlite4_blob_read(sqlite4_blob *pBlob, void *z, int n, int iOffset){
  return blobReadWrite(pBlob, z, n, iOffset, sqlite4BtreePayloadChecked); // [koreauniv] 수정필요
}

/*
** Write data to a blob handle.
*/
int sqlite4_blob_write(sqlite4_blob *pBlob, const void *z, int n, int iOffset){
  return blobReadWrite(pBlob, (void *)z, n, iOffset, sqlite3PutData); // [koreauniv] 수정필요
}

/*
** Query a blob handle for the size of the data.
**
** The Incrblob.nByte field is fixed for the lifetime of the Incrblob
** so no mutex is required for access.
*/
int sqlite4_blob_bytes(sqlite4_blob *pBlob){
  Incrblob *p = (Incrblob *)pBlob;
  return (p && p->pStmt) ? p->nByte : 0;
}

/*
** Move an existing blob handle to point to a different row of the same
** database table.
**
** If an error occurs, or if the specified row does not exist or does not
** contain a blob or text value, then an error code is returned and the
** database handle error code and message set. If this happens, then all 
** subsequent calls to sqlite4_blob_xxx() functions (except blob_close()) 
** immediately return SQLITE_ABORT.
*/
int sqlite4_blob_reopen(sqlite4_blob *pBlob, sqlite4_int64 iRow){
  int rc;
  Incrblob *p = (Incrblob *)pBlob;
  sqlite4 *db;

  if( p==0 ) return SQLITE4_MISUSE_BKPT;
  db = p->db;
  sqlite4_mutex_enter(db->mutex);
  if( p->pStmt==0 ){
    /* If there is no statement handle, then the blob-handle has
    ** already been invalidated. Return SQLITE4_ABORT in this case.
    */
    rc = SQLITE4_ABORT;
  }else{
    char *zErr;
    ((Vdbe*)p->pStmt)->rc = SQLITE4_OK;
    rc = blobSeekToRow(p, iRow, &zErr);
    if( rc!=SQLITE4_OK ){
      // sqlite4ErrorWithMsg(db, rc, (zErr ? "%s" : (char*)0), zErr);
      printf("[blob] reopen failed: %s with rc = %d\n", zErr ? zErr : "(no detail)", rc); // [koreauniv] 위 코드 대신 추가
      sqlite4DbFree(db, zErr);
    }
    assert( rc!=SQLITE4_SCHEMA );
  }

  rc = sqlite4ApiExit(db, rc);
  assert( rc==SQLITE4_OK || p->pStmt==0 );
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

#endif /* #ifndef SQLITE_OMIT_INCRBLOB */
