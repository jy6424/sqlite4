/*
** KV-backed "incremental" BLOB I/O for SQLite4 (LSM/KV storage).
**
** This is NOT sqlite3-style payload-in-page incremental I/O.
** It maps (db, table, column, rowid) -> KV key and stores blob as KV value.
**
** Designed for DiskANN usage:
**  - blob_open() may succeed even if the blob does not exist yet (wrFlag=1).
**  - first blob_write() defines the blob size (fixed afterward).
**  - blob_write() uses KVStoreReplace (append-only log, replace semantics).
*/

#include "sqliteInt.h"

#ifndef SQLITE4_OMIT_INCRBLOB

/* Forward decls (these exist in sqlite4) */
extern int sqlite4PutVarint64(unsigned char*, sqlite4_uint64);

/*
** Valid sqlite4_blob* handles point to Incrblob structures.
*/
typedef struct Incrblob Incrblob;
struct Incrblob {
  sqlite4 *db;
  KVStore *pStore;

  /* (optional but recommended for reopen) */
  char *zDb;
  char *zTable;
  char *zColumn;

  /* key identifying this blob in KV */
  KVByteArray *pKey;
  KVSize nKey;

  sqlite4_int64 nByte; /* >=0 fixed, <0 unknown (writable only) */
  int wrFlag;          /* 1 if opened read/write */
};


static int findDbIndex(sqlite4 *db, const char *zDb){
  int i;
  if( zDb==0 || zDb[0]==0 ) zDb = "main";
  for(i=0; i<db->nDb; i++){
    if( db->aDb[i].zName && sqlite4_stricmp(db->aDb[i].zName, zDb)==0 ){
      return i;
    }
  }
  return -1;
}

/*
** Build KV key for a blob:
**   "blob\0" + zTable + "\0" + zColumn + "\0" + varint64(rowid)
**
** Notes:
**  - Using \0 separators to avoid ambiguity without escaping.
**  - Rowid encoded as varint64 (sqlite4 format helper).
*/
static int buildBlobKey(
  sqlite4 *db,
  const char *zTable,
  const char *zColumn,
  sqlite4_int64 iRow,
  KVByteArray **ppKey,
  KVSize *pnKey
){
  int nTable, nCol;
  int nVar;
  KVSize nKey;
  KVByteArray *pKey;
  unsigned char aVar[16];

  if( zTable==0 || zTable[0]==0 || zColumn==0 || zColumn[0]==0 ){
    return SQLITE4_MISUSE_BKPT;
  }

  nTable = (int)sqlite4Strlen30(zTable);
  nCol   = (int)sqlite4Strlen30(zColumn);
  nVar   = sqlite4PutVarint64(aVar, (sqlite4_uint64)iRow);

  /* "blob\0" = 5 bytes including trailing \0 */
  nKey = 5 + (KVSize)nTable + 1 + (KVSize)nCol + 1 + (KVSize)nVar;

  pKey = (KVByteArray*)sqlite4DbMallocRaw(db, nKey);
  if( pKey==0 ) return SQLITE4_NOMEM;

  /* write */
  memcpy(pKey, "blob", 4);
  pKey[4] = 0x00;

  memcpy(pKey+5, zTable, nTable);
  pKey[5+nTable] = 0x00;

  memcpy(pKey+5+nTable+1, zColumn, nCol);
  pKey[5+nTable+1+nCol] = 0x00;

  memcpy(pKey+5+nTable+1+nCol+1, aVar, nVar);

  *ppKey = pKey;
  *pnKey = nKey;
  return SQLITE4_OK;
}

/*
** Fetch current blob size from KV (if it exists).
** Returns:
**   SQLITE4_OK with *pnByte set if found.
**   SQLITE4_NOTFOUND if no such key.
*/
static int kvBlobSize(KVStore *pStore, const KVByteArray *pKey, KVSize nKey, sqlite4_int64 *pnByte){
  KVCursor *pCur = 0;
  int rc;
  const KVByteArray *pData = 0;
  KVSize nData = 0;

  rc = sqlite4KVStoreOpenCursor(pStore, &pCur);
  if( rc!=SQLITE4_OK ) return rc;

  rc = sqlite4KVCursorSeek(pCur, pKey, nKey, 0);
  if( rc==SQLITE4_OK ){
    rc = sqlite4KVCursorData(pCur, 0, -1, &pData, &nData);
    if( rc==SQLITE4_OK ){
      *pnByte = (sqlite4_int64)nData;
    }
  }else if( rc==SQLITE4_NOTFOUND ){
    /* pass through */
  }else if( rc==SQLITE4_INEXACT ){
    /* INEXACT shouldn't happen for dir=0, but treat as NOTFOUND */
    rc = SQLITE4_NOTFOUND;
  }

  sqlite4KVCursorClose(pCur);
  return rc;
}

/*
** Open a blob handle.
**
** KV-backed policy:
**  - If blob exists: nByte fixed to existing length.
**  - If blob does NOT exist:
**      - wrFlag=0 -> SQLITE4_ERROR (no blob to read)
**      - wrFlag=1 -> allow open; nByte = -1 (unknown until first write)
*/
int sqlite4_blob_open(
  sqlite4* db,
  const char *zDb,
  const char *zTable,
  const char *zColumn,
  sqlite4_int64 iRow,
  int wrFlag,
  sqlite4_blob **ppBlob
){
  int rc = SQLITE4_OK;
  Incrblob *pBlob = 0;
  int iDb;
  KVStore *pStore = 0;
  KVByteArray *pKey = 0;
  KVSize nKey = 0;
  sqlite4_int64 nByte = 0;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( ppBlob==0 ) return SQLITE4_MISUSE_BKPT;
#endif
  *ppBlob = 0;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlite4SafetyCheckOk(db) || zTable==0 || zColumn==0 ){
    return SQLITE4_MISUSE_BKPT;
  }
#endif

  wrFlag = !!wrFlag;
  if( zDb==0 || zDb[0]==0 ) zDb = "main";

  sqlite4_mutex_enter(db->mutex);

  iDb = findDbIndex(db, zDb);
  if( iDb<0 ){
    rc = SQLITE4_ERROR;
    sqlite4Error(db, rc, "unknown database: %s", zDb);
    goto out;
  }
  pStore = db->aDb[iDb].pKV;
  if( pStore==0 ){
    rc = SQLITE4_ERROR;
    sqlite4Error(db, rc, "no KV store for db: %s", db->aDb[iDb].zName);
    goto out;
  }

  rc = buildBlobKey(db, zTable, zColumn, iRow, &pKey, &nKey);
  if( rc!=SQLITE4_OK ) goto out;

  rc = kvBlobSize(pStore, pKey, nKey, &nByte);
  if( rc==SQLITE4_OK ){
    /* exists -> nByte fixed */
  }else if( rc==SQLITE4_NOTFOUND ){
    if( wrFlag ){
      nByte = -1;      /* create-on-first-write */
      rc = SQLITE4_OK;
    }else{
      rc = SQLITE4_ERROR;
      sqlite4Error(db, rc, "no such blob (%s.%s.%s row=%lld)",
                   zDb, zTable, zColumn, (long long)iRow);
      goto out;
    }
  }else{
    sqlite4Error(db, rc, NULL);
    goto out;
  }

  pBlob = (Incrblob*)sqlite4DbMallocZero(db, sizeof(Incrblob));
  if( pBlob==0 ){
    rc = SQLITE4_NOMEM;
    goto out;
  }

  pBlob->db = db;
  pBlob->pStore = pStore;
  pBlob->pKey = pKey;
  pBlob->nKey = nKey;
  pBlob->nByte = nByte;
  pBlob->wrFlag = wrFlag;

  /* store identifiers for reopen */
  pBlob->zDb = sqlite4DbStrDup(db, zDb);
  pBlob->zTable = sqlite4DbStrDup(db, zTable);
  pBlob->zColumn = sqlite4DbStrDup(db, zColumn);
  if( pBlob->zDb==0 || pBlob->zTable==0 || pBlob->zColumn==0 ){
    rc = SQLITE4_NOMEM;
    goto out;
  }

  *ppBlob = (sqlite4_blob*)pBlob;
  pKey = 0; /* owned by pBlob now */

out:
  if( rc!=SQLITE4_OK ){
    if( pKey ) sqlite4DbFree(db, pKey);
    if( pBlob ){
      if( pBlob->pKey ) sqlite4DbFree(db, pBlob->pKey);
      if( pBlob->zDb ) sqlite4DbFree(db, pBlob->zDb);
      if( pBlob->zTable ) sqlite4DbFree(db, pBlob->zTable);
      if( pBlob->zColumn ) sqlite4DbFree(db, pBlob->zColumn);
      sqlite4DbFree(db, pBlob);
    }
  }

  rc = sqlite4ApiExit(db, rc);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

int sqlite4_blob_close(sqlite4_blob *pBlob){
  Incrblob *p = (Incrblob*)pBlob;
  sqlite4 *db;
  if( p==0 ) return SQLITE4_OK;

  db = p->db;
  sqlite4_mutex_enter(db->mutex);

  if( p->pKey ) sqlite4DbFree(db, p->pKey);
  if( p->zDb ) sqlite4DbFree(db, p->zDb);
  if( p->zTable ) sqlite4DbFree(db, p->zTable);
  if( p->zColumn ) sqlite4DbFree(db, p->zColumn);

  sqlite4DbFree(db, p);

  sqlite4_mutex_leave(db->mutex);
  return SQLITE4_OK;
}


/*
** Read blob bytes.
**
** If size unknown (-1), try to discover size now (if exists), else 0.
*/
int sqlite4_blob_bytes(sqlite4_blob *pBlob){
  Incrblob *p = (Incrblob*)pBlob;
  sqlite4_int64 nByte = 0;
  int rc;

  if( p==0 ) return 0;
  if( p->nByte>=0 ) return (int)p->nByte;

  /* size unknown: probe KV */
  sqlite4_mutex_enter(p->db->mutex);
  rc = kvBlobSize(p->pStore, p->pKey, p->nKey, &nByte);
  if( rc==SQLITE4_OK ){
    p->nByte = nByte; /* fix it */
  }else{
    nByte = 0;
  }
  sqlite4_mutex_leave(p->db->mutex);

  return (int)nByte;
}

/*
** Read data from a blob handle.
*/
int sqlite4_blob_read(sqlite4_blob *pBlob, void *z, int n, int iOffset){
  Incrblob *p = (Incrblob*)pBlob;
  int rc;
  const KVByteArray *pData = 0;
  KVSize nData = 0;
  KVCursor *pCur = 0;

  if( p==0 ) return SQLITE4_MISUSE_BKPT;
  if( n<0 || iOffset<0 ) return SQLITE4_ERROR;

  sqlite4_mutex_enter(p->db->mutex);

  /* Ensure we know size (or at least validate range against actual) */
  rc = sqlite4KVStoreOpenCursor(p->pStore, &pCur);
  if( rc!=SQLITE4_OK ) goto out;

  rc = sqlite4KVCursorSeek(pCur, p->pKey, p->nKey, 0);
  if( rc!=SQLITE4_OK ){
    if( rc==SQLITE4_NOTFOUND ) rc = SQLITE4_ERROR;
    goto out;
  }

  rc = sqlite4KVCursorData(pCur, 0, -1, &pData, &nData);
  if( rc!=SQLITE4_OK ) goto out;

  /* range check */
  if( (sqlite4_int64)iOffset + (sqlite4_int64)n > (sqlite4_int64)nData ){
    rc = SQLITE4_ERROR;
    goto out;
  }

  memcpy(z, pData + iOffset, (size_t)n);

  /* fix size if unknown */
  if( p->nByte<0 ) p->nByte = (sqlite4_int64)nData;

  rc = SQLITE4_OK;

out:
  if( pCur ) sqlite4KVCursorClose(pCur);
  sqlite4Error(p->db, rc, NULL);
  rc = sqlite4ApiExit(p->db, rc);
  sqlite4_mutex_leave(p->db->mutex);
  return rc;
}

/*
** Write data to a blob handle.
**
** KV-backed policy:
**  - If nByte fixed: enforce range and do replace of whole value (DiskANN friendly).
**  - If nByte unknown (-1): allow ONLY a "full define" write at offset 0,
**    and fix size to n (or iOffset+n if you want to allow non-zero first offset).
**
** To keep it simple and DiskANN-correct:
**   - require iOffset==0
**   - require either (fixed && n==nByte) or (unknown -> fix nByte=n)
*/
int sqlite4_blob_write(sqlite4_blob *pBlob, const void *z, int n, int iOffset){
  Incrblob *p = (Incrblob*)pBlob;
  int rc;

  if( p==0 ) return SQLITE4_MISUSE_BKPT;
  if( p->wrFlag==0 ) return SQLITE4_READONLY;
  if( n<0 || iOffset<0 ) return SQLITE4_ERROR;

  sqlite4_mutex_enter(p->db->mutex);

  if( p->nByte>=0 ){
    /* fixed size: DiskANN expects full overwrite */
    if( iOffset!=0 || (sqlite4_int64)n != p->nByte ){
      rc = SQLITE4_ERROR;
      goto out;
    }
  }else{
    /* unknown size: only allow define-at-once */
    if( iOffset!=0 ){
      rc = SQLITE4_ERROR;
      goto out;
    }
    p->nByte = (sqlite4_int64)n;
  }

  rc = sqlite4KVStoreReplace(
    p->pStore,
    p->pKey, p->nKey,
    (const KVByteArray*)z, (KVSize)n
  );

out:
  sqlite4Error(p->db, rc, NULL);
  rc = sqlite4ApiExit(p->db, rc);
  sqlite4_mutex_leave(p->db->mutex);
  return rc;
}

/*
** Move an existing blob handle to point to a different row of the same
** db/table/column.
*/
int sqlite4_blob_reopen(sqlite4_blob *pBlob, sqlite4_int64 iRow){
  Incrblob *p = (Incrblob*)pBlob;
  int rc;
  KVByteArray *pKeyNew = 0;
  KVSize nKeyNew = 0;
  sqlite4_int64 nByte = 0;

  if( p==0 ) return SQLITE4_MISUSE_BKPT;

  sqlite4_mutex_enter(p->db->mutex);

  if( p->zTable==0 || p->zColumn==0 ){
    rc = SQLITE4_MISUSE_BKPT;
    goto out;
  }

  rc = buildBlobKey(p->db, p->zTable, p->zColumn, iRow, &pKeyNew, &nKeyNew);
  if( rc!=SQLITE4_OK ) goto out;

  rc = kvBlobSize(p->pStore, pKeyNew, nKeyNew, &nByte);
  if( rc==SQLITE4_OK ){
    p->nByte = nByte;
  }else if( rc==SQLITE4_NOTFOUND ){
    if( p->wrFlag ){
      p->nByte = -1;     /* can create on first write */
      rc = SQLITE4_OK;
    }else{
      rc = SQLITE4_ERROR;
      sqlite4Error(p->db, rc, "no such blob for reopen (%s.%s.%s row=%lld)",
                   (p->zDb?p->zDb:"main"), p->zTable, p->zColumn, (long long)iRow);
      goto out;
    }
  }else{
    sqlite4Error(p->db, rc, NULL);
    goto out;
  }

  /* swap key */
  if( p->pKey ) sqlite4DbFree(p->db, p->pKey);
  p->pKey = pKeyNew;
  p->nKey = nKeyNew;
  pKeyNew = 0;

out:
  if( pKeyNew ) sqlite4DbFree(p->db, pKeyNew);

  sqlite4Error(p->db, rc, NULL);
  rc = sqlite4ApiExit(p->db, rc);
  sqlite4_mutex_leave(p->db->mutex);
  return rc;
}

#endif /* SQLITE4_OMIT_INCRBLOB */
