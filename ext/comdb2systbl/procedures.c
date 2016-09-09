/*
**
** Vtables interface for Schema Tables.
**
** Though this is technically an extension, currently it must be
** built as part of SQLITE_CORE, as comdb2 does not support
** run time extensions at this time.
**
** For a little while we had to use our own "fake" tables, because
** eponymous system tables did not exist. Now that they do, we
** have moved schema tables to their own extension.
**
** We have piggy backed off of SQLITE_BUILDING_FOR_COMDB2 here, though
** a new #define would also suffice.
*/
#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
    && !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
# define SQLITE_CORE 1
#endif

#include <stdlib.h>
#include <string.h>

#include "comdb2.h"
#include "bdb_api.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"

/* systbl_tables_cursor is a subclass of sqlite3_vtab_cursor which serves
** as the underlying cursor to enumerate the rows in this vtable. The 
** rows in this vtable are of course the list of tables in the database.
** That is, "select name from sqlite_master where type='table'"
*/
typedef struct systbl_sps_cursor systbl_sps_cursor;
struct systbl_sps_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
  sqlite3_int64 nRows;       /* Number of rows */
  char row[MAX_SPNAME];    /* Rows - we have to store results somewhere */
};

static int systblSPsConnect(
  sqlite3 *db,
  void *pAux,
  int argc,
  const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pErr
){
  sqlite3_vtab *pNew;
  int rc;

/* Column numbers */
#define STSP_NAME     0
#define STSP_VER      1
#define STSP_D        2

  rc = sqlite3_declare_vtab(db,
     "CREATE TABLE comdb2sys_procedures(name,version,definition)");
  /* Lunch start here allocate rows */
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

/*
** Destructor for sqlite3_vtab objects.
*/
static int systblSPsDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for systbl_sps_cursor objects.
*/
static int systblSPsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  systbl_sps_cursor *pCur;

  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;

  memset(pCur->row, 0, sizeof(pCur->row) );

  pCur->row[0] = 127; /* From dump_spfile() in sc_lua.c */

  return SQLITE_OK;
}

/*
** Destructor for systbl_sps_cursor.
*/
static int systblSPsClose(sqlite3_vtab_cursor *cur){
  systbl_sps_cursor *pCur = (systbl_sps_cursor *) cur;

  sqlite3_free(pCur);

  return SQLITE_OK;
}

/*
** Advance to the next table name from thedb.
*/
static int systblSPsNext(sqlite3_vtab_cursor *cur){
  systbl_sps_cursor *pCur = (systbl_sps_cursor*)cur;

  pCur->iRowid++;
  return SQLITE_OK;
}

/*
** Return the table name for the current row.
*/
static int systblSPsColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  systbl_sps_cursor *pCur = (systbl_sps_cursor*)cur;
  const char *sp_name = pCur->row;
  
  switch( i ){
    case STSP_NAME: {
      sqlite3_result_text(ctx, sp_name, -1, NULL);
      break;
    }
    case STSP_VER: {
      int bdberr;
      int version = bdb_get_sp_get_default_version(sp_name, &bdberr);

      sqlite3_result_int64(ctx, (sqlite3_int64)version);
      break;
    }
    case STSP_D: {
      int bdberr;
      int version = bdb_get_sp_get_default_version(sp_name, &bdberr);
      char *lua_file;
      int size;

      bdb_get_sp_lua_source(NULL, NULL,
       sp_name, &lua_file, version, &size, &bdberr);

      sqlite3_result_text(ctx, lua_file, size, NULL);
      break;
    }
  }
  return SQLITE_OK;
};

/*
** Return the rowid for the current row. The rowid is the just the
** index of this table into the db array.
*/
static int systblSPsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_sps_cursor *pCur = (systbl_sps_cursor*)cur;

  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
**
** We don't have any good test for this, currently bdb_get_sp_name will return
** -1 in this case. This acts as both systblSPsNext() and Eof(), because
** this function erroneously returns TRUE when the pCur->row is on a valid
** row.
*/
static int systblSPsEof(sqlite3_vtab_cursor *cur){
  systbl_sps_cursor *pCur = (systbl_sps_cursor*)cur;
  int bdberr;
  int rc;

  rc = bdb_get_sp_name(NULL, pCur->row, pCur->row, &bdberr);
  rc = ( rc < 0 ) ? 1 : 0;

  return rc;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblSPsFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  return SQLITE_OK;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblSPsBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlite3_module systblSPsModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  systblSPsConnect,          /* xConnect */
  systblSPsBestIndex,        /* xBestIndex */
  systblSPsDisconnect,       /* xDisconnect */
  0,                         /* xDestroy */
  systblSPsOpen,             /* xOpen - open a cursor */
  systblSPsClose,            /* xClose - close a cursor */
  systblSPsFilter,           /* xFilter - configure scan constraints */
  systblSPsNext,             /* xNext - advance a cursor */
  systblSPsEof,              /* xEof - check for end of scan */
  systblSPsColumn,           /* xColumn - read data */
  systblSPsRowid,            /* xRowid - read data */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindMethod */
  0,                         /* xRename */
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
