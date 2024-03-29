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
#include "comdb2systbl.h"
#include "comdb2systblInt.h"

/* systbl_tables_cursor is a subclass of sqlite3_vtab_cursor which serves
** as the underlying cursor to enumerate the rows in this vtable. The 
** rows in this vtable are of course the list of tables in the database.
** That is, "select name from sqlite_master where type='table'"
*/
typedef struct systbl_tables_cursor systbl_tables_cursor;
struct systbl_tables_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
};

static int systblTablesConnect(
  sqlite3 *db,
  void *pAux,
  int argc,
  const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pErr
){
  sqlite3_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db,
     "CREATE TABLE comdb2sys_tables(tablename)");
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
static int systblTablesDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for systbl_tables_cursor objects.
*/
static int systblTablesOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  systbl_tables_cursor *pCur;

  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for systbl_tables_cursor.
*/
static int systblTablesClose(sqlite3_vtab_cursor *cur){
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Advance to the next table name from thedb.
*/
static int systblTablesNext(sqlite3_vtab_cursor *cur){
  systbl_tables_cursor *pCur = (systbl_tables_cursor*)cur;

  pCur->iRowid++;
  return SQLITE_OK;
}

/*
** Return the table name for the current row.
*/
static int systblTablesColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  systbl_tables_cursor *pCur = (systbl_tables_cursor*)cur;
  struct db *pDb = thedb->dbs[pCur->iRowid];
  char *x = pDb->dbname;

  sqlite3_result_text(ctx, x, -1, NULL);
  return SQLITE_OK;
};

/*
** Return the rowid for the current row. The rowid is the just the
** index of this table into the db array.
*/
static int systblTablesRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_tables_cursor *pCur = (systbl_tables_cursor*)cur;

  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblTablesEof(sqlite3_vtab_cursor *cur){
  systbl_tables_cursor *pCur = (systbl_tables_cursor*)cur;

  return pCur->iRowid >= thedb->num_dbs;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblTablesFilter(
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
static int systblTablesBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlite3_module systblTablesModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  systblTablesConnect,       /* xConnect */
  systblTablesBestIndex,     /* xBestIndex */
  systblTablesDisconnect,    /* xDisconnect */
  0,                         /* xDestroy */
  systblTablesOpen,          /* xOpen - open a cursor */
  systblTablesClose,         /* xClose - close a cursor */
  systblTablesFilter,        /* xFilter - configure scan constraints */
  systblTablesNext,          /* xNext - advance a cursor */
  systblTablesEof,           /* xEof - check for end of scan */
  systblTablesColumn,        /* xColumn - read data */
  systblTablesRowid,         /* xRowid - read data */
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


/* This initializes this table but also a bunch of other schema tables
** that fall under the similar use. */
#ifdef SQLITE_BUILDING_FOR_COMDB2
int comdb2SystblInit(
  sqlite3 *db
){
  int rc = SQLITE_OK;
#ifndef SQLITE_OMIT_VIRTUALTABLE
  rc = sqlite3_create_module(db, "comdb2sys_tables", &systblTablesModule, 0);
  rc = sqlite3_create_module(db, "comdb2sys_columns", &systblColumnsModule, 0);
  rc = sqlite3_create_module(db, "comdb2sys_keys", &systblKeysModule, 0);
  rc = sqlite3_create_module(db, "comdb2sys_keyscomponents",
         &systblFieldsModule, 0);
  rc = sqlite3_create_module(db, "comdb2sys_constraints",
         &systblConstraintsModule, 0);
  rc = sqlite3_create_module(db, "comdb2sys_tablesizes",
         &systblTblSizeModule, 0);

#endif
  return rc;
}
#endif /* SQLITE_BUILDING_FOR_COMDB2 */
