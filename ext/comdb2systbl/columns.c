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
#include "sql.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"

/* systbl_columns_cursor is a subclass of sqlite3_vtab_cursor which serves
** as the underlying cursor to enumerate the rows in this vtable. The 
** rows in this vtable are of course the list of tables in the database.
** That is, "select name from sqlite_master where type='table'"
*/
typedef struct systbl_columns_cursor systbl_columns_cursor;
struct systbl_columns_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
  sqlite3_int64 iColid;      /* The column we're on */
};

static int systblColumnsConnect(
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
#define STCOL_TABLE     0
#define STCOL_COLUMN    1
#define STCOL_TYPE      2
#define STCOL_SIZE      3
#define STCOL_SQLTYPE   4
#define STCOL_INLINESZ  5
#define STCOL_DEFVAL    6
#define STCOL_DBLOAD    7
#define STCOL_ALLOWNULL 8

  rc = sqlite3_declare_vtab(db,
     "CREATE TABLE comdb2sys_tables(tablename,"
                                   "columnname,"
                                   "type,"
                                   "size,"
                                   "sqltype,"
                                   "varinlinesize,"
                                   "defaultvalue,"
                                   "dbload,"
                                   "isnullable)");
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
static int systblColumnsDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for systbl_columns_cursor objects.
*/
static int systblColumnsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  systbl_columns_cursor *pCur;

  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for systbl_columns_cursor.
*/
static int systblColumnsClose(sqlite3_vtab_cursor *cur){
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Advance to the next table name from thedb.
*/
static int systblColumnsNext(sqlite3_vtab_cursor *cur){
  systbl_columns_cursor *pCur = (systbl_columns_cursor*)cur;

  if( ++pCur->iColid == thedb->dbs[pCur->iRowid]->schema->nmembers ){
    pCur->iColid = 0;
    pCur->iRowid++;
  }
  return SQLITE_OK;
}

/*
** Return the table name for the current row.
*/
static int systblColumnsColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  systbl_columns_cursor *pCur = (systbl_columns_cursor*)cur;
  struct db *pDb = thedb->dbs[pCur->iRowid];
  struct field *pField = &pDb->schema->member[pCur->iColid];

  switch( i ){
    case STCOL_TABLE: {
      sqlite3_result_text(ctx, pDb->dbname, -1, NULL);
      break;
    }
    case STCOL_COLUMN: {
      sqlite3_result_text(ctx, pField->name, -1, NULL);
      break;
    }
    case STCOL_TYPE: {
      sqlite3_result_text(ctx, (char*) strtype(pField->type), -1, NULL);
      break;
    }
    case STCOL_SIZE: {
      sqlite3_result_int64(ctx, (sqlite3_int64)pField->len);
      break;
    }
    case STCOL_SQLTYPE: {
      char buf[128]; /* Space for sqltype field */

      sqlite3_result_text(ctx, sqltype(pField, buf, sizeof(buf)), -1, NULL);
      break;
    }
    case STCOL_INLINESZ: {
      if( pField->type == SERVER_BLOB2
       || pField->type == SERVER_VUTF8 ){
        sqlite3_result_int64(ctx, (sqlite3_int64)pField->len -5);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    }
    case STCOL_DEFVAL: {
      if( pField->in_default ){
        char *x = sql_field_default_trans(pField, 0);
        sqlite3_result_text(ctx, x, -1, NULL);
        sqlite3_free(x);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    }
    case STCOL_DBLOAD: {
      if( pField->out_default ){
        char *x = sql_field_default_trans(pField, 1);
        sqlite3_result_text(ctx, x, -1, NULL);
        sqlite3_free(x);
      }else{
        sqlite3_result_null(ctx);
      }
      break;
    }
    case STCOL_ALLOWNULL: {
      sqlite3_result_text(ctx, YESNO(!(pField->flags & NO_NULL)), -1, NULL);
    }
  }
  return SQLITE_OK;
};

/*
** Return the rowid for the current row. The rowid is the just the
** index of this column in the current row, multiplied by every columns
** of every row preceeding this one.
*/
static int systblColumnsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  systbl_columns_cursor *pCur = (systbl_columns_cursor*)cur;

  *pRowid = 0;
  for( int i = 0; i < pCur->iRowid - 1; i++ ){
    *pRowid += thedb->dbs[i]->schema->nmembers;
  }
  *pRowid += pCur->iColid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last row of output.
*/
static int systblColumnsEof(sqlite3_vtab_cursor *cur){
  systbl_columns_cursor *pCur = (systbl_columns_cursor*)cur;

  return pCur->iRowid >= thedb->num_dbs;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int systblColumnsFilter(
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
static int systblColumnsBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  return SQLITE_OK;
}

const sqlite3_module systblColumnsModule = {
  0,                          /* iVersion */
  0,                          /* xCreate */
  systblColumnsConnect,       /* xConnect */
  systblColumnsBestIndex,     /* xBestIndex */
  systblColumnsDisconnect,    /* xDisconnect */
  0,                          /* xDestroy */
  systblColumnsOpen,          /* xOpen - open a cursor */
  systblColumnsClose,         /* xClose - close a cursor */
  systblColumnsFilter,        /* xFilter - configure scan constraints */
  systblColumnsNext,          /* xNext - advance a cursor */
  systblColumnsEof,           /* xEof - check for end of scan */
  systblColumnsColumn,        /* xColumn - read data */
  systblColumnsRowid,         /* xRowid - read data */
  0,                          /* xUpdate */
  0,                          /* xBegin */
  0,                          /* xSync */
  0,                          /* xCommit */
  0,                          /* xRollback */
  0,                          /* xFindMethod */
  0,                          /* xRename */
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */



