#include <stdio.h>
#include "sqliteInt.h"
#include <vdbeInt.h>
#include "comdb2build.h"
#include "comdb2vdbe.h"
#include <stdlib.h>
#include <string.h>
#include <schemachange.h>
#include <sc_lua.h>
#include <comdb2.h>
#include <bdb_api.h>
#include <osqlsqlthr.h>
#include <sqloffload.h>
#include <analyze.h>
#include <bdb_access.h>
#include <bpfunc.h>
#include <bpfunc.pb-c.h>
#include <osqlcomm.h>
#include <net_types.h>
#include <views.h>

#define INCLUDE_KEYWORDHASH_H 
#define INCLUDE_FINALKEYWORD_H
#include <keywordhash.h>
extern pthread_key_t query_info_key;
/******************* Utility ****************************/

static inline int setError(Parse *pParse, int rc, char *msg)
{
    pParse->rc = rc;
    sqlite3ErrorMsg(pParse, "%s", msg);
    return rc;
}

// return 0 on failure to parse
int readIntFromToken(Token* t, int *rst)
{
    char nptr[t->n + 1];
    memcpy(nptr, t->z, t->n);
    nptr[t->n] = '\0';
    char *endptr;

    errno = 0;
    long int l = strtol(nptr, &endptr, 10);
    int i = l; // to detect int overflow

    if (errno || *endptr != '\0' || i != l)
        return 0;

    *rst = i;
    return 1;
}

static inline int isRemote(Token *t, Token *t2)
{
    return t->n > 0 && (t2 != NULL && t2->n > 0) ;
}

static inline int chkAndCopyTable(Vdbe* v, Parse* pParse, char *dst,
    const char* name, size_t max_length, int mustexist)
{
    strncpy(dst, name, max_length);
    /* Guarantee null termination. */
    dst[max_length - 1] = '\0';

    struct db *db = getdbbyname(dst);
    int rc;

    if (db == NULL && mustexist)
    {
        setError(pParse, SQLITE_ERROR, "Table not found");
        return SQLITE_ERROR;
    }

    if (db != NULL && !mustexist)
    {
        rc = setError(pParse, SQLITE_ERROR, "Table already exists");
        return SQLITE_ERROR;
    }
    return SQLITE_OK;
}

static inline int create_string_from_token(Vdbe* v, Parse* pParse, char** dst, Token* t)
{
    *dst = (char*) malloc (t->n + 1);
    
    if (*dst == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return SQLITE_NOMEM;
    }
    
    strncpy(*dst, t->z, t->n);
    (*dst)[t->n] = '\0';

    return SQLITE_OK;
}

static inline int copyNosqlToken(Vdbe* v, Parse *pParse, char** buf,
    Token *t)
{
    if (*buf == NULL)    
        *buf = (char*) malloc((t->n));

    if (*buf == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return SQLITE_NOMEM;
    }

    if (t->n < 3)
    {
        **buf = '\0';
    } else
    {
        strncpy(*buf, t->z + 1,t->n - 2);
        (*buf)[t->n - 2] = '\0';
    }
    
    return SQLITE_OK;
}

static inline int chkAndCopyTableTokens(Vdbe *v, Parse *pParse, char *dst,
   Token *t1, Token *t2, int mustexist)
{
 
    int rc;
    int max_size;
    
    if (t1->n + 1 <= MAXTABLELEN)
        max_size = t1->n + 1;
    else
        return setError(pParse, SQLITE_MISUSE, "Tablename is too long");

    if (isRemote(t1, t2))
       return setError(pParse, SQLITE_MISUSE, "DDL commands operate only on local dbs");

    if ((rc = chkAndCopyTable(v, pParse, dst, t1->z, max_size, mustexist)))
        return rc;

    return SQLITE_OK;
}

void fillTableOption(struct schema_change_type* sc, int opt)
{
    if (OPT_ON(opt, ODH_OFF))
        sc->headers = 0;
    else 
        sc->headers = 1;

    if (OPT_ON(opt, IPU_OFF))
        sc->ip_updates = 0;
    else
        sc->ip_updates = 1;

    if (OPT_ON(opt, ISC_OFF))
        sc->instant_sc = 0;
    else
        sc->instant_sc = 1;

    if (OPT_ON(opt, BLOB_RLE))
        sc->compress_blobs = BDB_COMPRESS_RLE8;
    if (OPT_ON(opt, BLOB_CRLE))
        sc->compress_blobs = BDB_COMPRESS_NONE; // FIX this it should not exist
    if (OPT_ON(opt, BLOB_ZLIB))
        sc->compress_blobs = BDB_COMPRESS_ZLIB;
    if (OPT_ON(opt, BLOB_LZ4))
        sc->compress_blobs = BDB_COMPRESS_LZ4;

    if (sc->compress_blobs != BDB_COMPRESS_RLE8 &&
        sc->compress_blobs != BDB_COMPRESS_ZLIB && 
        sc->compress_blobs != BDB_COMPRESS_LZ4)
                sc->compress_blobs = BDB_COMPRESS_NONE;


    if (OPT_ON(opt, REC_RLE))
        sc->compress = BDB_COMPRESS_RLE8;
    if (OPT_ON(opt, REC_CRLE))
        sc->compress = BDB_COMPRESS_CRLE;
    if (OPT_ON(opt, REC_ZLIB))
        sc->compress = BDB_COMPRESS_ZLIB;
    if (OPT_ON(opt, REC_LZ4))
        sc->compress = BDB_COMPRESS_LZ4;

    if (sc->compress != BDB_COMPRESS_RLE8 &&
        sc->compress != BDB_COMPRESS_ZLIB && 
        sc->compress != BDB_COMPRESS_LZ4)
                sc->compress_blobs = BDB_COMPRESS_NONE;

    if (OPT_ON(opt, FORCE_REBUILD))
        sc->force_rebuild = 1;
    else
        sc->force_rebuild = 0;
}

int comdb2AuthenticateUserOp(Vdbe* v, Parse* pParse)
{
     struct sql_thread *thd = pthread_getspecific(query_info_key);
     bdb_state_type *bdb_state = thedb->bdb_env;
     int bdberr; 
     char tablename[MAXTABLELEN] = {0};
     int authOn = bdb_authentication_get(bdb_state, NULL, &bdberr); 
    
     if (authOn != 0)
        return SQLITE_OK;

     if (thd->sqlclntstate && thd->sqlclntstate->user)
     {
        if (bdb_tbl_op_access_get(bdb_state, NULL, 0, 
            tablename, thd->sqlclntstate->user, &bdberr))
            setError(pParse, SQLITE_AUTH, "User does not have OP credentials");
        else
            return SQLITE_OK;
     }

     return SQLITE_AUTH;
}

int comdb2SqlSchemaChange(OpFunc *f)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);

    struct schema_change_type *s = (struct schema_change_type*)f->arg;
    
    thd->sqlclntstate->osql.long_request = 1;
    osql_sock_start(thd->sqlclntstate, OSQL_SOCK_REQ ,0);
    osql_schemachange_logic(s, thd);
    int rst = osql_sock_commit(thd->sqlclntstate, OSQL_SOCK_REQ);
    
    int rc = thd->sqlclntstate->osql.xerr.errval;
    thd->sqlclntstate->osql.xerr.errval = 0;
    thd->sqlclntstate->osql.xerr.errstr[0] = '\0';
    
    
    if (rst)
    {
        f->rc = rc;
        f->errorMsg = "FAIL"; // TODO This must be translated to a description
    } else
    {
        f->rc = SQLITE_OK;
        f->errorMsg = "";
    } 
    return SQLITE_OK;
}

void free_rstMsg(struct rstMsg* rec)
{
    if (rec)
    {
        if (!rec->staticMsg && rec->msg)
            free(rec->msg);
        free(rec);
    }
}

/* 
* Send a BPFUNC to the master
* Returns SQLITE_OK if successful.
*
*/
int comdb2SendBpfunc(OpFunc *f)
{
   struct sql_thread    *thd = pthread_getspecific(query_info_key);
   struct sqlclntstate  *clnt = thd->sqlclntstate;
   osqlstate_t          *osql = &clnt->osql;
   char                 *node = osql->host;
   int rc = 0;
   
   BpfuncArg *arg = (BpfuncArg*)f->arg;

   osql_sock_start(clnt, OSQL_SOCK_REQ ,0);

   rc = osql_send_bpfunc(node, osql->rqid, osql->uuid, arg, NET_OSQL_SOCK_RPL,osql->logsb);
   
   rc = osql_sock_commit(clnt, OSQL_SOCK_REQ);

   rc = osql->xerr.errval;
   osql->xerr.errval = 0;
   osql->xerr.errstr[0] = '\0';
    
    
    if (rc)
    {
        f->rc = rc;
        f->errorMsg = "FAIL"; // TODO This must be translated to a description
    } else
    {
        f->rc = SQLITE_OK;
        f->errorMsg = "";
    } 
   return rc;
}




/* ######################### Parser Reductions ############################ */

/**************************** Function prototypes ***************************/

static void comdb2rebuild(Parse *p, Token* nm, Token* lnm, uint8_t opt);

/************************** Function definitions ****************************/

void comdb2StartTable(
  Parse *pParse,   /* Parser context */
  Token *pName1,   /* First part of the name of the table or view */
  Token *pName2,   /* Second part of the name of the table or view */
  int opt,         /* True if this is a TEMP table */
  Token *csc2
)
{
    sqlite3 *db = pParse->db;
    Vdbe *v  = sqlite3GetVdbe(pParse);
    
    if (comdb2AuthenticateUserOp(v, pParse))
        return;

    struct schema_change_type* sc = new_schemachange_type();

    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(v, pParse, sc->table, pName1, pName2, 0))
        return;

    v->readOnly = 0;
    sc->addonly = 1;
    sc->nothrevent = 1;
    sc->live = 1;
    fillTableOption(sc, opt);

    copyNosqlToken(v, pParse, &sc->newcsc2, csc2);
    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree) &free_schema_change_type);

}

void comdb2AlterTable(
  Parse *pParse,   /* Parser context */
  Token *pName1,   /* First part of the name of the table or view */
  Token *pName2,   /* Second part of the name of the table or view */
  int opt,      /* True if this is a TEMP table */
  Token *csc2
)
{
    sqlite3 *db = pParse->db;
    Vdbe *v  = sqlite3GetVdbe(pParse);
    
    if (comdb2AuthenticateUserOp(v, pParse))
        return;

    struct schema_change_type* sc = new_schemachange_type();

    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(v, pParse,sc->table, pName1, pName2, 1))
        return;

    v->readOnly = 0;
    sc->alteronly = 1;
    sc->nothrevent = 1;
    sc->use_plan = 1;
    sc->live = 1;
    sc->scanmode = SCAN_PARALLEL;
    fillTableOption(sc, opt);

    copyNosqlToken(v, pParse, &sc->newcsc2, csc2);
    comdb2prepareNoRows(v, pParse, 0,  sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);

}


void comdb2DropTable(Parse *pParse, SrcList *pName)
{

    sqlite3 *db = pParse->db;
    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (comdb2AuthenticateUserOp(v, pParse))
        return;
    
    struct schema_change_type* sc = new_schemachange_type();

    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        free_schema_change_type(sc);
        return;
    }

    if (chkAndCopyTable(v, pParse, sc->table, pName->a[0].zName, MAXTABLELEN, 1))
        return;

    v->readOnly = 0;
    sc->same_schema = 1;
    sc->drop_table = 1;
    sc->fastinit = 1;
    sc->nothrevent = 1;
    
    if(get_csc2_file(sc->table, -1 , &sc->newcsc2, NULL ))
    {
        fprintf(stderr, "%s: table schema not found: \n",  sc->table);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        free_schema_change_type(sc); 
        return;
    }

    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);
}

static inline void comdb2rebuild(Parse *pParse, Token* nm, Token* lnm, uint8_t opt)
{
    sqlite3 *db = pParse->db;
    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (comdb2AuthenticateUserOp(v, pParse))
        return;     
    
    struct schema_change_type* sc = new_schemachange_type();

    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(v, pParse,sc->table, nm, lnm, 1))
        return;

    v->readOnly = 0;
    sc->nothrevent = 1;
    
    if (OPT_ON(opt, REBUILD_ALL))
        sc->force_rebuild = 1;

    if (OPT_ON(opt, REBUILD_DATA))
        sc->force_dta_rebuild = 1;
    
    if (OPT_ON(opt, REBUILD_BLOB))
        sc->force_blob_rebuild = 1;

    if(get_csc2_file(sc->table, -1 , &sc->newcsc2, NULL ))
    {
        fprintf(stderr, "%s: table schema not found: \n",  sc->table);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        free_schema_change_type(sc); 
        return;
    }
    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);

}


void comdb2rebuildFull(Parse* p, Token* nm,Token* lnm)
{
    comdb2rebuild(p, nm,lnm, REBUILD_ALL + REBUILD_DATA + REBUILD_BLOB); 
}


void comdb2rebuildData(Parse* p, Token* nm, Token* lnm)
{
    comdb2rebuild(p,nm,lnm,REBUILD_DATA);
}

void comdb2rebuildDataBlob(Parse* p,Token* nm, Token* lnm)
{
    comdb2rebuild(p, nm, lnm, REBUILD_BLOB);
}

void comdb2truncate(Parse* pParse, Token* nm, Token* lnm)
{
    sqlite3 *db = pParse->db;
    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (comdb2AuthenticateUserOp(v, pParse))
        return;     

    struct schema_change_type* sc = new_schemachange_type();

    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(v, pParse,sc->table, nm, lnm, 1))
        return;

    v->readOnly = 0;
    sc->fastinit = 1;
    sc->nothrevent = 1;

    if(get_csc2_file(sc->table, -1 , &sc->newcsc2, NULL ))
    {
        fprintf(stderr, "%s: table schema not found: \n",  sc->table);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        free_schema_change_type(sc); 
        return;
    }
    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);
}


void comdb2rebuildIndex(Parse* pParse, Token* nm, Token* lnm, Token* index)
{
    sqlite3 *db = pParse->db;
    Vdbe *v  = sqlite3GetVdbe(pParse);
    char* indexname;
    int index_num;

    if (comdb2AuthenticateUserOp(v, pParse))
        return;     
    
    struct schema_change_type* sc = new_schemachange_type();

    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(v,pParse,sc->table, nm, lnm, 1))
        return;


    if(get_csc2_file(sc->table, -1 , &sc->newcsc2, NULL ))
    {
        fprintf(stderr, "%s: table schema not found: \n",  sc->table);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        free_schema_change_type(sc); 
        return;
    }

    if (create_string_from_token(v, pParse, &indexname, index))
        return; // TODO RETURN ERROR

    int rc = getidxnumbyname(sc->table, indexname, &index_num );
    if(rc) {
        fprintf(stderr, "!table:index '%s:%s' not found\n", sc->table, indexname);
        free_schema_change_type(sc);
        setError(pParse, SQLITE_ERROR, "Index not found");
        return;
    }
    
    free(indexname);

    v->readOnly = 0;
    sc->nothrevent = 1;
    sc->rebuild_index = 1;
    sc->index_to_rebuild = index_num;
    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);

}

/********************** STORED PROCEDURES ****************************************/


void comdb2createproc(Parse* pParse, Token* nm, Token* proc)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    int rc;   
    struct schema_change_type* sc = new_schemachange_type();
    int max_length = nm->n < MAXTABLELEN ? nm->n : MAXTABLELEN;
    
    if (comdb2AuthenticateUserOp(v, pParse))
        return;     

    strncpy(sc->table, nm->z, max_length);
    
    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }
    sc->newcsc2 = malloc(proc->n);
    
    v->readOnly = 0;
    sc->addsp = 1;
    copyNosqlToken(v, pParse, &sc->newcsc2, proc);

    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);

  
}

void comdb2defaultProcedure(Parse* pParse, Token* nm, Token* ver)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    int rc;   
    struct schema_change_type* sc = new_schemachange_type();
    int max_length = nm->n < MAXTABLELEN ? nm->n : MAXTABLELEN;
    
    if (comdb2AuthenticateUserOp(v, pParse))
        return;     

    strncpy(sc->table, nm->z, max_length);
    
    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }
    sc->newcsc2 = malloc(ver->n + 1);
    strncpy(sc->newcsc2, ver->z, ver->n); 
    v->readOnly = 0;
    sc->defaultsp = 1;

    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);
}

void comdb2dropproc(Parse* pParse, Token* nm, Token* ver)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    int rc;   
    struct schema_change_type* sc = new_schemachange_type();
    int max_length = nm->n < MAXTABLELEN ? nm->n : MAXTABLELEN;

    if (comdb2AuthenticateUserOp(v, pParse))
        return;       

    strncpy(sc->table, nm->z, max_length);
    
    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }
    sc->newcsc2 = malloc(ver->n + 1);
    strncpy(sc->newcsc2, ver->z, ver->n); 
    v->readOnly = 0;
    sc->delsp = 1;
  
    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);    
}
/********************* PARTITIONS  **********************************************/


void comdb2CreateTimePartition(Parse* pParse, Token* table, Token* partition_name, Token* period, Token* retention, Token* start)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

    int max_length;

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
    if (arg)
        bpfunc_arg__init(arg);
    else
        goto err; 

    BpfuncCreateTimepart *tp = malloc(sizeof(BpfuncCreateTimepart));
    
    if (tp)
        bpfunc_create_timepart__init(tp);
    else
        goto err;
    
    arg->crt_tp = tp;
    arg->type = BPFUNC_CREATE_TIMEPART;
    tp->tablename = (char*) malloc(MAXTABLELEN);
    memset(tp->tablename, '\0', MAXTABLELEN);
    if (table && chkAndCopyTableTokens(v, pParse, tp->tablename, table, NULL, 1)) 
        goto err;


    max_length = partition_name->n < MAXTABLELEN ? partition_name->n : MAXTABLELEN;
    tp->partition_name = (char*) malloc(MAXTABLELEN);
    memset(tp->partition_name, '\0', MAXTABLELEN);
    strncpy(tp->partition_name, partition_name->z, max_length);

    char period_str[50];
    memset(period_str, '\0', sizeof(period_str));

    assert (*period->z == '\'' || *period->z == '\"');
    period->z++;
    period->n -= 2;
   
    
    max_length = period->n < 50 ? period->n : 50;
    strncpy(period_str, period->z, max_length);
    tp->period = name_to_period(period_str);
    
    if (tp->period == VIEW_TIMEPART_INVALID)
    {
        setError(pParse, SQLITE_ERROR, "Invalid period name");
        goto clean_arg;
    }

    char retention_str[10];
    memset(retention_str, '\0', sizeof(retention_str));
    max_length = retention->n < 10 ? retention->n : 10;
    strncpy(retention_str, retention->z, max_length);
    tp->retention = atoi(retention_str);

    char start_str[200];
    memset(start_str,0, sizeof(start_str));
    
    assert (*start->z == '\'' || *start->z == '\"');
    start->z++;
    start->n -= 2;

    max_length = start->n < 200 ? start->n : 200;
    strncpy(start_str, start->z, max_length);
    tp->start = convert_time_string_to_epoch(start_str);

    if (tp->start == -1 )
    {
        setError(pParse, SQLITE_ERROR, "Invalid start date");
        goto clean_arg;
    }

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, (vdbeFuncArgFree) &free_bpfunc_arg);
    return;

err:
        setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    if (arg)
        free_bpfunc_arg(arg);   
    
}


void comdb2DropTimePartition(Parse* pParse, Token* partition_name)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    int max_length;

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
    if (arg)
        bpfunc_arg__init(arg);
    else
        goto err; 
    
    BpfuncDropTimepart *tp = malloc(sizeof(BpfuncDropTimepart));
    

    if (tp)
        bpfunc_drop_timepart__init(tp);
    else
        goto err;
    
    arg->drop_tp = tp;
    arg->type = BPFUNC_DROP_TIMEPART;
    max_length = partition_name->n < MAXTABLELEN ? partition_name->n : MAXTABLELEN;
    tp->partition_name = (char*) malloc(MAXTABLELEN);
    memset(tp->partition_name, '\0', MAXTABLELEN);
    strncpy(tp->partition_name, partition_name->z, max_length);

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, (vdbeFuncArgFree) &free_bpfunc_arg);

    return;
err:
        setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    free_bpfunc_arg(arg);

}


/********************* BULK IMPORT ***********************************************/

void comdb2bulkimport(Parse* pParse, Token* nm,Token* lnm, Token* nm2, Token* lnm2)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

    setError(pParse, SQLITE_INTERNAL, "Not Implemented");
    fprintf(stderr, "Bulk import from %.*s to", nm->n + lnm->n, nm->z, nm2->n +lnm2->n, nm2->z);
}

/********************* ANALYZE ***************************************************/

int comdb2vdbeAnalyze(OpFunc *f)
{
    char *tablename = f->arg;
    int percentage = f->int_arg;
    int rc;

    if (tablename == NULL)
        rc = analyze_database(NULL, percentage, 1);
    else
        rc = analyze_table(tablename, NULL, percentage,1);

    if (rc)
    {
        f->rc = rc;
        f->errorMsg = "Analyze could not run because of internal problems";
    } else
    {
        f->rc = SQLITE_OK;
        f->errorMsg = "";
    } 

    return rc;
}


void comdb2analyze(Parse* pParse, int opt, Token* nm, Token* lnm, int pc)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

    int percentage = pc;
    int threads = GET_ANALYZE_THREAD(opt);
    int sum_threads = GET_ANALYZE_SUMTHREAD(opt);

  
    if (comdb2AuthenticateUserOp(v, pParse))
        return;       
  

    
    if (threads > 0)
        analyze_set_max_table_threads(threads);
    if (sum_threads)
        analyze_set_max_sampling_threads(sum_threads);

    if (nm == NULL)
    {
        comdb2prepareNoRows(v, pParse, pc, NULL, &comdb2vdbeAnalyze, (vdbeFuncArgFree) &free);
    } else
    {
        char *tablename = (char*) malloc(MAXTABLELEN);
        if (!tablename)
            goto err;

        if (nm && lnm && chkAndCopyTableTokens(v, pParse, tablename, nm, lnm, 1)) 
        {
            free(tablename);
            goto err;
        }
        else
           comdb2prepareNoRows(v, pParse, pc, tablename, &comdb2vdbeAnalyze, (vdbeFuncArgFree) &free); 
    }

    return;

err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");

}

void comdb2analyzeCoverage(Parse* pParse, Token* nm, Token* lnm, int newscale)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);


    if (comdb2AuthenticateUserOp(v, pParse))
        goto err;       

    if (newscale < -1 || newscale > 100)
    {
        setError(pParse, SQLITE_ERROR, "Coverage must be between -1 and 100");
        goto clean_arg;
    }

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
    if (arg)
        bpfunc_arg__init(arg);
    else
        goto err;
    BpfuncAnalyzeCoverage *ancov_f = (BpfuncAnalyzeCoverage*) malloc(sizeof(BpfuncAnalyzeCoverage));
    
    if (ancov_f)
        bpfunc_analyze_coverage__init(ancov_f);
    else
        goto err;

    arg->an_cov = ancov_f;
    arg->type = BPFUNC_ANALYZE_COVERAGE;
    ancov_f->tablename = (char*) malloc(MAXTABLELEN);
    
    if (!ancov_f->tablename)
        goto err;
        
    if (chkAndCopyTableTokens(v, pParse, ancov_f->tablename, nm, lnm, 1)) 
        return;  
    
    ancov_f->newvalue = newscale;

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, (vdbeFuncArgFree) &free_bpfunc_arg);

    return;
err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    if (arg)
        free_bpfunc_arg(arg);

}

void comdb2analyzeThreshold(Parse* pParse, Token* nm, Token* lnm, int newthreshold)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (comdb2AuthenticateUserOp(v, pParse))
        goto clean_arg;       

    if (newthreshold < -1 || newthreshold > 100)
    {
        setError(pParse, SQLITE_ERROR, "Threshold must be between -1 and 100");
        goto clean_arg;
    }
    
    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
    if (arg)
        bpfunc_arg__init(arg);
    else
        goto err;


    BpfuncAnalyzeThreshold *anthr_f = (BpfuncAnalyzeThreshold*) malloc(sizeof(BpfuncAnalyzeThreshold));
    
    if (anthr_f)
        bpfunc_analyze_threshold__init(anthr_f);
    else
        goto err;

    arg->an_thr = anthr_f;
    arg->type = BPFUNC_ANALYZE_THRESHOLD;
    anthr_f->tablename = (char*) malloc(MAXTABLELEN);

    if (!anthr_f->tablename)
        goto err;
        
    if (chkAndCopyTableTokens(v, pParse, anthr_f->tablename, nm, lnm, 1)) 
        return;  
    
    anthr_f->newvalue = newthreshold;
    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, (vdbeFuncArgFree) &free_bpfunc_arg);

    return;
err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    if (arg)
        free_bpfunc_arg(arg);
}

/********************* ALIAS **************************************************/

void comdb2setAlias(Parse* pParse, Token* name, Token* url)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (comdb2AuthenticateUserOp(v, pParse))
        return;       

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
    if (arg)
    {
        bpfunc_arg__init(arg);
    } else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        return;
    }


    BpfuncAlias *alias_f = (BpfuncAlias*) malloc(sizeof(BpfuncAlias));
    
    if (alias_f)
    {
        bpfunc_alias__init(alias_f);
    } else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }

    arg->alias = alias_f;
    arg->type = BPFUNC_ALIAS;
    alias_f->name = (char*) malloc(MAXTABLELEN);

    if (name && chkAndCopyTableTokens(v,pParse, alias_f->name, name, NULL, 0)) 
        goto clean_arg;

    assert (*url->z == '\'' || *url->z == '\"');
    url->z++;
    url->n -= 2;

    if (create_string_from_token(v, pParse, &alias_f->remote, url))
        goto clean_arg;

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, (vdbeFuncArgFree) &free_bpfunc_arg);

    return;

clean_arg:
    free_bpfunc_arg(arg);
}

void comdb2getAlias(Parse* pParse, Token* t1)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (comdb2AuthenticateUserOp(v, pParse))
        return;       

    setError(pParse, SQLITE_INTERNAL, "Not Implemented");
    fprintf(stderr, "Getting alias %.*s", t1->n, t1->z); 
}

/********************* GRANT AUTHORIZAZIONS ************************************/

void comdb2grant(Parse* pParse, int revoke, int permission, Token* nm,Token* lnm, Token* u)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (comdb2AuthenticateUserOp(v, pParse))
        return;  

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
    if (arg)
    {
        bpfunc_arg__init(arg);
    } else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        return;
    }


    BpfuncGrant *grant = (BpfuncGrant*) malloc(sizeof(BpfuncGrant));
    
    if (grant)
    {
        bpfunc_grant__init(grant);
    } else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }

    arg->grant = grant;
    arg->type = BPFUNC_GRANT;
    grant->yesno = revoke;
    grant->perm = permission;
    grant->table = (char*) malloc(MAXTABLELEN);
     

    if (nm && lnm && chkAndCopyTableTokens(v,pParse, grant->table, nm, lnm, 1)) 
        goto clean_arg;

    if (create_string_from_token(v, pParse, &grant->username, u))
        goto clean_arg;

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, (vdbeFuncArgFree) &free_bpfunc_arg);

    return;

clean_arg:
    free_bpfunc_arg(arg);

}

/****************************** AUTHENTICATION ON/OFF *******************************/

void comdb2enableAuth(Parse* pParse, int on)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

    int rc = SQLITE_OK;
 
    if (comdb2AuthenticateUserOp(v, pParse))
        return;       

    if(!on)
    {
        setError(pParse, SQLITE_INTERNAL, "SET AUTHENTICATION OFF is not allowed");
        return;
    }

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
    if (arg)
    {
        bpfunc_arg__init(arg);
    }else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        return;
    }   


    BpfuncAuthentication *auth = (BpfuncAuthentication*) malloc(sizeof(BpfuncAuthentication));
    
    if (auth)
    {
        bpfunc_authentication__init(auth);
    } else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }

    arg->auth = auth;
    arg->type = BPFUNC_AUTHENTICATION;
    auth->enabled = on;

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
        (vdbeFuncArgFree) &free_bpfunc_arg);

    return;

clean_arg:
    free_bpfunc_arg(arg);

}

/****************************** PASSWORD *******************************/

void comdb2setPassword(Parse* pParse, Token* pwd, Token* nm)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
    if (arg)
    {
        bpfunc_arg__init(arg);
    }else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        return;
    }  
  
    BpfuncPassword * password = (BpfuncPassword*) malloc(sizeof(BpfuncPassword));
    
    if (password)
    {
        bpfunc_password__init(password);
    } else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }

    arg->pwd = password;
    arg->type = BPFUNC_PASSWORD;
    password->disable = 0;
  
    if (create_string_from_token(v, pParse, &password->user, nm) ||
        create_string_from_token(v, pParse, &password->password, pwd))
            goto clean_arg;
        
    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
        (vdbeFuncArgFree) &free_bpfunc_arg);
    
    return;

clean_arg:
    free_bpfunc_arg(arg);  
}

void comdb2deletePassword(Parse* pParse, Token* nm)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
    if (arg)
    {
        bpfunc_arg__init(arg);
    }else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        return;
    }  
  
    BpfuncPassword * pwd = (BpfuncPassword*) malloc(sizeof(BpfuncPassword));
    
    if (pwd)
    {
        bpfunc_password__init(pwd);
    } else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }

    arg->pwd = pwd;
    arg->type = BPFUNC_PASSWORD;
    pwd->disable = 1;
  
    if (create_string_from_token(v, pParse, &pwd->user, nm))
        goto clean_arg;

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
        (vdbeFuncArgFree) &free_bpfunc_arg);
    
    return;

clean_arg:
    free_bpfunc_arg(arg);  
}

int comdb2genidcontainstime(void)
{
     bdb_state_type *bdb_state = thedb->bdb_env;
     return genid_contains_time(bdb_state);
}

int producekw(OpFunc *f)
{

    for (int i=0; i < SQLITE_N_KEYWORD; i++)
    {
        if ((f->int_arg == KW_ALL) ||
            (f->int_arg == KW_RES && f_keywords[i].reserved) ||
            (f->int_arg == KW_FB && !f_keywords[i].reserved))
                opFuncPrintf(f, "%s", f_keywords[i].name );
    }
    f->rc = SQLITE_OK;
    f->errorMsg = NULL;
    return SQLITE_OK;
}

void comdb2getkw(Parse* pParse, int arg)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    const char* colname[] = {"Keyword"};
    const int coltype = OPFUNC_STRING_TYPE;
    OpFuncSetup stp = {1, colname, &coltype, SQLITE_KEYWORD_LEN};
    comdb2prepareOpFunc(v, pParse, arg, NULL, &producekw, (vdbeFuncArgFree)  &free, &stp);

}

static int produceAnalyzeCoverage(OpFunc *f)
{

    char  *tablename = (char*) f->arg;
    int rst;
    int bdberr; 
    int rc = bdb_get_analyzecoverage_table(NULL, tablename, &rst, &bdberr);
    
    if (!rc)
    {
        opFuncWriteInteger(f, (int) rst );
        f->rc = SQLITE_OK;
        f->errorMsg = NULL;
    } else 
    {
        f->rc = SQLITE_INTERNAL;
        f->errorMsg = "Could not read value";
    }
    return SQLITE_OK;
}

void comdb2getAnalyzeCoverage(Parse* pParse, Token *nm, Token *lnm)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    const char* colname[] = {"Coverage"};
    const int coltype = OPFUNC_INT_TYPE;
    OpFuncSetup stp = {1, colname, &coltype, 256};
    char *tablename = (char*) malloc (MAXTABLELEN);

    if (nm && lnm && chkAndCopyTableTokens(v,pParse, tablename, nm, lnm, 1)) 
        goto clean;
    
    comdb2prepareOpFunc(v, pParse, 0, tablename, &produceAnalyzeCoverage, (vdbeFuncArgFree)  &free, &stp);

    return;

clean:
    free(tablename);
}

static int produceAnalyzeThreshold(OpFunc *f)
{

    char  *tablename = (char*) f->arg;
    long long int rst;
    int bdberr; 
    int rc = bdb_get_analyzethreshold_table(NULL, tablename, &rst, &bdberr);
    
    if (!rc)
    {
        opFuncWriteInteger(f, (int) rst );
        f->rc = SQLITE_OK;
        f->errorMsg = NULL;
    } else 
    {
        f->rc = SQLITE_INTERNAL;
        f->errorMsg = "Could not read value";
    }
    return SQLITE_OK;
}

void comdb2getAnalyzeThreshold(Parse* pParse, Token *nm, Token *lnm)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    const char* colname[] = {"Threshold"};
    const int coltype = OPFUNC_INT_TYPE;
    OpFuncSetup stp = {1, colname, &coltype, 256};
    char *tablename = (char*) malloc (MAXTABLELEN);

    if (nm && lnm && chkAndCopyTableTokens(v,pParse, tablename, nm, lnm, 1)) 
        goto clean;
    
    comdb2prepareOpFunc(v, pParse, 0, tablename, &produceAnalyzeThreshold, (vdbeFuncArgFree)  &free, &stp);

    return;

clean:
    free(tablename);
}

typedef struct 
{
    char spname[MAX_SPNAME];
    int version;
} read_sp_t;

static int produce_sp_versions(OpFunc *f)
{
    int def;
    int ver;
    int bdberr = 0;
    int max = INT_MAX;
    read_sp_t *arg = f->arg;

    def = bdb_get_sp_get_default_version(arg->spname, &bdberr);
    if (def < 1 || bdberr) {
        f->rc = SQLITE_ERROR;
        f->errorMsg = "bdb_get_sp_get_default_version failed";
        return SQLITE_ERROR;
    }

    while (bdb_get_lua_highest(NULL, arg->spname, &ver, max, &bdberr) == 0) {
        if (ver < 1)
            break;
        opFuncWriteInteger(f, ver);
        opFuncWriteInteger(f, ver == def);
        max = ver;
    }
    f->rc = SQLITE_OK;
    f->errorMsg = NULL;
    return SQLITE_OK;
}

static void list_procedure_versions(Parse* p, Token *name)
{
    char sp[name->n + 1];
    memcpy(sp, name->z, name->n);
    sp[name->n] = '\0';

    int bdb_err;
	if (bdb_get_sp_get_default_version(sp, &bdb_err) < 0) {
		sqlite3ErrorMsg(p, "no such procedure: %s", sp);
        return;
    }

    char* colname[] = {"version", "default"};
    int coltype[] = {OPFUNC_INT_TYPE, OPFUNC_INT_TYPE};
    OpFuncSetup stp = {2, colname, coltype, 4096};

    read_sp_t *arg = malloc(sizeof(read_sp_t));
    strcpy(arg->spname, sp);
    arg->version = -1;

    Vdbe *v  = sqlite3GetVdbe(p);
    comdb2prepareOpFunc(v, p, 0, arg, &produce_sp_versions, (vdbeFuncArgFree)  &free, &stp);
}

static int produce_df_sp(OpFunc *f)
{
    read_sp_t *arg = (read_sp_t*) f->arg;
    char  *spname = (char*) arg->spname;
    int version = arg->version;
    char *lua;
    int size;
    int bdberr; 
    int rc = bdb_get_sp_lua_source(NULL, NULL, spname, &lua, version, &size, &bdberr);
     
    if (!rc)
    {
        opFuncPrintf(f, "%s", lua );
        f->rc = SQLITE_OK;
        f->errorMsg = NULL;
    } else 
    {
        f->rc = SQLITE_INTERNAL;
        f->errorMsg = "Could not read code";
    }   

    return SQLITE_OK;
}

void comdb2getProcedure(Parse* pParse, Token *name, int version)
{
    if (version == -1) {
        list_procedure_versions(pParse, name);
        return;
    }

    if (version == 0) {
        setError(pParse, SQLITE_ERROR, "bad sp version");
        return;
    }

    Vdbe *v  = sqlite3GetVdbe(pParse);
    const char* colname[] = {"Code"};
    const int coltype = OPFUNC_STRING_TYPE;
    OpFuncSetup stp = {1, colname, &coltype, 4096};
    read_sp_t *arg = (read_sp_t*) malloc(sizeof(read_sp_t));

    size_t max_length = name->n < MAXTABLELEN ? name->n : MAXTABLELEN;
    memset(arg->spname, '\0', MAXTABLELEN);
    strncpy(arg->spname, name->z, max_length);
    arg->version = version;

    comdb2prepareOpFunc(v, pParse, 0, arg, &produce_df_sp, (vdbeFuncArgFree)  &free, &stp);
}
/* vim: set ts=4 sw=4 et: */
