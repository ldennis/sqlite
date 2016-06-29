#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */

const sqlite3_module systblTablesModule;
const sqlite3_module systblColumnsModule;
const sqlite3_module systblKeysModule;
const sqlite3_module systblFieldsModule;
const sqlite3_module systblConstraintsModule;
const sqlite3_module systblTblSizeModule;

/* Simple yes/no answer for booleans */
#define YESNO(x) ((x) ? "Y" : "N")

#ifdef __cplusplus
}  /* extern "C" */
#endif  /* __cplusplus */
