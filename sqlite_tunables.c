#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include "sqliteInt.h"

struct sqlite3_tunables_type sqlite3_gbl_tunables;

void sqlite3_tunables_init(void) {
#define DEF_ATTR(NAME, name, type, dflt) sqlite3_gbl_tunables.name = dflt;
#include "sqlite_tunables.h"
}
#undef DEF_ATTR

void sqlite3_dump_tunables(void) {
#define DEF_ATTR(NAME, name, type, dflt) \
    printf("%-35s", #NAME);              \
    switch (SQLITE_ATTR_##type) {        \
        default:                         \
        case SQLITE_ATTR_QUANTITY:       \
            printf("%d\n", sqlite3_gbl_tunables.name); \
            break;                       \
        case SQLITE_ATTR_BOOLEAN:        \
            printf("%s\n", sqlite3_gbl_tunables.name ? "ON" : "OFF");  \
    };
#include "sqlite_tunables.h"
}
#undef DEF_ATTR

void sqlite3_set_tunable_by_name(char *tname, char *val) {
    int nval;
    char *endp;
    nval = strtol(val, &endp, 10);
    if (0);
#define DEF_ATTR(NAME, name, type, dflt) \
    else if (strcasecmp(tname, #NAME) == 0) {     \
        switch (SQLITE_ATTR_##type) {    \
        default:                         \
        case SQLITE_ATTR_QUANTITY:       \
            sqlite3_gbl_tunables.name = nval;                  \
            break;                       \
        case SQLITE_ATTR_BOOLEAN:        \
            if (strcasecmp(val, "on")==0)\
                sqlite3_gbl_tunables.name = 1;                \
            else if (strcasecmp(val, "off")==0)  \
                sqlite3_gbl_tunables.name = 0;                \
            else {                       \
                printf("Expected ON or OFF\n");  \
            }                            \
            break; \
        }  \
    }
#include "sqlite_tunables.h"
    else {
        printf("Unknown tunable %s\n", tname);
    }
}
#undef DEF_ATTR
