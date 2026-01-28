#include "sqlite4.h"
#include <stdio.h>

int main(int argc, char **argv) {
    sqlite4 *db;
    int rc;
    char *errMsg = 0;

    rc = sqlite4_open(0, "test.db", &db);
    if (rc != SQLITE4_OK) {
        printf("open failed: %d\n", rc);
        return 1;
    }

    printf("sqlite4 opened\n");

    /* index create */
    rc = sqlite4_exec(
        db,
        "CREATE TABLE IF NOT EXISTS t (id INTEGER);"
        "CREATE INDEX IF NOT EXISTS t_idx ON t(id);",
        0, 0
    );

    if (rc != SQLITE4_OK) {
        printf("sql error: %s\n", errMsg);
        sqlite4_free(0, errMsg);
    }
    else {
        printf("Index created successfully\n");
    }
    sqlite4_close(db, 0);
    return 0;
}
