#include "sqlite4.h"
#include <stdio.h>

int main(int argc, char **argv) {
    sqlite4 *db;
    int rc;

    rc = sqlite4_open(0, "test.db", &db);
    if (rc != SQLITE4_OK) {
        printf("open failed: %d\n", rc);
        return 1;
    }

    printf("sqlite4 opened\n");

    sqlite4_close(db);
    return 0;
}
