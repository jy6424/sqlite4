#include "sqlite4.h"
#include <stdio.h>
#include <inttypes.h>

int main(int argc, char **argv) {
  (void)argc; (void)argv;

  sqlite4 *db = 0;
  int rc = 0;

  rc = sqlite4_open(0, "test.db", &db);
  if (rc) {
    printf("open failed: %d\n", rc);
    return 1;
  }
  printf("Database opened successfully\n");

  printf("\n-- creating table --\n");
  rc = sqlite4_exec(
    db,
    "CREATE TABLE x (name TEXT);",
    0, 0
  );
  if (rc) {
    printf("sql error (create): rc=%d\n", rc);
    sqlite4_close(db, 0);
    return 1;
  }
  printf("Table created successfully\n");

  printf("\n-- creating index --\n");
  rc = sqlite4_exec(
    db,
    "CREATE INDEX x_idx ON x (libsql_vector_idx(embedding));",
    0, 0
  );
  if (rc) {
    printf("sql error (create index): rc=%d\n", rc);
    sqlite4_close(db, 0);
    return 1;
  }

  printf("\n-- tables --\n");
  rc = sqlite4_exec(
    db,
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;",
    0, 0
  );
  if (rc) {
    printf("sql error (select tables): rc=%d\n", rc);
    sqlite4_close(db, 0);
    return 1;
  }

    printf("\n-- indexes --\n");
  rc = sqlite4_exec(
    db,
    "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name;",
    0, 0
  );
  if (rc) {
    printf("sql error (select indexes): rc=%d\n", rc);
    sqlite4_close(db, 0);
    return 1;
  }

  sqlite4_close(db, 0);
  return 0;
}
