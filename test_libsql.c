#include "sqlite4.h"
#include <stdio.h>

static int print_row(void *unused, int argc, char **argv, char **colname){
  (void)unused;
  for(int i = 0; i < argc; i++){
    const char *val = argv[i] ? argv[i] : "NULL";
    printf("%s=%s%s", colname[i], val, (i == argc-1) ? "\n" : " | ");
  }
  return 0; // 0이면 계속 진행
}

int main(int argc, char **argv) {
  sqlite4 *db = 0;
  int rc = 0;

  rc = sqlite4_open(0, "test.db", &db);
  if (rc) {
    printf("open failed: %d\n", rc);
    return 1;
  }
  printf("Database opened successfully\n");

  /* table + index create */
  rc = sqlite4_exec(
    db,
    "CREATE TABLE IF NOT EXISTS t (id INTEGER);"
    "CREATE INDEX IF NOT EXISTS t_idx ON t(id);",
    0, 0
  );
  if (rc) {
    printf("sql error (create): rc=%d\n", rc);
    sqlite4_close(db, 0);
    return 1;
  }
  printf("Index created successfully\n");

  /* SELECT results: indexes */
  printf("\n-- indexes --\n");
  rc = sqlite4_exec(
    db,
    "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name;",
    print_row, 0
  );
  if (rc) {
    printf("sql error (select indexes): rc=%d\n", rc);
    sqlite4_close(db, 0);
    return 1;
  }

  /* SELECT results: tables */
  printf("\n-- tables --\n");
  rc = sqlite4_exec(
    db,
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;",
    print_row, 0
  );
  if (rc) {
    printf("sql error (select tables): rc=%d\n", rc);
    sqlite4_close(db, 0);
    return 1;
  }

  sqlite4_close(db, 0);
  return 0;
}
