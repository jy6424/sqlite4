#include "sqlite4.h"
#include <stdio.h>

static const char *val_to_cstr(sqlite4_value *v){
  /* sqlite4_value -> printable string */
  int t = sqlite4_value_type(v);
  switch(t){
    case SQLITE4_NULL:    return "NULL";
    case SQLITE4_INTEGER: return sqlite4_value_text(v);  /* usually ok */
    case SQLITE4_FLOAT:   return sqlite4_value_text(v);
    case SQLITE4_TEXT:    return sqlite4_value_text(v);
    case SQLITE4_BLOB:    return "(BLOB)";
    default:              return "(UNKNOWN)";
  }
}

static int print_row(void *unused, int argc, sqlite4_value **argv, const char **colname){
  (void)unused;
  for(int i = 0; i < argc; i++){
    const char *name = colname[i] ? colname[i] : "(col)";
    const char *val  = argv[i] ? val_to_cstr(argv[i]) : "NULL";
    printf("%s=%s%s", name, val, (i == argc-1) ? "\n" : " | ");
  }
  return 0;
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
