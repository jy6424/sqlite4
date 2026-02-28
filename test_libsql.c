#include "sqlite4.h"
#include <stdio.h>
#include <inttypes.h>

static int print_callback(
  void *NotUsed,
  int argc,
  sqlite4_value **argv,
  const char **colName
){
  int i;
  for (i = 0; i < argc; i++) {
    const char *val;

    if (argv[i] == 0) {
      val = "NULL";
    } else {
      val = sqlite4_value_text(argv[i], NULL);
      if (!val) val = "(non-text)";
    }

    printf("%s=%s\t", colName[i], val);
  }
  printf("\n");
  return 0;
}


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
    "CREATE TABLE x (id INTEGER PRIMARY KEY, embedding F32_BLOB(4));",
    0, 0
  );
  if (rc) {
    printf("sql error (create): rc=%d\n with errmsg : %s\n", rc, sqlite4_errmsg(db));
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
    printf("sql error (create index): rc=%d\n with errmsg : %s\n", rc, sqlite4_errmsg(db));
    sqlite4_close(db, 0);
    return 1;
  }

  printf("\n-- created tables --\n");
  rc = sqlite4_exec(
    db,
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;",
    print_callback,
    0
  );
  if (rc) {
    printf("sql error (select tables): rc=%d\n with errmsg : %s\n", rc, sqlite4_errmsg(db));
    sqlite4_close(db, 0);
    return 1;
  }

  printf("\n-- created indexes --\n");
  rc = sqlite4_exec(
    db,
    "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name;",
    print_callback,
    0
  );
  if (rc) {
    printf("sql error (select indexes): rc=%d\n with errmsg : %s\n", rc, sqlite4_errmsg(db));
    sqlite4_close(db, 0);
    return 1;
  }

  printf("\n-- tables and indexes SQL--\n");
  rc = sqlite4_exec(
    db,
    "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name;",
    print_callback,
    0
  );
  if (rc) {
    printf("sql error (print tables and indexes): rc=%d\n with errmsg : %s\n", rc, sqlite4_errmsg(db));
    sqlite4_close(db, 0);
    return 1;
  }

  // data insertion
  printf("\n-- inserting data --\n");
  rc = sqlite4_exec(
    db,
    "INSERT INTO x (id, embedding) VALUES "
    " (1, vector32('[0.800, 0.579, 0.481, 0.229]'));",
    0, 0
  );
  if (rc) {
    printf("sql error (insert data): rc=%d\n with errmsg : %s\n", rc, sqlite4_errmsg(db));
    sqlite4_close(db, 0);
    return 1;
  }
  printf("Data inserted successfully\n");

  // select data
  printf("\n-- selecting data --\n");
  rc = sqlite4_exec(
    db,
    "SELECT embedding FROM x;",
    print_callback,
    0
  );
  if (rc) {
    printf("sql error (select data): rc=%d\n with errmsg : %s\n", rc, sqlite4_errmsg(db));
    sqlite4_close(db, 0);
    return 1;
  }
  printf("Data selected successfully\n");

  printf("\n-- number of data from table (x) --\n");
  sqlite4_exec(db, "SELECT count(*) AS n FROM x;", print_callback, 0);
  printf("\n-- number of data from shadow table (x_idx_shadow) --\n");
  sqlite4_exec(db, "SELECT count(*) AS n FROM x_idx_shadow;", print_callback, 0);
  printf("\n-- number of data from vector index table (x_idx) --\n");
  sqlite4_exec(db, "SELECT name, sql FROM sqlite_schema WHERE name LIKE 'x_idx%';", print_callback, 0);
  
  sqlite4_close(db, 0);

  return 0;
}