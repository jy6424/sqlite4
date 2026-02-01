#include "sqlite4.h"
#include <stdio.h>
#include <inttypes.h>

static void print_value(sqlite4_value *v){
  int t = sqlite4_value_type(v);
  switch(t){
    case SQLITE4_NULL:
      printf("NULL");
      break;

    case SQLITE4_INTEGER: {
      sqlite4_int64 x = sqlite4_value_int64(v);
      printf("%" PRId64, (int64_t)x);
      break;
    }

    case SQLITE4_FLOAT: {
      double d = sqlite4_value_double(v);
      printf("%.17g", d);
      break;
    }

    case SQLITE4_TEXT: {
      int n = 0;
      const char *z = sqlite4_value_text(v, &n);
      /* z may not be nul-terminated; print with length */
      if(z) printf("%.*s", n, z);
      else  printf("(null-text)");
      break;
    }

    case SQLITE4_BLOB: {
      int n = 0;
      const void *p = sqlite4_value_blob(v, &n);
      (void)p;
      printf("(BLOB %d bytes)", n);
      break;
    }

    default:
      printf("(UNKNOWN type=%d)", t);
      break;
  }
}

static int print_row(void *unused, int argc, sqlite4_value **argv, const char **colname){
  (void)unused;
  for(int i = 0; i < argc; i++){
    const char *name = colname[i] ? colname[i] : "(col)";
    printf("%s=", name);
    if(argv[i]) print_value(argv[i]);
    else printf("NULL");
    printf("%s", (i == argc-1) ? "\n" : " | ");
  }
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
    "CREATE TABLE x (embedding F32_BLOB(4));",
    0, 0
  );
  if (rc) {
    printf("sql error (create): rc=%d\n", rc);
    sqlite4_close(db, 0);
    return 1;
  }
  printf("Table created successfully\n");

  // printf("\n-- creating index --\n");
  // rc = sqlite4_exec(
  //   db,
  //   "CREATE INDEX x_idx ON x (libsql_vector_idx(embedding));",
  //   print_row, 0
  // );
  // if (rc) {
  //   printf("sql error (create index): rc=%d\n", rc);
  //   sqlite4_close(db, 0);
  //   return 1;
  // }

  // printf("\n-- tables --\n");
  // rc = sqlite4_exec(
  //   db,
  //   "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;",
  //   print_row, 0
  // );
  // if (rc) {
  //   printf("sql error (select tables): rc=%d\n", rc);
  //   sqlite4_close(db, 0);
  //   return 1;
  // }

  //   printf("\n-- indexes --\n");
  // rc = sqlite4_exec(
  //   db,
  //   "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name;",
  //   print_row, 0
  // );
  // if (rc) {
  //   printf("sql error (select indexes): rc=%d\n", rc);
  //   sqlite4_close(db, 0);
  //   return 1;
  // }

  sqlite4_close(db, 0);
  return 0;
}
