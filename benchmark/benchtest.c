#include "parse.h"
#include "opcodes.h"
#include "../src/sqliteInt.h"
#include "assert.h"
#include "stdbool.h"
#include "ctype.h"
#include "string.h"
#include "stdarg.h"
#include <time.h>
#include "sys/stat.h"

#include <stdint.h>

extern uint64_t diskann_get_poll_ns(void);
extern void     diskann_reset_poll_ns(void);
extern uint64_t diskann_get_poll_calls(void);
extern void     diskann_reset_poll_calls(void);
extern void     diskann_print_edge_fill_stats(void);

#define eprintf(...) fprintf(stderr, __VA_ARGS__)
#define ensure(condition, ...) { if (!(condition)) { eprintf(__VA_ARGS__); exit(1); } }

/* Wall-clock time (monotonic) in seconds */
static inline double now_sec_monotonic(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

int create_query_template(char* query, char* template, char** parameters, int* lengths, int* types) {
  bool in_quote = false; 
  bool in_digit = false;
  bool in_token = false;
  int parameter = 0;
  char* parameter_start = 0;

  if (strncmp(query, "INSERT", 6) != 0 && strncmp(query, "SELECT", 6) != 0) {
    return 0;
  }
  for (; *query != '\0'; query++) {
    if (*query == '\'') {
      if (!in_quote) {
        parameter_start = query;
      } else if (in_quote && (query - parameter_start < 3 || strncmp(query - 3, "idx", 3) != 0)){
        *(template++) = '?';
        parameters[parameter] = parameter_start + 1;
        lengths[parameter] = (int)(query - parameter_start - 1);
        types[parameter] = 1;
        parameter++;
      } else {
        while (query - parameter_start >= 0) {
          *(template++) = *(parameter_start++);
        }
      }
      in_quote = !in_quote;
      continue;
    }
    if (in_quote) {
      continue;
    }
    if (isalpha((unsigned char)*query) || (isdigit((unsigned char)*query) && in_token)) {
      in_token = true;
      *(template++) = *query;
      continue;
    }
    if (isdigit((unsigned char)*query)) {
      if (!in_digit) {
        parameter_start = query;
      }
      in_digit = true;
      continue;
    }
    if (in_digit) {
      *(template++) = '?';
      parameters[parameter] = parameter_start;
      lengths[parameter] = (int)(query - parameter_start + 1);
      types[parameter] = 0;
      parameter++;
    }
    in_token = false;
    in_digit = false;
    *(template++) = *query;
  }
  *(template++) = '\0';
  return parameter;
}

int get_int(char* s, int length) {
  char buffer[32];
  if (length < 0) length = 0;
  if (length > 30) length = 30;          // avoid overflow
  memcpy(buffer, s, (size_t)length);
  buffer[length] = '\0';
  return atoi(buffer); 
}

int main(int argc, char* argv[]) {
  ensure(argc == 3, "provide path to the query file and db file\n");
  FILE* queries_f = fopen(argv[1], "r");
  ensure(queries_f != NULL, "failed to open queries file\n");
  printf("open queries file at %s\n", argv[1]);

  sqlite4* db;
  int rc = sqlite4_open(0, argv[2], &db);
  ensure(rc == 0, "failed to open db: rc=%d\n", rc);
  printf("open sqlite db at '%s'\n", argv[2]);
  
  char line[65536 * 32];
  char template[65536 * 32];
  char* parameters[16];
  int parameter_lengths[16];
  int parameter_types[16];

  sqlite4_stmt* statement = NULL;   // IMPORTANT: init
  char prepared[65536 * 32];
  prepared[0] = '\0';

  double total_select_time = 0;
  double total_insert_time = 0;
  double total_delete_time = 0;
  long long total_reads = 0;
  long long total_writes = 0;
  int total_selects = 0;
  int total_inserts = 0;
  int total_deletes = 0;
  int index = 0;

  while (fgets(line, sizeof(line), queries_f)) {
    if (index % 100 == 0) {
      eprintf("progress: %d lines\n", index);
    }
    index++;

    int len = (int)strlen(line);
    if (len == 0) continue;

    char* end = line + len - 1;
    while (end >= line && (*end == '\n' || *end == '\r' || *end == ' ')) {
      *(end--) = '\0';
    }

    if (strncmp(line, "---", 3) == 0) {
      rc = sqlite4_wal_checkpoint_v2(db, 0, SQLITE4_CHECKPOINT_FULL, 0, 0);
      ensure(rc == 0, "failed to checkpoint db: %s\n", sqlite4_errmsg(db));

      // print & reset stat
      printf("%s (%s):\n", line + 3, argv[1]);
      int total_queries = total_selects + total_inserts + total_deletes;

      if (total_selects > 0) {
        printf("  select: %.2f micros (avg.), %d (count)\n",
               total_select_time / total_selects * 1000000.0, total_selects);
      }
      if (total_inserts > 0) {
        printf("  insert: %.2f micros (avg.), %d (count)\n",
               total_insert_time / total_inserts * 1000000.0, total_inserts);
      }
      if (total_deletes > 0) {
        printf("  delete: %.2f micros (avg.), %d (count)\n",
               total_delete_time / total_deletes * 1000000.0, total_deletes);
      }

      struct stat st;
      stat(argv[2], &st);
      printf("  size  : %.4f MB\n", st.st_size / 1024.0 / 1024.0);

      if (total_queries > 0 && total_reads > 0) {
        printf("  reads : %.2f (avg.), %lld (total)\n",
               total_reads * 1.0 / total_queries, total_reads);
      }
      if (total_queries > 0 && total_writes > 0) {
        printf("  writes: %.2f (avg.), %lld (total)\n",
               total_writes * 1.0 / total_queries, total_writes); // FIXED
      }

      uint64_t poll_ns = diskann_get_poll_ns();
      uint64_t poll_calls = diskann_get_poll_calls();

      if (poll_calls > 0) {
        printf("  polling: %.2f micros/call (avg.), %.2f ms (total), %llu (calls)\n",
              (double)poll_ns / (double)poll_calls / 1000.0,
              (double)poll_ns / 1e6,
              (unsigned long long)poll_calls);
      } else {
        printf("  polling: 0.00 micros/call (avg.), 0.00 ms (total), 0 (calls)\n");
      }

      if (total_inserts > 0) {
        double total_time_ns = total_insert_time * 1e9;
        printf("           polling share of total insert time: %.2f %%\n",
               (double)poll_ns / total_time_ns * 100.0);
      }
      if (total_selects > 0) {
        double total_time_ns = total_select_time * 1e9;
        printf("           polling share of total select time: %.2f %%\n",
               (double)poll_ns / total_time_ns * 100.0);
      }


      diskann_reset_poll_ns();
      diskann_reset_poll_calls();
      diskann_print_edge_fill_stats();

      fflush(stdout);

      total_reads = 0; 
      total_writes = 0; 
      total_select_time = 0; 
      total_insert_time = 0; 
      total_delete_time = 0; 
      total_selects = 0; 
      total_inserts = 0; 
      total_deletes = 0; 
      continue;
    }

    char* error = NULL;
    int count = create_query_template(line, template, (char**)&parameters,
                                      parameter_lengths, parameter_types);
    if (count > 0) {
      if (strcmp(template, prepared) != 0) {
        if (statement) {
          sqlite4_finalize(statement);
          statement = NULL;
        }
        memcpy(prepared, template, strlen(template) + 1);

        rc = sqlite4_prepare(db, template, (int)strlen(template), &statement, 0);
        ensure(rc == 0, "failed to prepare statement '%s': %d\n", template, rc);
        eprintf("prepared statement: '%s'\n", template);
      }

      rc = sqlite4_reset(statement);
      ensure(rc == 0, "failed to reset prepared statement: %s", sqlite4_errmsg(db));
      rc = sqlite4_clear_bindings(statement);
      ensure(rc == 0, "failed to clear bindings for prepared statement: %s", sqlite4_errmsg(db));

      // bind parameters
      for (int i = 0; i < count; i++) {
        if (parameter_types[i] == 0) {
          rc = sqlite4_bind_int(statement, i + 1, get_int(parameters[i], parameter_lengths[i]));
          ensure(rc == 0, "failed to bind int parameter (%d): %s\n", i, sqlite4_errmsg(db));
        } else if (parameter_types[i] == 1) {
          rc = sqlite4_bind_text(statement, i + 1, parameters[i], parameter_lengths[i], SQLITE4_TRANSIENT);
          ensure(rc == 0, "failed to bind string parameter: %d\n", rc);
        } else {
          ensure(false, "unexpected parameter type\n");
        }
      }

      double start_time = now_sec_monotonic();   // WALL TIME START

      double* total_time = NULL;
      int* total_count = NULL;

      if (strncmp(prepared, "SELECT", 6) == 0) {
        total_time = &total_select_time;
        total_count = &total_selects;

        do {
          rc = sqlite4_step(statement); 
        } while (rc == SQLITE4_ROW);
        ensure(rc == SQLITE4_DONE, "SELECT query finished incorrectly: %s", sqlite4_errmsg(db));

      } else if (strncmp(prepared, "INSERT", 6) == 0) {
        total_time = &total_insert_time;
        total_count = &total_inserts;

        rc = sqlite4_step(statement);
        ensure(rc == SQLITE4_DONE, "INSERT query finished incorrectly: %s", sqlite4_errmsg(db));

      } else if (strncmp(prepared, "DELETE", 6) == 0) {
        total_time = &total_delete_time;
        total_count = &total_deletes;

        rc = sqlite4_step(statement);
        ensure(rc == SQLITE4_DONE, "DELETE query finished incorrectly: %s", sqlite4_errmsg(db));

      } else {
        ensure(false, "unexpected query type: %s\n", prepared);
      }

      double end_time = now_sec_monotonic();     // WALL TIME END

      total_reads += sqlite4_stmt_status(statement, 1025, 1);
      total_writes += sqlite4_stmt_status(statement, 1026, 1);

      *total_time += (end_time - start_time);    // seconds
      *total_count += 1;

    } else {
      rc = sqlite4_exec(db, line, 0, 0, &error);
      ensure(rc == 0, "failed to exec simple statement '%s': %s\n", line, error ? error : "(null)");
      eprintf("executed simple statement: '%s'\n", line);
      if (error) sqlite4_free(error);
    }
  }

  if (statement) sqlite4_finalize(statement);
  fclose(queries_f);
  sqlite4_close(0, db);
  return 0;
}
