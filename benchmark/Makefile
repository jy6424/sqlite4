CC ?= gcc

SQLITE4_ROOT := ..

INCLUDES := -I$(SQLITE4_ROOT) -I$(SQLITE4_ROOT)/src

LIBDIR := $(SQLITE4_ROOT)
LIBS   := -L$(LIBDIR) -lsqlite4

CFLAGS  ?= -g -O0 -fno-omit-frame-pointer
LDFLAGS ?= -pthread -lm

all: diskann bruteforce no_vectors

anntest: anntest.c
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@ $(LIBS) $(LDFLAGS)

benchtest: benchtest.c
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@ $(LIBS) $(LDFLAGS)

blobtest: blobtest.c
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@ $(LIBS) $(LDFLAGS)

diskann.sql:
	python3 workload.py diskann 64 1000 1000 > diskann.sql
diskann: benchtest diskann.sql
	-rm -f test.db test.db-* || true
	./benchtest diskann.sql test.db

diskann_build.sql:
	python3 workload.py diskann_build 64 1000 > diskann_build.sql
diskann_build: benchtest diskann_build.sql
	-rm -f test.db test.db-* || true
	./benchtest diskann_build.sql test.db

diskann_search.sql:
	python3 workload.py diskann_search 64 1000 > diskann_search.sql
diskann_search: benchtest diskann_search.sql
	./benchtest diskann_search.sql test.db

bruteforce.sql:
	python3 workload.py bruteforce 64 1000 1000 > bruteforce.sql
bruteforce: benchtest bruteforce.sql
	-rm -f test.db test.db-* || true
	./benchtest bruteforce.sql test.db

no_vectors.sql:
	python3 workload.py no_vectors 1000 1000 > no_vectors.sql
no_vectors: benchtest no_vectors.sql
	-rm -f test.db test.db-* || true
	./benchtest no_vectors.sql test.db

clean:
	-rm -f benchtest blobtest anntest *.sql *.db *.db-*
