FLEXKVS_COMMON=hashtable.o ialloc.o jenkins_hash.o #settings.o
BENCH_COMMON=rng.o commandline.o workload.o

#TAS_CODE?=/home/tstamler/tas
#HEMEM_CODE=/home/amanda/hemem/

CFLAGS = -std=gnu99 -g -Wall -Icommon/include -O3 -march=native -I./server/
#CFLAGS += -DDEL_TEST
LDFLAGS = -pthread -g -O3
LDLIBS = -lrt -lm -lpthread -lrt -ldl

all: kvsbench #flexkvs 
all-sockets: flexkvs kvsbench
all-ll: flexkvs-ll

flexkvs: $(addprefix server/,$(FLEXKVS_COMMON) main.o) $(addprefix bench/,$(BENCH_COMMON) main.o) common/utils.o
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

kvsbench: $(addprefix server/,$(FLEXKVS_COMMON)) $(addprefix bench/,$(BENCH_COMMON) main.o) common/utils.o
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

flexkvs-ll: $(addprefix server/,$(FLEXKVS_COMMON) main-ll.o) common/utils.o
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

common/utils.o: common/src/utils.c
	$(CC) $(CFLAGS) $(LDFLAGS) -c -o $@ $^ $(LDLIBS)


clean:
	rm -f */*.o flexkvs kvsbench flexkvs-ll

.PHONY: all all-sockets all-mtcp all-ll clean
