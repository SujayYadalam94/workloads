/*
 * =====================================================================================
 *
 *       Filename:  gups-random.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  02/21/2018 02:36:27 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Sujay Yadalam,
 *   Original src:  Hemem artifact (https://bitbucket.org/ajaustin/hemem/src/master/)
 * 
 *          Notes:  This is a simple implementation of the GUPS benchmark. Instead of
 *                  the original pointer chasing, the indices accessed are generated
 *                  randomly in this version.
 *                  Remember to enable hugepages before running this benchmark.
 *                  echo X > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
 *
 * =====================================================================================
 */

#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <math.h>
#include <string.h>
#include <pthread.h>
#include <sys/mman.h>
#include <errno.h>
#include <stdint.h>
#include <stdbool.h>

#include "timer.h"

#include "gups.h"

#define MAX_THREADS     64

int threads;

struct gups_args {
  int tid;                      // thread id
  uint64_t *indices;       // array of indices to access
  void* field;                  // pointer to start of thread's region
  uint64_t iters;          // iterations to perform
  uint64_t size;           // size of region
  uint64_t elt_size;       // size of elements
};

static unsigned long updates, nelems;

static uint64_t lfsr_fast(uint64_t lfsr)
{
  lfsr ^= lfsr >> 7;
  lfsr ^= lfsr << 9;
  lfsr ^= lfsr >> 13;
  return lfsr;
}

static void *do_gups(void *arguments)
{
  //printf("do_gups entered\n");
  struct gups_args *args = (struct gups_args*)arguments;
  uint64_t *field = (uint64_t*)(args->field);
  uint64_t i;
  uint64_t index;
  uint64_t elt_size = args->elt_size;
  char data[elt_size];
  uint64_t lfsr;

  srand(args->tid);
  lfsr = rand();

  index = 0;

  for (i = 0; i < args->iters; i++) {
    lfsr = lfsr_fast(lfsr);
    index = lfsr % (args->size);
    if (elt_size == 8) {
      uint64_t tmp = field[index];
      tmp = tmp + i;
      field[index] = tmp;
    }
    else {
      memcpy(data, &field[index * elt_size], elt_size);
      memset(data, data[0] + i, elt_size);
      memcpy(&field[index * elt_size], data, elt_size);
    }
  }

  return 0;
}

int main(int argc, char **argv)
{
  unsigned long expt;
  unsigned long size, elt_size;
  struct timeval starttime, stoptime;
  double secs, gups;
  int i;
  void *p;
  struct gups_args** ga;
  pthread_t t[MAX_THREADS];

  if (argc != 5) {
    fprintf(stderr, "Usage: %s [threads] [updates per thread] [exponent] [data size (bytes)] [noremap/remap]\n", argv[0]);
    fprintf(stderr, "  threads\t\t\tnumber of threads to launch\n");
    fprintf(stderr, "  updates per thread\t\tnumber of updates per thread\n");
    fprintf(stderr, "  exponent\t\t\tlog size of region\n");
    fprintf(stderr, "  data size\t\t\tsize of data in array (in bytes)\n");
    return 0;
  }

  gettimeofday(&starttime, NULL);

  threads = atoi(argv[1]);
  assert(threads <= MAX_THREADS);
  ga = (struct gups_args**)malloc(threads * sizeof(struct gups_args*));

  updates = atol(argv[2]);
  updates -= updates % 256;
  expt = atoi(argv[3]);
  assert(expt > 8);
  assert(updates > 0 && (updates % 256 == 0));
  size = (unsigned long)(1) << expt;
  size -= (size % 256);
  assert(size > 0 && (size % 256 == 0));
  elt_size = atoi(argv[4]);
  
  fprintf(stderr, "%lu updates per thread (%d threads)\n", updates, threads);
  fprintf(stderr, "field of 2^%lu (%lu) bytes\n", expt, size);
  fprintf(stderr, "%ld byte element size (%ld elements total)\n", elt_size, size / elt_size);

  p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS | MAP_HUGETLB | MAP_POPULATE, -1, 0);
  if (p == MAP_FAILED) {
    perror("mmap");
    assert(0);
  }

  gettimeofday(&stoptime, NULL);
  fprintf(stderr, "Init took %.4f seconds\n", elapsed(&starttime, &stoptime));
  fprintf(stderr, "Region address: %p - %p\t size: %ld\n", p, (p + size), size);
  
  nelems = (size / threads) / elt_size; // number of elements per thread
  fprintf(stderr, "Elements per thread: %lu\n", nelems);

  gettimeofday(&starttime, NULL);
  for (i = 0; i < threads; i++) {
    //printf("starting thread [%d]\n", i);
    ga[i] = (struct gups_args*)malloc(sizeof(struct gups_args));
    ga[i]->tid = i;
    ga[i]->field = p + (i * nelems * elt_size);
    ga[i]->iters = updates;
    ga[i]->size = nelems;
    ga[i]->elt_size = elt_size;
  }

  // touch all memory once
  for (i = 0; i < nelems*threads; i++) {
    *(int *)(p + i) = 0;
  }

  fprintf(stderr, "Timing.\n");
  gettimeofday(&starttime, NULL);

  // spawn gups worker threads
  for (i = 0; i < threads; i++) {
    int r = pthread_create(&t[i], NULL, do_gups, (void*)ga[i]);
    assert(r == 0);
  }

  // wait for worker threads
  for (i = 0; i < threads; i++) {
    int r = pthread_join(t[i], NULL);
    assert(r == 0);
  }
  gettimeofday(&stoptime, NULL);

  secs = elapsed(&starttime, &stoptime);
  printf("Elapsed time: %.4f seconds.\n", secs);
  gups = threads * ((double)updates) / (secs * 1.0e9);
  printf("GUPS = %.10f\n", gups);

  for (i = 0; i < threads; i++) { 
    free(ga[i]);
  }
  free(ga);

  munmap(p, size);

  return 0;
}


