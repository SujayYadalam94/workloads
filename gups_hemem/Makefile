HEMEMDIR = $(CURDIR)/../src/

CC = gcc
CFLAGS = -g -Wall -O3
#CFLAGS = -g3 -Wall -O0
INCLUDES = -I../linux/usr/include
LIBS = -lm -lpthread

default: gups-random gups-skewed gups-hotset-move

gups-random: gups-random.o
	$(CC) $(CFLAGS) $(INCLUDES) -o gups-random gups-random.o timer.o zipf.o $(LIBS)

gups-skewed: gups-skewed.o
	$(CC) $(CFLAGS) $(INCLUDES) -o gups-skewed gups-skewed.o timer.o zipf.o $(LIBS)

gups-hotset-move: gups-hotset-move.o
	$(CC) $(CFLAGS) $(INCLUDES) -o gups-hotset-move gups-hotset-move.o timer.o zipf.o $(LIBS)

gups-random.o: gups-random.c timer.c zipf.c gups.h
	$(CC) $(CFLAGS) $(INCLUDES) -c gups-random.c timer.c zipf.c

gups-skewed.o: gups-skewed.c timer.c zipf.c gups.h
	$(CC) $(CFLAGS) $(INCLUDES) -c gups-skewed.c timer.c zipf.c

gups-hotset-move.o: gups-hotset-move.c timer.c zipf.c gups.h
	$(CC) $(CFLAGS) $(INCLUDES) -c gups-hotset-move.c timer.c zipf.c

clean:
	$(RM) *.o gups-random gups-skewed gups-hotset-move
