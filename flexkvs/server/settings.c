/*
 * Copyright 2019 University of Washington, Max Planck Institute for
 * Software Systems, and The University of Texas at Austin
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#include <stdio.h>
#include <stdlib.h>

#include "iokvs.h"

struct settings settings;

int settings_init(int argc, char *argv[])
{
    settings.udpport = 11211;
    settings.verbose = 1;
    settings.segsize = (1024 * (16 * 1024 + 32 + sizeof(struct item)));
    //settings.segsize = 128 * 65536;
    //settings.segsize = 1024 * 1024 * 1024;
    //settings.segmaxnum = 4096;
    //settings.segmaxnum = 64 * 4096;
    //settings.segcqsize = 32 * 1024;
    if(argc >= 4) {
        size_t total_size = atoll(argv[3]);
        // Ratio: 1:31 hashtable:segments
        settings.hasht_size = total_size / 32;
        settings.segmaxnum = total_size * 31 / (32 * settings.segsize);
    }
    else {
        settings.hasht_size = (1ull << 31);
        settings.segmaxnum = 700;
    }
    double hash_size = (double)(settings.hasht_size / (1024 * 1024 * 1024));
    double alloc_size = (double)(settings.segmaxnum * settings.segsize / (1024 * 1024 * 1024));
    printf("Total memory size: %.2f GB (hashtable: %.2f GB, allocator %.2f GB)\n",
        hash_size + alloc_size, hash_size, alloc_size);
    settings.segcqsize = 1500;
    settings.clean_ratio = 0.8;
    //settings.clean_ratio = 1.1;

    if (argc < 3) {
        fprintf(stderr, "Usage: flexkvs CONFIG THREADS [DATA_SIZE]\n");
        return -1;
    }

    settings.numcores = atoi(argv[2]);
    settings.config_file = argv[1];
    return 0;
}
