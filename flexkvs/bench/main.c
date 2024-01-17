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

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#include <netdb.h>
#include <errno.h>
#include <assert.h>
#include <locale.h>
#include <inttypes.h>
#include <sys/un.h>

#include <protocol_binary.h>

#include "benchmark.h"
#include "socket_shim.h"
#include "iokvs.h"

#ifdef USE_MTCP
# include <mtcp_api.h>
# include <mtcp_epoll.h>
#else
# include <sys/epoll.h>
#endif

#define MIN(a,b) ((b) < (a) ? (b) : (a))

#define CONN_DEBUG(c, co, x...) do { } while (0)
/*#define CONN_DEBUG(c, co, x...) \
    do { printf("%d.%d: ", (int) c->id, co->fd); \
         printf(x); } while (0)*/

#define PRINT_STATS
#ifdef PRINT_STATS
#   define STATS_ADD(c, f, n) c->f += n
#else
#   define STATS_ADD(c, f, n) do { } while (0)
#endif

#define HIST_START_NS 0
#define HIST_BUCKET_NS 10
#define HIST_BUCKETS 4096
#define BUFSIZE 1000000

enum conn_state {
    CONN_CLOSED = 0,
    CONN_CONNECTING = 1,
    CONN_OPEN = 2,
};

enum benchmark_phase {
    BENCHMARK_INIT,
    BENCHMARK_PRELOAD,
    BENCHMARK_WARMUP,
    BENCHMARK_RUNNING,
    BENCHMARK_DYN_HOTSET,
    BENCHMARK_COOLDOWN,
    BENCHMARK_DONE,
};

struct settings settings;
static struct workload workload1;
static struct workload workload2;
static volatile enum benchmark_phase phase;
static volatile uint16_t init_count = 0;
static bool skip_load = false;
//static uint32_t max_pending = 64;*/

struct connection {
    enum conn_state state;
    int fd;
    int ep_wr;
    uint32_t pending;
    uint32_t tx_len;
    uint32_t tx_off;
    uint32_t rx_len;
    void *rx_buf;
    void *tx_buf;
    struct connection *next;
#ifdef PRINT_STATS
    uint64_t cnt;
#endif
};

struct core {
    struct connection *conns;
#ifdef PRINT_STATS
    uint64_t tx_get;
    uint64_t tx_set;
    uint64_t rx_get;
    uint64_t rx_set;
    uint64_t rx_success;
    uint64_t rx_fail;
    uint64_t rx_calls;
    uint64_t rx_nanos;
    uint64_t tx_calls;
    uint64_t tx_nanos;
    uint64_t epupd;
    uint32_t *hist;
#endif
    int ep;
    ssctx_t sc;
#ifdef USE_MTCP
    mctx_t mc;
#endif
    struct workload_core wlc;
    uint64_t id;
    pthread_t pthread;
    uint16_t conn_pending;

    uint32_t msgs_pending;
    struct connection *q_first;
    struct connection *q_last;
} __attribute__((aligned(64)));

static inline uint64_t get_nanos(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t) ts.tv_sec * 1000 * 1000 * 1000 + ts.tv_nsec;
}

static inline void record_latency(struct core *c, uint64_t nanos)
{
    size_t bucket = (nanos - HIST_START_NS) / HIST_BUCKET_NS;
    if (bucket >= HIST_BUCKETS) {
        bucket = HIST_BUCKETS - 1;
    }
    c->hist[bucket]++;
    //__sync_fetch_and_add(&c->hist[bucket], 1);
}

#ifdef PRINT_STATS
static inline uint64_t read_cnt(uint64_t *p)
{
  uint64_t v = *p;
  __sync_fetch_and_sub(p, v);
  return v;
}
#endif

// Open connection 
static inline void conn_connect(struct core *c, struct connection *co)
{
    int fd, cn, ret;
    ssctx_t sc;
    ss_epev_t ev;
    struct sockaddr_un addr;
    char buf[32];
    
    cn = c->id;
    sc = c->sc;
    CONN_DEBUG(c, co, "Opening new connection\n");

    fprintf(stderr, "opening conn %d\n", cn);
    sprintf(buf, "kvs_sock%d", cn);
    // create socket 
    //if ((fd = ss_socket(sc, AF_INET, SOCK_STREAM, IPPROTO_TCP))
    if ((fd = ss_socket(sc, AF_UNIX, SOCK_STREAM, 0))
        < 0)
    {
        perror("creating socket failed");
        fprintf(stderr, "[%d] socket failed\n", cn);
        abort();
    }

    // make socket non-blocking 
    if ((ret = ss_set_nonblock(sc, fd)) != 0) {
        fprintf(stderr, "[%d] set_nonblock failed: %d\n", cn, ret);
        abort();
    }

    // disable nagling 
    //if (ss_set_nonagle(sc, fd) != 0) {
    //    fprintf(stderr, "[%d] setsockopt TCP_NODELAY failed\n", cn);
    //    abort();
    //}

    // add to epoll 
    ev.data.ptr = co;
    ev.events = SS_EPOLLIN | SS_EPOLLOUT | SS_EPOLLHUP | SS_EPOLLERR;
    if (ss_epoll_ctl(sc, c->ep, SS_EPOLL_CTL_ADD, fd, &ev) < 0) {
      fprintf(stderr, "[%d] adding to epoll failed\n", cn);
    }

    // initialize address of socket 
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, buf, sizeof(addr.sun_path)-1);
    //addr.sin_addr.s_addr = htonl(settings.dstip);
    //addr.sin_port = htons(settings.dstport);

    // initiate non-blocking connect
    ret = ss_connect(sc, fd, (struct sockaddr *) &addr, sizeof(addr));
    if (ret == 0) {
        // success 
        CONN_DEBUG(c, co, "Connection succeeded\n");
        co->state = CONN_OPEN;
    } else if (ret < 0 && errno == EINPROGRESS) {
        // still going on 
        CONN_DEBUG(c, co, "Connection pending: %d\n", fd);
        co->state = CONN_CONNECTING;
    } else {
        // opening connection failed 
        fprintf(stderr, "[%d] connect failed: %d\n", cn, ret);
        abort();
    }

    co->fd = fd;
    co->ep_wr = 1;
    co->pending = 0;
    co->rx_len = 0;
    co->tx_len = 0;
    co->tx_off = 0;
#ifdef PRINT_STATS
    co->cnt = 0;
#endif
}

static inline void conn_epupdate(struct core *c, struct connection *co, int wr)
{
    ss_epev_t ev;
    int cn = c->id;

    if (co->ep_wr == wr) {
        return;
    }
    //printf("conn_epupdate(%p, %d)\n", co, wr);

    ev.data.ptr = co;
    ev.events = SS_EPOLLHUP | SS_EPOLLERR | (wr ? SS_EPOLLOUT : 0) | SS_EPOLLIN;
    if (ss_epoll_ctl(c->sc, c->ep, SS_EPOLL_CTL_MOD, co->fd, &ev) != 0) {
        perror("epoll_ctl failed");
        fprintf(stderr, "[%d] epoll_ctl failed\n", cn);
        abort();
    }
    co->ep_wr = wr;

    STATS_ADD(c, epupd, 1);
}

// receive response on connection 
static inline int conn_recv(struct core *c, struct connection *co,
        enum workload_op *op, uint16_t *err, uint32_t *opaque)
{
    protocol_binary_response_header *res =
        (protocol_binary_response_header *) co->rx_buf;
    int cn = c->id;
    size_t reqlen;
    ssize_t ret;
    //uint64_t tsc;

    // while response incomplete: receive more 
    while (co->rx_len < sizeof(*res) ||
            co->rx_len < sizeof(*res) + ntohl(res->response.bodylen))
    {
        //tsc = get_nanos();
        ret = ss_read(c->sc, co->fd, (uint8_t *) co->rx_buf + co->rx_len,
            BUFSIZE - co->rx_len);
        /*STATS_ADD(c, rx_nanos, get_nanos() - tsc);
        STATS_ADD(c, rx_calls, 1);*/

        if (ret > 0) {
            co->rx_len += ret;
	    //printf("got %zu, waiting for %zu\n", ret, sizeof(*res) + ntohl(res->response.bodylen));
        } else if (ret < 0 && errno == EAGAIN) {
            // nothing to receive 
            //printf("nothing to receive...\n");
	    return 1;
        } else {
            // errror, close connection 
            fprintf(stderr, "[%d] read failed: %d\n", cn, (int) ret);
            abort();
            return -1;
        }
    }

    // response is complete now 
    if (res->response.magic != PROTOCOL_BINARY_RES) {
        fprintf(stderr, "[%d] invalid magic on response: %x\n", cn,
                res->response.magic);
        abort();
    }

    reqlen = sizeof(*res) + ntohl(res->response.bodylen);

    if (res->response.opcode == PROTOCOL_BINARY_CMD_GET) {
        *op = WL_OP_GET;
    } else if (res->response.opcode == PROTOCOL_BINARY_CMD_SET) {
        *op = WL_OP_SET;
    } else if (res->response.opcode == PROTOCOL_BINARY_CMD_DELETE) {
        *op = WL_OP_DELETE;
    } else {
        fprintf(stderr, "[%d] conn_recv: unknown opcode=%x\n", cn,
                res->response.opcode);
    }
    *err = ntohs(res->response.status);
    *opaque = res->response.opaque;

    if (co->rx_len > reqlen) {
        memmove(co->rx_buf, (uint8_t *) co->rx_buf + reqlen,
                co->rx_len - reqlen);
    }
    co->rx_len -= reqlen;
    return 0;
}

// send out request on connection 
static inline int conn_send(struct core *c, struct connection *co)
{
    int cn;
    ssize_t ret;
    //uint64_t tsc;

    assert(co->tx_len > 0);
    assert(co->tx_off < co->tx_len);

    cn = c->id;

    //tsc = get_nanos();
    ret = ss_write(c->sc, co->fd, (uint8_t *) co->tx_buf + co->tx_off,
            co->tx_len - co->tx_off);
    //STATS_ADD(c, tx_nanos, get_nanos() - tsc);
    //STATS_ADD(c, tx_calls, 1);
    if (ret > 0) {
        co->tx_off += ret;
        if (co->tx_off == co->tx_len) {
            // sent whole message 
            co->tx_off = 0;
            co->tx_len = 0;
            return 0;
        } else {
            return 1;
        }
    } else if (ret < 0 && errno != EAGAIN) {
        // send failed 
        fprintf(stderr, "[%d] write failed: %d\n", cn, (int) ret);
        abort();
        return -1;
    } else if (ret < 0 && errno == EAGAIN) {
        // send would block 
        return 1;
    }

    return -1;
}

static size_t clean_log(struct item_allocator *ia, bool idle)
{
    struct item *it, *nit;
    size_t n;

    if (!idle) {
        // We're starting processing for a new request 
        ialloc_cleanup_nextrequest(ia);
    }

    n = 0;
    while ((it = ialloc_cleanup_item(ia, idle)) != NULL) {
        n++;
        if (it->refcount != 1) {
            if ((nit = ialloc_alloc(ia, sizeof(*nit) + it->keylen + it->vallen,
                    true)) == NULL)
            {
                fprintf(stderr, "Warning: ialloc_alloc failed during cleanup :-/\n");
                abort();
            }

            nit->hv = it->hv;
            nit->vallen = it->vallen;
            nit->keylen = it->keylen;
            memcpy(item_key(nit), item_key(it), it->keylen + it->vallen);
            hasht_put(nit, it);
            item_unref(nit);
        }
        item_unref(it);
    }
    return n;
}

static inline void set_request(struct core *c, struct connection *co,
        struct key *k, uint32_t opaque, struct item_allocator *ia)
{

    protocol_binary_request_set *set =
        (protocol_binary_request_set *) co->tx_buf;
    //protocol_binary_request_header *req = &set->message.header;

    assert(co->tx_off == 0);
    assert(co->tx_len == 0);
    assert(sizeof(*set) + k->keylen + settings.valuesize <= BUFSIZE);

    // Begin insertion
    void *v = (uint8_t *)set + sizeof(*set) + k->keylen;
    uint16_t kl = k->keylen;
    uint32_t vl = settings.valuesize;
    
    // allocate item 
    assert(ia != NULL);
    uint32_t h = jenkins_hash(k->key, kl);

    struct item *it = hasht_get(k->key, kl, h);

    if(it == NULL) {
        //printf("item not found\n");
        it = ialloc_alloc(ia, sizeof(struct item) + kl + vl, false);
        //printf("Allocated %p: size %lu\n", it, sizeof(struct item) + kl + vl);
        it->hv = h;
        it->vallen = vl;
        it->keylen = kl;
        memcpy(item_key(it), k->key, kl);
        memcpy(item_value(it), v, vl);

        hasht_put(it, NULL);
        // Ensure successful insert
        assert(it == hasht_get(k->key, kl, h));
        item_unref(it);
    } else {
        //printf("item found\n");
        it->hv = h;
        it->vallen = vl;
        it->keylen = kl;
        memcpy(item_key(it), k->key, kl);
        memcpy(item_value(it), v, vl);
    }
    item_unref(it);
    clean_log(ia, 1);
}

static inline void get_request(struct core *c, struct connection *co,
        struct key *k, uint32_t opaque, struct item_allocator *ia)
{
    protocol_binary_request_get *get =
        (protocol_binary_request_get *) co->tx_buf;
    assert(sizeof(*get) + k->keylen <= BUFSIZE);

    // lookup item
    uint16_t kl = k->keylen;
    uint32_t h = jenkins_hash(k->key, kl);
    struct item *it = hasht_get(k->key, kl, h);

    // calculate response length
    uint64_t rsl = sizeof(protocol_binary_response_get);
    if (it != NULL) {
        rsl += it->vallen;
    }
    
    // add item value
    if (it != NULL) {
        memcpy(get + 1, item_value(it), it->vallen);
        item_unref(it);
        clean_log(ia, 1);
    }
}

static inline void delete_request(struct core *c, struct connection *co,
        struct key *k, uint32_t opaque)
{
    protocol_binary_request_delete *get =
        (protocol_binary_request_get *) co->tx_buf;
    protocol_binary_request_header *req = &get->message.header;

    assert(co->tx_off == 0);
    assert(co->tx_len == 0);
    assert(sizeof(*get) + k->keylen <= BUFSIZE);

    req->request.magic = PROTOCOL_BINARY_REQ;
    req->request.opcode = PROTOCOL_BINARY_CMD_DELETE;
    req->request.keylen = htons(k->keylen);
    req->request.extlen = 0;
    req->request.datatype = 0;
    req->request.reserved = 0;
    req->request.bodylen = htonl(k->keylen);
    req->request.opaque = opaque;
    req->request.cas = 0;

    memcpy(get + 1, k->key, k->keylen);

    co->tx_len = sizeof(*get) + k->keylen;
}

static void poll_conns(struct core *c, int timeout)
{
    struct connection *co;
    int ret, j, cn, status;
    ss_epev_t evs[32];
    socklen_t slen;
    uint32_t events;

    cn = c->id;

    // get events 
    if ((ret = ss_epoll_wait(c->sc, c->ep, evs, 32, timeout)) < 0) {
        fprintf(stderr, "[%d] wait_conns epoll_wait failed\n", cn);
        abort();
    }

    for (j = 0; j < ret; j++) {
        co = evs[j].data.ptr;
        events = evs[j].events;

        /* we only exepect events for connections that are currently
         * connecting. */
        //if (co->state != CONN_CONNECTING) {
        //    fprintf(stderr, "[%d] wait_conns event on non-connecting "
        //            "connection\n", cn);
        //    abort();
        //}

        // check for errors on the connection 
        if ((events & (SS_EPOLLERR | SS_EPOLLHUP)) != 0) {
            fprintf(stderr, "[%d] wait_conns error on conn\n", cn);
            abort();
        }

        // check connection state 
        slen = sizeof(status);
        if (ss_getsockopt(c->sc, co->fd, SOL_SOCKET, SO_ERROR,
                    &status, &slen) < 0)
        {
            fprintf(stderr, "[%d] wait_conns getsockopt failed\n", cn);
            abort();
        }
        if (status != 0) {
            fprintf(stderr, "[%d] wait_conns conn failed\n", cn);
            abort();
        }

        c->conn_pending--;
        co->state = CONN_OPEN;
        conn_epupdate(c, co, 0);

        co->next = NULL;
        if (c->q_last == NULL) {
          c->q_last = co;
          c->q_first = co;
        } else {
          c->q_last->next = co;
          c->q_last = co;
        }

    }
}

// initialize core 
static void prepare_core(struct core *c)
{
    int cn = c->id;
    uint32_t i;
    uint8_t *buf;
    ssctx_t sc;
#ifdef USE_MTCP
    int ret;
    struct sockaddr_in addr;
#endif

    // Affinitize threads 
#ifdef USE_MTCP
    if ((ret = mtcp_core_affinitize(cn)) != 0) {
        fprintf(stderr, "[%d] mtcp_core_affinitize failed: %d\n", cn, ret);
        abort();
    }

    if ((sc = mtcp_create_context(cn)) == NULL) {
        fprintf(stderr, "[%d] mtcp_create_context failed\n", cn);
        abort();
    }

    // initialize address of socket 
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(settings.dstip);
    addr.sin_port = htons(settings.dstport);

    if ((ret = mtcp_init_rss(sc, INADDR_ANY, 1, addr.sin_addr.s_addr,
                    addr.sin_port)) != 0)
    {
        fprintf(stderr, "[%d] mtcp_init_rss failed\n", cn);
        abort();
    }
#else
    sc = NULL;
#endif
    c->sc = sc;

    // Allocate histogram 
    if ((c->hist = calloc(HIST_BUCKETS, sizeof(*c->hist))) == NULL) {
        fprintf(stderr, "[%d] allocating histogram failed\n", cn);
        abort();
    }

    // Allocate connection structs 
    if ((c->conns = calloc(settings.conns, sizeof(*c->conns))) == NULL) {
        fprintf(stderr, "[%d] allocating connection structs failed\n", cn);
        abort();
    }

    // Initiate connections 
    c->conn_pending = 0;
    c->msgs_pending = 0;
    c->q_first = c->q_last = NULL;
    for (i = 0; i < settings.conns; i++) {
        // STILL NEED THIS! Buffers used for storing data read from KVS
        if ((buf = malloc(BUFSIZE * 2)) == NULL) {
            fprintf(stderr, "[%d] allocating conn buffer failed\n", cn);
        }
        c->conns[i].rx_buf = buf;
        c->conns[i].tx_buf = buf + BUFSIZE;
        c->conns[i].state = CONN_CONNECTING;
        c->conns[i].fd = -1;
        c->conn_pending++;
    }
    fprintf(stderr, "core ready: %d\n", cn);

}

// wait for all connections to be established 
static void wait_conns(struct core *c)
{
    uint32_t i;

    for (i = 0; i < settings.conns; i++) {
        while (c->conns[i].state == CONN_CONNECTING) {
            poll_conns(c, 0);
        }
    }
}

// wait for all connections to be established 
static void load_keys(struct core *c, struct item_allocator *ia)
{
    int cn;
    size_t i;
    struct key *k;
    cn = c->id;

    i = cn;
    //pending = 0;
    while (i < workload1.keys_num) {
        // send out new request 
        //printf("[%d:%p] i=%zu  keys_num=%zu\n", cn, co, i, workload1.keys_num);
        k = &workload1.keys[i];
        set_request(c, &c->conns[0], k, 0, ia);
        i += settings.threads;
    }
}

#ifdef DEL_TEST
// wait for all connections to be established 
static void load_other_keys(struct core *c)
{
    int cn, ret, l;
    size_t i, j, pending;
    struct key *k;
    struct connection *co;
    ss_epev_t evs[8];
    uint32_t events, opaque;
    uint16_t err;
    enum workload_op op;
    int rng;

    cn = c->id;

    // set connections to trigger epoll OUT events 
    for (i = 0; i < MIN(32, settings.conns); i++) {
        conn_epupdate(c, &c->conns[i], 1);
    }

    i = j = cn;
    pending = 0;
    while (i < workload1.keys_num || j < workload2.keys_num || pending > 0) {
        if ((ret = ss_epoll_wait(c->sc, c->ep, evs, 8, -1)) < 0) {
            fprintf(stderr, "[%d] load_keys epoll_wait failed\n", cn);
            abort();
        }

	//printf("epoll returned on %d with %d\n", cn, ret);
        for (l = 0; l < ret; l++) {
            co = evs[l].data.ptr;
            events = evs[l].events;

            // check for errors on the connection 
            if ((events & (SS_EPOLLERR | SS_EPOLLHUP)) != 0) {
                fprintf(stderr, "[%d] load_keys error on conn\n", cn);
                perror("epoll");
		abort();
            }

            if ((events & SS_EPOLLIN) == SS_EPOLLIN) {
                // data ready to receive 
                if (conn_recv(c, co, &op, &err, &opaque) == 0) {
                    if (err != 0) {
                        fprintf(stderr, "[%d] load_keys set failed:%x\n", cn, err);
                    }
                    pending--;
                    co->pending--;
                    conn_epupdate(c, co, 1);
                }
		//printf("received data, pending %d\n", pending);
            } else {
                // ready to send 
                assert((events & SS_EPOLLOUT) == SS_EPOLLOUT);
                if (co->tx_len == 0 && i < workload2.keys_num) {
                    // send out new request 
                    printf("[%d:%p] i=%zu j=%zu  keys_num=%zu\n", cn, co, i, j, workload2.keys_num);
                    rng = (rand() % 10) + 1;
	
		    if(i == workload1.keys_num) rng = 10;

		    if(j == workload2.keys_num) rng = 1;

	  	    if(rng < 10*(1-DEL_RATIO))	    
		    	k = &workload1.keys[i];
		    else
			k = &workload2.keys[j];

		    set_request(c, co, k, 0);
                    if (conn_send(c, co) == 0) {
                        // fully sent out -> poll for RX 
                        conn_epupdate(c, co, 0);
                    }

	  	    if(rng < 10*(1-DEL_RATIO))	    
                    	i += settings.threads;
		    else
			j += settings.threads;

                    pending++;
                    co->pending++;
                } else if (co->tx_len == 0) {
                    // no more keys to initialize -> poll for RX 
		    //printf("no more keys, i=%d, workload1=%d, pending=%d\n", i, workload2.key_num, pending);
                    conn_epupdate(c, co, 0);
                } else {
                    if (conn_send(c, co) == 0) {
                        // fully sent out -> poll for RX 
			//printf("can't send\n");
                        conn_epupdate(c, co, 0);
                    }
		    //printf("tried  send\n");
                }
            }

        }
    }

    // set connections to trigger epoll OUT events 
    /*for (i = 0; i < settings.conns; i++) {
        conn_epupdate(c, &c->conns[i], 1);
    }*/
}

// wait for all connections to be established 
static void delete_keys(struct core *c)
{
    int cn, ret, l;
    size_t i, pending;
    struct key *k;
    struct connection *co;
    ss_epev_t evs[8];
    uint32_t events, opaque;
    uint16_t err;
    enum workload_op op;

    cn = c->id;

    // set connections to trigger epoll OUT events 
    for (i = 0; i < MIN(32, settings.conns); i++) {
        conn_epupdate(c, &c->conns[i], 1);
    }

    i = cn;
    pending = 0;
    while (i < workload2.keys_num || pending > 0) {
        if ((ret = ss_epoll_wait(c->sc, c->ep, evs, 8, -1)) < 0) {
            fprintf(stderr, "[%d] load_keys epoll_wait failed\n", cn);
            abort();
        }
	//printf("epoll returned on %d with %d\n", cn, ret);

        for (l = 0; l < ret; l++) {
            co = evs[l].data.ptr;
            events = evs[l].events;

            // check for errors on the connection 
            if ((events & (SS_EPOLLERR | SS_EPOLLHUP)) != 0) {
                fprintf(stderr, "[%d] delete_keys error on conn\n", cn);
                perror("epoll");
		abort();
            }

            if ((events & SS_EPOLLIN) == SS_EPOLLIN) {
                // data ready to receive 
                if (conn_recv(c, co, &op, &err, &opaque) == 0) {
                    if (err != 0) {
                        fprintf(stderr, "[%d] load_keys set failed:%x\n", cn, err);
                    }
                    pending--;
                    co->pending--;
                    conn_epupdate(c, co, 1);
		    //printf("received data, pending %d\n", pending);
                } else printf("conn_recv failed\n");
            } else {
                // ready to send 
                assert((events & SS_EPOLLOUT) == SS_EPOLLOUT);
                if (co->tx_len == 0 && i < workload2.keys_num) {
                    // send out new request 
                    //printf("[%d:%p] i=%zu  keys_num=%zu\n", cn, co, i, workload2.keys_num);
                    k = &workload2.keys[i];
                    delete_request(c, co, k, 0);
                    if (conn_send(c, co) == 0) {
                        // fully sent out -> poll for RX 
                        conn_epupdate(c, co, 0);
                    }
                    i += settings.threads;
                    pending++;
                    co->pending++;
                } else if (co->tx_len == 0) {
                    // no more keys to initialize -> poll for RX 
                    conn_epupdate(c, co, 0);
		    //printf("no more keys\n");
                } else {
                    if (conn_send(c, co) == 0) {
                        // fully sent out -> poll for RX 
			//printf("can't send\n");
                        conn_epupdate(c, co, 0);
                    }
		    //printf("tried  send\n");
                }
            }

        }
    }
}
#endif


static inline void conn_events(struct core *c, struct connection *co,
        uint32_t events)
{
    int cn;
    uint32_t opaque;
    uint16_t err;
    enum workload_op op = 0;

    cn = c->id;

    // check for errors on the connection 
    if ((events & (SS_EPOLLERR | SS_EPOLLHUP)) != 0) {
        fprintf(stderr, "[%d] error on connection\n", cn);
        abort();
    }

    // receive responses 
    while (co->pending > 0 && conn_recv(c, co, &op, &err, &opaque) == 0) {
        record_latency(c, (uint32_t) get_nanos() - opaque);

        STATS_ADD(co, cnt, 1);
        if (op == WL_OP_GET) {
            STATS_ADD(c, rx_get, 1);
        } else {
            STATS_ADD(c, rx_set, 1);
        }
        if (err == 0) {
            STATS_ADD(c, rx_success, 1);
        } else {
            STATS_ADD(c, rx_fail, 1);
            fprintf(stderr, "[%d] load_keys set failed:%x\n", cn, err);
        }
        co->pending--;
        c->msgs_pending--;

        if (co->pending == 0) {
          co->next = NULL;
          if (c->q_last == NULL) {
            c->q_last = co;
            c->q_first = co;
          } else {
            c->q_last->next = co;
            c->q_last = co;
          }
        }
    }

    // try to send out any remaining tx buffer contents 
    if (co->tx_len != 0) {
        conn_send(c, co);
    }

    // make sure we epoll for write iff we're actually blocked on writes 
    conn_epupdate(c, co, co->tx_len != 0);

}

static inline void send_pending(struct core *c, struct item_allocator *ia)
{
    //struct connection *co;
    uint32_t opaque;
    enum workload_op op = 0;
    struct key *k;

    // pick a key and operation 
    if(phase != BENCHMARK_DYN_HOTSET)
        workload_op(&workload1, &c->wlc, &k, &op);
    else
        workload_op(&workload2, &c->wlc, &k, &op);

    // assign a time stamp 
    opaque = get_nanos();

    // assemble request 
    if (op == WL_OP_GET) {
        get_request(c, &c->conns[0], k, opaque, ia);
        STATS_ADD(c, tx_get, 1);
        STATS_ADD(c, rx_get, 1);
    } else {
        set_request(c, &c->conns[0], k, opaque, ia);
        STATS_ADD(c, tx_set, 1);
        STATS_ADD(c, rx_set, 1);
    }
    // Arbitrarily mark as success, I guess
    STATS_ADD(c, rx_success, 1);
    record_latency(c, (uint32_t) get_nanos() - opaque);
}


static void *thread_run(void *arg)
{
    struct item_allocator ia;
    struct core *c = arg;
    int i, cn, ret, ep, num_evs;
    ss_epev_t *evs;

    // initiate core struct and connections 
    prepare_core(c);

    cn = c->id;
    ialloc_init_allocator(&ia);

   // wait until we start running 
    while (phase < BENCHMARK_PRELOAD) {
        sched_yield();
    }

    printf("[%d] Preloading keys...\n", cn);
    fflush(stdout);
    // pre-load keys 
#ifdef DEL_TEST
    printf("[%d] Preloading more keys...\n", cn);
    load_other_keys(c);
#else
    if (!skip_load) {
        load_keys(c, &ia);
    }
#endif

    printf("[%d] Preloaded keys...\n", cn);
    fflush(stdout);

#ifdef DEL_TEST
    printf("[%d] Deleting keys...\n", cn);
    delete_keys(c);
    printf("[%d] Deleted keys...\n", cn);
#endif


    __sync_fetch_and_add(&init_count, 1);

   // wait until we start warmup 
    while (phase < BENCHMARK_WARMUP) {
        sched_yield();
    }

    num_evs = 32;
    if ((evs = calloc(num_evs, sizeof(*evs))) == NULL) {
        fprintf(stderr, "[%d] malloc failed\n", cn);
        abort();
    }

    // wait until we start running 
    while (phase < BENCHMARK_RUNNING) {
        sched_yield();
    }
    printf("[%d] Start running...\n", cn);
    fflush(stdout);

    while (phase == BENCHMARK_RUNNING || phase == BENCHMARK_DYN_HOTSET) {
        send_pending(c, &ia);
    }

    return NULL;
}

static inline void hist_fract_buckets(uint32_t *hist, uint64_t total,
        double *fracs, size_t *idxs, size_t num)
{
    size_t i, j;
    uint64_t sum = 0, goals[num];
    for (j = 0; j < num; j++) {
        goals[j] = total * fracs[j];
    }
    for (i = 0, j = 0; i < HIST_BUCKETS && j < num; i++) {
        sum += hist[i];
        for (; j < num && sum >= goals[j]; j++) {
            idxs[j] = i;
        }
    }
}

static inline int hist_value(size_t i)
{
    if (i == HIST_BUCKETS - 1) {
        return -1;
    }

    return i * HIST_BUCKET_NS + HIST_START_NS;
}

int main(int argc, char *argv[])
{
#ifdef USE_MTCP
    int ret;
#endif
    int i, j, num_threads;
    struct core *cs;
    uint64_t t_prev, t_cur, t_start;
    long double *ttp, tp, tp_total;
    uint32_t *hist, *glbl_hist, hx;
    uint64_t msg_total;
    double fracs[6] = { 0.5, 0.9, 0.95, 0.99, 0.999, 0.9999 };
    size_t fracs_pos[sizeof(fracs) / sizeof(fracs[0])];

    setlocale(LC_NUMERIC, "");

    // parse settings from command line 
    //fprintf(stderr, "settings1 %d\n", settings.valuesize);

    init_settings(&settings);
    if (parse_settings(argc, argv, &settings) != 0) {
        print_usage();
        return EXIT_FAILURE;
    }
    num_threads = settings.threads;
    skip_load = settings.skip_load;

    printf("Running for %d seconds\n", settings.run_time);

    // initialize workload 1
    workload_init(&workload1);

    if(settings.dyn_hotset_time)
        workload_init_dyn(&workload1, &workload2);

    printf("initiating hash table\n");
    hasht_init(settings.hasht_size);
    printf("initiating ialloc\n");
    ialloc_init();

    //iallocs = calloc(num_threads, sizeof(*iallocs));

#ifdef DEL_TEST
    workload_adjust(&workload1, &workload2);
#endif

#ifdef USE_MTCP
    if ((ret = mtcp_init("/tmp/mtcp.conf")) != 0) {
        fprintf(stderr, "mtcp_init failed: %d\n", ret);
        return EXIT_FAILURE;
    }
#endif

    srand(0);
    // allocate core structs 
    assert(sizeof(*cs) % 64 == 0);
    cs = calloc(num_threads, sizeof(*cs));

    // allocate instrumentation structs 
    ttp = calloc(num_threads, sizeof(*ttp));
    hist = calloc(HIST_BUCKETS, sizeof(*hist));
    glbl_hist = calloc(HIST_BUCKETS, sizeof(*glbl_hist));

    if (cs == NULL || ttp == NULL || hist == NULL) {
        fprintf(stderr, "allocation failed failed\n");
        return EXIT_FAILURE;
    }

    phase = BENCHMARK_INIT;

    for (i = 0; i < num_threads; i++) {
        cs[i].id = i;
        workload_core_init(&workload1, &cs[i].wlc);
        if (pthread_create(&cs[i].pthread, NULL, thread_run, cs + i)) {
            fprintf(stderr, "pthread_create failed\n");
            return EXIT_FAILURE;
        }
    }

    //sleep(10);
    phase = BENCHMARK_PRELOAD;
    //sleep(10);
    phase = BENCHMARK_WARMUP;

    while (init_count < num_threads) {
        sched_yield();
    }
    printf("Preloading completed\n");
    fflush(stdout);
    phase = BENCHMARK_RUNNING;

    //sleep(settings.warmup_time);


    t_start = t_prev = get_nanos();
    uint64_t glbl_ops = 0;
    uint64_t current_runtime = 0, warmup_time = settings.warmup_time;
    while (current_runtime < settings.run_time) {
        sleep(1);
        ++current_runtime;
        if(current_runtime == warmup_time)
            printf("Warmup complete\n");

        if(settings.dyn_hotset_time && 
            current_runtime == settings.dyn_hotset_time) {
            printf("Dynamically changing hotset to %f%%\n", 
                settings.dyn_hotset_size);
            phase = BENCHMARK_DYN_HOTSET;
        }
        t_cur = get_nanos();
        tp_total = 0;
        msg_total = 0;
        for (i = 0; i < num_threads; i++) {
            tp = read_cnt(&cs[i].rx_success);
            if(current_runtime >= warmup_time)
                glbl_ops += tp;
            tp /= (double) (t_cur - t_prev) / 1000000000.;
            ttp[i] = tp;
            tp_total += tp;

            for (j = 0; j < HIST_BUCKETS; j++) {
                hx = cs[i].hist[j];
                msg_total += hx;
                hist[j] += hx;
                if(current_runtime >= warmup_time)
                    glbl_hist[j] += hx;
                cs[i].hist[j] = 0;
            }
        }

        hist_fract_buckets(hist, msg_total, fracs, fracs_pos,
                sizeof(fracs) / sizeof(fracs[0]));


        printf("seconds=%ld TP: total=%'.4Lf mops  50p=%d us  90p=%d us  95p=%d us  "
                "99p=%d us  99.9p=%d us  99.99p=%d us  \n",
                current_runtime,
                tp_total / 1000000.,
                hist_value(fracs_pos[0]), hist_value(fracs_pos[1]),
                hist_value(fracs_pos[2]), hist_value(fracs_pos[3]),
                hist_value(fracs_pos[4]), hist_value(fracs_pos[5]));
        fflush(stdout);
        memset(hist, 0, sizeof(*hist) * HIST_BUCKETS);

        t_prev = t_cur;
    }
    phase = BENCHMARK_COOLDOWN;

    for (i = 0; i < num_threads; i++)
        pthread_join(cs[i].pthread, NULL);

    phase = BENCHMARK_DONE;

    printf("Final throughput = %.4f mops\n", (double)glbl_ops * 1000. / 
        (double)(get_nanos() - t_start - warmup_time * 1000000000UL));
    for(i = 0; i < HIST_BUCKETS; ++i)
        if(glbl_hist[i] != 0)
            printf("Hist[%d]=%d\n", i*HIST_BUCKET_NS, glbl_hist[i]);

#ifdef USE_MTCP
    mtcp_destroy();
#endif
}
