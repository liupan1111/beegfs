/* SPDX-License-Identifier: BSD-3-Clause
 *
 * Standalone adaptation of the DPDK rte_ring pointer queue.
 *
 * It keeps the DPDK-style API and the classic head/tail algorithm, while
 * replacing DPDK EAL dependencies with C11 atomics and posix_memalign.
 */
#include "rte_ring_standalone.h"

#include <errno.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sched.h>

#ifndef RTE_CACHE_LINE_SIZE
#define RTE_CACHE_LINE_SIZE 64
#endif

enum queue_behavior {
    RTE_RING_QUEUE_FIXED,
    RTE_RING_QUEUE_VARIABLE
};

enum ring_sync_type {
    RTE_RING_SYNC_MT,
    RTE_RING_SYNC_ST
};

struct rte_ring_headtail {
    atomic_uint_least32_t head;
    atomic_uint_least32_t tail;
    enum ring_sync_type sync_type;
};

struct rte_ring {
    alignas(RTE_CACHE_LINE_SIZE) char name[RTE_RING_NAMESIZE];
    unsigned int flags;
    uint32_t size;
    uint32_t mask;
    uint32_t capacity;

    alignas(RTE_CACHE_LINE_SIZE) struct rte_ring_headtail prod;
    alignas(RTE_CACHE_LINE_SIZE) struct rte_ring_headtail cons;

    alignas(RTE_CACHE_LINE_SIZE) void *ring[];
};

static int
is_power_of_2(unsigned int x)
{
    return x != 0 && (x & (x - 1)) == 0;
}

static uint32_t
next_power_of_2(uint32_t x)
{
    if (x <= 2)
        return 2;

    x--;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    return x + 1;
}

ssize_t
rte_ring_get_memsize(unsigned int count)
{
    uint32_t size;

    if (count == 0 || count > 0x7fffffffU) {
        errno = EINVAL;
        return -1;
    }

    size = is_power_of_2(count) ? count : next_power_of_2(count + 1);
    return (ssize_t)(sizeof(struct rte_ring) + sizeof(void *) * size);
}

struct rte_ring *
rte_ring_create(const char *name, unsigned int count, int socket_id,
                unsigned int flags)
{
    struct rte_ring *r;
    uint32_t size;
    size_t memsz;

    (void)socket_id;

    if (count == 0 || count > 0x7fffffffU) {
        errno = EINVAL;
        return NULL;
    }

    if (flags & RING_F_EXACT_SZ) {
        size = next_power_of_2(count + 1);
    } else {
        if (!is_power_of_2(count)) {
            errno = EINVAL;
            return NULL;
        }
        size = count;
    }

    memsz = sizeof(*r) + sizeof(void *) * size;
    if (posix_memalign((void **)&r, RTE_CACHE_LINE_SIZE, memsz) != 0)
        return NULL;

    memset(r, 0, memsz);
    if (name != NULL) {
        strncpy(r->name, name, sizeof(r->name) - 1);
        r->name[sizeof(r->name) - 1] = '\0';
    }

    r->flags = flags;
    r->size = size;
    r->mask = size - 1;
    r->capacity = (flags & RING_F_EXACT_SZ) ? count : size - 1;
    r->prod.sync_type = (flags & RING_F_SP_ENQ) ?
        RTE_RING_SYNC_ST : RTE_RING_SYNC_MT;
    r->cons.sync_type = (flags & RING_F_SC_DEQ) ?
        RTE_RING_SYNC_ST : RTE_RING_SYNC_MT;
    atomic_init(&r->prod.head, 0);
    atomic_init(&r->prod.tail, 0);
    atomic_init(&r->cons.head, 0);
    atomic_init(&r->cons.tail, 0);

    return r;
}

void
rte_ring_free(struct rte_ring *r)
{
    free(r);
}

unsigned int
rte_ring_get_size(const struct rte_ring *r)
{
    return r->size;
}

unsigned int
rte_ring_get_capacity(const struct rte_ring *r)
{
    return r->capacity;
}

unsigned int
rte_ring_count(const struct rte_ring *r)
{
    uint32_t prod_tail = atomic_load_explicit(&r->prod.tail,
                                             memory_order_acquire);
    uint32_t cons_tail = atomic_load_explicit(&r->cons.tail,
                                             memory_order_acquire);
    uint32_t count = (prod_tail - cons_tail) & r->mask;

    return count > r->capacity ? r->capacity : count;
}

unsigned int
rte_ring_free_count(const struct rte_ring *r)
{
    return r->capacity - rte_ring_count(r);
}

int
rte_ring_empty(const struct rte_ring *r)
{
    return atomic_load_explicit(&r->cons.tail, memory_order_acquire) ==
           atomic_load_explicit(&r->prod.tail, memory_order_acquire);
}

int
rte_ring_full(const struct rte_ring *r)
{
    return rte_ring_free_count(r) == 0;
}

int
rte_ring_is_prod_single(const struct rte_ring *r)
{
    return r->prod.sync_type == RTE_RING_SYNC_ST;
}

int
rte_ring_is_cons_single(const struct rte_ring *r)
{
    return r->cons.sync_type == RTE_RING_SYNC_ST;
}

static void
enqueue_ptrs(struct rte_ring *r, const void *obj_table, uint32_t prod_head,
             unsigned int n)
{
    const void * const *obj = obj_table;
    uint32_t idx = prod_head & r->mask;
    unsigned int i;

    for (i = 0; i < n && idx < r->size; i++, idx++)
        r->ring[idx] = (void *)obj[i];

    for (idx = 0; i < n; i++, idx++)
        r->ring[idx] = (void *)obj[i];
}

static void
dequeue_ptrs(struct rte_ring *r, void **obj_table, uint32_t cons_head,
             unsigned int n)
{
    uint32_t idx = cons_head & r->mask;
    unsigned int i;

    for (i = 0; i < n && idx < r->size; i++, idx++)
        obj_table[i] = r->ring[idx];

    for (idx = 0; i < n; i++, idx++)
        obj_table[i] = r->ring[idx];
}

static unsigned int
move_prod_head(struct rte_ring *r, unsigned int n, enum queue_behavior behavior,
               enum ring_sync_type sync_type, uint32_t *old_head,
               uint32_t *new_head, uint32_t *free_entries)
{
    unsigned int max = n;
    uint32_t cons_tail;

    *old_head = atomic_load_explicit(&r->prod.head,
        sync_type == RTE_RING_SYNC_ST ? memory_order_relaxed :
        memory_order_acquire);

    do {
        n = max;
        cons_tail = atomic_load_explicit(&r->cons.tail,
                                         memory_order_acquire);
        *free_entries = r->capacity + cons_tail - *old_head;

        if (n > *free_entries)
            n = behavior == RTE_RING_QUEUE_FIXED ? 0 : *free_entries;
        if (n == 0)
            return 0;

        *new_head = *old_head + n;

        if (sync_type == RTE_RING_SYNC_ST) {
            atomic_store_explicit(&r->prod.head, *new_head,
                                  memory_order_relaxed);
            return n;
        }
    } while (!atomic_compare_exchange_weak_explicit(&r->prod.head, old_head,
               *new_head, memory_order_acquire, memory_order_relaxed));

    return n;
}

static unsigned int
move_cons_head(struct rte_ring *r, unsigned int n, enum queue_behavior behavior,
               enum ring_sync_type sync_type, uint32_t *old_head,
               uint32_t *new_head, uint32_t *entries)
{
    unsigned int max = n;
    uint32_t prod_tail;

    *old_head = atomic_load_explicit(&r->cons.head,
        sync_type == RTE_RING_SYNC_ST ? memory_order_relaxed :
        memory_order_acquire);

    do {
        n = max;
        prod_tail = atomic_load_explicit(&r->prod.tail,
                                         memory_order_acquire);
        *entries = prod_tail - *old_head;

        if (n > *entries)
            n = behavior == RTE_RING_QUEUE_FIXED ? 0 : *entries;
        if (n == 0)
            return 0;

        *new_head = *old_head + n;

        if (sync_type == RTE_RING_SYNC_ST) {
            atomic_store_explicit(&r->cons.head, *new_head,
                                  memory_order_relaxed);
            return n;
        }
    } while (!atomic_compare_exchange_weak_explicit(&r->cons.head, old_head,
               *new_head, memory_order_acquire, memory_order_relaxed));

    return n;
}

static void
update_tail(atomic_uint_least32_t *tail, uint32_t old_head, uint32_t new_head,
            enum ring_sync_type sync_type)
{
    if (sync_type == RTE_RING_SYNC_MT) {
        while (atomic_load_explicit(tail, memory_order_acquire) != old_head)
            sched_yield();
    }

    atomic_store_explicit(tail, new_head, memory_order_release);
}

static unsigned int
do_enqueue(struct rte_ring *r, const void *obj_table, unsigned int n,
           enum queue_behavior behavior, enum ring_sync_type sync_type,
           unsigned int *free_space)
{
    uint32_t prod_head, prod_next;
    uint32_t free_entries;

    n = move_prod_head(r, n, behavior, sync_type, &prod_head, &prod_next,
                       &free_entries);
    if (n == 0) {
        if (free_space != NULL)
            *free_space = free_entries;
        return 0;
    }

    enqueue_ptrs(r, obj_table, prod_head, n);
    update_tail(&r->prod.tail, prod_head, prod_next, sync_type);

    if (free_space != NULL)
        *free_space = free_entries - n;

    return n;
}

static unsigned int
do_dequeue(struct rte_ring *r, void **obj_table, unsigned int n,
           enum queue_behavior behavior, enum ring_sync_type sync_type,
           unsigned int *available)
{
    uint32_t cons_head, cons_next;
    uint32_t entries;

    n = move_cons_head(r, n, behavior, sync_type, &cons_head, &cons_next,
                       &entries);
    if (n == 0) {
        if (available != NULL)
            *available = entries;
        return 0;
    }

    dequeue_ptrs(r, obj_table, cons_head, n);
    update_tail(&r->cons.tail, cons_head, cons_next, sync_type);

    if (available != NULL)
        *available = entries - n;

    return n;
}

unsigned int
rte_ring_mp_enqueue_bulk(struct rte_ring *r, void *const *obj_table,
                         unsigned int n, unsigned int *free_space)
{
    return do_enqueue(r, obj_table, n, RTE_RING_QUEUE_FIXED,
                      RTE_RING_SYNC_MT, free_space);
}

unsigned int
rte_ring_sp_enqueue_bulk(struct rte_ring *r, void *const *obj_table,
                         unsigned int n, unsigned int *free_space)
{
    return do_enqueue(r, obj_table, n, RTE_RING_QUEUE_FIXED,
                      RTE_RING_SYNC_ST, free_space);
}

unsigned int
rte_ring_enqueue_bulk(struct rte_ring *r, void *const *obj_table,
                      unsigned int n, unsigned int *free_space)
{
    return do_enqueue(r, obj_table, n, RTE_RING_QUEUE_FIXED,
                      r->prod.sync_type, free_space);
}

unsigned int
rte_ring_mp_enqueue_burst(struct rte_ring *r, void *const *obj_table,
                          unsigned int n, unsigned int *free_space)
{
    return do_enqueue(r, obj_table, n, RTE_RING_QUEUE_VARIABLE,
                      RTE_RING_SYNC_MT, free_space);
}

unsigned int
rte_ring_sp_enqueue_burst(struct rte_ring *r, void *const *obj_table,
                          unsigned int n, unsigned int *free_space)
{
    return do_enqueue(r, obj_table, n, RTE_RING_QUEUE_VARIABLE,
                      RTE_RING_SYNC_ST, free_space);
}

unsigned int
rte_ring_enqueue_burst(struct rte_ring *r, void *const *obj_table,
                       unsigned int n, unsigned int *free_space)
{
    return do_enqueue(r, obj_table, n, RTE_RING_QUEUE_VARIABLE,
                      r->prod.sync_type, free_space);
}

int
rte_ring_mp_enqueue(struct rte_ring *r, void *obj)
{
    return rte_ring_mp_enqueue_bulk(r, &obj, 1, NULL) == 1 ? 0 : -ENOBUFS;
}

int
rte_ring_sp_enqueue(struct rte_ring *r, void *obj)
{
    return rte_ring_sp_enqueue_bulk(r, &obj, 1, NULL) == 1 ? 0 : -ENOBUFS;
}

int
rte_ring_enqueue(struct rte_ring *r, void *obj)
{
    return rte_ring_enqueue_bulk(r, &obj, 1, NULL) == 1 ? 0 : -ENOBUFS;
}

unsigned int
rte_ring_mc_dequeue_bulk(struct rte_ring *r, void **obj_table,
                         unsigned int n, unsigned int *available)
{
    return do_dequeue(r, obj_table, n, RTE_RING_QUEUE_FIXED,
                      RTE_RING_SYNC_MT, available);
}

unsigned int
rte_ring_sc_dequeue_bulk(struct rte_ring *r, void **obj_table,
                         unsigned int n, unsigned int *available)
{
    return do_dequeue(r, obj_table, n, RTE_RING_QUEUE_FIXED,
                      RTE_RING_SYNC_ST, available);
}

unsigned int
rte_ring_dequeue_bulk(struct rte_ring *r, void **obj_table,
                      unsigned int n, unsigned int *available)
{
    return do_dequeue(r, obj_table, n, RTE_RING_QUEUE_FIXED,
                      r->cons.sync_type, available);
}

unsigned int
rte_ring_mc_dequeue_burst(struct rte_ring *r, void **obj_table,
                          unsigned int n, unsigned int *available)
{
    return do_dequeue(r, obj_table, n, RTE_RING_QUEUE_VARIABLE,
                      RTE_RING_SYNC_MT, available);
}

unsigned int
rte_ring_sc_dequeue_burst(struct rte_ring *r, void **obj_table,
                          unsigned int n, unsigned int *available)
{
    return do_dequeue(r, obj_table, n, RTE_RING_QUEUE_VARIABLE,
                      RTE_RING_SYNC_ST, available);
}

unsigned int
rte_ring_dequeue_burst(struct rte_ring *r, void **obj_table,
                       unsigned int n, unsigned int *available)
{
    return do_dequeue(r, obj_table, n, RTE_RING_QUEUE_VARIABLE,
                      r->cons.sync_type, available);
}

int
rte_ring_mc_dequeue(struct rte_ring *r, void **obj_p)
{
    return rte_ring_mc_dequeue_bulk(r, obj_p, 1, NULL) == 1 ? 0 : -ENOENT;
}

int
rte_ring_sc_dequeue(struct rte_ring *r, void **obj_p)
{
    return rte_ring_sc_dequeue_bulk(r, obj_p, 1, NULL) == 1 ? 0 : -ENOENT;
}

int
rte_ring_dequeue(struct rte_ring *r, void **obj_p)
{
    return rte_ring_dequeue_bulk(r, obj_p, 1, NULL) == 1 ? 0 : -ENOENT;
}
