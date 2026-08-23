/* SPDX-License-Identifier: BSD-3-Clause
 *
 * Standalone adaptation of the DPDK rte_ring pointer queue API.
 * This file intentionally avoids DPDK EAL dependencies.
 */
#ifndef RTE_RING_STANDALONE_H
#define RTE_RING_STANDALONE_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RTE_RING_NAMESIZE 64

#define RING_F_SP_ENQ    0x0001u
#define RING_F_SC_DEQ    0x0002u
#define RING_F_EXACT_SZ  0x0004u

struct rte_ring;

ssize_t rte_ring_get_memsize(unsigned int count);

struct rte_ring *rte_ring_create(const char *name, unsigned int count,
                                 int socket_id, unsigned int flags);
void rte_ring_free(struct rte_ring *r);

unsigned int rte_ring_get_size(const struct rte_ring *r);
unsigned int rte_ring_get_capacity(const struct rte_ring *r);
unsigned int rte_ring_count(const struct rte_ring *r);
unsigned int rte_ring_free_count(const struct rte_ring *r);
int rte_ring_empty(const struct rte_ring *r);
int rte_ring_full(const struct rte_ring *r);
int rte_ring_is_prod_single(const struct rte_ring *r);
int rte_ring_is_cons_single(const struct rte_ring *r);

int rte_ring_mp_enqueue(struct rte_ring *r, void *obj);
int rte_ring_sp_enqueue(struct rte_ring *r, void *obj);
int rte_ring_enqueue(struct rte_ring *r, void *obj);

unsigned int rte_ring_mp_enqueue_bulk(struct rte_ring *r,
                                      void *const *obj_table,
                                      unsigned int n,
                                      unsigned int *free_space);
unsigned int rte_ring_sp_enqueue_bulk(struct rte_ring *r,
                                      void *const *obj_table,
                                      unsigned int n,
                                      unsigned int *free_space);
unsigned int rte_ring_enqueue_bulk(struct rte_ring *r,
                                   void *const *obj_table,
                                   unsigned int n,
                                   unsigned int *free_space);

unsigned int rte_ring_mp_enqueue_burst(struct rte_ring *r,
                                       void *const *obj_table,
                                       unsigned int n,
                                       unsigned int *free_space);
unsigned int rte_ring_sp_enqueue_burst(struct rte_ring *r,
                                       void *const *obj_table,
                                       unsigned int n,
                                       unsigned int *free_space);
unsigned int rte_ring_enqueue_burst(struct rte_ring *r,
                                    void *const *obj_table,
                                    unsigned int n,
                                    unsigned int *free_space);

int rte_ring_mc_dequeue(struct rte_ring *r, void **obj_p);
int rte_ring_sc_dequeue(struct rte_ring *r, void **obj_p);
int rte_ring_dequeue(struct rte_ring *r, void **obj_p);

unsigned int rte_ring_mc_dequeue_bulk(struct rte_ring *r,
                                      void **obj_table,
                                      unsigned int n,
                                      unsigned int *available);
unsigned int rte_ring_sc_dequeue_bulk(struct rte_ring *r,
                                      void **obj_table,
                                      unsigned int n,
                                      unsigned int *available);
unsigned int rte_ring_dequeue_bulk(struct rte_ring *r,
                                   void **obj_table,
                                   unsigned int n,
                                   unsigned int *available);

unsigned int rte_ring_mc_dequeue_burst(struct rte_ring *r,
                                       void **obj_table,
                                       unsigned int n,
                                       unsigned int *available);
unsigned int rte_ring_sc_dequeue_burst(struct rte_ring *r,
                                       void **obj_table,
                                       unsigned int n,
                                       unsigned int *available);
unsigned int rte_ring_dequeue_burst(struct rte_ring *r,
                                    void **obj_table,
                                    unsigned int n,
                                    unsigned int *available);

#ifdef __cplusplus
}
#endif

#endif /* RTE_RING_STANDALONE_H */
