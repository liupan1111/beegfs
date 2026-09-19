# BeeGFS Single-Replica RDMA Send CQ Asynchronization

## 1. Scope

This document changes the completion handling of RDMA data operations issued by a
storage IO Worker for the single-replica client-to-storage path.

In scope:

- `ReadLocalFileRDMA`: local disk read followed by RDMA WRITE to the client.
- `WriteLocalFileRDMA`: RDMA READ from the client followed by local disk write.
- The `BEEGFS_NVFS` libaio path, `AsyncRDMARequest`, `IBVSocket`, and the IO
  Worker's existing epoll loop.

Out of scope:

- Primary-secondary mirror traffic and connection pools.
- Replacing libaio with io_uring.
- Batching `io_submit()`.
- MR/PD redesign.
- Making normal BeeGFS control-message SEND operations asynchronous.

The objective is narrow: remove the busy wait for the completion of the data
RDMA operation from the IO Worker hot path, while retaining the existing socket
ownership and listener-worker protocol.

## 2. Current Behavior And Cost

`AsyncRDMARequest` makes disk I/O asynchronous, but calls the synchronous RDMA
socket API for the data transfer:

```
ReadLocalFileRDMA:
  libaio pread completion -> RDMA WRITE -> poll send CQ until complete

WriteLocalFileRDMA:
  RDMA READ -> poll send CQ until complete -> libaio pwrite
```

The synchronous RDMA API reaches `__IBVSocket_postRDMA()`. It posts a signaled
work request and immediately calls `__IBVSocket_waitForRDMACompletion()`, which
continuously calls `ibv_poll_cq(sendCQ, ...)` until the matching work request
ID appears. Therefore, if `io_getevents()` returns several completed disk I/Os,
the Worker still serially burns CPU waiting for each corresponding network
completion before it can advance the next request.

The send CQ already exists per QP, but it was created without a completion
channel. The receive CQ is event-driven; the send CQ is not.

## 3. Design Principles

1. A data-RDMA completion is a state-machine event, not a reason to busy poll.
2. A socket remains owned by one `AsyncRDMARequest` until that request either
   returns it to the listener or invalidates it. The listener does not re-arm
   the socket during this interval.
3. A socket has at most one outstanding data RDMA operation in this design.
   This matches the existing request flow and makes the mapping from a send CQ
   completion to its `AsyncRDMARequest` unambiguous.
4. Existing control SEND behavior stays intact. If a control SEND completion is
   observed while draining a data operation's send CQ, its existing
   `incompleteSend.numAvailable` accounting must still be performed.
5. Completion notification must use the normal verbs arm-then-poll sequence so
   that a completion cannot be lost in the interval between polling and
   sleeping in `epoll_wait()`.

## 4. Target Architecture

Each connected IBV socket gains a completion channel for its send CQ. When an
`AsyncRDMARequest` starts data transfer on that socket, its channel FD is
registered dynamically in the owning IO Worker's epoll instance. The
registration stays in place for the entire request, including all of its data
chunks, and is removed immediately before the socket is returned to the
listener or invalidated.

```
                         IO Worker epoll
                +--------------------------------+
request eventfd | high-priority queue             |
                | request queue                   |
libaio eventfd  | libaio completion eventfd        |
send-CQ fd      | AsyncRDMARequest send CQ source  |
                +----------------+---------------+
                                 |
                                 v
                 AsyncRDMARequest state machine
```

The send-CQ epoll source belongs to the active `AsyncRDMARequest`; it contains
the request pointer and the socket's send completion-channel FD. Its lifetime
therefore covers both epoll registration and completion handling. It is removed
from epoll before the request completes and can be destroyed.

This is deliberately a dynamic registration model. Keeping every connected
socket's send CQ permanently in every Worker epoll would conflict with dynamic
listener-to-worker scheduling and would make socket ownership difficult to
reason about.

## 5. Data Paths

### 5.1 ReadLocalFileRDMA

```
request ring
  -> AsyncRDMARequest::start()
  -> libaio pread submitted                         [AIO_PENDING]
  -> libaio completion eventfd
  -> reap AIO completion
  -> register CQ fd once, post RDMA WRITE, arm CQ  [RDMA_WRITE_PENDING]
  -> send-CQ completion eventfd
  -> drain send CQ and validate matching WR
  -> advance local/remote offsets
  -> next pread, or send read-length response
  -> response ring -> listener re-arms socket
```

The read buffer remains owned by the request until the RDMA WRITE completion is
received. It must not be returned to `IOWorkerAsyncContext`'s buffer pool when
the disk read completes.

### 5.2 WriteLocalFileRDMA

```
request ring
  -> AsyncRDMARequest::start()
  -> register CQ fd once, post RDMA READ, arm CQ   [RDMA_READ_PENDING]
  -> send-CQ completion eventfd
  -> drain send CQ and validate matching WR
  -> libaio pwrite submitted                       [AIO_PENDING]
  -> libaio completion eventfd
  -> reap AIO completion
  -> next RDMA READ, or send write response
  -> response ring -> listener re-arms socket
```

The disk write must not be submitted until the RDMA READ completion confirms
that the client data has arrived in the Worker buffer.

## 6. State Machine Changes

`AsyncRDMARequest` gains two explicit pending states:

```
INIT
  -> AIO_PENDING
  -> RDMA_WRITE_PENDING   (read path)
  -> RDMA_READ_PENDING    (write path)
  -> DONE
```

The legal transitions are:

- Read: `INIT -> AIO_PENDING -> RDMA_WRITE_PENDING -> INIT` for each chunk.
- Write: `INIT -> RDMA_READ_PENDING -> AIO_PENDING -> INIT` for each chunk.
- Any pending state can transition to `DONE` on a terminal error or shutdown.

`onAIOComplete()` handles only `AIO_PENDING`. A new
`onSendCQComplete()` handles only `RDMA_READ_PENDING` or
`RDMA_WRITE_PENDING`. Receiving an event in another state is an invariant
violation; the request/socket is treated as failed rather than being advanced.

## 7. Verbs And Epoll Protocol

### 7.1 Send CQ creation

`IBVCommContext` gains:

- `sendCompChannel`
- a count of unacknowledged send completion-channel events

`sendCQ` is created with `sendCompChannel` instead of `NULL`. Destruction order
is QP, send CQ, then send completion channel, matching the receive-side
resource relationship.

### 7.2 Posting an asynchronous data RDMA operation

The IBV socket layer gets a nonblocking data-RDMA operation that:

1. obtains the local lkey using the existing path when necessary;
2. allocates a unique `wr_id`;
3. posts one signaled RDMA READ or RDMA WRITE;
4. requests notification on the socket's send CQ;
5. immediately polls the send CQ once;
6. returns either a completed status or a pending `wr_id`.

The Worker registers the send completion FD once, before its first data RDMA
operation. If the immediate poll finds the matching completion, the request
advances inline; the registration remains available for the request's next
chunk. It is removed only when the request reaches its terminal state.

The precise order is important:

```
ibv_req_notify_cq(sendCQ, 0)
ibv_poll_cq(sendCQ, ...)
if matching completion was not found:
    epoll_wait(send completion channel FD)
```

Arming before polling closes the completion race. Completion-channel events are
retrieved with `ibv_get_cq_event()` and later acknowledged with
`ibv_ack_cq_events()`. After each event-driven drain, the CQ is re-armed and
polled again before the Worker relies on epoll.

### 7.3 Completion drain

The send-CQ handler drains all available work completions from that socket's
CQ. It distinguishes:

- matching `IBV_WC_RDMA_READ` / `IBV_WC_RDMA_WRITE`: complete the pending
  `AsyncRDMARequest` transition;
- `IBV_WC_SEND`: preserve existing `incompleteSend.numAvailable` decrement;
- an unexpected work-request ID, opcode, or non-success status: fail the
  socket/request conservatively.

There is one outstanding data RDMA request per socket, so no global
`cqCompletions` map or CQ mutex is needed for this asynchronous path. Existing
synchronous callers continue to use their existing behavior until they are
explicitly converted.

## 8. Worker Integration

`Worker::IOEventSource` gains a `SEND_CQ` type. The source identifies the
active `AsyncRDMARequest`, rather than a queue.

On `SEND_CQ` readiness the Worker:

1. obtains and acknowledges the completion-channel event;
2. asks the request to drain and process its send CQ;
3. lets the request submit its next AIO/RDMA step or finish normally;
4. removes the send-CQ FD from epoll only when the request has reached a
   terminal state and the socket is about to leave the Worker.

The fixed epoll event array size must be raised from three to accommodate
queue, AIO, and dynamically registered send-CQ FDs. The capacity should cover
all active async request slots, plus the three fixed sources.

## 9. Error, Cancellation, And Ownership Rules

- `ibv_post_send` failure, a completion-channel failure, or a non-success data
  completion invalidates the socket and finishes the request with the existing
  communication-error response semantics.
- Before `AsyncRDMARequest` returns or invalidates a socket, it unregisters
  its send-CQ FD if registered. No epoll event may retain a pointer to a
  destroyed request.
- Worker shutdown cancels active requests only after unregistering their
  send-CQ sources. A pending DMA buffer is held until its request has reached a
  terminal state; it is not recycled early.
- The listener sees no new behavior: it receives the same
  `IOWorkerResponse`, re-arms valid sockets, and decrements `load_map` only
  after request completion.

## 10. Testing Plan

Unit tests should isolate the state transitions through a mockable verbs
adapter. They do not require an RDMA NIC.

- Read: AIO completion posts RDMA WRITE; a synthetic matching send completion
  advances to the next chunk or final response.
- Write: RDMA READ completion is required before AIO pwrite submission.
- A control `IBV_WC_SEND` observed during a data completion drain updates the
  existing send accounting without completing the data request.
- Wrong `wr_id`, wrong opcode, failed WC status, and completion-channel errors
  invalidate the socket and release the async slot exactly once.
- Immediate completion during arm/poll does not leave an epoll registration.
- Teardown with a pending send-CQ source removes the registration before the
  request/source lifetime ends.

A hardware smoke test should exercise direct-I/O read and write with enough
parallel clients to keep multiple Worker slots active. `perf top -t <worker>`
should no longer show `__IBVSocket_waitForRDMACompletion` as a busy-poll hot
path.

## 11. Decisions To Confirm

### D1. Dynamic send-CQ registration per request (confirmed)

Register a socket's send completion-channel FD with the owning Worker's epoll
once when the request begins its data transfer. Keep it registered for all
chunks, then remove it immediately before the socket returns to the listener
or is invalidated. This costs at most one `EPOLL_CTL_ADD` and one
`EPOLL_CTL_DEL` per BeeGFS request, preserves current listener load balancing,
and has simple ownership.

Alternative: permanently register all socket send-CQ FDs. This avoids those
two syscalls but requires a stable Worker assignment for each socket and is not
compatible with the current dynamic listener dispatch model.

### D2. Normal control SEND completions remain unchanged (confirmed)

The new event-driven logic only waits asynchronously for RDMA READ/WRITE. It
does drain `IBV_WC_SEND` when it happens to encounter one, preserving existing
counter bookkeeping, but it does not convert ordinary message SEND completion
handling into a new Worker state machine.

### D3. One outstanding data RDMA operation per socket (confirmed)

Keep one outstanding data RDMA operation for a socket. This is already implied
by listener one-shot ownership and allows one request-owned CQ source without
per-socket completion multiplexing. Increasing data RDMA depth per socket is a
separate design, because it requires independent buffer lifetime and WR-ID
tracking for several transfers on one QP.

### D4. Level-triggered send completion-channel monitoring (confirmed)

Register the send completion-channel FD with `EPOLLIN`, without `EPOLLET`.
This matches the request-ring and libaio eventfd handling already used by the
Worker. The handler drains completion-channel events and CQ work completions;
if an event remains due to an error or race, level-triggered epoll reports it
again instead of relying on a new edge. The one-data-RDMA-per-socket rule keeps
normal completion handling bounded.

### D5. Send-CQ failure response semantics (confirmed)

For an RDMA post failure, completion-channel failure, failed work completion,
or an unexpected work-request ID/opcode, invalidate the socket and finish the
request with the existing communication-error response semantics. The request
still produces an `IOWorkerResponse`, so the listener performs its normal
`load_map` decrement and cleanup. The invalid socket is not re-armed.

### D6. Compile-time scope (confirmed)

The send completion channel, asynchronous data-RDMA APIs, Worker epoll source,
and `AsyncRDMARequest` state changes are compiled only with `BEEGFS_NVFS`.
Non-NVFS builds retain the original send-CQ allocation and synchronous
behavior, with no extra completion-channel resource.

### D7. Do not mix synchronous and asynchronous data-RDMA consumers (confirmed)

While an `AsyncRDMARequest` owns a socket, all client data RDMA operations on
that socket use the new asynchronous post/completion path. No synchronous
data-RDMA caller may poll that socket's send CQ concurrently. This prevents
the old polling path from consuming a work completion owned by the request.
Normal protocol-message SEND operations remain on their existing path as
specified by D2.
