# Storage RDMA AIO Design

## Scope

This branch implements a first-stage fully asynchronous disk IO path for the
storage RDMA read/write messages.

In scope:

- `ReadLocalFileRDMAMsgEx`
- `WriteLocalFileRDMAMsgEx`
- single-replica requests only
- direct IO only
- direct-IO alignment required by the opened file and target filesystem
- libaio for local disk reads and writes
- one worker-local `io_context_t` and AIO eventfd per IO worker
- one unified worker `epoll_wait()` for request rings, high-priority rings, and
  AIO completions

Out of scope:

- buddy mirroring
- primary-to-secondary forwarding
- resync and mirror consistency state handling
- ordinary non-RDMA read/write as a target path
- fully asynchronous RDMA CQ state machines
- request pipelining on the same socket

## Confirmed Semantics

The client-to-storage data path is RDMA. RDMA read/write operations keep using
the existing BeeGFS socket RDMA API in this phase. Disk IO is changed from
blocking `pread()` / `pwrite()` to libaio submit and completion.

The worker owns the socket while a request is in flight. The listener does not
re-add the socket to epoll until the IO worker has sent the protocol response and
returned an `IOWorkerResponse`. The current RDMA calls are still synchronous, so
this phase overlaps disk IO from different socket requests; it does not yet
pipeline RDMA and disk IO for one socket.

`IOWorkerResponse` is not the protocol response. It only returns socket
ownership to the listener and lets the listener update worker load accounting.

## Worker Dispatch Contract

The ordinary worker loop currently assumes that `Work::process()` has completed
the request: it creates the `IOWorkerResponse` and deletes the `Work` when the
call returns. Async processing needs an explicit disposition instead of relying
on that assumption:

```text
COMPLETE
  process() sent (or prepared) the final protocol response.
  The worker follows the existing response and delete path.

ASYNC_PENDING
  ownership of IncomingPreprocessedMsgWork moved to AsyncRDMARequest.
  The worker must neither create IOWorkerResponse nor delete Work.
```

Only the completion or cancellation path may finish an `ASYNC_PENDING` request.
It sends the protocol response, creates exactly one `IOWorkerResponse`, enqueues
it to the listener response ring, and then releases the async request and its
work. This keeps the socket disabled in listener epoll until the request is
actually complete.

## Request Model

Each asynchronous RDMA request is represented by an `AsyncRDMARequest`.

An `AsyncRDMARequest`:

- owns the `IncomingPreprocessedMsgWork` while the request is active
- stores the long-lived read/write state that used to be stack state
- holds a lifetime-safe reference to the session file and its file descriptor;
  a raw `SessionLocalFile*` is not sufficient after `process()` returns
- uses one registered aligned buffer at a time
- has one `struct iocb`
- has at most one outstanding AIO operation
- is linked into the worker active request intrusive list

Large requests are split into chunks. The first-stage chunk size is 1 MiB.

For one request:

```text
chunk N submits at most one AIO
chunk N+1 is submitted only after chunk N completes
```

Across requests:

```text
different requests handled by the same worker may have AIO in flight concurrently
```

The worker has a hard active request limit:

```text
activeRequestLimit = DEFAULT_ASYNC_REQUEST_SLOTS
```

When the active limit is reached, work remains in the existing high-priority or
request ring. The worker drains a ring eventfd but does not dequeue its work
while full, avoiding a level-triggered eventfd busy loop without bypassing ring
backpressure. After an AIO completion frees a slot, the worker checks the rings
before sleeping again, always consuming the high-priority ring first.

## Worker Async Context

Each IO worker owns an async context:

```text
IOWorkerAsyncContext
  io_context_t aioContext
  int aioEventFD
  AsyncRequestList activeRequests
  AsyncIOBufferPool bufferPool
```

The initial libaio queue depth is 128 per worker. One shared
`DEFAULT_ASYNC_REQUEST_SLOTS` constant is both the active-request limit and the
buffer-pool size; phase one sets it to 16. With 1-MiB buffers this reserves
16 MiB per IO worker. The AIO queue depth remains independent because it is only
the libaio context capacity.

The AIO eventfd is added to the same epoll instance as the worker request queues:

```text
request ring eventfd
high-priority ring eventfd
aio completion eventfd
```

Completion ownership is per worker:

```text
worker0 submits to ctx0 and wakes on efd0
worker1 submits to ctx1 and wakes on efd1
```

`io_getevents(ctx0)` must only recover completions submitted through `ctx0`.

Each submitted `iocb` uses `io_set_eventfd()` with its owner's `aioEventFD`.
After epoll reports that FD, the worker drains the eventfd counter and repeatedly
calls `io_getevents(..., min_nr = 0, ...)` until no completion remains. A single
eventfd count is only a wakeup hint: it is not a one-to-one completion record.

The unified epoll dispatcher must tag event sources. Existing ring events store
`RteRingQueue*` in `data.ptr`; an AIO event must not be cast to that type. Use a
small tagged event descriptor or an equivalent discriminator that identifies
request ring, high-priority ring, and AIO completion event.

## Active Request List

The active requests use an intrusive doubly-linked list. The completion path
gets the request pointer from `io_event.data`, so removing a completed request
must be O(1).

```text
iocb.data = AsyncRDMARequest*
io_getevents() -> io_event.data -> AsyncRDMARequest*
```

The list is still useful for shutdown, cleanup, debugging, and future active
request accounting.

## Buffer Pool And RDMA Registration

Each IO worker owns a buffer pool:

```text
buffer size = 1 MiB
alignment   = 4096 bytes
```

Buffers are acquired when an `AsyncRDMARequest` starts and released when the
request reaches done or failed state.

Buffers are not allocated and freed in the hot path.

BeeGFS currently lazily registers worker buffers per RDMA socket:

```text
Socket::read/write(..., lkey = 0, remoteAddr, rkey)
  -> IBVSocket lazy local buffer registration
  -> commContext->workerMRs
```

The current lazy registration assumes `WORKER_BUFOUT_SIZE`. The async path needs
the RDMA registration cache to register the buffer's capacity, not the length of
its first transfer. With fixed 1-MiB buffers, each registration covers the full
1 MiB and later chunks may use any prefix. If variable-size buffers are
introduced later, the cache key must include the registered capacity or
re-register on growth. The first implementation keeps memory ownership in the
worker buffer pool and MR ownership/cache in the socket `IBVCommContext`.

## Direct IO

The async RDMA path requires:

```text
the session file descriptor was opened with O_DIRECT
offset, count, chunk length, and buffer address are 4096-byte aligned
```

The implementation must check the `O_DIRECT` flag directly. The existing
`getIsDirectIO()` helper also accepts `O_SYNC`, which is not sufficient for this
path. Phase one explicitly supports storage targets whose direct-IO alignment
contract is 4096 bytes; it does not dynamically probe other alignment values.

No buffered IO fallback is implemented in phase one. Invalid requests return
`FhgfsOpsErr_INVAL`.

The server does not force-reopen the file with `O_DIRECT`. It requires the
existing session file descriptor to already be direct IO capable.

## RDMA Write Request

`WriteLocalFileRDMAMsgEx` means the client writes file data.

The phase-one state machine is:

```text
validate request
open/reference session local file
reject buddy mirror
require direct IO
acquire async buffer

loop:
  RDMA read client buffer -> worker async buffer
  submit aio write worker async buffer -> disk
  return to worker epoll
  aio completion (`io_event.res` is either a byte count or negative errno)
  update counters and offsets
  advance next chunk

send WriteLocalFileRDMARespMsg
return socket to listener via IOWorkerResponse
release buffer and request
```

The RDMA read step uses the existing synchronous socket RDMA wrapper in phase
one. A partial AIO write must resubmit the remaining range; treating every short
write as `INTERNAL` would regress the current synchronous `pwrite()` loop.

## RDMA Read Request

`ReadLocalFileRDMAMsgEx` means the client reads file data.

The phase-one state machine is:

```text
validate request
open/reference session local file
reject buddy mirror
require direct IO
acquire async buffer

loop:
  submit aio read disk -> worker async buffer
  return to worker epoll
  aio completion (`io_event.res` is either a byte count or negative errno)
  RDMA write worker async buffer -> client buffer
  update counters and offsets
  advance next chunk

send final length/result information
return socket to listener via IOWorkerResponse
release buffer and request
```

The RDMA write step uses the existing synchronous socket RDMA wrapper in phase
one. A short read or EOF sends only the completed byte count through the existing
RDMA-read response protocol and finishes the request; it must not attempt to
RDMA-write uninitialized buffer bytes.

## Error Semantics

Buddy mirror requests return an error in phase one.

Recommended results:

- unsupported buddy mirror path: `FhgfsOpsErr_NOTSUPP`
- non-direct session: `FhgfsOpsErr_INVAL`
- unaligned offset/count/chunk/buffer: `FhgfsOpsErr_INVAL`
- RDMA read/write failure: `FhgfsOpsErr_COMMUNICATION`
- AIO negative result: `FhgfsOpsErrTk::fromSysErr(-io_event.res)`
- AIO write short write: resubmit the unwritten suffix; fail only when no
  forward progress is possible
- AIO read short read or zero: EOF semantics

On every error path the IO worker should try to send the protocol response, then
return the socket to the listener with `IOWorkerResponse`.

## Shutdown And Cancellation

Worker shutdown must stop admitting new async requests, retain the worker and
session-file references for every submitted `iocb`, and drain or cancel all
in-flight operations before calling `io_destroy()`. It must then finish each
in-flight work through the same single-response ownership path. Buffers,
work objects, sockets, and session-file references must not be released while an
`iocb` can still complete against them.

## Commit Plan

1. `common: add async io worker foundation`
   - common async request/context abstractions
   - worker libaio context and eventfd
   - unified epoll AIO completion handling
   - intrusive active request list
   - bounded active request scheduling using the existing work rings
   - worker buffer pool with an explicit memory budget

2. `common/ib: make rdma buffer registration capacity-aware`
   - remove the hardcoded `WORKER_BUFOUT_SIZE` assumption from lazy MR
     registration
   - let RDMA read/write register the local buffer capacity

3. `storage: add async rdma request state machine`
   - `AsyncRDMARequest`
   - RDMA read and write single-replica direct IO state machine
   - libaio submit and completion handling
   - protocol response and socket return

4. `storage: route rdma read/write messages to async path`
   - route `ReadLocalFileRDMAMsgEx` and `WriteLocalFileRDMAMsgEx` through the
     async hook
   - reject invalid phase-one paths

## Test Plan

Unit tests should cover:

- buffer pool acquire/release
- intrusive list add/remove
- alignment validation
- async request state transitions with mocked completions where practical
- worker `COMPLETE` versus `ASYNC_PENDING` ownership and exactly-one-response
  behavior
- active-limit backpressure, high-priority ring preference, and shutdown cleanup
- epoll source dispatch and eventfd/io_getevents draining
- partial AIO write retry and short-read/EOF protocol behavior

Build verification should cover:

- `BEEGFS_NVFS` compilation of affected RDMA code
- existing storage/common test targets that can run without RDMA hardware

Real RDMA end-to-end tests are deferred because they need a suitable RDMA and
direct IO environment.
