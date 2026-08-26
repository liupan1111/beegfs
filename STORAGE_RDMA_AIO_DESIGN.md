# Storage RDMA AIO Design

## Scope

This branch implements a first-stage fully asynchronous disk IO path for the
storage RDMA read/write messages.

In scope:

- `ReadLocalFileRDMAMsgEx`
- `WriteLocalFileRDMAMsgEx`
- single-replica requests only
- direct IO only
- 4096-byte aligned offset and length
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
returned an `IOWorkerResponse`.

`IOWorkerResponse` is not the protocol response. It only returns socket
ownership to the listener and lets the listener update worker load accounting.

## Request Model

Each asynchronous RDMA request is represented by an `AsyncRDMARequest`.

An `AsyncRDMARequest`:

- owns the `IncomingPreprocessedMsgWork` while the request is active
- stores the long-lived read/write state that used to be stack state
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

No extra active request limit is added in phase one. Backpressure is exposed by
the existing request ring capacity. If a listener cannot enqueue because the
ring is full, the existing assert-style tuning behavior applies.

## Worker Async Context

Each IO worker owns an async context:

```text
IOWorkerAsyncContext
  io_context_t aioContext
  int aioEventFD
  AsyncRequestList activeRequests
  AsyncIOBufferPool bufferPool
```

The AIO queue depth is 128 per worker.

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
the RDMA registration cache to register the real local buffer length. The first
implementation should keep memory ownership in the worker buffer pool and keep
MR ownership/cache in the socket `IBVCommContext`.

## Direct IO

The async RDMA path requires:

```text
sessionLocalFile->getIsDirectIO() == true
offset % 4096 == 0
count  % 4096 == 0
chunk length % 4096 == 0
buffer address % 4096 == 0
```

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
  aio completion
  update counters and offsets
  advance next chunk

send WriteLocalFileRDMARespMsg
return socket to listener via IOWorkerResponse
release buffer and request
```

The RDMA read step uses the existing synchronous socket RDMA wrapper in phase
one.

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
  aio completion
  RDMA write worker async buffer -> client buffer
  update counters and offsets
  advance next chunk

send final length/result information
return socket to listener via IOWorkerResponse
release buffer and request
```

The RDMA write step uses the existing synchronous socket RDMA wrapper in phase
one.

## Error Semantics

Buddy mirror requests return an error in phase one.

Recommended results:

- unsupported buddy mirror path: `FhgfsOpsErr_NOTSUPP` if available, otherwise
  `FhgfsOpsErr_INVAL`
- non-direct session: `FhgfsOpsErr_INVAL`
- unaligned offset/count/chunk/buffer: `FhgfsOpsErr_INVAL`
- RDMA read/write failure: `FhgfsOpsErr_COMMUNICATION`
- AIO negative errno: `FhgfsOpsErrTk::fromSysErr(errno)`
- AIO write short write: `FhgfsOpsErr_INTERNAL`
- AIO read short read or zero: EOF semantics

On every error path the IO worker should try to send the protocol response, then
return the socket to the listener with `IOWorkerResponse`.

## Commit Plan

1. `common: add async io worker foundation`
   - common async request/context abstractions
   - worker libaio context and eventfd
   - unified epoll AIO completion handling
   - intrusive active request list
   - worker buffer pool

2. `common/ib: make rdma buffer registration length-aware`
   - remove the hardcoded `WORKER_BUFOUT_SIZE` assumption from lazy MR
     registration
   - let RDMA read/write register the real local buffer length

3. `storage: add async rdma request state machine`
   - `AsyncRDMARequest`
   - RDMA read and write single-replica direct IO state machine
   - libaio submit and completion handling
   - protocol response and socket return

4. `storage: wire rdma read/write messages to async path`
   - route `ReadLocalFileRDMAMsgEx` and `WriteLocalFileRDMAMsgEx` through the
     async hook
   - reject invalid phase-one paths

## Test Plan

Unit tests should cover:

- buffer pool acquire/release
- intrusive list add/remove
- alignment validation
- async request state transitions with mocked completions where practical

Build verification should cover:

- `BEEGFS_NVFS` compilation of affected RDMA code
- existing storage/common test targets that can run without RDMA hardware

Real RDMA end-to-end tests are deferred because they need a suitable RDMA and
direct IO environment.
