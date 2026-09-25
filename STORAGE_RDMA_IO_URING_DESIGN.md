# BeeGFS RDMA io_uring Fixed-Buffer Design

## 1. Goal

This design adds an `io_uring` implementation for the existing asynchronous
single-replica RDMA storage path. Its primary goal is to reduce steady-state IO
Worker CPU use for direct IO by removing repeated user-page pinning from the
per-request path.

The existing `libaio` implementation remains available as an explicitly
selected backend. It is not a fallback for an `io_uring` failure.

The target steady-state path is:

```text
IO Worker startup
  -> allocate aligned async buffers
  -> register all buffers with io_uring
  -> kernel pins those pages once

each direct IO request
  -> submit READ_FIXED / WRITE_FIXED with a registered buffer index
  -> receive CQE
  -> no per-request internal_get_user_pages_fast()
```

The page-pinning work still occurs during `io_uring_register_buffers()`, and
again during teardown. It is intentionally moved out of the IOPS hot path.

## 2. Scope

In scope:

- `BEEGFS_NVFS` single-replica `ReadLocalFileRDMA` and `WriteLocalFileRDMA`.
- Existing direct-IO-only asynchronous requests.
- One local io_uring instance per IO Worker.
- Fixed-buffer registration for every buffer in that Worker's
  `AsyncIOBufferPool`.
- Explicit runtime selection between `libaio` and `io_uring`.
- Reuse of the Worker epoll loop through one completion eventfd per backend.

Out of scope:

- Buddy mirroring, resync, and primary-secondary communication.
- Buffered IO or an `O_DIRECT` fallback.
- io_uring SQPOLL, IOPOLL, linked operations, registered files, and request
  batching.
- Changing the request-ring, listener-worker, response-ring, or socket
  ownership contracts.
- Changing the asynchronous send-CQ state machine.
- Automatic fallback from io_uring to libaio.

## 3. Existing Path

`AsyncRDMARequest` currently submits each local disk operation directly to
libaio:

```text
AsyncRDMARequest
  -> io_prep_pread/io_prep_pwrite
  -> io_set_eventfd(iocb, aioEventFD)
  -> io_submit(aioContext, ...)
  -> Worker epoll sees aioEventFD
  -> io_getevents(aioContext, ...)
  -> AsyncRDMARequest::onAIOComplete(io_event)
```

The async buffer is reused by the Worker, but libaio receives an ordinary
userspace pointer for every direct IO. The block layer must pin that pointer's
pages per request, which appears as `internal_get_user_pages_fast` in the
Worker's steady-state CPU profile.

## 4. Backend Abstraction

`IOWorkerAsyncContext` becomes the owner of a small local-disk-IO backend.
`AsyncRDMARequest` must no longer call `io_submit()` directly.

The common interface is intentionally narrow:

```text
AsyncIOBackend
  submitRead(request, fd, AsyncIOBuffer, offset, length)
  submitWrite(request, fd, AsyncIOBuffer, bufferOffset, offset, length)
  reapCompletions()
```

The submit methods associate the completion with `AsyncIORequest*`. The reap
method returns a backend-neutral completion record:

```text
AsyncIOCompletion
  AsyncIORequest* request
  int64_t result             # byte count or negative errno
```

`IOWorkerAsyncContext` receives these records and invokes a renamed,
backend-neutral request callback such as `onLocalIOComplete(result)`. The
request state machine remains responsible for short reads, short writes,
protocol responses, and request completion.

This separation avoids presenting a fabricated `struct io_event` to io_uring
code and keeps all libaio-specific data inside the libaio backend.

## 5. Backend Selection

A storage configuration option selects the local disk backend:

```text
tuneAsyncIOBackend = libaio | io_uring
```

`libaio` is the default to preserve current deployment behavior. `io_uring` is
an opt-in choice for the asynchronous RDMA path.

When `io_uring` is selected, the following are startup failures:

- io_uring queue creation failure;
- completion eventfd registration failure;
- fixed-buffer registration failure;
- an unsupported kernel operation needed by this design.

There is no silent or automatic downgrade to libaio. Startup diagnostics must
identify the failed operation and errno.

## 6. Buffer Ownership And Fixed Registration

The existing per-Worker pool remains the sole owner of async buffers:

```text
AsyncIOBufferPool
  DEFAULT_ASYNC_REQUEST_SLOTS buffers
  1 MiB per buffer
  4096-byte alignment
```

`AsyncIOBuffer` gains a stable `bufferIndex`. The index is assigned when the
pool constructs the buffer and does not change while the Worker exists.

For an io_uring Worker, context initialization builds one `iovec` per pool
buffer and calls:

```text
io_uring_register_buffers(ring, iovecs, numBuffers)
```

Every local disk submission then uses the matching index:

```text
io_uring_prep_read_fixed(..., buffer->data, length, offset, buffer->bufferIndex)
io_uring_prep_write_fixed(..., buffer->data + bufferOffset, length, offset,
                          buffer->bufferIndex)
```

The offset used for a partial write remains within the registered buffer. The
backend must preserve direct-IO alignment checks already performed by
`AsyncRDMARequest`.

The pool has exactly one buffer for every active request slot. Therefore a
buffer is never submitted by two requests concurrently, and a buffer index
identifies one in-flight local I/O at most.

## 7. io_uring Completion Integration

The Worker continues to have one epoll instance. No new Worker thread and no
second wait loop are introduced.

At io_uring context creation, the backend creates or owns a nonblocking
completion eventfd and registers it with the ring. The Worker adds that eventfd
to epoll as its existing `AIO` event source.

```text
request/high-priority ring eventfd
io_uring completion eventfd
send-CQ completion-channel fd
  -> one Worker epoll_wait()
```

When the completion eventfd is readable:

1. drain its counter;
2. repeatedly collect all ready CQEs from the io_uring completion queue;
3. translate each CQE into `AsyncIOCompletion`;
4. call the request's local-IO completion handler;
5. finish and release a request only after the existing state machine reaches
   its terminal state.

The eventfd counter is only a wakeup signal. It is not treated as a completion
count, just as the current libaio eventfd is not treated as an `io_event`
count.

## 8. Request State Machine

The current request phases are unchanged:

```text
INIT
  -> AIO_PENDING
  -> RDMA_READ_PENDING / RDMA_WRITE_PENDING
  -> DONE
```

Only the local I/O submission and completion transport differs by backend.

### ReadLocalFileRDMA

```text
fixed buffer
  -> local READ_FIXED
  -> io_uring CQE
  -> existing RDMA WRITE to client
  -> existing send-CQ completion event
  -> next chunk or response
```

### WriteLocalFileRDMA

```text
fixed buffer
  -> existing RDMA READ from client
  -> existing send-CQ completion event
  -> local WRITE_FIXED
  -> io_uring CQE
  -> next chunk or response
```

The fixed buffer is retained through the complete request, including its RDMA
phase. It is released only when the request is terminal, exactly as in the
current libaio design.

## 9. Lifetime And Shutdown

The shutdown ordering for an io_uring Worker is:

```text
stop accepting new async work
  -> complete or cancel active AsyncRDMARequest objects
  -> ensure no submitted local IO remains
  -> unregister completion eventfd
  -> unregister fixed buffers
  -> destroy io_uring
  -> destroy AsyncIOBufferPool
```

No request, CQE user-data pointer, or in-flight kernel operation may outlive an
`AsyncRDMARequest` or an `AsyncIOBuffer`.

The existing active-request list remains the shutdown ownership mechanism.

## 10. Build Dependency

The implementation uses the maintained `liburing` userspace library rather
than embedding raw io_uring syscalls. The build gains a `liburing` dependency
for the common async-worker code and the storage package/runtime dependency is
updated accordingly.

The selected backend is runtime-configurable, but binaries including this code
must be built on a system with liburing development headers and library.

## 11. Tests And Verification

Unit tests:

- existing libaio behavior remains selected by default;
- `AsyncIOBufferPool` assigns stable, unique buffer indexes;
- io_uring backend registers every pool buffer and reports a completion for a
  direct-IO-compatible temporary file;
- a read and a write use the expected fixed-buffer index;
- request cancellation and normal completion release their buffers and request
  slots under both backends.

Integration verification on a Linux NVMe/XFS host:

- run the same direct-IO single-replica fio profile with `libaio` and
  `io_uring` selected;
- verify data correctness and request completion;
- compare IOPS, latency, Worker CPU, and `perf record -t <worker-tid>`;
- confirm that `internal_get_user_pages_fast` is absent or negligible in the
  steady-state io_uring fixed-buffer profile.

## 12. Confirmed Decisions

- `tuneAsyncIOBackend` selects `libaio` or `io_uring`; the default is
  `libaio`.
- `liburing` is a regular build and runtime dependency. No separate build flag
  is required to enable the runtime `io_uring` selection.
- The first version submits each operation immediately, matching the existing
  libaio behavior. Submission batching is a separate future optimization.
- `io_uring` initialization, eventfd registration, and fixed-buffer
  registration are mandatory when the backend is selected. Any failure is a
  startup failure; there is no automatic fallback to libaio.
- Fixed buffers reuse the existing 16 async slots and 1-MiB buffer size. This
  pins 16 MiB per IO Worker for its lifetime. Operators must provide a matching
  `RLIMIT_MEMLOCK` budget; failure to do so is reported at startup.
