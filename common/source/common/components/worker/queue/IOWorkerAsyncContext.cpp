#include <common/components/worker/queue/IOWorkerAsyncContext.h>
#include <common/components/streamlistenerv2/IncomingPreprocessedMsgWork.h>
#include <common/system/System.h>
#include <common/Common.h>

#include <array>
#include <assert.h>
#include <errno.h>
#include <libaio.h>
#include <liburing.h>
#include <new>
#include <stdexcept>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/uio.h>
#include <unistd.h>

#define IOWORKER_AIO_COMPLETION_BURST 64

const size_t IOWorkerAsyncContext::DEFAULT_ASYNC_REQUEST_SLOTS;

class AsyncIOBackend
{
   public:
      virtual ~AsyncIOBackend() {}

      virtual int submitRead(AsyncIORequest* request, int fd, AsyncIOBuffer* buffer,
         size_t length, off_t offset) = 0;
      virtual int submitWrite(AsyncIORequest* request, int fd, AsyncIOBuffer* buffer,
         size_t bufferOffset, size_t length, off_t offset) = 0;
      virtual unsigned reapCompletions(AsyncIOCompletion* completions,
         unsigned maxCompletions) = 0;
};

namespace
{

struct LibAIOOperation
{
   struct iocb iocb;
   AsyncIORequest* request;

   LibAIOOperation() : request(NULL)
   {
      memset(&iocb, 0, sizeof(iocb));
   }
};

class LibAIOBackend : public AsyncIOBackend
{
   public:
      explicit LibAIOBackend(int eventFD) : aioContext(0), eventFD(eventFD)
      {
         int setupRes = io_setup(IOWorkerAsyncContext::DEFAULT_AIO_QUEUE_DEPTH, &aioContext);
         if(setupRes < 0)
            throw std::runtime_error("unable to setup aio context");
      }

      ~LibAIOBackend()
      {
         if(aioContext)
            io_destroy(aioContext);
      }

      int submitRead(AsyncIORequest* request, int fd, AsyncIOBuffer* buffer, size_t length,
         off_t offset) override
      {
         return submit(request, fd, buffer, 0, length, offset, false);
      }

      int submitWrite(AsyncIORequest* request, int fd, AsyncIOBuffer* buffer,
         size_t bufferOffset, size_t length, off_t offset) override
      {
         return submit(request, fd, buffer, bufferOffset, length, offset, true);
      }

      unsigned reapCompletions(AsyncIOCompletion* completions,
         unsigned maxCompletions) override
      {
         std::array<struct io_event, IOWORKER_AIO_COMPLETION_BURST> events;
         unsigned maxEvents = BEEGFS_MIN(maxCompletions, (unsigned)events.size());
         int numEvents = io_getevents(aioContext, 0, maxEvents, events.data(), NULL);
         if(numEvents <= 0)
            return 0;

         for(int i = 0; i < numEvents; i++)
         {
            AsyncIORequest* request = (AsyncIORequest*)events[i].data;
            releaseOperation((struct iocb*)events[i].obj, request);
            completions[i].request = request;
            completions[i].result = events[i].res;
         }

         return numEvents;
      }

   private:
      int submit(AsyncIORequest* request, int fd, AsyncIOBuffer* buffer, size_t bufferOffset,
         size_t length, off_t offset, bool write)
      {
         LibAIOOperation* operation = acquireOperation();
         if(!operation)
            return EAGAIN;

         memset(&operation->iocb, 0, sizeof(operation->iocb));
         if(write)
            io_prep_pwrite(&operation->iocb, fd, buffer->data + bufferOffset, length, offset);
         else
            io_prep_pread(&operation->iocb, fd, buffer->data, length, offset);

         operation->iocb.data = request;
         io_set_eventfd(&operation->iocb, eventFD);
         operation->request = request;

         struct iocb* iocbs[] = { &operation->iocb };
         int submitRes = io_submit(aioContext, 1, iocbs);
         if(submitRes == 1)
            return 0;

         operation->request = NULL;
         return submitRes < 0 ? -submitRes : EIO;
      }

      LibAIOOperation* acquireOperation()
      {
         for(auto iter = operations.begin(); iter != operations.end(); iter++)
         {
            if(!iter->request)
               return &*iter;
         }

         return NULL;
      }

      void releaseOperation(struct iocb* iocb, AsyncIORequest* request)
      {
         for(auto iter = operations.begin(); iter != operations.end(); iter++)
         {
            if(&iter->iocb == iocb)
            {
               assert(iter->request == request);
               iter->request = NULL;
               return;
            }
         }

         assert(false);
      }

      io_context_t aioContext;
      int eventFD;
      std::array<LibAIOOperation, IOWorkerAsyncContext::DEFAULT_ASYNC_REQUEST_SLOTS> operations;
};

class IOUringBackend : public AsyncIOBackend
{
   public:
      IOUringBackend(int eventFD, const AsyncIOBufferPool& bufferPool) :
         eventFD(eventFD), ringInitialized(false), buffersRegistered(false),
         eventFDRegistered(false)
      {
         try
         {
            int initRes = io_uring_queue_init(IOWorkerAsyncContext::DEFAULT_AIO_QUEUE_DEPTH,
               &ring, 0);
            if(initRes < 0)
               throw std::runtime_error("unable to setup io_uring: " +
                  System::getErrString(-initRes));

            ringInitialized = true;

            const std::vector<AsyncIOBuffer*>& buffers = bufferPool.getBuffers();
            iovecs.reserve(buffers.size());
            for(auto iter = buffers.begin(); iter != buffers.end(); iter++)
            {
               struct iovec iovec;
               iovec.iov_base = (*iter)->data;
               iovec.iov_len = (*iter)->length;
               iovecs.push_back(iovec);
            }

            int registerRes = io_uring_register_buffers(&ring, iovecs.data(), iovecs.size());
            if(registerRes < 0)
               throw std::runtime_error("unable to register io_uring buffers: " +
                  System::getErrString(-registerRes));

            buffersRegistered = true;

            registerRes = io_uring_register_eventfd(&ring, eventFD);
            if(registerRes < 0)
               throw std::runtime_error("unable to register io_uring eventfd: " +
                  System::getErrString(-registerRes));

            eventFDRegistered = true;
         }
         catch(...)
         {
            cleanup();
            throw;
         }
      }

      ~IOUringBackend()
      {
         cleanup();
      }

   private:
      void cleanup()
      {
         if(eventFDRegistered)
            io_uring_unregister_eventfd(&ring);

         if(buffersRegistered)
            io_uring_unregister_buffers(&ring);

         if(ringInitialized)
            io_uring_queue_exit(&ring);

         eventFDRegistered = false;
         buffersRegistered = false;
         ringInitialized = false;
      }

   public:

      int submitRead(AsyncIORequest* request, int fd, AsyncIOBuffer* buffer, size_t length,
         off_t offset) override
      {
         return submit(request, fd, buffer, 0, length, offset, false);
      }

      int submitWrite(AsyncIORequest* request, int fd, AsyncIOBuffer* buffer,
         size_t bufferOffset, size_t length, off_t offset) override
      {
         return submit(request, fd, buffer, bufferOffset, length, offset, true);
      }

      unsigned reapCompletions(AsyncIOCompletion* completions,
         unsigned maxCompletions) override
      {
         std::array<struct io_uring_cqe*, IOWORKER_AIO_COMPLETION_BURST> cqes;
         unsigned maxCQEs = BEEGFS_MIN(maxCompletions, (unsigned)cqes.size());
         unsigned numCQEs = io_uring_peek_batch_cqe(&ring, cqes.data(), maxCQEs);

         for(unsigned i = 0; i < numCQEs; i++)
         {
            completions[i].request =
               static_cast<AsyncIORequest*>(io_uring_cqe_get_data(cqes[i]));
            completions[i].result = cqes[i]->res;
            io_uring_cqe_seen(&ring, cqes[i]);
         }

         return numCQEs;
      }

   private:
      int submit(AsyncIORequest* request, int fd, AsyncIOBuffer* buffer, size_t bufferOffset,
         size_t length, off_t offset, bool write)
      {
         struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
         if(!sqe)
            return EAGAIN;

         if(write)
         {
            io_uring_prep_write_fixed(sqe, fd, buffer->data + bufferOffset, length, offset,
               buffer->bufferIndex);
         }
         else
            io_uring_prep_read_fixed(sqe, fd, buffer->data, length, offset,
               buffer->bufferIndex);

         io_uring_sqe_set_data(sqe, request);

         int submitRes = io_uring_submit(&ring);
         if(submitRes == 1)
            return 0;

         return submitRes < 0 ? -submitRes : EIO;
      }

      int eventFD;
      struct io_uring ring;
      bool ringInitialized;
      bool buffersRegistered;
      bool eventFDRegistered;
      std::vector<struct iovec> iovecs;
};

}

AsyncIORequest::AsyncIORequest() : prev(NULL), next(NULL), inActiveList(false)
{
}

AsyncIORequestList::AsyncIORequestList() : head(NULL), tail(NULL), numRequests(0)
{
}

void AsyncIORequestList::pushBack(AsyncIORequest* request)
{
   assert(!request->inActiveList);

   request->prev = tail;
   request->next = NULL;
   request->inActiveList = true;

   if(tail)
      tail->next = request;
   else
      head = request;

   tail = request;
   numRequests++;
}

void AsyncIORequestList::remove(AsyncIORequest* request)
{
   assert(request->inActiveList);

   if(request->prev)
      request->prev->next = request->next;
   else
      head = request->next;

   if(request->next)
      request->next->prev = request->prev;
   else
      tail = request->prev;

   request->prev = NULL;
   request->next = NULL;
   request->inActiveList = false;
   numRequests--;
}

bool AsyncIORequestList::empty() const
{
   return !numRequests;
}

size_t AsyncIORequestList::size() const
{
   return numRequests;
}

AsyncIORequest* AsyncIORequestList::front() const
{
   return head;
}

AsyncIOBufferPool::AsyncIOBufferPool(size_t numBuffers, size_t bufferSize, size_t alignment) :
   bufferSize(bufferSize), alignment(alignment)
{
   allBuffers.reserve(numBuffers);
   freeBuffers.reserve(numBuffers);

   for(size_t i = 0; i < numBuffers; i++)
   {
      AsyncIOBuffer* buffer = new AsyncIOBuffer();
      void* data = NULL;

      int allocRes = posix_memalign(&data, alignment, bufferSize);
      if(allocRes)
         throw std::bad_alloc();

      memset(data, 0, bufferSize);

      buffer->data = (char*)data;
      buffer->length = bufferSize;
      buffer->bufferIndex = i;

      allBuffers.push_back(buffer);
      freeBuffers.push_back(buffer);
   }
}

AsyncIOBufferPool::~AsyncIOBufferPool()
{
   for(size_t i = 0; i < allBuffers.size(); i++)
   {
      SAFE_FREE(allBuffers[i]->data);
      delete allBuffers[i];
   }
}

AsyncIOBuffer* AsyncIOBufferPool::acquire()
{
   assert(!freeBuffers.empty());
   if(freeBuffers.empty())
      return NULL;

   AsyncIOBuffer* buffer = freeBuffers.back();
   freeBuffers.pop_back();
   return buffer;
}

void AsyncIOBufferPool::release(AsyncIOBuffer* buffer)
{
   if(!buffer)
      return;

   assert(buffer->length == bufferSize);
   assert(((uintptr_t)buffer->data % alignment) == 0);

   freeBuffers.push_back(buffer);
}

IOWorkerAsyncContext::IOWorkerAsyncContext(IOWorkerContext* workerContext) :
   aioEventFD(-1),
   workerContext(workerContext),
   bufferPool(DEFAULT_ASYNC_REQUEST_SLOTS, DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_ALIGNMENT),
   requestCompletionHandler(NULL),
   requestCompletionContext(NULL)
{
   aioEventFD = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
   if(aioEventFD == -1)
      throw std::runtime_error("unable to create aio eventfd");

   try
   {
      if(!workerContext || workerContext->asyncIOBackend == AsyncIOBackendType::LIBAIO)
         backend.reset(new LibAIOBackend(aioEventFD));
      else
         backend.reset(new IOUringBackend(aioEventFD, bufferPool));
   }
   catch(...)
   {
      close(aioEventFD);
      aioEventFD = -1;
      throw;
   }
}

IOWorkerAsyncContext::~IOWorkerAsyncContext()
{
   assert(activeRequests.empty());

   backend.reset();

   if(aioEventFD != -1)
      close(aioEventFD);
}

void IOWorkerAsyncContext::addRequest(AsyncIORequest* request)
{
   activeRequests.pushBack(request);
}

void IOWorkerAsyncContext::completeRequest(AsyncIORequest* request)
{
   activeRequests.remove(request);

   if(requestCompletionHandler)
      requestCompletionHandler(requestCompletionContext, request);

   request->release();
}

void IOWorkerAsyncContext::cancelAllRequests()
{
   while(!activeRequests.empty())
   {
      AsyncIORequest* request = activeRequests.front();
      activeRequests.remove(request);

      if(requestCompletionHandler)
         requestCompletionHandler(requestCompletionContext, request);

      request->cancel();
      request->release();
   }
}

int IOWorkerAsyncContext::submitRead(AsyncIORequest* request, int fd, AsyncIOBuffer* buffer,
   size_t length, off_t offset)
{
   return backend->submitRead(request, fd, buffer, length, offset);
}

int IOWorkerAsyncContext::submitWrite(AsyncIORequest* request, int fd, AsyncIOBuffer* buffer,
   size_t bufferOffset, size_t length, off_t offset)
{
   return backend->submitWrite(request, fd, buffer, bufferOffset, length, offset);
}

void IOWorkerAsyncContext::drainEventFD()
{
   uint64_t value;

   for(;;)
   {
      ssize_t readRes = read(aioEventFD, &value, sizeof(value));
      if(readRes == sizeof(value))
         return;

      if(readRes == -1 && errno == EINTR)
         continue;

      if(readRes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
         return;

      return;
   }
}

void IOWorkerAsyncContext::reapCompletions()
{
   AsyncIOCompletion completions[IOWORKER_AIO_COMPLETION_BURST];

   for(;;)
   {
      unsigned numCompletions = backend->reapCompletions(completions,
         IOWORKER_AIO_COMPLETION_BURST);
      if(!numCompletions)
         return;

      for(unsigned i = 0; i < numCompletions; i++)
      {
         AsyncIORequest* request = completions[i].request;
         request->onLocalIOComplete(completions[i].result);

         if(request->isComplete())
            completeRequest(request);
      }
   }
}

void IOWorkerAsyncContext::returnSocket(IncomingPreprocessedMsgWork* work)
{
   IOWorkerResponse* response = work->createIOWorkerResponse(
      workerContext->osdID, workerContext->workerIndex);

   workerContext->responseQueue->enqueueWait(response);
   workerContext->responseQueue->notify();
}
