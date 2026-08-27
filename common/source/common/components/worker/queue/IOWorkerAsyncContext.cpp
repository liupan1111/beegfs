#include <common/components/worker/queue/IOWorkerAsyncContext.h>
#include <common/components/streamlistenerv2/IncomingPreprocessedMsgWork.h>
#include <common/components/worker/queue/IOWorkerContext.h>
#include <common/system/System.h>
#include <common/Common.h>

#include <assert.h>
#include <errno.h>
#include <new>
#include <stdexcept>
#include <string.h>
#include <sys/eventfd.h>
#include <unistd.h>

#define IOWORKER_AIO_COMPLETION_BURST 64

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
   aioContext(0),
   aioEventFD(-1),
   workerContext(workerContext),
   bufferPool(DEFAULT_BUFFER_COUNT, DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_ALIGNMENT)
{
   aioEventFD = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
   if(aioEventFD == -1)
      throw std::runtime_error("unable to create aio eventfd");

   int setupRes = io_setup(DEFAULT_AIO_QUEUE_DEPTH, &aioContext);
   if(setupRes < 0)
   {
      close(aioEventFD);
      aioEventFD = -1;
      throw std::runtime_error("unable to setup aio context");
   }
}

IOWorkerAsyncContext::~IOWorkerAsyncContext()
{
   cancelAllRequests();

   if(aioContext)
      io_destroy(aioContext);

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
   delete request;
}

void IOWorkerAsyncContext::cancelAllRequests()
{
   while(!activeRequests.empty())
   {
      AsyncIORequest* request = activeRequests.front();
      activeRequests.remove(request);
      request->cancel();
      delete request;
   }
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
   struct io_event events[IOWORKER_AIO_COMPLETION_BURST];

   for(;;)
   {
      int numEvents = io_getevents(aioContext, 0, IOWORKER_AIO_COMPLETION_BURST, events, NULL);
      if(numEvents <= 0)
         return;

      for(int i = 0; i < numEvents; i++)
      {
         AsyncIORequest* request = (AsyncIORequest*)events[i].data;
         request->onAIOComplete(events[i]);

         if(request->isComplete())
            completeRequest(request);
      }
   }
}

void IOWorkerAsyncContext::returnSocket(IncomingPreprocessedMsgWork* work)
{
   IOWorkerResponse* response = work->detachIOWorkerResponse(
      workerContext->osdID, workerContext->workerIndex);

   workerContext->responseQueue->enqueueWait(response);
   workerContext->responseQueue->notify();
}
