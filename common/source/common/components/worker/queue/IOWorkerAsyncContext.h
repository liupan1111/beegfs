#pragma once

#include <common/Common.h>
#include <common/components/worker/queue/IOWorkerContext.h>

#include <memory>
#include <stdint.h>
#include <sys/types.h>
#include <vector>

class IncomingPreprocessedMsgWork;
class AsyncIOBackend;

struct AsyncIOBuffer
{
   char* data;
   size_t length;
   unsigned bufferIndex;

   AsyncIOBuffer() : data(NULL), length(0), bufferIndex(0)
   {
   }
};

class AsyncIORequest
{
   friend class AsyncIORequestList;

   public:
      AsyncIORequest();
      virtual ~AsyncIORequest() {}

      AsyncIORequest(const AsyncIORequest&) = delete;
      AsyncIORequest(AsyncIORequest&&) = delete;
      AsyncIORequest& operator=(const AsyncIORequest&) = delete;
      AsyncIORequest& operator=(AsyncIORequest&&) = delete;

      virtual bool start() = 0;
      virtual void onLocalIOComplete(int64_t result) = 0;
      virtual int getSendCompletionFD() const
      {
         return -1;
      }
      virtual void onSendCQComplete() {}
      virtual bool isComplete() const = 0;
      virtual void cancel() {}
      virtual void release()
      {
         delete this;
      }

      bool isInActiveList() const
      {
         return inActiveList;
      }

   private:
      AsyncIORequest* prev;
      AsyncIORequest* next;
   bool inActiveList;
};

struct AsyncIOCompletion
{
   AsyncIORequest* request;
   int64_t result;
};

class AsyncIORequestList
{
   public:
      AsyncIORequestList();

      void pushBack(AsyncIORequest* request);
      void remove(AsyncIORequest* request);
      bool empty() const;
      size_t size() const;
      AsyncIORequest* front() const;

   private:
      AsyncIORequest* head;
      AsyncIORequest* tail;
      size_t numRequests;
};

class AsyncIOBufferPool
{
   public:
      AsyncIOBufferPool(size_t numBuffers, size_t bufferSize, size_t alignment);
      ~AsyncIOBufferPool();

      AsyncIOBuffer* acquire();
      void release(AsyncIOBuffer* buffer);

      size_t getNumFree() const
      {
         return freeBuffers.size();
      }

      size_t getBufferSize() const
      {
         return bufferSize;
      }

      const std::vector<AsyncIOBuffer*>& getBuffers() const
      {
         return allBuffers;
      }

   private:
      std::vector<AsyncIOBuffer*> allBuffers;
      std::vector<AsyncIOBuffer*> freeBuffers;
      size_t bufferSize;
      size_t alignment;
};

class IOWorkerAsyncContext
{
   public:
      typedef void (*RequestCompletionHandler)(void* context, AsyncIORequest* request);

      static const unsigned DEFAULT_AIO_QUEUE_DEPTH = 128;
      static const size_t DEFAULT_BUFFER_SIZE = 1024 * 1024;
      static const size_t DEFAULT_BUFFER_ALIGNMENT = 4096;
      static const size_t DEFAULT_ASYNC_REQUEST_SLOTS = 16;

      IOWorkerAsyncContext(IOWorkerContext* workerContext);
      ~IOWorkerAsyncContext();

      IOWorkerAsyncContext(const IOWorkerAsyncContext&) = delete;
      IOWorkerAsyncContext(IOWorkerAsyncContext&&) = delete;
      IOWorkerAsyncContext& operator=(const IOWorkerAsyncContext&) = delete;
      IOWorkerAsyncContext& operator=(IOWorkerAsyncContext&&) = delete;

      int getEventFD() const
      {
         return aioEventFD;
      }

      AsyncIOBuffer* acquireBuffer()
      {
         return bufferPool.acquire();
      }

      void releaseBuffer(AsyncIOBuffer* buffer)
      {
         bufferPool.release(buffer);
      }

      void addRequest(AsyncIORequest* request);
      void completeRequest(AsyncIORequest* request);
      void cancelAllRequests();
      int submitRead(AsyncIORequest* request, int fd, AsyncIOBuffer* buffer, size_t length,
         off_t offset);
      int submitWrite(AsyncIORequest* request, int fd, AsyncIOBuffer* buffer,
         size_t bufferOffset, size_t length, off_t offset);
      void setRequestCompletionHandler(RequestCompletionHandler handler, void* context)
      {
         requestCompletionHandler = handler;
         requestCompletionContext = context;
      }

      void drainEventFD();
      void reapCompletions();
      void returnSocket(IncomingPreprocessedMsgWork* work);

      size_t getNumActiveRequests() const
      {
         return activeRequests.size();
      }

      size_t getNumAvailableRequestSlots() const
      {
         return DEFAULT_ASYNC_REQUEST_SLOTS - activeRequests.size();
      }

   private:
      int aioEventFD;
      IOWorkerContext* workerContext;
      AsyncIORequestList activeRequests;
      AsyncIOBufferPool bufferPool;
      std::unique_ptr<AsyncIOBackend> backend;
      RequestCompletionHandler requestCompletionHandler;
      void* requestCompletionContext;
};
