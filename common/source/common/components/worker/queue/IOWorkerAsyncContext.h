#pragma once

#include <common/Common.h>

#include <libaio.h>

#include <stdint.h>
#include <vector>

struct io_event;
class IncomingPreprocessedMsgWork;
struct IOWorkerContext;

struct AsyncIOBuffer
{
   char* data;
   size_t length;

   AsyncIOBuffer() : data(NULL), length(0)
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
      virtual void onAIOComplete(const io_event& event) = 0;
      virtual bool isComplete() const = 0;
      virtual void cancel() {}

      bool isInActiveList() const
      {
         return inActiveList;
      }

   private:
      AsyncIORequest* prev;
      AsyncIORequest* next;
      bool inActiveList;
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

   private:
      std::vector<AsyncIOBuffer*> allBuffers;
      std::vector<AsyncIOBuffer*> freeBuffers;
      size_t bufferSize;
      size_t alignment;
};

class IOWorkerAsyncContext
{
   public:
      static const unsigned DEFAULT_AIO_QUEUE_DEPTH = 128;
      static const size_t DEFAULT_BUFFER_SIZE = 1024 * 1024;
      static const size_t DEFAULT_BUFFER_ALIGNMENT = 4096;
      static const size_t DEFAULT_BUFFER_COUNT = 16;

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

      io_context_t getAIOContext() const
      {
         return aioContext;
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

      void drainEventFD();
      void reapCompletions();
      void returnSocket(IncomingPreprocessedMsgWork* work);

      size_t getNumActiveRequests() const
      {
         return activeRequests.size();
      }

   private:
      io_context_t aioContext;
      int aioEventFD;
      IOWorkerContext* workerContext;
      AsyncIORequestList activeRequests;
      AsyncIOBufferPool bufferPool;
};
