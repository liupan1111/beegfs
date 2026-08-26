#include <common/components/worker/queue/IOWorkerAsyncContext.h>

#include <gtest/gtest.h>

class TestAsyncIORequest : public AsyncIORequest
{
   public:
      bool start()
      {
         return true;
      }

      void onAIOComplete(const io_event& event)
      {
      }

      bool isComplete() const
      {
         return false;
      }
};

TEST(IOWorkerAsyncContext, requestSlotsMatchBufferPool)
{
   IOWorkerAsyncContext context(NULL);

   EXPECT_EQ(IOWorkerAsyncContext::DEFAULT_ASYNC_REQUEST_SLOTS,
      context.getNumAvailableRequestSlots());

   std::vector<AsyncIOBuffer*> buffers;
   for(size_t i = 0; i < IOWorkerAsyncContext::DEFAULT_ASYNC_REQUEST_SLOTS; i++)
   {
      AsyncIOBuffer* buffer = context.acquireBuffer();
      ASSERT_NE((AsyncIOBuffer*)NULL, buffer);
      buffers.push_back(buffer);
   }

   for(size_t i = 0; i < buffers.size(); i++)
      context.releaseBuffer(buffers[i]);
}

TEST(IOWorkerAsyncContext, activeRequestsConsumeSlots)
{
   IOWorkerAsyncContext context(NULL);

   TestAsyncIORequest* request = new TestAsyncIORequest();
   context.addRequest(request);

   EXPECT_EQ(1u, context.getNumActiveRequests());
   EXPECT_EQ(IOWorkerAsyncContext::DEFAULT_ASYNC_REQUEST_SLOTS - 1,
      context.getNumAvailableRequestSlots());

   context.completeRequest(request);

   EXPECT_EQ(0u, context.getNumActiveRequests());
   EXPECT_EQ(IOWorkerAsyncContext::DEFAULT_ASYNC_REQUEST_SLOTS,
      context.getNumAvailableRequestSlots());
}
