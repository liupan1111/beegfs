#include <common/components/worker/queue/IOWorkerAsyncContext.h>

#include <gtest/gtest.h>

#include <map>
#include <set>

class TestAsyncIORequest : public AsyncIORequest
{
   public:
      TestAsyncIORequest(bool* cancelled = NULL, bool* destroyed = NULL) :
         cancelled(cancelled), destroyed(destroyed)
      {
      }

      ~TestAsyncIORequest()
      {
         if(destroyed)
            *destroyed = true;
      }

      bool start()
      {
         return true;
      }

      void onLocalIOComplete(int64_t result) override
      {
      }

      bool isComplete() const
      {
         return false;
      }

      void cancel()
      {
         if(cancelled)
            *cancelled = true;
      }

   private:
      bool* cancelled;
      bool* destroyed;
};

struct RequestCompletionTracker
{
   unsigned completions;
   bool requestDestroyed;
};

static void trackRequestCompletion(void* context, AsyncIORequest*)
{
   RequestCompletionTracker* tracker = (RequestCompletionTracker*)context;
   tracker->completions++;
   EXPECT_FALSE(tracker->requestDestroyed);
}

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

TEST(IOWorkerAsyncContext, bufferIndexesAreStableAndUnique)
{
   IOWorkerAsyncContext context(NULL);
   std::vector<AsyncIOBuffer*> buffers;
   std::set<unsigned> indexes;
   std::map<AsyncIOBuffer*, unsigned> assignedIndexes;

   for(size_t i = 0; i < IOWorkerAsyncContext::DEFAULT_ASYNC_REQUEST_SLOTS; i++)
   {
      AsyncIOBuffer* buffer = context.acquireBuffer();
      ASSERT_NE((AsyncIOBuffer*)NULL, buffer);
      EXPECT_TRUE(indexes.insert(buffer->bufferIndex).second);
      assignedIndexes[buffer] = buffer->bufferIndex;
      buffers.push_back(buffer);
   }

   for(auto iter = buffers.rbegin(); iter != buffers.rend(); iter++)
      context.releaseBuffer(*iter);

   for(size_t i = 0; i < buffers.size(); i++)
   {
      AsyncIOBuffer* buffer = context.acquireBuffer();
      ASSERT_NE((AsyncIOBuffer*)NULL, buffer);
      EXPECT_EQ(assignedIndexes[buffer], buffer->bufferIndex);
      EXPECT_LT(buffer->bufferIndex, IOWorkerAsyncContext::DEFAULT_ASYNC_REQUEST_SLOTS);
      context.releaseBuffer(buffer);
   }
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

TEST(IOWorkerAsyncContext, completionHandlerRunsBeforeRequestDestruction)
{
   IOWorkerAsyncContext context(NULL);
   RequestCompletionTracker tracker = {0, false};
   context.setRequestCompletionHandler(trackRequestCompletion, &tracker);

   TestAsyncIORequest* request = new TestAsyncIORequest(NULL, &tracker.requestDestroyed);
   context.addRequest(request);
   context.completeRequest(request);

   EXPECT_EQ(1u, tracker.completions);
   EXPECT_TRUE(tracker.requestDestroyed);
}

TEST(IOWorkerAsyncContext, cancellationRunsCompletionHandlerBeforeRequestDestruction)
{
   IOWorkerAsyncContext context(NULL);
   RequestCompletionTracker tracker = {0, false};
   bool cancelled = false;
   context.setRequestCompletionHandler(trackRequestCompletion, &tracker);

   context.addRequest(new TestAsyncIORequest(&cancelled, &tracker.requestDestroyed));
   context.cancelAllRequests();

   EXPECT_EQ(1u, tracker.completions);
   EXPECT_TRUE(cancelled);
   EXPECT_TRUE(tracker.requestDestroyed);
}
