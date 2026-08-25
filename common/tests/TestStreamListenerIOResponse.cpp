#include <common/components/streamlistenerv2/StreamListenerV2.h>

#include <gtest/gtest.h>

class TestStreamListenerForIOResponse : public StreamListenerV2
{
   public:
      TestStreamListenerForIOResponse() : StreamListenerV2("TestStreamLis", NULL, NULL),
         completed(0), lastOSDID(0), lastWorkerIndex(0)
      {
      }

      void handle(IOWorkerResponse* response)
      {
         handleIOWorkerResponse(response);
      }

      unsigned completed;
      uint16_t lastOSDID;
      unsigned lastWorkerIndex;

   protected:
      virtual void onIOWorkerResponseDequeued(IOWorkerResponse* response)
      {
         completed++;
         lastOSDID = response->osdID;
         lastWorkerIndex = response->workerIndex;
      }
};

TEST(StreamListenerIOResponse, closedSocketStillCompletesLoadAccounting)
{
   TestStreamListenerForIOResponse listener;
   IOWorkerResponse response(NULL, 42, 7, false);

   listener.handle(&response);

   EXPECT_EQ(listener.completed, 1u);
   EXPECT_EQ(listener.lastOSDID, 42u);
   EXPECT_EQ(listener.lastWorkerIndex, 7u);
}
