#include <components/worker/StorageIOWorkerRouter.h>

#include <gtest/gtest.h>

class RouterTestWork : public Work
{
   public:
      virtual void process(char*, unsigned, char*, unsigned)
      {
      }
};

static void drainAndDelete(IOWorkerContext* context)
{
   void* items[16];

   for(;;)
   {
      unsigned numItems = context->requestQueue->dequeueBurst(items, 16);
      if(!numItems)
         return;

      for(unsigned i = 0; i < numItems; i++)
         delete (Work*)items[i];
   }
}

TEST(StorageIOWorkerRouter, rejectsTooFewWorkers)
{
   EXPECT_THROW(StorageIOWorkerRouter(4, 3), StorageIOWorkerRouterException);
}

TEST(StorageIOWorkerRouter, splitsWorkersByListener)
{
   StorageIOWorkerRouter router(3, 10);
   router.addOSD(1);

   EXPECT_EQ(router.getResponseContextsForListener(0).size(), 4u);
   EXPECT_EQ(router.getResponseContextsForListener(1).size(), 3u);
   EXPECT_EQ(router.getResponseContextsForListener(2).size(), 3u);

   EXPECT_EQ(router.getWorkerContext(1, 0)->listenerIndex, 0u);
   EXPECT_EQ(router.getWorkerContext(1, 3)->listenerIndex, 0u);
   EXPECT_EQ(router.getWorkerContext(1, 4)->listenerIndex, 1u);
   EXPECT_EQ(router.getWorkerContext(1, 7)->listenerIndex, 2u);
}

TEST(StorageIOWorkerRouter, responseContextsSpanAllOSDsForListener)
{
   StorageIOWorkerRouter router(2, 4);
   router.addOSD(1);
   router.addOSD(2);

   std::vector<IOWorkerContext*> listener0Contexts = router.getResponseContextsForListener(0);
   std::vector<IOWorkerContext*> listener1Contexts = router.getResponseContextsForListener(1);

   ASSERT_EQ(listener0Contexts.size(), 4u);
   ASSERT_EQ(listener1Contexts.size(), 4u);

   for(auto iter = listener0Contexts.begin(); iter != listener0Contexts.end(); iter++)
      EXPECT_EQ((*iter)->listenerIndex, 0u);

   for(auto iter = listener1Contexts.begin(); iter != listener1Contexts.end(); iter++)
      EXPECT_EQ((*iter)->listenerIndex, 1u);
}

TEST(StorageIOWorkerRouter, selectByMinLoad)
{
   StorageIOWorkerRouter router(2, 4);
   router.addOSD(1);

   IOWorkerContext* first = router.submit(1, 0, new RouterTestWork());
   IOWorkerContext* second = router.submit(1, 0, new RouterTestWork());
   IOWorkerContext* third = router.submit(1, 0, new RouterTestWork());

   EXPECT_EQ(first->workerIndex, 0u);
   EXPECT_EQ(second->workerIndex, 1u);
   EXPECT_EQ(third->workerIndex, 0u);

   EXPECT_EQ(router.getWorkerPending(1, 0), 2u);
   EXPECT_EQ(router.getWorkerPending(1, 1), 1u);
   EXPECT_EQ(router.getListenerGroupPending(1, 0), 3u);

   router.complete(1, 0);
   EXPECT_EQ(router.getWorkerPending(1, 0), 1u);

   drainAndDelete(router.getWorkerContext(1, 0));
   drainAndDelete(router.getWorkerContext(1, 1));
}
