#include <common/components/worker/queue/RteRingQueue.h>

#include <gtest/gtest.h>

TEST(RteRingQueue, spscOrderAndBurst)
{
   RteRingQueue queue("test-spsc", 8, RING_F_SP_ENQ | RING_F_SC_DEQ | RING_F_EXACT_SZ);

   int one = 1;
   int two = 2;
   int three = 3;

   queue.enqueueWait(&one);
   queue.enqueueWait(&two);
   queue.enqueueWait(&three);

   void* items[4];
   unsigned numItems = queue.dequeueBurst(items, 4);

   ASSERT_EQ(numItems, 3u);
   EXPECT_EQ(items[0], &one);
   EXPECT_EQ(items[1], &two);
   EXPECT_EQ(items[2], &three);
   EXPECT_EQ(queue.count(), 0u);
}

TEST(RteRingQueue, eventfdNotifyAndDrain)
{
   RteRingQueue queue("test-eventfd", 8, RING_F_SP_ENQ | RING_F_SC_DEQ | RING_F_EXACT_SZ);

   queue.notify();
   queue.notify();
   queue.drainEventFD();

   int value = 1;
   queue.enqueueWait(&value);

   void* item = NULL;
   ASSERT_EQ(queue.dequeueBurst(&item, 1), 1u);
   EXPECT_EQ(item, &value);
}

TEST(RteRingQueue, mpscModeAcceptsItems)
{
   RteRingQueue queue("test-mpsc", 8, RING_F_SC_DEQ | RING_F_EXACT_SZ);

   int one = 1;
   int two = 2;

   queue.enqueueWait(&one);
   queue.enqueueWait(&two);

   void* items[2];
   ASSERT_EQ(queue.dequeueBurst(items, 2), 2u);
   EXPECT_EQ(items[0], &one);
   EXPECT_EQ(items[1], &two);
}
