#pragma once

#include <common/toolkit/NamedException.h>
#include <common/Common.h>

#include <string>

extern "C"
{
#include <common/toolkit/ring/rte_ring_standalone.h>
}

DECLARE_NAMEDEXCEPTION(RteRingQueueException, "RteRingQueueException")

class RteRingQueue
{
   public:
      RteRingQueue(const std::string& name, unsigned capacity, unsigned flags);
      ~RteRingQueue();

      RteRingQueue(const RteRingQueue&) = delete;
      RteRingQueue& operator=(const RteRingQueue&) = delete;

      void enqueueWait(void* item);
      unsigned dequeueBurst(void** outItems, unsigned maxItems);

      int getEventFD() const
      {
         return eventFD;
      }

      void notify();
      void drainEventFD();
      unsigned count() const;
      unsigned capacity() const;

   private:
      struct rte_ring* ring;
      int eventFD;
};
