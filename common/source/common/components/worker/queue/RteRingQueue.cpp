#include <common/system/System.h>
#include "RteRingQueue.h"

#include <errno.h>
#include <sched.h>
#include <sys/eventfd.h>
#include <unistd.h>

RteRingQueue::RteRingQueue(const std::string& name, unsigned capacity, unsigned flags) :
   ring(NULL), eventFD(-1)
{
   ring = rte_ring_create(name.c_str(), capacity, -1, flags);
   if(!ring)
      throw RteRingQueueException("Unable to create rte_ring: " + name + ": " +
         System::getErrString());

   eventFD = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
   if(eventFD == -1)
   {
      rte_ring_free(ring);
      ring = NULL;
      throw RteRingQueueException("Unable to create eventfd for ring: " + name + ": " +
         System::getErrString());
   }
}

RteRingQueue::~RteRingQueue()
{
   if(eventFD != -1)
      close(eventFD);

   if(ring)
      rte_ring_free(ring);
}

void RteRingQueue::enqueueWait(void* item)
{
   unsigned spins = 0;

   while(rte_ring_enqueue(ring, item) == -ENOBUFS)
   {
      if((++spins & 0x3f) == 0)
         sched_yield();
   }
}

unsigned RteRingQueue::dequeueBurst(void** outItems, unsigned maxItems)
{
   return rte_ring_dequeue_burst(ring, outItems, maxItems, NULL);
}

void RteRingQueue::notify()
{
   uint64_t value = 1;

   for(;;)
   {
      ssize_t writeRes = write(eventFD, &value, sizeof(value));
      if(likely(writeRes == (ssize_t)sizeof(value)))
         return;

      if(errno == EINTR)
         continue;

      throw RteRingQueueException("Unable to write eventfd: " + System::getErrString());
   }
}

void RteRingQueue::drainEventFD()
{
   uint64_t value;

   for(;;)
   {
      ssize_t readRes = read(eventFD, &value, sizeof(value));
      if(readRes == (ssize_t)sizeof(value))
         return;

      if(readRes == -1 && errno == EINTR)
         continue;

      if(readRes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
         return;

      if(readRes == 0)
         return;

      throw RteRingQueueException("Unable to read eventfd: " + System::getErrString());
   }
}

unsigned RteRingQueue::count() const
{
   return rte_ring_count(ring);
}

unsigned RteRingQueue::capacity() const
{
   return rte_ring_get_capacity(ring);
}
