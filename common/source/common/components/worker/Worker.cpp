#include <common/toolkit/TimeFine.h>
#include <common/components/worker/queue/RteRingQueue.h>
#include "Worker.h"

#include <errno.h>
#include <sys/epoll.h>
#include <unistd.h>

#define WORKER_IO_EPOLL_EVENTS 1
#define WORKER_IO_DEQUEUE_BURST 64

Worker::Worker(const std::string& workerID, MultiWorkQueue* workQueue, QueueWorkType workType)
    : PThread(workerID),
      log(workerID),
      terminateWithFullQueue(true),
      bufInLen(WORKER_BUFIN_SIZE),
      bufIn(NULL),
      bufOutLen(WORKER_BUFOUT_SIZE),
      bufOut(NULL),
      workQueue(workQueue),
      workType(workType),
      ioContext(NULL),
      personalWorkQueue(new PersonalWorkQueue() )
{
   HighResolutionStatsTk::resetStats(&this->stats);
}

void Worker::run()
{
   try
   {
      registerSignalHandler();

      initBuffers();

      /* note: we're not directly calling workLoop(workType) below, because:
         1) we want to check that the given workType value is really valid.
         2) we hope that the compiler can use this explicit check and value passing to optimize away
            the if-condition inside waitForWorkByType (so that a worker does not need to check its
            worktype for every incoming work). */

      if(workType == QueueWorkType_DIRECT)
         workLoop(QueueWorkType_DIRECT);
      else
      if(workType == QueueWorkType_INDIRECT)
         workLoop(QueueWorkType_INDIRECT);
      else
      if(workType == QueueWorkType_IO)
         workLoop(QueueWorkType_IO);
      else
         throw ComponentInitException(
            "Unknown/invalid work type given: " + StringTk::intToStr(workType) );

      log.log(Log_DEBUG, "Component stopped.");
   }
   catch(std::exception& e)
   {
      PThread::getCurrentThreadApp()->handleComponentException(e);
   }

}

int Worker::initIOEpollFD()
{
   if(!ioContext)
      throw ComponentInitException("IO worker has no queue context.");

   int epollFD = epoll_create(2);
   if(epollFD == -1)
      throw ComponentInitException("Unable to create IO worker epoll fd: " + System::getErrString());

   struct epoll_event event;
   event.events = EPOLLIN;
   event.data.ptr = ioContext->requestQueue.get();
   if(epoll_ctl(epollFD, EPOLL_CTL_ADD, ioContext->requestQueue->getEventFD(), &event) == -1)
   {
      close(epollFD);
      throw ComponentInitException("Unable to add request eventfd: " + System::getErrString());
   }

   return epollFD;
}

void Worker::waitForIOWorks(int epollFD, WorkList& outWorks)
{
   for(;;)
   {
      struct epoll_event events[WORKER_IO_EPOLL_EVENTS];
      int epollRes = epoll_wait(epollFD, events, WORKER_IO_EPOLL_EVENTS, -1);

      if(unlikely(epollRes < 0) )
      {
         if(errno == EINTR)
            continue;

         close(epollFD);
         throw WorkerException("Unrecoverable IO worker epoll_wait error: " +
            System::getErrString());
      }

      RteRingQueue* queue = (RteRingQueue*)events[0].data.ptr;
      queue->drainEventFD();

      drainIOQueue(ioContext->requestQueue.get(), outWorks);

      if(!outWorks.empty() )
         return;
   }
}

void Worker::drainIOQueue(RteRingQueue* queue, WorkList& outWorks)
{
   void* items[WORKER_IO_DEQUEUE_BURST];
   unsigned maxItems = queue->capacity();

   if(maxItems > WORKER_IO_DEQUEUE_BURST)
      maxItems = WORKER_IO_DEQUEUE_BURST;

   for(;;)
   {
      unsigned numItems = queue->dequeueBurst(items, maxItems);
      if(!numItems)
         return;

      for(unsigned i = 0; i < numItems; i++)
         outWorks.push_back((Work*)items[i]);
   }
}

void Worker::workLoop(QueueWorkType workType)
{
   LOG(WORKQUEUES, DEBUG, "Ready", ("TID", System::getTID()), workType);

   const bool isIOWorkType = workType == QueueWorkType_IO;
   int ioEpollFD = -1;

   if(isIOWorkType)
      ioEpollFD = initIOEpollFD();
   else
      workQueue->incNumWorkers(); // add this worker to queue stats

   while(!getSelfTerminate() || !maySelfTerminateNow() )
   {
      WorkList readyWorks;

      if(isIOWorkType)
         waitForIOWorks(ioEpollFD, readyWorks);
      else
         waitForWorkByType(stats, personalWorkQueue, workType, readyWorks);

      for(WorkListIter iter = readyWorks.begin(); iter != readyWorks.end(); iter++)
      {
         Work* work = *iter;
#ifdef BEEGFS_DEBUG_PROFILING
         TimeFine workStartTime;
#endif

         HighResolutionStatsTk::resetStats(&stats); // prepare stats

         // process the work packet
         work->process(bufIn, bufInLen, bufOut, bufOutLen);

         // update stats
         stats.incVals.workRequests = 1;
         HighResolutionStatsTk::addHighResIncStats(*work->getHighResolutionStats(), stats);

#ifdef BEEGFS_DEBUG_PROFILING
         TimeFine workEndTime;
         const auto workElapsedMS = workEndTime.elapsedSinceMS(&workStartTime);
         const auto workLatencyMS = workEndTime.elapsedSinceMS(work->getAgeTime());

         if (workElapsedMS >= 10)
         {
            if (workLatencyMS >= 10)
               LOG(WORKQUEUES, DEBUG, "Work processed.",
                     ("Elapsed ms", workElapsedMS), ("Total latency (ms)", workLatencyMS));
            else
               LOG(WORKQUEUES, DEBUG, "Work processed.", ("Elapsed ms", workElapsedMS),
                     ("Total latency (us)", workEndTime.elapsedSinceMicro(work->getAgeTime())));
         }
         else
         {
            if (workLatencyMS >= 10)
            {
               LOG(WORKQUEUES, DEBUG, "Work processed.",
                     ("Elapsed us", workEndTime.elapsedSinceMicro(&workStartTime)),
                     ("Total latency (ms)", workEndTime.elapsedSinceMS(work->getAgeTime())));

            }
            else
            {
               LOG(WORKQUEUES, DEBUG, "Work processed.",
                     ("Elapsed us", workEndTime.elapsedSinceMicro(&workStartTime)),
                     ("Total latency (us)", workEndTime.elapsedSinceMicro(work->getAgeTime())));
            }
         }
#endif

         // cleanup
         delete(work);
      }
   }

   if(ioEpollFD != -1)
      close(ioEpollFD);
}

void Worker::waitForWorkByType(HighResolutionStats& newStats, PersonalWorkQueue* personalWorkQueue,
   QueueWorkType workType, WorkList& outWorks)
{
   /* note: we hope the if-conditions below are optimized away when this is called from
      Worker::workLoop(), that's why we have the explicit work type arg in Worker::run() */

   if(workType == QueueWorkType_DIRECT)
      outWorks.push_back(workQueue->waitForDirectWork(newStats, personalWorkQueue) );
   else
   if(workType == QueueWorkType_INDIRECT)
      outWorks.push_back(workQueue->waitForAnyWork(newStats, personalWorkQueue) );
   else // should never happen
      throw WorkerException("Unknown/invalid work type given: " + StringTk::intToStr(workType));
}


/**
 * Note: For delayed buffer allocation during run(), because of NUMA-archs.
 */
void Worker::initBuffers()
{
   if(this->bufInLen)
   {
      void* bufInVoid = NULL;
      int inAllocRes = posix_memalign(&bufInVoid, sysconf(_SC_PAGESIZE), bufInLen);
      IGNORE_UNUSED_VARIABLE(inAllocRes);
      this->bufIn = (char*)bufInVoid;
   }

   if(this->bufOutLen)
   {
      void* bufOutVoid = NULL;
      int outAllocRes = posix_memalign(&bufOutVoid, sysconf(_SC_PAGESIZE), bufOutLen);
      IGNORE_UNUSED_VARIABLE(outAllocRes);
      this->bufOut = (char*)bufOutVoid;
   }
}
