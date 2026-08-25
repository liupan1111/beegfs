#pragma once

#include <common/components/worker/queue/RteRingQueue.h>
#include <common/Common.h>

#include <memory>

class Socket;

struct IOWorkerContext
{
   uint16_t osdID;
   unsigned workerIndex;
   unsigned listenerIndex;

   std::unique_ptr<RteRingQueue> requestQueue;
   std::unique_ptr<RteRingQueue> responseQueue;
   std::unique_ptr<RteRingQueue> highPrioQueue;
   size_t load;
};

struct IOWorkerResponse
{
   Socket* sock;
   uint16_t osdID;
   unsigned workerIndex;
   bool hasImmediateData;

   IOWorkerResponse(Socket* sock, uint16_t osdID, unsigned workerIndex,
      bool hasImmediateData) :
      sock(sock), osdID(osdID), workerIndex(workerIndex), hasImmediateData(hasImmediateData)
   {
   }
};
