#pragma once

#include <common/Common.h>

class RteRingQueue;
class Socket;

struct IOWorkerQueues
{
   uint16_t osdID;
   unsigned workerIndex;
   unsigned listenerIndex;

   RteRingQueue* requestQueue;
   RteRingQueue* responseQueue;
   RteRingQueue* highPrioQueue;
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
