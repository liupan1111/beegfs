#pragma once

#include <common/components/streamlistenerv2/IncomingPreprocessedMsgWork.h>
#include <common/components/streamlistenerv2/StreamListenerV2.h>
#include <common/components/worker/queue/MultiWorkQueue.h>
#include <components/worker/StorageIOWorkerRouter.h>

#include <set>


/**
 * Other than common StreamListenerV2, this class can handle multiple work queues through an
 * overridden getWorkQueue() method.
 */
class StorageStreamListenerV2 : public StreamListenerV2
{
   public:
      StorageStreamListenerV2(std::string listenerID, AbstractApp* app, unsigned listenerIndex);

      virtual ~StorageStreamListenerV2() {}

      void registerIOWorkerResponseQueues();


   protected:
      // getters & setters

      virtual MultiWorkQueue* getWorkQueue(uint16_t osdID) const;

      virtual bool enqueueIncomingWork(Socket* sock, NetMessageHeader* msgHeader,
         IncomingPreprocessedMsgWork* work);

      virtual void flushIncomingWorkNotifications();

      virtual void onIOWorkerResponseDequeued(IOWorkerResponse* response);

   private:
      unsigned listenerIndex;
      std::set<IOWorkerContext*> notifyContexts;
};
