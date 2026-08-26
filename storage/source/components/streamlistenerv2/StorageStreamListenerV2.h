#pragma once

#include <app/App.h>
#include <common/components/streamlistenerv2/IncomingPreprocessedMsgWork.h>
#include <common/components/streamlistenerv2/StreamListenerV2.h>
#include <components/worker/StorageIOWorkerRouter.h>
#include <program/Program.h>

#include <set>


/**
 * Other than common StreamListenerV2, this class can handle mutliple work queues through an
 * overridden getWorkQueue() method.
 */
class StorageStreamListenerV2 : public StreamListenerV2
{
   public:
      StorageStreamListenerV2(std::string listenerID, AbstractApp* app, unsigned listenerIndex):
         StreamListenerV2(listenerID, app, NULL), listenerIndex(listenerIndex)
      {
         // nothing to be done here
      }

      virtual ~StorageStreamListenerV2() {}


   protected:
      // getters & setters

      virtual MultiWorkQueue* getWorkQueue(uint16_t osdID) const
      {
         return Program::getApp()->getWorkQueue(osdID);
      }

      virtual bool enqueueIncomingWork(Socket* sock, NetMessageHeader* msgHeader,
         IncomingPreprocessedMsgWork* work)
      {
         if(sock->getIsDirect() )
            return StreamListenerV2::enqueueIncomingWork(sock, msgHeader, work);

         uint16_t osdID = msgHeader->msgTargetID;
         IOWorkerContext* context = Program::getApp()->getIOWorkerRouter()->submit(
            osdID, listenerIndex, work);
         notifyContexts.insert(context);
         return true;
      }

      virtual void flushIncomingWorkNotifications()
      {
         for(auto iter = notifyContexts.begin(); iter != notifyContexts.end(); iter++)
            (*iter)->requestQueue->notify();

         notifyContexts.clear();
      }

   private:
      unsigned listenerIndex;
      std::set<IOWorkerContext*> notifyContexts;
};
