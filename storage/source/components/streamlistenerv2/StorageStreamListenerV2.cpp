#include <components/streamlistenerv2/StorageStreamListenerV2.h>

#include <program/Program.h>

StorageStreamListenerV2::StorageStreamListenerV2(std::string listenerID, AbstractApp* app,
   unsigned listenerIndex) :
   StreamListenerV2(listenerID, app, NULL), listenerIndex(listenerIndex)
{
   // nothing to be done here
}

void StorageStreamListenerV2::registerIOWorkerResponseQueues()
{
   StorageIOWorkerRouter* router = Program::getApp()->getIOWorkerRouter();
   if(!router)
      return;

   std::vector<IOWorkerContext*> responseContexts =
      router->getResponseContextsForListener(listenerIndex);

   for(auto iter = responseContexts.begin(); iter != responseContexts.end(); iter++)
      addIOWorkerResponseQueue((*iter)->responseQueue.get());
}

MultiWorkQueue* StorageStreamListenerV2::getWorkQueue(uint16_t osdID) const
{
   return Program::getApp()->getWorkQueue(osdID);
}

bool StorageStreamListenerV2::enqueueIncomingWork(Socket* sock, NetMessageHeader* msgHeader,
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

void StorageStreamListenerV2::flushIncomingWorkNotifications()
{
   for(auto iter = notifyContexts.begin(); iter != notifyContexts.end(); iter++)
      (*iter)->requestQueue->notify();

   notifyContexts.clear();
}

void StorageStreamListenerV2::onIOWorkerResponseDequeued(IOWorkerResponse* response)
{
   Program::getApp()->getIOWorkerRouter()->complete(response->osdID, response->workerIndex);
}
