#include "StorageIOWorkerRouter.h"

#include <common/toolkit/StringTk.h>

#include <algorithm>

StorageIOWorkerRouter::StorageIOWorkerRouter(unsigned numListeners, unsigned numWorkersPerOSD) :
   numListeners(numListeners), numWorkersPerOSD(numWorkersPerOSD)
{
   if(!numListeners)
      throw StorageIOWorkerRouterException("numListeners must not be zero.");

   if(numWorkersPerOSD < numListeners)
      throw StorageIOWorkerRouterException("numWorkers must be greater than or equal to "
         "numStreamListeners for lockless storage IO worker routing.");
}

void StorageIOWorkerRouter::addOSD(uint16_t osdID)
{
   if(osds.find(osdID) != osds.end() )
      return;

   OSDWorkerContexts workers;

   for(unsigned workerIndex = 0; workerIndex < numWorkersPerOSD; workerIndex++)
   {
      unsigned listenerIndex = getListenerIndexForWorker(workerIndex);
      std::string prefix = "storage-io-osd" + StringTk::uintToStr(osdID) + "-w" +
         StringTk::uintToStr(workerIndex);

      std::unique_ptr<IOWorkerContext> context(new IOWorkerContext());
      context->requestQueue.reset(new RteRingQueue(prefix + "-request", DEFAULT_RING_CAPACITY,
         RING_F_SP_ENQ | RING_F_SC_DEQ | RING_F_EXACT_SZ));
      context->responseQueue.reset(new RteRingQueue(prefix + "-response", DEFAULT_RING_CAPACITY,
         RING_F_SP_ENQ | RING_F_SC_DEQ | RING_F_EXACT_SZ));
      context->highPrioQueue.reset(new RteRingQueue(prefix + "-high", DEFAULT_RING_CAPACITY,
         RING_F_SC_DEQ | RING_F_EXACT_SZ));
      context->load = 0;

      context->osdID = osdID;
      context->workerIndex = workerIndex;
      context->listenerIndex = listenerIndex;

      workers.push_back(std::move(context));
   }

   osds[osdID] = std::move(workers);
}

IOWorkerContext* StorageIOWorkerRouter::getWorkerContext(uint16_t osdID, unsigned workerIndex)
{
   OSDWorkerContexts& workers = getOSDWorkerContexts(osdID);
   if(workerIndex >= workers.size() )
      throw StorageIOWorkerRouterException("Invalid workerIndex.");

   return workers[workerIndex].get();
}

IOWorkerContext* StorageIOWorkerRouter::submit(uint16_t osdID, unsigned listenerIndex, Work* work)
{
   if(listenerIndex >= numListeners)
      throw StorageIOWorkerRouterException("Invalid listenerIndex.");

   OSDWorkerContexts& workers = getOSDWorkerContexts(osdID);
   std::pair<unsigned, unsigned> range = getGroupRange(listenerIndex);
   unsigned begin = range.first;
   unsigned end = range.second;
   unsigned bestWorkerIndex = begin;
   size_t bestLoad = (size_t)-1;

   for(unsigned workerIndex = begin; workerIndex < end; workerIndex++)
   {
      size_t currentLoad = workers[workerIndex]->load;

      if(currentLoad < bestLoad)
      {
         bestLoad = currentLoad;
         bestWorkerIndex = workerIndex;
      }
   }

   IOWorkerContext* context = workers[bestWorkerIndex].get();
   context->requestQueue->enqueueWait(work);
   context->load++;

   return context;
}

void StorageIOWorkerRouter::complete(uint16_t osdID, unsigned workerIndex)
{
   OSDWorkerContexts& workers = getOSDWorkerContexts(osdID);
   if(workerIndex >= workers.size() )
      throw StorageIOWorkerRouterException("Invalid workerIndex.");

   if(workers[workerIndex]->load)
      workers[workerIndex]->load--;
}

std::vector<IOWorkerContext*> StorageIOWorkerRouter::getResponseContextsForListener(
   unsigned listenerIndex) const
{
   std::vector<IOWorkerContext*> responseContexts;
   std::pair<unsigned, unsigned> range = getGroupRange(listenerIndex);

   for(auto iter = osds.begin(); iter != osds.end(); iter++)
   {
      const OSDWorkerContexts& workers = iter->second;
      for(unsigned workerIndex = range.first; workerIndex < range.second; workerIndex++)
         responseContexts.push_back(workers[workerIndex].get());
   }

   return responseContexts;
}

size_t StorageIOWorkerRouter::getPendingCount(uint16_t osdID) const
{
   const OSDWorkerContexts& workers = getOSDWorkerContexts(osdID);
   size_t sum = 0;

   for(auto iter = workers.begin(); iter != workers.end(); iter++)
      sum += (*iter)->load;

   return sum;
}

size_t StorageIOWorkerRouter::getWorkerPending(uint16_t osdID, unsigned workerIndex) const
{
   const OSDWorkerContexts& workers = getOSDWorkerContexts(osdID);
   if(workerIndex >= workers.size() )
      throw StorageIOWorkerRouterException("Invalid workerIndex.");

   return workers[workerIndex]->load;
}

size_t StorageIOWorkerRouter::getListenerGroupPending(uint16_t osdID,
   unsigned listenerIndex) const
{
   const OSDWorkerContexts& workers = getOSDWorkerContexts(osdID);
   std::pair<unsigned, unsigned> range = getGroupRange(listenerIndex);
   size_t sum = 0;

   for(unsigned workerIndex = range.first; workerIndex < range.second; workerIndex++)
      sum += workers[workerIndex]->load;

   return sum;
}

std::pair<unsigned, unsigned> StorageIOWorkerRouter::getGroupRange(unsigned listenerIndex) const
{
   if(listenerIndex >= numListeners)
      throw StorageIOWorkerRouterException("Invalid listenerIndex.");

   unsigned base = numWorkersPerOSD / numListeners;
   unsigned rem = numWorkersPerOSD % numListeners;
   unsigned begin = listenerIndex * base + std::min(listenerIndex, rem);
   unsigned size = base + (listenerIndex < rem ? 1 : 0);

   return std::make_pair(begin, begin + size);
}

unsigned StorageIOWorkerRouter::getListenerIndexForWorker(unsigned workerIndex) const
{
   for(unsigned listenerIndex = 0; listenerIndex < numListeners; listenerIndex++)
   {
      std::pair<unsigned, unsigned> range = getGroupRange(listenerIndex);
      if(workerIndex >= range.first && workerIndex < range.second)
         return listenerIndex;
   }

   throw StorageIOWorkerRouterException("Unable to map workerIndex to listenerIndex.");
}

StorageIOWorkerRouter::OSDWorkerContexts& StorageIOWorkerRouter::getOSDWorkerContexts(uint16_t osdID)
{
   auto iter = osds.find(osdID);
   if(iter != osds.end() )
      return iter->second;

   if(osds.empty() )
      throw StorageIOWorkerRouterException("No OSDs registered in StorageIOWorkerRouter.");

   return osds.begin()->second;
}

const StorageIOWorkerRouter::OSDWorkerContexts& StorageIOWorkerRouter::getOSDWorkerContexts(
   uint16_t osdID) const
{
   auto iter = osds.find(osdID);
   if(iter != osds.end() )
      return iter->second;

   if(osds.empty() )
      throw StorageIOWorkerRouterException("No OSDs registered in StorageIOWorkerRouter.");

   return osds.begin()->second;
}
