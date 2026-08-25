#pragma once

#include <common/components/worker/queue/IOWorkerContext.h>
#include <common/components/worker/Work.h>
#include <common/toolkit/NamedException.h>
#include <common/Common.h>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

DECLARE_NAMEDEXCEPTION(StorageIOWorkerRouterException, "StorageIOWorkerRouterException")

class StorageIOWorkerRouter
{
   public:
      static const unsigned DEFAULT_RING_CAPACITY = 16;

      StorageIOWorkerRouter(unsigned numListeners, unsigned numWorkersPerOSD);
      ~StorageIOWorkerRouter() {}

      void addOSD(uint16_t osdID);

      IOWorkerContext* getWorkerContext(uint16_t osdID, unsigned workerIndex);
      IOWorkerContext* submit(uint16_t osdID, unsigned listenerIndex, Work* work);

      void complete(uint16_t osdID, unsigned workerIndex);

      std::vector<IOWorkerContext*> getResponseContextsForListener(unsigned listenerIndex) const;

      size_t getPendingCount(uint16_t osdID) const;
      size_t getWorkerPending(uint16_t osdID, unsigned workerIndex) const;
      size_t getListenerGroupPending(uint16_t osdID, unsigned listenerIndex) const;

      unsigned getNumWorkersPerOSD() const
      {
         return numWorkersPerOSD;
      }

   private:
      typedef std::vector<std::unique_ptr<IOWorkerContext>> OSDWorkerContexts;

      unsigned numListeners;
      unsigned numWorkersPerOSD;
      std::map<uint16_t, OSDWorkerContexts> osds;

      std::pair<unsigned, unsigned> getGroupRange(unsigned listenerIndex) const;
      unsigned getListenerIndexForWorker(unsigned workerIndex) const;
      OSDWorkerContexts& getOSDWorkerContexts(uint16_t osdID);
      const OSDWorkerContexts& getOSDWorkerContexts(uint16_t osdID) const;
};
