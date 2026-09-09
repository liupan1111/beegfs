#pragma once

#include <common/components/worker/UnixConnWorker.h>
#include <common/net/sock/NetworkInterfaceCard.h>
#include <common/threading/Mutex.h>
#include <common/threading/Condition.h>
#include <common/nodes/NodeConnPool.h>
#include <common/Common.h>


typedef std::list<UnixConnWorker*> UnixConnWorkerList;
typedef UnixConnWorkerList::iterator UnixConnWorkerListIter;

class LocalConnWorker;

/**
 * This is the conn pool which is used when a node sends network messages to itself, e.g. like a
 * single mds would do in case of an incoming mkdir msg.
 * It is based on unix sockets for inter-process communication instead of network sockets and
 * creates a handler thread for each established connection to process "incoming" msgs.
 */
class LocalNodeConnPool : public NodeConnPool
{
   public:
      struct LocalConnection
      {
         LocalConnWorker* worker;
         Socket* socket;

         LocalConnection(LocalConnWorker* worker = NULL, Socket* socket = NULL) :
            worker(worker), socket(socket)
         {
         }
      };

      LocalNodeConnPool(Node& parentNode, NicAddressList& nicList);
      virtual ~LocalNodeConnPool();

      Socket* acquireStreamSocketEx(bool allowWaiting, bool pooled = true) override;
      void releaseStreamSocket(Socket* sock);
      void invalidateStreamSocket(Socket* sock);

      LocalConnection createLocalConnection(const std::string& workerID);
      void disconnectLocalConnection(LocalConnection& connection);

   private:
      NicAddressList nicList;
      Mutex nicListMutex;
      UnixConnWorkerList connWorkerList;

      unsigned availableConns; // available established conns
      unsigned establishedConns; // not equal to connList.size!!
      unsigned maxConns;

      int numCreatedWorkers;

      Mutex mutex;
      Condition changeCond;

   public:
      // getters & setters

      NicAddressList getNicList()
      {
         // lock nicListMutex instead of mutex. acquireStreamSocketEx may lock
         // mutex for "long" periods
         const std::lock_guard<Mutex> lock(nicListMutex);
         return nicList;
      }

      bool updateInterfaces(unsigned short streamPort, const NicAddressList& nicList);

};
