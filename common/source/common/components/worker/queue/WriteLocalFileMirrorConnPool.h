#pragma once

#include <common/Common.h>
#include <common/nodes/NumNodeID.h>

class NodeConnPool;
class Socket;

class WriteLocalFileMirrorConnPool
{
   public:
      typedef Socket* (*CreateSocketFn)(NodeConnPool* connPool);
      typedef bool (*IsSocketReusableFn)(NodeConnPool* connPool, Socket* sock);
      typedef void (*DisconnectSocketFn)(NodeConnPool* connPool, Socket* sock);

      WriteLocalFileMirrorConnPool();
      WriteLocalFileMirrorConnPool(CreateSocketFn createSocket,
         IsSocketReusableFn isSocketReusable, DisconnectSocketFn disconnectSocket);
      ~WriteLocalFileMirrorConnPool();

      Socket* acquire(NumNodeID nodeID, NodeConnPool* connPool, uint16_t mirrorTargetID);
      void release(NumNodeID nodeID, NodeConnPool* connPool, Socket* sock);
      void invalidate(NumNodeID nodeID, NodeConnPool* connPool, Socket* sock);
      void dropNode(NumNodeID nodeID);
      void shutdown();

   private:
      enum { MAX_SOCKETS_PER_NODE = 2 };

      struct PooledSocket
      {
         NodeConnPool* ownerPool;
         Socket* sock;
      };

      struct SocketBucket
      {
         PooledSocket sockets[MAX_SOCKETS_PER_NODE];
         unsigned numSockets;

         SocketBucket() : numSockets(0) {}
      };

      std::map<NumNodeID, SocketBucket> availableSockets;
      CreateSocketFn createSocket;
      IsSocketReusableFn isSocketReusable;
      DisconnectSocketFn disconnectSocket;

      void disconnect(NodeConnPool* ownerPool, Socket* sock);
};
