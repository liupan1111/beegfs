#pragma once

#include <common/Common.h>
#include <common/nodes/NumNodeID.h>

class NodeConnPool;
class LocalConnWorker;
class Socket;

class WriteLocalFileMirrorConnPool
{
   public:
      enum ConnectionKind
      {
         ConnectionKind_REMOTE,
         ConnectionKind_LOCAL,
      };

      struct Connection
      {
         ConnectionKind kind;
         NodeConnPool* ownerPool;
         Socket* sock;
         LocalConnWorker* localWorker;

         Connection(ConnectionKind kind = ConnectionKind_REMOTE, NodeConnPool* ownerPool = NULL,
            Socket* sock = NULL, LocalConnWorker* localWorker = NULL) :
            kind(kind), ownerPool(ownerPool), sock(sock), localWorker(localWorker)
         {
         }
      };

      typedef Connection (*CreateConnectionFn)(NodeConnPool* connPool,
         const std::string& workerID, bool isLocalMirrorNode);
      typedef bool (*IsConnectionReusableFn)(const Connection& connection);
      typedef void (*DisconnectConnectionFn)(Connection& connection);

      WriteLocalFileMirrorConnPool();
      WriteLocalFileMirrorConnPool(CreateConnectionFn createConnection,
         IsConnectionReusableFn isConnectionReusable, DisconnectConnectionFn disconnectConnection);
      ~WriteLocalFileMirrorConnPool();

      Socket* acquire(NumNodeID nodeID, NodeConnPool* connPool, uint16_t mirrorTargetID,
         bool isLocalMirrorNode = false);
      void release(NumNodeID nodeID, NodeConnPool* connPool, Socket* sock);
      void invalidate(NumNodeID nodeID, NodeConnPool* connPool, Socket* sock);
      void dropNode(NumNodeID nodeID);
      void shutdown();

   private:
      enum { MAX_REMOTE_SOCKETS_PER_NODE = 2, MAX_LOCAL_SOCKETS_PER_NODE = 1 };

      struct SocketBucket
      {
         Connection sockets[MAX_REMOTE_SOCKETS_PER_NODE];
         unsigned numSockets;

         SocketBucket() : numSockets(0) {}
      };

      std::map<NumNodeID, SocketBucket> availableSockets;
      std::map<Socket*, Connection> activeConnections;
      unsigned numCreatedLocalWorkers;
      CreateConnectionFn createConnection;
      IsConnectionReusableFn isConnectionReusable;
      DisconnectConnectionFn disconnectConnection;

      void disconnect(Connection& connection);
      unsigned getMaxSockets(const Connection& connection) const;
};
