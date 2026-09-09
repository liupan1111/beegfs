#include <common/components/worker/queue/WriteLocalFileMirrorConnPool.h>
#include <common/app/AbstractApp.h>
#include <common/app/log/LogContext.h>
#include <common/net/message/AbstractNetMessageFactory.h>
#include <common/nodes/LocalNodeConnPool.h>
#include <common/nodes/NodeConnPool.h>
#include <common/toolkit/StringTk.h>

namespace
{
WriteLocalFileMirrorConnPool::Connection createDefaultConnection(NodeConnPool* connPool,
   const std::string& workerID, bool isLocalMirrorNode)
{
   if(isLocalMirrorNode)
   {
      LocalNodeConnPool* localPool = static_cast<LocalNodeConnPool*>(connPool);
      LocalNodeConnPool::LocalConnection connection = localPool->createLocalConnection(workerID);
      return { WriteLocalFileMirrorConnPool::ConnectionKind_LOCAL, connPool, connection.socket,
         connection.worker };
   }

   return { WriteLocalFileMirrorConnPool::ConnectionKind_REMOTE, connPool,
      connPool->acquireStreamSocketEx(true, false), NULL };
}

bool isDefaultConnectionReusable(const WriteLocalFileMirrorConnPool::Connection& connection)
{
   return connection.kind == WriteLocalFileMirrorConnPool::ConnectionKind_LOCAL ||
      connection.ownerPool->isStreamSocketReusable(connection.sock);
}

void disconnectDefaultConnection(WriteLocalFileMirrorConnPool::Connection& connection)
{
   if(connection.kind == WriteLocalFileMirrorConnPool::ConnectionKind_LOCAL)
   {
      LocalNodeConnPool::LocalConnection localConnection(connection.localWorker, connection.sock);
      static_cast<LocalNodeConnPool*>(connection.ownerPool)->disconnectLocalConnection(
         localConnection);
      connection.localWorker = NULL;
      connection.sock = NULL;
      return;
   }

   connection.ownerPool->disconnectStreamSocket(connection.sock);
   connection.sock = NULL;
}
}

WriteLocalFileMirrorConnPool::WriteLocalFileMirrorConnPool() :
   numCreatedLocalWorkers(0), createConnection(createDefaultConnection),
   isConnectionReusable(isDefaultConnectionReusable), disconnectConnection(disconnectDefaultConnection)
{
}

WriteLocalFileMirrorConnPool::WriteLocalFileMirrorConnPool(CreateConnectionFn createConnection,
   IsConnectionReusableFn isConnectionReusable, DisconnectConnectionFn disconnectConnection) :
   numCreatedLocalWorkers(0), createConnection(createConnection),
   isConnectionReusable(isConnectionReusable), disconnectConnection(disconnectConnection)
{
}

WriteLocalFileMirrorConnPool::~WriteLocalFileMirrorConnPool()
{
   shutdown();
}

Socket* WriteLocalFileMirrorConnPool::acquire(NumNodeID nodeID, NodeConnPool* connPool,
   uint16_t mirrorTargetID, bool isLocalMirrorNode)
{
   (void)mirrorTargetID;

   auto iter = availableSockets.find(nodeID);

   if(iter != availableSockets.end() && iter->second.numSockets)
   {
      SocketBucket& socketBucket = iter->second;
      Connection connection = socketBucket.sockets[--socketBucket.numSockets];

      if(!socketBucket.numSockets)
         availableSockets.erase(iter);

      activeConnections[connection.sock] = connection;
      return connection.sock;
   }

   std::string workerID;
   if(isLocalMirrorNode)
      workerID = "WriteLocalFileLocalConnWorker-" + StringTk::uintToStr(mirrorTargetID) + "-" +
         StringTk::uintToStr(++numCreatedLocalWorkers);

   Connection connection = createConnection(connPool, workerID, isLocalMirrorNode);
   activeConnections[connection.sock] = connection;
   return connection.sock;
}

void WriteLocalFileMirrorConnPool::release(NumNodeID nodeID, NodeConnPool* connPool, Socket* sock)
{
   if(!sock)
      return;

   auto activeIter = activeConnections.find(sock);
   if(activeIter == activeConnections.end() )
      return;

   Connection connection = activeIter->second;
   activeConnections.erase(activeIter);

   if(!isConnectionReusable(connection) )
   {
      disconnect(connection);
      return;
   }

   SocketBucket& socketBucket = availableSockets[nodeID];
   if(socketBucket.numSockets == getMaxSockets(connection))
   {
      disconnect(connection);
      return;
   }

   socketBucket.sockets[socketBucket.numSockets++] = connection;
}

void WriteLocalFileMirrorConnPool::invalidate(NumNodeID nodeID, NodeConnPool* connPool,
   Socket* sock)
{
   if(!sock)
      return;

   auto activeIter = activeConnections.find(sock);
   if(activeIter != activeConnections.end() )
   {
      Connection connection = activeIter->second;
      activeConnections.erase(activeIter);
      disconnect(connection);
      return;
   }

   auto iter = availableSockets.find(nodeID);
   if(iter != availableSockets.end() )
   {
      SocketBucket& socketBucket = iter->second;

      for(unsigned i = 0; i < socketBucket.numSockets; i++)
      {
         if(socketBucket.sockets[i].sock != sock)
            continue;

         Connection connection = socketBucket.sockets[i];
         socketBucket.sockets[i] = socketBucket.sockets[--socketBucket.numSockets];

         if(!socketBucket.numSockets)
            availableSockets.erase(iter);

         disconnect(connection);
         return;
      }
   }

   Connection connection(ConnectionKind_REMOTE, connPool, sock);
   disconnect(connection);
}

void WriteLocalFileMirrorConnPool::dropNode(NumNodeID nodeID)
{
   auto iter = availableSockets.find(nodeID);
   if(iter == availableSockets.end() )
      return;

   SocketBucket& socketBucket = iter->second;
   for(unsigned i = 0; i < socketBucket.numSockets; i++)
      disconnect(socketBucket.sockets[i]);

   availableSockets.erase(iter);
}

void WriteLocalFileMirrorConnPool::shutdown()
{
   for(auto iter = availableSockets.begin(); iter != availableSockets.end(); iter++)
      for(unsigned i = 0; i < iter->second.numSockets; i++)
         disconnect(iter->second.sockets[i]);

   availableSockets.clear();
}

void WriteLocalFileMirrorConnPool::disconnect(Connection& connection)
{
   disconnectConnection(connection);
}

unsigned WriteLocalFileMirrorConnPool::getMaxSockets(const Connection& connection) const
{
   return connection.kind == ConnectionKind_LOCAL ? MAX_LOCAL_SOCKETS_PER_NODE :
      MAX_REMOTE_SOCKETS_PER_NODE;
}
