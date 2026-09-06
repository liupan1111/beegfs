#include <common/components/worker/queue/WriteLocalFileMirrorConnPool.h>
#include <common/nodes/NodeConnPool.h>

namespace
{
Socket* createDefaultSocket(NodeConnPool* connPool)
{
   return connPool->acquireStreamSocketEx(true, false);
}

bool isDefaultSocketReusable(NodeConnPool* connPool, Socket* sock)
{
   return connPool->isStreamSocketReusable(sock);
}

void disconnectDefaultSocket(NodeConnPool* connPool, Socket* sock)
{
   connPool->disconnectStreamSocket(sock);
}
}

WriteLocalFileMirrorConnPool::WriteLocalFileMirrorConnPool() :
   createSocket(createDefaultSocket), isSocketReusable(isDefaultSocketReusable),
   disconnectSocket(disconnectDefaultSocket)
{
}

WriteLocalFileMirrorConnPool::WriteLocalFileMirrorConnPool(CreateSocketFn createSocket,
   IsSocketReusableFn isSocketReusable, DisconnectSocketFn disconnectSocket) :
   createSocket(createSocket), isSocketReusable(isSocketReusable),
   disconnectSocket(disconnectSocket)
{
}

WriteLocalFileMirrorConnPool::~WriteLocalFileMirrorConnPool()
{
   shutdown();
}

Socket* WriteLocalFileMirrorConnPool::acquire(NumNodeID nodeID, NodeConnPool* connPool,
   uint16_t mirrorTargetID)
{
   (void)mirrorTargetID;

   auto iter = availableSockets.find(nodeID);

   if(iter != availableSockets.end() && iter->second.numSockets)
   {
      SocketBucket& socketBucket = iter->second;
      Socket* sock = socketBucket.sockets[--socketBucket.numSockets].sock;

      if(!socketBucket.numSockets)
         availableSockets.erase(iter);

      return sock;
   }

   return createSocket(connPool);
}

void WriteLocalFileMirrorConnPool::release(NumNodeID nodeID, NodeConnPool* connPool, Socket* sock)
{
   if(!sock)
      return;

   if(!isSocketReusable(connPool, sock) )
   {
      disconnect(connPool, sock);
      return;
   }

   SocketBucket& socketBucket = availableSockets[nodeID];
   if(socketBucket.numSockets == MAX_SOCKETS_PER_NODE)
   {
      disconnect(connPool, sock);
      return;
   }

   socketBucket.sockets[socketBucket.numSockets++] = { connPool, sock };
}

void WriteLocalFileMirrorConnPool::invalidate(NumNodeID nodeID, NodeConnPool* connPool,
   Socket* sock)
{
   if(!sock)
      return;

   auto iter = availableSockets.find(nodeID);
   if(iter != availableSockets.end() )
   {
      SocketBucket& socketBucket = iter->second;

      for(unsigned i = 0; i < socketBucket.numSockets; i++)
      {
         if(socketBucket.sockets[i].sock != sock)
            continue;

         socketBucket.sockets[i] = socketBucket.sockets[--socketBucket.numSockets];

         if(!socketBucket.numSockets)
            availableSockets.erase(iter);

         break;
      }
   }

   disconnect(connPool, sock);
}

void WriteLocalFileMirrorConnPool::dropNode(NumNodeID nodeID)
{
   auto iter = availableSockets.find(nodeID);
   if(iter == availableSockets.end() )
      return;

   SocketBucket& socketBucket = iter->second;
   for(unsigned i = 0; i < socketBucket.numSockets; i++)
      disconnect(socketBucket.sockets[i].ownerPool, socketBucket.sockets[i].sock);

   availableSockets.erase(iter);
}

void WriteLocalFileMirrorConnPool::shutdown()
{
   for(auto iter = availableSockets.begin(); iter != availableSockets.end(); iter++)
      for(unsigned i = 0; i < iter->second.numSockets; i++)
         disconnect(iter->second.sockets[i].ownerPool, iter->second.sockets[i].sock);

   availableSockets.clear();
}

void WriteLocalFileMirrorConnPool::disconnect(NodeConnPool* ownerPool, Socket* sock)
{
   disconnectSocket(ownerPool, sock);
}
