#pragma once

#include <common/Common.h>
#include <common/toolkit/RandomReentrant.h>
#include "PooledSocket.h"
#include "common/net/sock/IPAddress.h"

class StandardSocket : public PooledSocket
{
   public:
      StandardSocket(int type, int protocol=0, bool epoll = true);
      virtual ~StandardSocket();

      // create UNIX domain socket pair
      static void createSocketPair(int type, int protocol,
         StandardSocket** outEndpointA, StandardSocket** outEndpointB);

      virtual void connect(const char* hostname, uint16_t port);
      virtual void connect(const SocketAddress& serv_addr);
      virtual void bindToAddr(const SocketAddress& ipAddr);
      virtual void listen();
      virtual Socket* accept(struct sockaddr_storage* addr, socklen_t* addrLen);
      virtual void shutdown();
      virtual void shutdownAndRecvDisconnect(int timeoutMS);

#ifdef BEEGFS_NVFS
      virtual ssize_t read(const void *buf, size_t len, unsigned lkey, const uint64_t rbuf,
         unsigned rkey, size_t localBufLen = 0);
      virtual ssize_t write(const void *buf, size_t len, unsigned lkey, const uint64_t rbuf,
         unsigned rkey, size_t localBufLen = 0);
#endif /* BEEGFS_NVFS */

      virtual ssize_t send(const void *buf, size_t len, int flags);
      virtual ssize_t sendto(const void *buf, size_t len, int flags, const SocketAddress* to);

      virtual ssize_t recv(void *buf, size_t len, int flags);
      virtual ssize_t recvT(void *buf, size_t len, int flags, int timeoutMS);

      ssize_t recvfrom(void *buf, size_t len, int flags, struct sockaddr_storage* from, socklen_t* fromLen);
      ssize_t recvfromT(void *buf, size_t len, int flags,
         struct sockaddr_storage *from, socklen_t* fromLen, int timeoutMS);

      void setSoKeepAlive(bool enable);
      void setSoReuseAddr(bool enable);
      int getSoRcvBuf();
      void setSoRcvBuf(int size);
      void setTcpNoDelay(bool enable);
      void setTcpCork(bool enable);

   protected:
      int sock;
      const bool isDgramSocket;
      int epollFD; // only valid for connected or dgram sockets, not valid (-1) for listening sockets
      int family = 0;

      StandardSocket(int fd, const IPAddress& peerIP, std::string peername);

      void addToEpoll(StandardSocket* other);

   private:
      RandomReentrant rand;

   public:
      // getters & setters
      virtual int getFD() const
      {
         return sock;
      }

      int getFamily() const
      {
         return family;
      }

      // inliners

      /**
       * @return true if incoming data is available, false if a timeout occurred
       * @throw SocketException on error
       */
      inline bool waitForIncomingData(int timeoutMS)
      {
            struct pollfd pollStruct = {sock, POLLIN, 0};
            int pollRes = poll(&pollStruct, 1, timeoutMS);

            if( (pollRes > 0) && (pollStruct.revents & POLLIN) )
            {
               return true;
            }
            else
            if(!pollRes)
               return false;
            else
            if(pollStruct.revents & POLLERR)
               throw SocketException(
                  std::string("waitForIncomingData(): poll(): ") + peername + ": Error condition");
            else
            if(pollStruct.revents & POLLHUP)
               throw SocketException(
                  std::string("waitForIncomingData(): poll(): ") + peername + ": Hung up");
            else
            if(pollStruct.revents & POLLNVAL)
               throw SocketException(
                  std::string("waitForIncomingData(): poll(): ") + peername +
                  ": Invalid request/fd");
            else
            if(errno == EINTR)
               throw SocketInterruptedPollException(
                  std::string("waitForIncomingData(): poll(): ") + peername + ": " +
                  System::getErrString() );
            else
               throw SocketException(
                  std::string("waitForIncomingData(): poll(): ") + peername +
                  ": " + System::getErrString() );
      }
};

/**
 * A "main" StandardSocket (this) that manages a set of subordinate StandardSocket
 * instances.
 *
 * The general idea is that "this" and subordinates can be used for sending. recvT() and
 * recfromT() are invoked upon "this" to wait for packets from any of the sockets
 * in this group.
 */
class StandardSocketGroup : public StandardSocket
{
   private:
      std::vector<std::shared_ptr<StandardSocket>> subordinates;

   public:
      StandardSocketGroup(int type, int protocol=0);

      /**
       * Create a new StandardSocket that is subordinate to "this". The socket can be used
       * for anything except methods that require epoll (i.e. recvT(), recvfromT().
       */
      std::shared_ptr<StandardSocket> createSubordinate(int type, int protocol=0);

      virtual ~StandardSocketGroup();
};
