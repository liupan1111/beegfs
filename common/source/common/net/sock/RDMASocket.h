#pragma once

#include <common/Common.h>
#include "PooledSocket.h"


class RDMASocket : public PooledSocket
{
   public:
      struct ImplCallbacks
      {
         bool (*rdma_devices_exist)();
         void (*rdma_fork_init_once)();
         RDMASocket* (*rdma_socket_create)();
      };

      RDMASocket() : PooledSocket() {}

      static bool isRDMAAvailable();

      static std::unique_ptr<RDMASocket> create();

      static bool rdmaDevicesExist();
      static void rdmaForkInitOnce();

      virtual void checkConnection() = 0;
      virtual ssize_t nonblockingRecvCheck() = 0;
      virtual bool checkDelayedEvents() = 0;

      virtual void setBuffers(unsigned bufNum, unsigned bufSize) = 0;
      virtual void setTimeouts(int connectMS, int flowSendMS, int pollMS) = 0;
      virtual void setTypeOfService(uint8_t typeOfService) = 0;

      virtual void setConnectionRejectionRate(unsigned rate) = 0;

#ifdef BEEGFS_NVFS
      virtual ssize_t postAsyncRead(const void* buf, size_t len, unsigned lkey, uint64_t rbuf,
         unsigned rkey, size_t localBufLen, uint64_t* outWRID) = 0;
      virtual ssize_t postAsyncWrite(const void* buf, size_t len, unsigned lkey, uint64_t rbuf,
         unsigned rkey, size_t localBufLen, uint64_t* outWRID) = 0;
      virtual int consumeSendCompletion(uint64_t expectedWRID) = 0;
      virtual int drainSendCompletion() = 0;
      virtual int getSendCompletionFD() const = 0;
#endif /* BEEGFS_NVFS */
};
