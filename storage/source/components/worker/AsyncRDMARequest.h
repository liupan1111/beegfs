#pragma once

#ifdef BEEGFS_NVFS

#include <common/app/log/LogContext.h>
#include <common/components/worker/queue/IOWorkerAsyncContext.h>
#include <common/nodes/NumNodeID.h>
#include <common/storage/PathInfo.h>
#include <common/storage/RdmaInfo.h>
#include <common/storage/StorageErrors.h>
#include <common/toolkit/HighResolutionStats.h>
#include <common/Common.h>

#include <memory>
#include <string>

class IncomingPreprocessedMsgWork;
class RDMASocket;
class SessionLocalFile;
class Socket;
class AsyncRDMARequestPool;

class AsyncRDMARequest : public AsyncIORequest
{
   public:
      enum Operation
      {
         READ,
         WRITE
      };

      struct Params
      {
         Operation operation;
         NumNodeID clientNumID;
         std::string fileHandleID;
         uint16_t targetID;
         PathInfo pathInfo;
         unsigned accessFlags;
         int64_t offset;
         int64_t count;
         unsigned featureFlags;
         unsigned msgUserID;
         unsigned quotaUserID;
         unsigned quotaGroupID;
         RdmaInfo rdmaInfo;
      };

      static AsyncRDMARequest* create(IOWorkerAsyncContext& asyncContext,
         IncomingPreprocessedMsgWork* work, Socket* sock, HighResolutionStats* stats,
         const Params& params);
      ~AsyncRDMARequest();

      bool start();
      void onLocalIOComplete(int64_t result) override;
      int getSendCompletionFD() const override;
      void onSendCQComplete() override;
      bool isComplete() const;
      void cancel();
      void release() override;

   private:
      friend class AsyncRDMARequestPool;

      AsyncRDMARequest(IOWorkerAsyncContext& asyncContext, IncomingPreprocessedMsgWork* work,
         Socket* sock, HighResolutionStats* stats, const Params& params);

      enum Phase
      {
         INIT,
         AIO_PENDING,
         RDMA_READ_PENDING,
         RDMA_WRITE_PENDING,
         DONE
      };

      IOWorkerAsyncContext& asyncContext;
      IncomingPreprocessedMsgWork* work;
      Socket* sock;
      RDMASocket* rdmaSocket;
      HighResolutionStats* stats;
      Params params;
      Phase phase;
      bool socketValid;

      std::shared_ptr<SessionLocalFile> sessionLocalFile;
      AsyncIOBuffer* buffer;

      uint64_t remoteBuf;
      uint64_t remoteLen;
      uint64_t remoteOff;
      bool hasRemote;

      int64_t remaining;
      int64_t completedBytes;
      off_t fileOffset;
      size_t currentLen;
      size_t bufferOffset;
      size_t rdmaLen;
      uint64_t rdmaWRID;

      bool setup();
      bool submitNext();
      bool submitRead();
      bool submitWrite();
      bool submitWriteAIO();
      bool postRDMAWrite(size_t length);
      bool postRDMARead(size_t length);
      void completePendingRDMA();
      void completeRDMAWrite();
      void completeRDMARead();

      bool initRemote();
      bool advanceRemote(size_t length);
      size_t nextTransferLen() const;

      void finishSuccess();
      void finishError(FhgfsOpsErr err);
      void finishWriteResult(int64_t result);
      void finishReadLength(int64_t lengthInfo);
      void finishCommunicationError();
      void finishAndReturnSocket();
      void finishAndInvalidateSocket();

      bool sendReadLength(int64_t lengthInfo);
      bool sendWriteResponse(int64_t result);

      bool validateRequest(FhgfsOpsErr& outErr) const;
      bool validateRDMABuffers() const;
      bool isRead() const;
      bool isWrite() const;
      static bool isAligned(uint64_t value);
};

#endif /* BEEGFS_NVFS */
