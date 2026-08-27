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
class SessionLocalFile;
class Socket;
struct io_event;

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

      AsyncRDMARequest(IOWorkerAsyncContext& asyncContext, IncomingPreprocessedMsgWork* work,
         Socket* sock, HighResolutionStats* stats, const Params& params);
      ~AsyncRDMARequest();

      bool start();
      void onAIOComplete(const io_event& event);
      bool isComplete() const;
      void cancel();

   private:
      enum Phase
      {
         INIT,
         AIO_PENDING,
         DONE
      };

      IOWorkerAsyncContext& asyncContext;
      IncomingPreprocessedMsgWork* work;
      Socket* sock;
      HighResolutionStats* stats;
      Params params;
      Phase phase;
      bool socketValid;

      std::shared_ptr<SessionLocalFile> sessionLocalFile;
      AsyncIOBuffer* buffer;
      struct iocb iocb;

      uint64_t remoteBuf;
      uint64_t remoteLen;
      uint64_t remoteOff;
      bool hasRemote;

      int64_t remaining;
      int64_t completedBytes;
      off_t fileOffset;
      size_t currentLen;

      bool setup();
      bool submitNext();
      bool submitRead();
      bool submitWrite();

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
      bool rdmaWriteToClient(size_t length);
      bool rdmaReadFromClient(size_t length);

      bool validateRequest(FhgfsOpsErr& outErr) const;
      bool validateRDMABuffers() const;
      bool isRead() const;
      bool isWrite() const;
      static bool isAligned(uint64_t value);
};

#endif /* BEEGFS_NVFS */
