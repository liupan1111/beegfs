#ifdef BEEGFS_NVFS

#include <components/worker/AsyncRDMARequest.h>

#include <app/App.h>
#include <common/components/streamlistenerv2/IncomingPreprocessedMsgWork.h>
#include <common/net/message/session/rw/WriteLocalFileRDMARespMsg.h>
#include <common/net/sock/Socket.h>
#include <common/storage/quota/ExceededQuotaPerTarget.h>
#include <common/storage/quota/ExceededQuotaStore.h>
#include <common/storage/quota/QuotaData.h>
#include <common/system/System.h>
#include <common/toolkit/MessagingTk.h>
#include <common/toolkit/SessionTk.h>
#include <net/message/session/rw/ReadLocalFileV2MsgEx.h>
#include <net/message/session/rw/WriteLocalFileMsgEx.h>
#include <nodes/StorageNodeOpStats.h>
#include <program/Program.h>
#include <session/SessionLocalFile.h>
#include <session/SessionStore.h>
#include <storage/StorageTargets.h>

#include <errno.h>
#include <libaio.h>
#include <string.h>

AsyncRDMARequest::AsyncRDMARequest(IOWorkerAsyncContext& asyncContext,
   IncomingPreprocessedMsgWork* work, Socket* sock, HighResolutionStats* stats,
   const Params& params) :
   asyncContext(asyncContext),
   work(work),
   sock(sock),
   stats(stats),
   params(params),
   phase(INIT),
   socketValid(true),
   buffer(NULL),
   remoteBuf(0),
   remoteLen(0),
   remoteOff(0),
   hasRemote(false),
   remaining(params.count),
   completedBytes(0),
   fileOffset(params.offset),
   currentLen(0)
{
   memset(&iocb, 0, sizeof(iocb));
}

AsyncRDMARequest::~AsyncRDMARequest()
{
   if(buffer)
      asyncContext.releaseBuffer(buffer);

   delete work;
}

bool AsyncRDMARequest::start()
{
   if(!setup())
      return true;

   if(remaining == 0)
   {
      finishSuccess();
      return true;
   }

   return submitNext();
}

void AsyncRDMARequest::onAIOComplete(const io_event& event)
{
   if(phase != AIO_PENDING)
      return;

   phase = INIT;

   const ssize_t aioRes = event.res;

   if(isRead())
   {
      if(aioRes < 0)
      {
         finishError(FhgfsOpsErrTk::fromSysErr(-aioRes));
         return;
      }

      if(aioRes > 0 && !rdmaWriteToClient(aioRes))
      {
         finishCommunicationError();
         return;
      }

      completedBytes += aioRes;
      remaining -= aioRes;
      fileOffset += aioRes;

      sessionLocalFile->setOffset(params.offset + completedBytes);
      sessionLocalFile->incReadCounter(aioRes);
      stats->incVals.diskReadBytes += aioRes;

      if(aioRes != (ssize_t)currentLen || remaining == 0)
      {
         finishReadLength(completedBytes);
         return;
      }

      if(!advanceRemote(aioRes))
      {
         finishCommunicationError();
         return;
      }

      submitNext();
      return;
   }

   if(aioRes < 0)
   {
      finishWriteResult(-FhgfsOpsErrTk::fromSysErr(-aioRes));
      return;
   }

   if(aioRes != (ssize_t)currentLen)
   {
      finishWriteResult(-FhgfsOpsErr_INTERNAL);
      return;
   }

   completedBytes += aioRes;
   remaining -= aioRes;
   fileOffset += aioRes;
   stats->incVals.diskWriteBytes += aioRes;

   if(!advanceRemote(aioRes))
   {
      finishWriteResult(-FhgfsOpsErr_COMMUNICATION);
      return;
   }

   if(remaining == 0)
      finishSuccess();
   else
      submitNext();
}

bool AsyncRDMARequest::isComplete() const
{
   return phase == DONE;
}

void AsyncRDMARequest::cancel()
{
   finishAndInvalidateSocket();
}

bool AsyncRDMARequest::setup()
{
   FhgfsOpsErr validateErr = FhgfsOpsErr_SUCCESS;
   if(!validateRequest(validateErr))
   {
      finishError(validateErr);
      return false;
   }

   App* app = Program::getApp();
   SessionStore* sessions = app->getSessions();
   auto session = sessions->referenceOrAddSession(params.clientNumID);
   SessionLocalFileStore* sessionLocalFiles = session->getLocalFiles();

   const bool isMirrorSession = false;
   const bool useSessionCheck = isRead()
      ? (params.featureFlags & READLOCALFILEMSG_FLAG_SESSION_CHECK)
      : (params.featureFlags & WRITELOCALFILEMSG_FLAG_SESSION_CHECK);

   auto* const target = app->getStorageTargets()->getTarget(params.targetID);
   if(!target)
   {
      finishError(FhgfsOpsErr_UNKNOWNTARGET);
      return false;
   }

   sessionLocalFile = sessionLocalFiles->referenceSession(
      params.fileHandleID, params.targetID, isMirrorSession);

   if(!sessionLocalFile)
   {
      if(useSessionCheck)
      {
         finishError(FhgfsOpsErr_STORAGE_SRV_CRASHED);
         return false;
      }

      std::string fileID = SessionTk::fileIDFromHandleID(params.fileHandleID);
      int openFlags = SessionTk::sysOpenFlagsFromFhgfsAccessFlags(params.accessFlags);

      std::unique_ptr<SessionLocalFile> newFile(
         new SessionLocalFile(params.fileHandleID, params.targetID, fileID, openFlags, false));

      sessionLocalFile = sessionLocalFiles->addAndReferenceSession(std::move(newFile));
   }
   else if(useSessionCheck && sessionLocalFile->isServerCrashed())
   {
      finishError(FhgfsOpsErr_STORAGE_SRV_CRASHED);
      return false;
   }

   if(isWrite() &&
      (params.featureFlags & WRITELOCALFILEMSG_FLAG_USE_QUOTA) &&
      app->getConfig()->getQuotaEnableEnforcement())
   {
      QuotaExceededErrorType quotaExceeded =
         app->getExceededQuotaStores()->get(params.targetID)->isQuotaExceeded(
            params.quotaUserID, params.quotaGroupID, QuotaLimitType_SIZE);

      if(quotaExceeded != QuotaExceededErrorType_NOT_EXCEEDED)
      {
         LogContext("AsyncRDMARequest").log(Log_NOTICE,
            QuotaData::QuotaExceededErrorTypeToString(quotaExceeded) + " "
            "UID: " + StringTk::uintToStr(params.quotaUserID) + "; "
            "GID: " + StringTk::uintToStr(params.quotaGroupID));

         finishError(FhgfsOpsErr_DQUOT);
         return false;
      }
   }

   const int targetFD = *target->getChunkFD();
   SessionQuotaInfo quotaInfo(
      isWrite() && (params.featureFlags & WRITELOCALFILEMSG_FLAG_USE_QUOTA),
      app->getConfig()->getQuotaEnableEnforcement(),
      params.quotaUserID,
      params.quotaGroupID);

   FhgfsOpsErr openRes = sessionLocalFile->openFile(
      targetFD, &params.pathInfo, isWrite(), isWrite() ? &quotaInfo : NULL);

   if(openRes != FhgfsOpsErr_SUCCESS)
   {
      finishError(openRes);
      return false;
   }

   if(isRead() && !sessionLocalFile->getFD().valid())
   {
      finishReadLength(0);
      return false;
   }

   if(!sessionLocalFile->getIsDirectIO())
   {
      finishError(FhgfsOpsErr_INVAL);
      return false;
   }

   if(!initRemote())
   {
      finishError(FhgfsOpsErr_COMMUNICATION);
      return false;
   }

   int64_t oldOffset = sessionLocalFile->getOffset();
   if(oldOffset < 0 || oldOffset != params.offset)
   {
      if(isRead())
      {
         sessionLocalFile->resetReadCounter();
         sessionLocalFile->resetLastReadAheadTrigger();
      }
      else
         sessionLocalFile->resetWriteCounter();
   }

   buffer = asyncContext.acquireBuffer();
   return true;
}

bool AsyncRDMARequest::submitNext()
{
   currentLen = nextTransferLen();
   if(!currentLen)
   {
      finishError(FhgfsOpsErr_COMMUNICATION);
      return true;
   }

   return isRead() ? submitRead() : submitWrite();
}

bool AsyncRDMARequest::submitRead()
{
   memset(&iocb, 0, sizeof(iocb));
   io_prep_pread(&iocb, *sessionLocalFile->getFD(), buffer->data, currentLen, fileOffset);
   iocb.data = this;
   io_set_eventfd(&iocb, asyncContext.getEventFD());

   struct iocb* iocbs[] = { &iocb };
   int submitRes = io_submit(asyncContext.getAIOContext(), 1, iocbs);
   if(submitRes != 1)
   {
      int errCode = submitRes < 0 ? -submitRes : EIO;
      finishError(FhgfsOpsErrTk::fromSysErr(errCode));
      return true;
   }

   phase = AIO_PENDING;
   return true;
}

bool AsyncRDMARequest::submitWrite()
{
   if(!rdmaReadFromClient(currentLen))
   {
      finishCommunicationError();
      return true;
   }

   memset(&iocb, 0, sizeof(iocb));
   io_prep_pwrite(&iocb, *sessionLocalFile->getFD(), buffer->data, currentLen, fileOffset);
   iocb.data = this;
   io_set_eventfd(&iocb, asyncContext.getEventFD());

   struct iocb* iocbs[] = { &iocb };
   int submitRes = io_submit(asyncContext.getAIOContext(), 1, iocbs);
   if(submitRes != 1)
   {
      int errCode = submitRes < 0 ? -submitRes : EIO;
      finishWriteResult(-FhgfsOpsErrTk::fromSysErr(errCode));
      return true;
   }

   phase = AIO_PENDING;
   return true;
}

bool AsyncRDMARequest::initRemote()
{
   if(params.count == 0)
      return true;

   uint64_t len = 0;
   uint64_t off = 0;
   uint64_t buf = 0;

   if(!params.rdmaInfo.next(buf, len, off))
      return false;

   remoteBuf = buf;
   remoteLen = len;
   remoteOff = off;
   hasRemote = true;
   return remoteOff < remoteLen;
}

bool AsyncRDMARequest::advanceRemote(size_t length)
{
   if(remaining == 0)
      return true;

   remoteOff += length;
   if(remoteOff < remoteLen)
      return true;

   uint64_t len = 0;
   uint64_t off = 0;
   uint64_t buf = 0;

   if(!params.rdmaInfo.next(buf, len, off))
      return false;

   remoteBuf = buf;
   remoteLen = len;
   remoteOff = off;
   return remoteOff < remoteLen;
}

size_t AsyncRDMARequest::nextTransferLen() const
{
   if(!hasRemote)
      return 0;

   uint64_t remoteLeft = remoteLen - remoteOff;
   uint64_t maxLen = BEEGFS_MIN((uint64_t)buffer->length, remoteLeft);
   maxLen = BEEGFS_MIN(maxLen, (uint64_t)remaining);
   return maxLen;
}

void AsyncRDMARequest::finishSuccess()
{
   if(isRead())
      finishReadLength(completedBytes);
   else
      finishWriteResult(completedBytes);
}

void AsyncRDMARequest::finishError(FhgfsOpsErr err)
{
   if(isRead())
      finishReadLength(-err);
   else
      finishWriteResult(-err);
}

void AsyncRDMARequest::finishWriteResult(int64_t result)
{
   if(result > 0 && sessionLocalFile)
      sessionLocalFile->setOffset(params.offset + result);
   else if(sessionLocalFile)
      sessionLocalFile->setOffset(-1);

   if(!sendWriteResponse(result))
   {
      finishAndInvalidateSocket();
      return;
   }

   if(result > 0)
   {
      Program::getApp()->getNodeOpStats()->updateNodeOp(sock->getPeerIP(),
         StorageOpCounter_WRITEOPS, result, params.msgUserID);
   }

   finishAndReturnSocket();
}

void AsyncRDMARequest::finishReadLength(int64_t lengthInfo)
{
   if(lengthInfo < 0 && sessionLocalFile)
      sessionLocalFile->setOffset(-1);

   if(!sendReadLength(lengthInfo))
   {
      finishAndInvalidateSocket();
      return;
   }

   if(lengthInfo > 0)
   {
      Program::getApp()->getNodeOpStats()->updateNodeOp(sock->getPeerIP(),
         StorageOpCounter_READOPS, lengthInfo, params.msgUserID);
   }

   finishAndReturnSocket();
}

void AsyncRDMARequest::finishCommunicationError()
{
   if(sessionLocalFile)
      sessionLocalFile->setOffset(-1);

   finishError(FhgfsOpsErr_COMMUNICATION);
}

void AsyncRDMARequest::finishAndReturnSocket()
{
   if(phase == DONE)
      return;

   phase = DONE;

   if(socketValid)
   {
      work->completeAsyncSocket(asyncContext);
      sock = NULL;
      socketValid = false;
   }
}

void AsyncRDMARequest::finishAndInvalidateSocket()
{
   if(phase == DONE)
      return;

   phase = DONE;

   if(socketValid)
   {
      work->invalidateAsyncConnection();
      asyncContext.returnSocket(work);
      sock = NULL;
      socketValid = false;
   }
}

bool AsyncRDMARequest::sendReadLength(int64_t lengthInfo)
{
   try
   {
      int64_t wireLengthInfo = HOST_TO_LE_64(lengthInfo);
      sock->send(&wireLengthInfo, sizeof(int64_t), 0);
      return true;
   }
   catch(SocketException& e)
   {
      LogContext("AsyncRDMARequest").log(Log_WARNING,
         std::string("Unable to send RDMA read response: ") + e.what());
      return false;
   }
}

bool AsyncRDMARequest::sendWriteResponse(int64_t result)
{
   try
   {
      WriteLocalFileRDMARespMsg response(result);
      const auto responseBuf = MessagingTk::createMsgVec(response);
      sock->send(&responseBuf[0], responseBuf.size(), 0);
      return true;
   }
   catch(SocketException& e)
   {
      LogContext("AsyncRDMARequest").log(Log_WARNING,
         std::string("Unable to send RDMA write response: ") + e.what());
      return false;
   }
}

bool AsyncRDMARequest::rdmaWriteToClient(size_t length)
{
   try
   {
      ssize_t writeRes = sock->write(buffer->data, length, 0,
         remoteBuf + remoteOff, params.rdmaInfo.key, buffer->length);
      return writeRes == (ssize_t)length;
   }
   catch(SocketException& e)
   {
      LogContext("AsyncRDMARequest").log(Log_WARNING,
         std::string("Unable to RDMA-write file data to client: ") + e.what());
      return false;
   }
}

bool AsyncRDMARequest::rdmaReadFromClient(size_t length)
{
   try
   {
      ssize_t readRes = sock->read(buffer->data, length, 0,
         remoteBuf + remoteOff, params.rdmaInfo.key, buffer->length);
      return readRes == (ssize_t)length;
   }
   catch(SocketException& e)
   {
      LogContext("AsyncRDMARequest").log(Log_WARNING,
         std::string("Unable to RDMA-read file data from client: ") + e.what());
      return false;
   }
}

bool AsyncRDMARequest::validateRequest(FhgfsOpsErr& outErr) const
{
   if(params.count < 0 || params.offset < 0)
   {
      outErr = FhgfsOpsErr_INVAL;
      return false;
   }

   if(!isAligned((uint64_t)params.offset) || !isAligned((uint64_t)params.count))
   {
      outErr = FhgfsOpsErr_INVAL;
      return false;
   }

   if(!params.rdmaInfo.isValid())
   {
      outErr = FhgfsOpsErr_INVAL;
      return false;
   }

   if(!validateRDMABuffers())
   {
      outErr = FhgfsOpsErr_INVAL;
      return false;
   }

   if(isRead())
   {
      if(params.featureFlags &
         (READLOCALFILEMSG_FLAG_BUDDYMIRROR | READLOCALFILEMSG_FLAG_BUDDYMIRROR_SECOND |
          READLOCALFILEMSG_FLAG_DISABLE_IO))
      {
         outErr = FhgfsOpsErr_COMMUNICATION;
         return false;
      }
   }
   else
   {
      if(params.featureFlags &
         (WRITELOCALFILEMSG_FLAG_BUDDYMIRROR | WRITELOCALFILEMSG_FLAG_BUDDYMIRROR_SECOND |
          WRITELOCALFILEMSG_FLAG_BUDDYMIRROR_FORWARD | WRITELOCALFILEMSG_FLAG_DISABLE_IO))
      {
         outErr = FhgfsOpsErr_COMMUNICATION;
         return false;
      }
   }

   outErr = FhgfsOpsErr_SUCCESS;
   return true;
}

bool AsyncRDMARequest::validateRDMABuffers() const
{
   if(params.count == 0)
      return true;

   uint64_t total = 0;

   for(size_t i = 0; i < params.rdmaInfo.count; i++)
   {
      uint64_t addr = params.rdmaInfo.dmaAddress[i];
      uint64_t len = params.rdmaInfo.dmaLength[i];
      uint64_t off = params.rdmaInfo.dmaOffset[i];

      if(off >= len)
         return false;

      uint64_t usableLen = len - off;
      if(!isAligned(addr + off) || !isAligned(usableLen))
         return false;

      total += usableLen;
      if(total >= (uint64_t)params.count)
         return true;
   }

   return false;
}

bool AsyncRDMARequest::isRead() const
{
   return params.operation == READ;
}

bool AsyncRDMARequest::isWrite() const
{
   return params.operation == WRITE;
}

bool AsyncRDMARequest::isAligned(uint64_t value)
{
   return (value & (IOWorkerAsyncContext::DEFAULT_BUFFER_ALIGNMENT - 1)) == 0;
}

#endif /* BEEGFS_NVFS */
