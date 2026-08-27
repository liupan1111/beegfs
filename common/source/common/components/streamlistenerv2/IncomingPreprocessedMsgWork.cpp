#include <common/app/log/LogContext.h>
#include <common/app/AbstractApp.h>
#include <common/components/worker/queue/IOWorkerAsyncContext.h>
#include <common/components/streamlistenerv2/StreamListenerV2.h>
#include <common/threading/PThread.h>
#include <common/net/message/NetMessage.h>
#include "IncomingPreprocessedMsgWork.h"

#define INCOMING_PREPROCESSED_MSG_RECV_TIMEOUT_MS 5000

class CompletedIncomingAsyncRequest : public AsyncIORequest
{
   public:
      CompletedIncomingAsyncRequest(IncomingPreprocessedMsgWork* work) : work(work)
      {
      }

      ~CompletedIncomingAsyncRequest()
      {
         delete work;
      }

      bool start()
      {
         return true;
      }

      void onAIOComplete(const io_event& event)
      {
      }

      bool isComplete() const
      {
         return true;
      }

   private:
      IncomingPreprocessedMsgWork* work;
};

static AsyncIORequest* completeInvalidAsyncStart(IOWorkerAsyncContext& asyncContext,
   IncomingPreprocessedMsgWork* work)
{
   work->invalidateAsyncConnection();
   asyncContext.returnSocket(work);
   return new CompletedIncomingAsyncRequest(work);
}

void IncomingPreprocessedMsgWork::process(char* bufIn, unsigned bufInLen,
   char* bufOut, unsigned bufOutLen)
{
   const char* logContextStr = "Work (process incoming msg)";

   unsigned numReceived = NETMSG_HEADER_LENGTH; // (header actually received by stream listener)
   std::unique_ptr<NetMessage> msg;


   try
   {
      // attach stats to sock (stream listener already received the msg header)

      stats.incVals.netRecvBytes += NETMSG_HEADER_LENGTH;
      sock->setStats(&stats);


      // make sure msg length fits into our receive buffer

      unsigned msgLength = msgHeader.msgLength;
      unsigned msgPayloadLength = msgLength - numReceived;

      if(unlikely(msgPayloadLength > bufInLen) )
      { // message too big
         LogContext(logContextStr).log(Log_NOTICE,
            std::string("Received a message that is too large. Disconnecting: ") +
            sock->getPeername() );

         sock->unsetStats();
         invalidateConnection(sock);
         sock = NULL;

         return;
      }

      // receive the message payload

      if(msgPayloadLength)
         sock->recvExactT(bufIn, msgPayloadLength, 0, INCOMING_PREPROCESSED_MSG_RECV_TIMEOUT_MS);

      // we got the complete message buffer => create msg object

      AbstractApp* app = PThread::getCurrentThreadApp();
      auto cfg = app->getCommonConfig();
      auto netMessageFactory = app->getNetMessageFactory();

      msg = netMessageFactory->createFromPreprocessedBuf(&msgHeader, bufIn, msgPayloadLength);

      if(unlikely(msg->getMsgType() == NETMSGTYPE_Invalid) )
      { // message invalid
         LogContext(logContextStr).log(Log_NOTICE,
            std::string("Received an invalid message. Disconnecting: ") + sock->getPeername() );

         sock->unsetStats();
         invalidateConnection(sock);
         sock = NULL;
         return;
      }

      // process the received msg

      bool processRes = false;

      if(likely(!cfg->getConnAuthHash() ||
         sock->getIsAuthenticated() ||
         (msg->getMsgType() == NETMSGTYPE_AuthenticateChannel) ) )
      { // auth disabled or channel is auth'ed or this is an auth msg => process
         NetMessage::ResponseContext rctx(NULL, sock, bufOut, bufOutLen, &stats);
         LOG_DBG(COMMUNICATION, DEBUG, "Beginning message processing.", sock->getPeername(),
               msg->getMsgTypeStr());
         processRes = msg->processIncoming(rctx);
      }
      else
         LogContext(logContextStr).log(Log_NOTICE,
            std::string("Rejecting message from unauthenticated peer: ") + sock->getPeername() );

      // processing completed => cleanup

      bool needSockRelease = msg->getReleaseSockAfterProcessing();

      msg.reset();

      if(!needSockRelease)
         return; // sock release was already done within msg->processIncoming() method

      if(unlikely(!processRes) )
      { // processIncoming encountered messaging error => invalidate connection
         LogContext(logContextStr).log(Log_NOTICE,
            std::string("Problem encountered during processing of a message. Disconnecting: ") +
            sock->getPeername() );

         invalidateConnection(sock);
         sock = NULL;

         return;
      }

      releaseSocket(NULL);

      return;

   }
   catch(SocketTimeoutException& e)
   {
      LogContext(logContextStr).log(Log_NOTICE,
         std::string("Connection timed out: ") + sock->getPeername() );
   }
   catch(SocketDisconnectException& e)
   {
      // (note: level Log_DEBUG here to avoid spamming the log until we have log topics)
      LogContext(logContextStr).log(Log_DEBUG, std::string(e.what() ) );
   }
   catch(SocketException& e)
   {
      LogContext(logContextStr).log(Log_NOTICE,
         std::string("Connection error: ") + sock->getPeername() + std::string(": ") +
         std::string(e.what() ) );
   }

   // socket exception occurred => cleanup

   if(msg && msg->getReleaseSockAfterProcessing() )
   {
      sock->unsetStats();
      invalidateConnection(sock);
      sock = NULL;
   }
}

bool IncomingPreprocessedMsgWork::supportsAsyncIO() const
{
#ifdef BEEGFS_NVFS
   return msgHeader.msgType == NETMSGTYPE_ReadLocalFileRDMA ||
      msgHeader.msgType == NETMSGTYPE_WriteLocalFileRDMA;
#else
   return false;
#endif
}

AsyncIORequest* IncomingPreprocessedMsgWork::startAsyncIO(IOWorkerAsyncContext& asyncContext,
   char* bufIn, unsigned bufInLen, char* bufOut, unsigned bufOutLen)
{
   const char* logContextStr = "Work (start async incoming msg)";
   const unsigned numReceived = NETMSG_HEADER_LENGTH;
   std::unique_ptr<NetMessage> msg;

   try
   {
      stats.incVals.netRecvBytes += NETMSG_HEADER_LENGTH;
      sock->setStats(&stats);

      unsigned msgLength = msgHeader.msgLength;
      unsigned msgPayloadLength = msgLength - numReceived;

      if(unlikely(msgPayloadLength > bufInLen) )
      {
         LogContext(logContextStr).log(Log_NOTICE,
            std::string("Received a message that is too large. Disconnecting: ") +
            sock->getPeername() );

         return completeInvalidAsyncStart(asyncContext, this);
      }

      if(msgPayloadLength)
         sock->recvExactT(bufIn, msgPayloadLength, 0, INCOMING_PREPROCESSED_MSG_RECV_TIMEOUT_MS);

      AbstractApp* app = PThread::getCurrentThreadApp();
      auto cfg = app->getCommonConfig();
      auto netMessageFactory = app->getNetMessageFactory();

      msg = netMessageFactory->createFromPreprocessedBuf(&msgHeader, bufIn, msgPayloadLength);

      if(unlikely(msg->getMsgType() == NETMSGTYPE_Invalid) )
      {
         LogContext(logContextStr).log(Log_NOTICE,
            std::string("Received an invalid message. Disconnecting: ") + sock->getPeername() );

         return completeInvalidAsyncStart(asyncContext, this);
      }

      if(unlikely(cfg->getConnAuthHash() &&
         !sock->getIsAuthenticated() &&
         (msg->getMsgType() != NETMSGTYPE_AuthenticateChannel) ) )
      {
         LogContext(logContextStr).log(Log_NOTICE,
            std::string("Rejecting message from unauthenticated peer: ") + sock->getPeername() );

         return completeInvalidAsyncStart(asyncContext, this);
      }

      if(!msg->supportsAsyncIO() )
         return completeInvalidAsyncStart(asyncContext, this);

      LOG_DBG(COMMUNICATION, DEBUG, "Beginning async message processing.", sock->getPeername(),
            msg->getMsgTypeStr());

      AsyncIORequest* request = msg->startAsyncIO(asyncContext, this, sock, &stats);
      if(request)
         return request;

      return completeInvalidAsyncStart(asyncContext, this);
   }
   catch(SocketTimeoutException& e)
   {
      LogContext(logContextStr).log(Log_NOTICE,
         std::string("Connection timed out: ") + sock->getPeername() );
   }
   catch(SocketDisconnectException& e)
   {
      LogContext(logContextStr).log(Log_DEBUG, std::string(e.what() ) );
   }
   catch(SocketException& e)
   {
      LogContext(logContextStr).log(Log_NOTICE,
         std::string("Connection error: ") + sock->getPeername() + std::string(": ") +
         std::string(e.what() ) );
   }

   return completeInvalidAsyncStart(asyncContext, this);
}

IOWorkerResponse* IncomingPreprocessedMsgWork::detachIOWorkerResponse(uint16_t osdID,
   unsigned workerIndex)
{
   Socket* responseSock = sock;
   sock = NULL;

   return new IOWorkerResponse(responseSock, osdID, workerIndex, hasImmediateData);
}

void IncomingPreprocessedMsgWork::completeAsyncSocket(IOWorkerAsyncContext& asyncContext)
{
   releaseSocket(NULL);
   asyncContext.returnSocket(this);
}

void IncomingPreprocessedMsgWork::invalidateAsyncConnection()
{
   if(!sock)
      return;

   sock->unsetStats();
   invalidateConnection(sock);
   sock = NULL;
}

void IncomingPreprocessedMsgWork::releaseSocket(NetMessage* msg)
{
   if(msg)
      msg->setReleaseSockAfterProcessing(false);

   sock->unsetStats();

   this->hasImmediateData = checkRDMASocketImmediateData();
}

/**
 * Release a valid incoming socket by returning it to the StreamListenerV2.
 *
 * Intended to be called
 *  a) either directly from within a msg->processIncoming() method (e.g. after sending an early
 *     response to a client)
 *  b) or by IncomingPreprocessedMsgWork::process() for cleanup after msg->processIncoming()
 *     returned true.
 *
 * @param sock pointer will be set to NULL to avoid accidental usage by caller after calling this
 *    method.
 * @param msg may be NULL; if provided, msg->setReleaseSockAfterProcessing(false) will be called.
 */
void IncomingPreprocessedMsgWork::releaseSocket(AbstractApp* app, Socket** sock, NetMessage* msg)
{
   Socket* sockCopy = *sock; // copy of sock pointer, so that we can set caller's sock to NULL

   *sock = NULL;

   if(msg)
      msg->setReleaseSockAfterProcessing(false);

   sockCopy->unsetStats();

   // check for immediate data on rdma sockets

   bool gotImmediateDataAvailable = checkRDMASocketImmediateData(app, sockCopy);
   if(gotImmediateDataAvailable)
   { // immediate data available
      // (socket was returned to stream listener by checkRDMASocketImmediateData() )
      return;
   }

   // no immediate data available => return the socket to a stream listener

   StreamListenerV2* listener = app->getStreamListenerByFD(sockCopy->getFD() );
   StreamListenerV2::SockReturnPipeInfo returnInfo(
      StreamListenerV2::SockPipeReturn_MSGDONE_NOIMMEDIATE, sockCopy);

   listener->getSockReturnFD()->write(&returnInfo, sizeof(returnInfo) );
}

void IncomingPreprocessedMsgWork::invalidateConnection(Socket* sock)
{
   const int recvTimeoutMS = 1;

   try
   {
      sock->shutdownAndRecvDisconnect(recvTimeoutMS);
   }
   catch(SocketException& e)
   {
      // don't care, because the conn is invalid anyway
   }

   delete(sock);
}

/**
 * Checks whether there is more immediate data available on an RDMASocket.
 *
 * If immediate data is available, we need to notify the stream listener about it.
 * The reason for this is the fact that the filedescriptor will only send a notification
 * in case of new incoming data, so we need to check for old incoming data (that has already
 * been buffered internally by our RDMASocket impl) here.
 *
 * @return true if immediate data is available or if a conn error occurred (meaning the socket
 *    should not be returned to the streamListener by the caller)
 */
bool IncomingPreprocessedMsgWork::checkRDMASocketImmediateData(AbstractApp* app, Socket* sock)
{
   const char* logContextStr = "Work (incoming data => check RDMA immediate data)";


   // the immediate data check only applies to RDMA sockets

   if(sock->getSockType() != NICADDRTYPE_RDMA)
      return false;


   // we got a RDMASocket

   try
   {
      RDMASocket* rdmaSock = (RDMASocket*)sock;

      if(!rdmaSock->nonblockingRecvCheck() )
         return false; // no more data available at the moment


      // we have immediate data available => inform the stream listener

      LOG_DEBUG(logContextStr, Log_SPAM,
         std::string("Got immediate data: ") + sock->getPeername() );

      StreamListenerV2* listener = app->getStreamListenerByFD(sock->getFD() );
      StreamListenerV2::SockReturnPipeInfo returnInfo(
         StreamListenerV2::SockPipeReturn_MSGDONE_WITHIMMEDIATE, sock);

      listener->getSockReturnFD()->write(&returnInfo, sizeof(returnInfo) );

      return true;
   }
   catch(SocketDisconnectException& e)
   {
      LogContext(logContextStr).log(Log_DEBUG, std::string(e.what() ) );
   }
   catch(SocketException& e)
   {
      LogContext(logContextStr).log(Log_NOTICE,
         std::string("Connection error: ") + sock->getPeername() + std::string(": ") +
         std::string(e.what() ) );
   }

   // conn error occurred

   delete(sock);

   return true;
}

bool IncomingPreprocessedMsgWork::checkRDMASocketImmediateData()
{
   const char* logContextStr = "Work (incoming data => check RDMA immediate data)";

   if(sock->getSockType() != NICADDRTYPE_RDMA)
      return false;

   try
   {
      RDMASocket* rdmaSock = (RDMASocket*)sock;

      if(!rdmaSock->nonblockingRecvCheck() )
         return false;

      LOG_DEBUG(logContextStr, Log_SPAM,
         std::string("Got immediate data: ") + sock->getPeername() );

      return true;
   }
   catch(SocketDisconnectException& e)
   {
      LogContext(logContextStr).log(Log_DEBUG, std::string(e.what() ) );
   }
   catch(SocketException& e)
   {
      LogContext(logContextStr).log(Log_NOTICE,
         std::string("Connection error: ") + sock->getPeername() + std::string(": ") +
         std::string(e.what() ) );
   }

   delete(sock);
   sock = NULL;
   return false;
}

