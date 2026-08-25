#pragma once

#include <common/app/log/LogContext.h>
#include <common/components/worker/queue/StreamListenerWorkQueue.h>
#include <common/components/worker/queue/IOWorkerContext.h>
#include <common/components/worker/queue/RteRingQueue.h>
#include <common/components/ComponentInitException.h>
#include <common/net/sock/StandardSocket.h>
#include <common/net/sock/RDMASocket.h>
#include <common/net/message/NetMessage.h>
#include <common/nodes/Node.h>
#include <common/threading/PThread.h>
#include <common/toolkit/poll/PollList.h>
#include <common/toolkit/Pipe.h>
#include <common/Common.h>

#include <set>

class AbstractApp; // forward declaration
class IncomingPreprocessedMsgWork;


class StreamListenerV2 : public PThread
{
   public:
      // type definitions...

      enum SockPipeReturnType
      {
         SockPipeReturn_NEWCONN = 0, /* a new connection from ConnAcceptor */
         SockPipeReturn_MSGDONE_NOIMMEDIATE = 1, /* returned from msg worker, no immediate data */
         SockPipeReturn_MSGDONE_WITHIMMEDIATE = 2, /* from worker with immediate data available */
      };

      /**
       * This is what we will send over the socket return pipe
       */
      struct SockReturnPipeInfo
      {
         /**
          * Standard constructor for creating/sending a returnInfo.
          */
         SockReturnPipeInfo(SockPipeReturnType returnType, Socket* sock) :
            returnType(returnType), sock(sock) {}

         /**
          * For receiving only (no initialization of members).
          */
         SockReturnPipeInfo() {}

         SockPipeReturnType returnType;
         Socket* sock;
      };


   public:
      StreamListenerV2(const std::string& listenerID, AbstractApp* app,
         StreamListenerWorkQueue* workQueue);
      virtual ~StreamListenerV2();


   private:
      AbstractApp*      app;
      LogContext        log;

      StreamListenerWorkQueue* workQueue; // may be NULL in derived classes with own getWorkQueue()

      int               epollFD;
      PollList          pollList;
      Pipe*             sockReturnPipe;
      std::set<RteRingQueue*> ioWorkerResponseQueues;

      Time              rdmaCheckT;
      int               rdmaCheckForceCounter;

      bool              useAggressivePoll; // true to not sleep on epoll and burn CPU

      bool initSockReturnPipe();
      bool initSocks(unsigned short listenPort, NicListCapabilities* localNicCaps);

      virtual void run();
      void listenLoop();

      void onIncomingData(Socket* sock);
      void onSockReturn();
      void onIOWorkerResponse(RteRingQueue* responseQueue);
      void rdmaConnIdleCheck();

      bool isFalseAlarm(RDMASocket* sock);
      void rearmSocket(Socket* sock);

      void deleteAllConns();


   public:
      // getters & setters
      FileDescriptor* getSockReturnFD() const
      {
         return sockReturnPipe->getWriteFD();
      }

      void addIOWorkerResponseQueue(RteRingQueue* responseQueue);

      /**
       * Only effective when set before running this component.
       */
      void setUseAggressivePoll()
      {
         this->useAggressivePoll = true;
      }


   protected:
      // getters & setters

      /**
       * Note: This default implementation just ignores the given targetID. Derived classes will
       * make use of it.
       */
      virtual StreamListenerWorkQueue* getWorkQueue(uint16_t targetID) const
      {
         return this->workQueue;
      }

      virtual bool enqueueIncomingWork(Socket* sock, NetMessageHeader* msgHeader,
         IncomingPreprocessedMsgWork* work);

      virtual void flushIncomingWorkNotifications()
      {
      }

      virtual void onIOWorkerResponseDequeued(IOWorkerResponse* response)
      {
         IGNORE_UNUSED_VARIABLE(response);
      }

      void handleIOWorkerResponse(IOWorkerResponse* response);
};

