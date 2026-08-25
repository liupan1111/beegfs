#pragma once

#include <common/app/AbstractApp.h>
#include <common/components/worker/queue/IOWorkerContext.h>
#include <common/components/worker/Work.h>
#include <common/net/message/NetMessage.h>
#include <common/net/sock/Socket.h>
#include <common/Common.h>


class IncomingPreprocessedMsgWork : public Work
{
   public:
      /**
       * Note: Be aware that this class is only for stream connections that need to be returned
       * to a StreamListenerV2 after processing.
       *
       * @param msgHeader contents will be copied
       */
      IncomingPreprocessedMsgWork(AbstractApp* app, Socket* sock, NetMessageHeader* msgHeader)
      {
         this->app = app;
         this->sock = sock;
         this->msgHeader = *msgHeader;
         this->hasImmediateData = false;
      }

      virtual void process(char* bufIn, unsigned bufInLen, char* bufOut, unsigned bufOutLen);
      virtual IncomingPreprocessedMsgWork* asIncomingPreprocessedMsgWork()
      {
         return this;
      }

      IOWorkerResponse* detachIOWorkerResponse(uint16_t osdID, unsigned workerIndex);

      static void releaseSocket(AbstractApp* app, Socket** sock, NetMessage* msg);
      static void invalidateConnection(Socket* sock);
      static bool checkRDMASocketImmediateData(AbstractApp* app, Socket* sock);


   private:
      AbstractApp* app;
      Socket* sock;
      NetMessageHeader msgHeader;
      bool hasImmediateData;

      void releaseSocket(NetMessage* msg);
      bool checkRDMASocketImmediateData();
};

