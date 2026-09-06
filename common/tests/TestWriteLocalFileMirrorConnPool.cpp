#include <common/components/worker/queue/WriteLocalFileMirrorConnPool.h>
#include <common/net/sock/Socket.h>

#include <gtest/gtest.h>

#include <vector>

namespace
{
struct PoolTestState
{
   std::vector<Socket*> created;
   std::vector<Socket*> disconnected;
   bool reusable;
};

PoolTestState* state;

class PoolTestSocket : public Socket
{
   public:
      virtual void connect(const char*, uint16_t) {}
      virtual void connect(const SocketAddress&) {}
      virtual void bindToAddr(const SocketAddress&) {}
      virtual void listen() {}
      virtual Socket* accept(struct sockaddr_storage*, socklen_t*) { return NULL; }
      virtual void shutdown() {}
      virtual void shutdownAndRecvDisconnect(int) {}
      virtual ssize_t send(const void*, size_t len, int) { return len; }
      virtual ssize_t sendto(const void*, size_t len, int, const SocketAddress*) { return len; }
      virtual ssize_t recv(void*, size_t, int) { return 0; }
      virtual ssize_t recvT(void*, size_t, int, int) { return 0; }
      virtual int getFD() const { return -1; }
};

Socket* createSocket(NodeConnPool*)
{
   Socket* sock = new PoolTestSocket();
   state->created.push_back(sock);
   return sock;
}

bool isSocketReusable(NodeConnPool*, Socket*)
{
   return state->reusable;
}

void disconnectSocket(NodeConnPool*, Socket* sock)
{
   state->disconnected.push_back(sock);
   delete sock;
}
}

class WriteLocalFileMirrorConnPoolTest : public ::testing::Test
{
   protected:
      PoolTestState testState;
      WriteLocalFileMirrorConnPool pool;

      WriteLocalFileMirrorConnPoolTest() :
         pool(createSocket, isSocketReusable, disconnectSocket)
      {
         testState.reusable = true;
         state = &testState;
      }

      ~WriteLocalFileMirrorConnPoolTest()
      {
         state = NULL;
      }
};

TEST_F(WriteLocalFileMirrorConnPoolTest, reusesReleasedSocketForSameNode)
{
   NumNodeID nodeID(1);

   Socket* first = pool.acquire(nodeID, NULL, 10);
   pool.release(nodeID, NULL, first);
   Socket* second = pool.acquire(nodeID, NULL, 10);

   EXPECT_EQ(second, first);
   EXPECT_EQ(testState.created.size(), 1u);
   EXPECT_TRUE(testState.disconnected.empty());

   pool.invalidate(nodeID, NULL, second);
}

TEST_F(WriteLocalFileMirrorConnPoolTest, reusesTwoReleasedSocketsForSameNode)
{
   NumNodeID nodeID(1);
   Socket* first = pool.acquire(nodeID, NULL, 10);
   Socket* second = pool.acquire(nodeID, NULL, 10);

   pool.release(nodeID, NULL, first);
   pool.release(nodeID, NULL, second);

   EXPECT_EQ(pool.acquire(nodeID, NULL, 10), second);
   EXPECT_EQ(pool.acquire(nodeID, NULL, 10), first);
   EXPECT_EQ(testState.created.size(), 2u);

   pool.invalidate(nodeID, NULL, first);
   pool.invalidate(nodeID, NULL, second);
}

TEST_F(WriteLocalFileMirrorConnPoolTest, dropsThirdReleasedSocketForSameNode)
{
   NumNodeID nodeID(1);
   Socket* first = pool.acquire(nodeID, NULL, 10);
   Socket* second = pool.acquire(nodeID, NULL, 10);
   Socket* third = pool.acquire(nodeID, NULL, 10);

   pool.release(nodeID, NULL, first);
   pool.release(nodeID, NULL, second);
   pool.release(nodeID, NULL, third);

   EXPECT_EQ(testState.disconnected.size(), 1u);
   EXPECT_EQ(testState.disconnected[0], third);

   pool.dropNode(nodeID);
   EXPECT_EQ(testState.disconnected.size(), 3u);
}

TEST_F(WriteLocalFileMirrorConnPoolTest, keepsSeparateNodeBuckets)
{
   Socket* first = pool.acquire(NumNodeID(1), NULL, 10);
   Socket* second = pool.acquire(NumNodeID(2), NULL, 20);

   pool.release(NumNodeID(1), NULL, first);
   pool.release(NumNodeID(2), NULL, second);

   EXPECT_EQ(pool.acquire(NumNodeID(1), NULL, 10), first);
   EXPECT_EQ(pool.acquire(NumNodeID(2), NULL, 20), second);
   EXPECT_EQ(testState.created.size(), 2u);

   pool.invalidate(NumNodeID(1), NULL, first);
   pool.invalidate(NumNodeID(2), NULL, second);
}

TEST_F(WriteLocalFileMirrorConnPoolTest, dropsUnreusableSocketOnRelease)
{
   Socket* sock = pool.acquire(NumNodeID(1), NULL, 10);
   testState.reusable = false;

   pool.release(NumNodeID(1), NULL, sock);

   EXPECT_EQ(testState.disconnected.size(), 1u);
   Socket* replacement = pool.acquire(NumNodeID(1), NULL, 10);
   EXPECT_EQ(replacement, testState.created.back());
   EXPECT_EQ(testState.created.size(), 2u);

   testState.reusable = true;
   pool.invalidate(NumNodeID(1), NULL, replacement);
}

TEST_F(WriteLocalFileMirrorConnPoolTest, dropNodeDisconnectsPooledSockets)
{
   Socket* first = pool.acquire(NumNodeID(1), NULL, 10);
   Socket* second = pool.acquire(NumNodeID(1), NULL, 10);
   pool.release(NumNodeID(1), NULL, first);
   pool.release(NumNodeID(1), NULL, second);

   pool.dropNode(NumNodeID(1));

   EXPECT_EQ(testState.disconnected.size(), 2u);
}

TEST_F(WriteLocalFileMirrorConnPoolTest, shutdownDisconnectsPooledSockets)
{
   Socket* first = pool.acquire(NumNodeID(1), NULL, 10);
   Socket* second = pool.acquire(NumNodeID(1), NULL, 10);
   Socket* third = pool.acquire(NumNodeID(2), NULL, 20);

   pool.release(NumNodeID(1), NULL, first);
   pool.release(NumNodeID(1), NULL, second);
   pool.release(NumNodeID(2), NULL, third);

   pool.shutdown();

   EXPECT_EQ(testState.disconnected.size(), 3u);
}
