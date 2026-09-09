# WriteLocalFile 的 IO Worker 本地连接池设计

## 背景

当前 storage mirror 写路径中，primary storage target 向 secondary storage target 转发 `WriteLocalFile` 时，会通过 primary 进程中代表 secondary node 的 `Node` 对象上的 `NodeConnPool` 获取连接。

相关调用链：

```text
WriteLocalFileMsgEx::processIncoming()
  -> WriteLocalFileMsgEx::write()
  -> prepareMirroring()
  -> mirrorToNode->getConnPool()->acquireStreamSocket()
  -> NodeConnPool::acquireStreamSocketEx()
  -> NodeConnPool::mutex
```

`NodeConnPool::mutex` 保护共享连接池状态，例如 `connList`、`availableConns`、`establishedConns`。高并发 mirror 写场景下，每个 `WriteLocalFile` mirror 请求都可能竞争这把共享锁。

此外，当前 `NodeConnPool` 获取已有连接时，即使已经知道 `availableConns > 0`，也需要在持锁状态下从 `connList` 头部开始遍历，找到第一个 `isAvailable()` 的 socket。也就是说，当前热路径不仅有 mutex 竞争，还有锁内 list scan、available 状态切换、共享计数更新以及可能的 cache-line bouncing。

这里的 `NodeConnPool` 需要特别说明：它不是 secondary 机器上的连接池，而是 primary 进程本地维护的连接池。BeeGFS 会在本地进程中为远端 storage node 维护对应的 `Node` 对象，每个远端 `Node` 对象都有自己的 `NodeConnPool`。因此，primary 向 secondary 转发请求时，实际访问的是 primary 本地内存中“代表 secondary 的 Node 对象”的连接池。

可以理解为：

```text
primary storage process
  -> Node(remote storage A)
       -> NodeConnPool(to remote storage A)
  -> Node(remote storage B)
       -> NodeConnPool(to remote storage B)
```

所以，`NodeConnPool` 的粒度是“每个远端 node 一个 pool”。但同一个远端 node 的 pool 会被 primary 进程里的多个 worker 线程共享，高并发写同一个 secondary node 时，这些 worker 会竞争同一把 `NodeConnPool::mutex`。

本次优化范围很窄：

- 优化 `WriteLocalFile` / `WriteLocalFileResp` mirror 写性能。
- `TruncLocalFileResp` 等其他操作继续使用现有 `NodeConnPool` 路径。
- 不改变 `MessagingTk::requestResponseComm()` 的语义。

## 当前 IO Worker 路由

storage 在 `storage/source/app/App.cpp` 中创建 `QueueWorkType_IO` worker。每个 worker 从 `StorageIOWorkerRouter` 获取一个 `IOWorkerContext`。

`StorageStreamListenerV2::enqueueIncomingWork()` 将 `WriteLocalFile` 和 `WriteLocalFileRDMA` 路由到 `StorageIOWorkerRouter::submit()`，再由它把 work 放入某个 IO worker 的私有 request queue。其他消息类型继续走默认的 `StreamListenerV2` direct/indirect queue。

关键前提：

`QueueWorkType_IO` 是 `WriteLocalFile` 类 storage IO 消息的执行通道。本地连接池只服务 `WriteLocalFile` mirror 路径。

## 目标

给每个 IO worker 增加私有连接池，让 `WriteLocalFile` mirror 流量不再在热路径上反复竞争共享的 `NodeConnPool::mutex`。

同时，本地连接池命中时直接取得当前 IO worker 私有的 socket，避免在共享 `NodeConnPool::connList` 中持锁遍历可用连接。

优化后的热路径应变为：

```text
查找 IO Worker 本地连接池
  -> 发送 mirror WriteLocalFile header
  -> 发送写数据
  -> 接收 WriteLocalFileResp
  -> socket 归还到同一个 IO Worker 本地连接池
```

本地连接池命中时，不应获取共享连接池 mutex。

## 非目标

- 不优化通用 RPC 流量。
- 不改变 `TruncLocalFile`、`CloseChunkFile`、`SetLocalAttr`、quota、stat、management message 的通信路径。
- 不替换 `NodeConnPool`。
- 不改变客户端可见的写语义。
- 本阶段不把 mirror 写改成异步。
- 不跨 worker 共享本地连接池 socket。

## 术语

本文区分两种容易混淆的“本地”：

- **worker-local**：`WriteLocalFileMirrorConnPool` 归属于单个 IO Worker，不在 worker 间共享。
- **local-node**：mirror peer 就是当前 storage 进程所在的 `LocalNode`，使用 Unix socket pair，
  而不是 TCP 或 RDMA 连接。

## 设计方案

给每个 `QueueWorkType_IO` worker 增加一个独享的 `WriteLocalFileMirrorConnPool`。

所有权模型：

```text
Worker(QueueWorkType_IO)
  -> IOWorkerContext
     -> WriteLocalFileMirrorConnPool
        -> 按 mirror node 分桶
           -> Remote peer: 最多 2 条可用 socket
           -> Local-node peer: 最多 1 条可用 socket
```

这个连接池是单消费者、worker 私有的。正常 acquire/release 都发生在所属 IO worker 线程内，因此不需要 mutex。

首版明确限制：

```text
每个 IO worker 对每个远端 mirror node 最多保留 2 条 socket；对 local-node mirror peer 首版最多
保留 1 条 socket。
```

当前同步写模型一次只会使用其中一条连接；远端 peer 的两条是为未来多个副本可能落在同一 peer
node、以及后续 mirror 写并行化预留的容量。socket 彼此独立，不在 IO worker 之间共享。

### 使用范围

只有 `WriteLocalFileMsgExBase` 使用这个连接池：

- `prepareMirroring()` 从 IO worker 本地连接池获取 mirror socket。
- `sendToMirror()` 复用已获取的 socket 转发数据，并在重试时重新取连接。
- `finishMirroring()` 接收 `WriteLocalFileResp`，然后归还或失效 socket。

其他消息类型继续使用：

```text
node.getConnPool()->acquireStreamSocket()
MessagingTk::requestResponseComm()
node.getConnPool()->releaseStreamSocket()
```

## 接口

建议保持很小的接口：

```text
class WriteLocalFileMirrorConnPool
{
   Socket* acquire(Node& mirrorNode, uint16_t mirrorTargetID);
   void release(Node& mirrorNode, Socket* sock);
   void invalidate(Node& mirrorNode, Socket* sock);
   void dropNode(NumNodeID nodeID);
   void shutdown();
};
```

接口规则：

- `acquire()` 返回一个已连接、可用于向 mirror node 发送 `WriteLocalFile` 的 socket。
- `release()` 只把健康 socket 放回本地连接池；如果该 mirror node 的连接槽位已满，则关闭多余 socket。
- `invalidate()` 从本地连接池移除失败 socket，并关闭它。
- `dropNode()` 在 node 状态、target 映射或网络接口变化后，移除该 node 的所有 socket。
- `shutdown()` 关闭当前 IO worker 拥有的所有 socket。

该接口有意不向 `WriteLocalFileMsgEx` 暴露连接列表、计数器或路由细节。

## 连接分桶

每个 IO worker 本地连接池按 mirror node 存储 socket。远端 peer 的 bucket 最多保留两条可用
socket；local-node peer 的 bucket 首版最多保留一条。

key：

```text
mirror node numeric ID
```

## Local-node Mirror 连接

### 为什么需要单独处理

当 mirror peer 是当前进程的 `LocalNode` 时，`LocalNodeConnPool` 使用的不是 TCP/RDMA，
而是：

```text
client endpoint
  -> socketpair(AF_UNIX, SOCK_STREAM)
  -> worker endpoint
  -> LocalConnWorker
  -> NetMessage::processIncoming()
```

普通本机 RPC 通过共享 `LocalNodeConnPool` 的 `mutex`、`connWorkerList` 和可用连接计数管理
这些 socket pair。`WriteLocalFile` mirror 写不能把 worker-local 连接加入这套共享状态，否则会
重新引入锁竞争和锁内 list scan。

### 连接记录和所有权

`WriteLocalFileMirrorConnPool` 的连接记录按 peer 类型保存：

```text
Remote connection:
  NodeConnPool* + Socket*

Local-node connection:
  LocalNodeConnPool* + LocalConnWorker* + Socket*
```

池同时维护当前借出的 `activeConnections[socket]`。这样在 `release()`、`invalidate()`、
`dropNode()` 和 `shutdown()` 中，都可以准确找到 local-node socket 对应的
`LocalConnWorker` 并完成回收。调用 `WriteLocalFileMsgExBase` 的接口仍是 `Socket*`，无需改变
发送和接收流程。

### 创建、归还和销毁

远端 peer 未命中时调用 `NodeConnPool::acquireStreamSocketEx(true, false)`，明确要求新连接
不加入共享 pool。local-node peer 未命中时使用不登记到共享池的 helper：

```text
LocalNodeConnPool::createLocalConnection(workerID)
  -> new LocalConnWorker
  -> start()
  -> 返回 LocalConnWorker* 和 client endpoint
```

这条连接不进入 `LocalNodeConnPool::connWorkerList`，也不更新其 `availableConns`、
`establishedConns` 或 `maxConns`。健康连接由所属 IO Worker 的 mirror pool 复用；本机 bucket
最多保留一条空闲连接。发送、接收或 response 校验失败时，或在 node 移除和 IO Worker 退出时，
mirror pool 调用：

```text
LocalNodeConnPool::disconnectLocalConnection()
  -> LocalConnWorker::selfTerminate()
  -> clientEndpoint->shutdownAndRecvDisconnect()
  -> join()
  -> delete LocalConnWorker
```

因此 local-node mirror 写的稳定路径只访问当前 IO Worker 私有状态，不访问
`LocalNodeConnPool::mutex`。

### 容量预算

每个实际使用 local-node mirror 写的 IO Worker 最多增加一条 `LocalConnWorker + socketpair`。
它不计入 `LocalNodeConnPool::maxConns`，应独立按可能访问本机 mirror 的 IO Worker 总数规划。
若未来同一 IO Worker 支持多个并行 local-node mirror 写，可单独提高本机上限，而不必改变远端
peer 的两条连接策略。

## 获取连接路径

本地连接池命中：

```text
WriteLocalFileMsgEx::prepareMirroring()
  -> 获取当前 IOWorkerContext
  -> localPool.acquire(mirrorNode, secondaryTargetID)
  -> 从 worker 私有 bucket 弹出 socket
  -> 不获取 NodeConnPool mutex
```

本地连接池未命中：

```text
localPool.acquire()
  -> 当前 mirror node bucket 没有已建立连接
  -> 通过复用/抽取 NodeConnPool 的现有建连能力创建一条新连接
  -> 执行现有连接创建 / handshake / socket options 逻辑
  -> 将 socket 返回给 WriteLocalFile 路径
```

每个 IO worker 维护自己的本地连接池。这个本地池不重新实现 BeeGFS 的完整建连策略，而是复用或抽取 `NodeConnPool` 中已有的建连能力，确保 TCP/RDMA 选择、socket options、认证、direct/indirect channel、net filter、fallback route 等语义保持一致。

这里的“复用/抽取建连能力”指复用建连代码和建连语义，不是从共享 `NodeConnPool` 中长期借走 socket。`WriteLocalFileMirrorConnPool` 创建和持有的 socket 不加入 `NodeConnPool::connList`，也不计入 `NodeConnPool::establishedConns`、`availableConns` 或 `maxConns`。

稳定状态下，local pool 命中时不访问共享 `NodeConnPool::mutex`。只有 pool miss 或连接失效后重建时，才可能进入共享建连路径。

由于本地 mirror socket 不从共享 `NodeConnPool::connList` 中借用，pool miss 时也不应该调用 `NodeConnPool::acquireStreamSocket()`。因此，冷启动阶段不会出现多个 IO worker 同时抢同一个 `NodeConnPool::mutex`、修改 `establishedConns`、插入 `connList` 的问题。冷启动阶段仍然可能有多个 IO worker 同时向同一个 secondary node 建立独立连接，这属于并发建连压力，而不是共享连接池锁竞争。

## IO Worker 连接数量

IO worker 处理 `WriteLocalFile` 是同步的。一个 IO worker 同一时刻只处理一个 `WriteLocalFile` work，而一个 `WriteLocalFile` mirror 写会在该 worker 内同步完成：

```text
send mirror WriteLocalFile header
send write data
recv WriteLocalFileResp
```

远端 peer 为支持多个副本可能落在同一 node，一个 IO worker 最多保留两条连接：

```text
remoteLimitPerWorkerPerNode = 2
```

local-node peer 首版为一条连接：

```text
localNodeLimitPerWorker = 1
```

对某个 primary storage node `A` 和远端 mirror storage node `B`，worker-local mirror socket 的
上界为：

```text
workerLocalRemoteMirrorSockets(A, B)
  <= 2 * sum(workerCount(target))
     for each primary target on A that may mirror writes to B
```

每个 target 的 `workerCount(target)` 可以是 `nr_io_worker`，但一个 node 可以承载多个
primary target。因此，不能把 node 到 node 的上界简化成单个 `nr_io_worker`，更不能用
`nr_io_worker <= nr_conn_socks_pernode` 描述它。

`workerLocalRemoteMirrorSockets(A, B)` 与共享 `NodeConnPool` 的 `nr_conn_socks_pernode` 是独立的
连接预算：前者是 worker-local mirror pool 持有的连接数，后者只限制共享 `NodeConnPool`。
容量规划时应把二者相加，并按 peer node 评估总连接数：

```text
totalConnections(A, B)
  = workerLocalRemoteMirrorSockets(A, B) + sharedNodeConnPoolConnections(A, B)
```

如果这个总数超过网络、RDMA 设备或对端可接受的连接预算，应调整 worker 数量、target
布局，或增加独立的 worker-local mirror socket 上限；不应让多个 IO worker 共享同一条本地池连接。
共享连接会重新引入跨 worker 同步，破坏本地连接池去锁的核心目标。

远端 peer 不应突破每 worker、每 mirror node 2 条 socket 的上限；local-node peer 首版不应突破
1 条。连接失效后，该 worker 可以重新创建一条替代连接。

## 归还连接路径

`finishMirroring()` 收到合法 `WriteLocalFileResp` 后：

```text
finishMirroring()
  -> 解析 WriteLocalFileResp
  -> localPool.release(mirrorNode, mirrorToSock)
  -> mirrorToSock = NULL
```

成功写路径上，socket 不再归还给全局 `NodeConnPool`，而是继续由该 IO worker 本地连接池持有。

如果 release 时本地 bucket 已满，说明该 socket 不应继续保留在本地池中。首版建议直接关闭该 socket，避免把本地池 socket 再归还给全局 `NodeConnPool` 造成所有权混杂。

所有权规则：

```text
acquire() 成功返回后，socket 临时归 WriteLocalFileMsgExBase 持有。
finishMirroring() 必须对该 socket 执行 release() 或 invalidate() 之一。
release()/invalidate() 返回后，WriteLocalFileMsgExBase 必须把 mirrorToSock 置为 NULL。
acquire() 抛异常时，调用者不持有任何新 socket；清理由 pool 内部完成。
```

## 失败路径

出现以下情况时，socket 不允许复用：

- `SocketConnectException`
- `SocketException`
- send 失败
- receive 失败
- response 类型错误
- peer disconnect
- target 或 node 状态显示 mirror 已不再有效

失败处理：

```text
localPool.invalidate(mirrorNode, mirrorToSock)
mirrorToSock = NULL
```

下一次写请求可以由本地连接池重新创建连接，并把新 socket 留在本地连接池中。重试路径也必须使用同一个本地连接池，不能回退到 `mirrorToNode->getConnPool()`，否则热路径会重新引入共享锁。

## 与 WriteLocalFileMsgEx 的集成

当前 mirror acquire：

```text
mirrorToSock = mirrorToNode->getConnPool()->acquireStreamSocket();
```

建议改为：

```text
mirrorToSock = ioWorkerContext->writeMirrorConnPool.acquire(*mirrorToNode, secondaryTargetID);
```

当前成功完成 mirror 写：

```text
mirrorToNode->getConnPool()->releaseStreamSocket(mirrorToSock);
```

建议改为：

```text
ioWorkerContext->writeMirrorConnPool.release(*mirrorToNode, mirrorToSock);
```

当前 mirror 失败：

```text
mirrorToNode->getConnPool()->invalidateStreamSocket(mirrorToSock);
```

建议改为：

```text
ioWorkerContext->writeMirrorConnPool.invalidate(*mirrorToNode, mirrorToSock);
```

## 保持其他操作行为不变

新连接池不能暴露给通用通信 helper。

以下路径保持不变：

```text
MessagingTk::requestResponseComm()
MessagingTk::requestResponse()
MessagingTk::requestResponseNode()
MessagingTk::requestResponseTarget()
TruncLocalFileMsgEx
CloseChunkFileMsgEx
SetLocalAttrMsgEx
GetChunkFileAttribsMsgEx
```

这样 `TruncLocalFileResp` 和其他响应仍保持现有全局 `NodeConnPool` 行为。

分发规则：

```text
NETMSGTYPE_WriteLocalFile     -> QueueWorkType_IO
NETMSGTYPE_WriteLocalFileRDMA -> QueueWorkType_IO
all other message types       -> existing StreamListenerV2 direct/indirect routing
```

## Node 和 Target 状态变化

当本地连接池的前提失效时，必须清理本地 socket。

需要触发清理的事件：

- mirror node 被移除或标记为不可达
- target mapping 变化
- buddy group primary/secondary 映射变化
- 网络接口列表变化
- RDMA 可用性变化
- storage shutdown

首版可采用的实际策略：

- socket 级错误一律 invalidate。
- worker shutdown 时关闭所有本地 socket。
- pool miss 或连接失效后的重建，重新读取当前 node/interface 状态。
- 不从其他线程直接修改某个 IO worker 的本地连接池。

后续增强：

- 在当前已有 node/interface invalidation 广播的位置增加显式 `dropNode()` 调用。
- 或者维护 generation number，让 IO worker 在下一次 `acquire()` 前发现状态变化并自行清理。

## 线程模型

正常操作无锁，原因是：

- 连接池只属于一个 IO worker。
- 连接池只在该 worker 处理 `WriteLocalFile` 时访问。
- socket 归还到同一个 worker-local pool。

跨线程清理不应直接修改其他 worker 的本地连接池。可选方案是向目标 IO worker 投递 high-priority control work，或者维护 generation number，由 worker 在下一次 acquire 前自行观察并清理。

## 预期性能影响

优化前：

```text
每次 mirror write acquire/release 都访问共享 NodeConnPool 状态。
高并发下可能在 NodeConnPool::mutex 上串行化。
```

优化后：

```text
稳定状态下，mirror write acquire/release 只访问 IO worker 本地连接池。
本地连接池命中时没有共享 mutex。
本地连接池命中时没有锁内 list scan。
pool miss 或连接失效后重建时，才复用共享建连路径。
```

预期收益：

- 降低高并发 `WriteLocalFile` mirror 流量下的锁竞争。
- 避免在 `NodeConnPool::mutex` 内遍历 `connList` 查找空闲 socket。
- 降低 `WriteLocalFileResp` 尾延迟。
- 减少 `NodeConnPool::availableConns` 和 `connList` 的 cache-line bouncing。
- 非写 RPC 的性能和行为不变。

## 风险和缓解

风险：连接数随 IO worker 数增加。

缓解：每个 IO worker 到每个远端 mirror node 最多 2 条 socket；local-node peer 首版最多 1 条。
对同一对远端 node，连接上界是 `2` 倍 primary node 上可能向该 mirror node 转发写入的所有
primary target 的 IO worker 数之和；它独立于 `nr_conn_socks_pernode`。容量规划时应同时统计
worker-local mirror socket 与共享 `NodeConnPool` socket，必要时新增独立的 per-peer
worker-local mirror socket 上限。

风险：冷启动或连接失效后，多个 IO worker 可能同时向同一个 secondary node 建立连接。

缓解：该路径不访问共享 `NodeConnPool::connList`，不会形成 `NodeConnPool::mutex` 热点。首版可以通过懒加载和压测前 warmup 降低抖动；如果后续观察到 connect/auth/RDMA 建连压力，再考虑分批预热、connect rate limit 或失败退避。

风险：node 或 target 状态变化后残留过期 socket。

缓解：首版先在所有 socket 错误上 invalidate，并在 pool miss/reconnect 时读取当前状态；后续把 `dropNode()` 或 generation check 接入现有 node/interface update 路径。

风险：socket 被归还给错误 owner。

缓解：debug build 中给 local-pool socket 标记 owner worker identity，并断言 release 发生在同一个 IO worker。

风险：非 `WriteLocalFile` 操作开始使用本地连接池。

缓解：本地连接池接口只从 `WriteLocalFileMsgEx` mirror 代码可达，不放入 `NodeConnPool` 或 `MessagingTk`。

风险：shutdown 遗漏本地 socket。

缓解：让 `Worker` 或 `IOWorkerContext` 析构时调用 `WriteLocalFileMirrorConnPool::shutdown()`。

## 待定问题

1. 哪条现有 node/interface update 路径负责调用 `dropNode()` 或发布 generation change？
2. pool miss 时，是否继续复用 `NodeConnPool::acquireStreamSocketEx(..., pooled=false)`，还是抽出
   一个独立的建连 helper 供两个 pool 复用？无论选择哪种方式，新 socket 都不能加入共享
   `NodeConnPool::connList`。

## 推荐首版实现

实现最小但有用的版本：

```text
IOWorkerContext 拥有 WriteLocalFileMirrorConnPool。
Pool key 使用 mirror node numeric ID。
远端 peer bucket 最多保留 2 条 socket；local-node peer bucket 首版最多保留 1 条。
本地连接池负责持有、归还、销毁和失效处理。
pool miss 时复用/抽取 NodeConnPool 的现有建连能力。
worker-local mirror socket 不加入 NodeConnPool::connList，不占用共享 pool 的 maxConns。
每个远端 peer node 的 worker-local mirror socket 上界按所有相关 primary target 的 IO worker 数量
之和的两倍计算；它与 `nr_conn_socks_pernode` 独立，容量规划时要与共享 pool 的连接数合并评估。
只有 WriteLocalFileMsgEx mirror 路径使用该连接池。
成功收到 WriteLocalFileResp 后，将 socket 归还到本地连接池。
任何通信错误都关闭或 invalidate socket。
shutdown 关闭所有本地连接池 socket。
其他 RPC 路径保持不变。
```

这个版本直接针对当前性能问题，同时把改动限制在 `WriteLocalFile` mirror 流量内。
