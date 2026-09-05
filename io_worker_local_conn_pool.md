# WriteLocalFile 的 IO Worker 本地连接池设计

## 背景

当前 storage mirror 写路径中，primary storage target 向 secondary storage target 转发 `WriteLocalFile` 时，会通过 secondary node 的 `NodeConnPool` 获取连接。

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

## 设计方案

给每个 `QueueWorkType_IO` worker 增加一个独享的 `WriteLocalFileMirrorConnPool`。

所有权模型：

```text
Worker(QueueWorkType_IO)
  -> IOWorkerContext
     -> WriteLocalFileMirrorConnPool
        -> 按 mirror node 分桶
           -> 可用 mirror sockets
```

这个连接池是单消费者、worker 私有的。正常 acquire/release 都发生在所属 IO worker 线程内，因此不需要 mutex。

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
- `release()` 只把健康 socket 放回本地连接池。
- `invalidate()` 从本地连接池移除失败 socket，并关闭它。
- `dropNode()` 在 node 状态、target 映射或网络接口变化后，移除该 node 的所有 socket。
- `shutdown()` 关闭当前 IO worker 拥有的所有 socket。

该接口有意不向 `WriteLocalFileMsgEx` 暴露连接列表、计数器或路由细节。

## 连接分桶

每个 IO worker 本地连接池按 mirror node 存储 socket。

key：

```text
mirror node numeric ID
```

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
  -> 本地连接池自行创建新连接
  -> 执行现有连接创建 / handshake / socket options 逻辑
  -> 将 socket 返回给 WriteLocalFile 路径
```

每个 IO worker 维护自己的本地连接池。这个本地池的行为应与 `NodeConnPool` 保持一致，包括连接创建、连接销毁、连接可用状态维护、错误失效和等待逻辑。区别在于本地池只被一个 IO worker 访问，正常 acquire/release 不需要共享 mutex。

## IO Worker 连接数量

IO worker 处理 `WriteLocalFile` 是同步的。一个 IO worker 同一时刻只处理一个 `WriteLocalFile` work，而一个 `WriteLocalFile` mirror 写会在该 worker 内同步完成：

```text
send mirror WriteLocalFile header
send write data
recv WriteLocalFileResp
```

因此，一个 IO worker 到某个 mirror node 的连接数量为 1 即可：

```text
localLimitPerWorkerPerNode = 1
```

整体连接数量关系为：

```text
node_a 到 node_b 的最大 IO 连接数 = nr_io_worker
```

这要求配置满足：

```text
nr_io_worker <= nr_conn_socks_pernode
```

如果该约束不满足，首选调整配置，而不是让多个 IO worker 共享同一条本地池连接。共享连接会重新引入跨 worker 同步，破坏本地连接池去锁的核心目标。

本地连接池不应突破每 worker、每 mirror node 1 条 socket 的上限。连接失效后，该 worker 可以重新创建一条替代连接。

## 归还连接路径

`finishMirroring()` 收到合法 `WriteLocalFileResp` 后：

```text
finishMirroring()
  -> 解析 WriteLocalFileResp
  -> localPool.release(mirrorNode, mirrorToSock)
  -> mirrorToSock = NULL
```

成功写路径上，socket 不再归还给全局 `NodeConnPool`，而是继续由该 IO worker 本地连接池持有。

如果 release 时本地 bucket 已满，说明该 socket 不应继续保留在本地池中。首版建议直接关闭该 socket 并减少本地 `established` 计数，避免把本地池 socket 再归还给全局 `NodeConnPool` 造成所有权混杂。

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

下一次写请求可以由本地连接池重新创建连接，并把新 socket 留在本地连接池中。

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
- 只在当前已有 node/interface invalidation 广播的位置增加显式 `dropNode()` 调用。

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
本地连接池负责首次连接、补充连接、等待、销毁和错误恢复。
```

预期收益：

- 降低高并发 `WriteLocalFile` mirror 流量下的锁竞争。
- 降低 `WriteLocalFileResp` 尾延迟。
- 减少 `NodeConnPool::availableConns` 和 `connList` 的 cache-line bouncing。
- 非写 RPC 的性能和行为不变。

## 风险和缓解

风险：连接数随 IO worker 数增加。

缓解：每个 IO worker 到每个 mirror node 最多 1 条 socket，并要求 `nr_io_worker <= nr_conn_socks_pernode`。

风险：node 或 target 状态变化后残留过期 socket。

缓解：首版先在所有 socket 错误上 invalidate；后续把 `dropNode()` 接入现有 node/interface update 路径。

风险：socket 被归还给错误 owner。

缓解：debug build 中给 local-pool socket 标记 owner worker identity，并断言 release 发生在同一个 IO worker。

风险：非 `WriteLocalFile` 操作开始使用本地连接池。

缓解：本地连接池接口只从 `WriteLocalFileMsgEx` mirror 代码可达，不放入 `NodeConnPool` 或 `MessagingTk`。

风险：shutdown 遗漏本地 socket。

缓解：让 `Worker` 或 `IOWorkerContext` 析构时调用 `WriteLocalFileMirrorConnPool::shutdown()`。

## 待定问题

1. 哪条现有 node/interface update 路径负责调用 `dropNode()` 或发布 generation change？
2. 首版是否同时覆盖 `WriteLocalFile` 和 `WriteLocalFileRDMA`，还是先只覆盖非 RDMA？

## 推荐首版实现

实现最小但有用的版本：

```text
IOWorkerContext 拥有 WriteLocalFileMirrorConnPool。
Pool key 使用 mirror node numeric ID。
每个 bucket 的连接上限为 1 条 socket。
本地连接池自行完成连接创建、销毁、等待和失效处理。
配置保证 nr_io_worker <= nr_conn_socks_pernode。
只有 WriteLocalFileMsgEx mirror 路径使用该连接池。
成功收到 WriteLocalFileResp 后，将 socket 归还到本地连接池。
任何通信错误都关闭或 invalidate socket。
shutdown 关闭所有本地连接池 socket。
其他 RPC 路径保持不变。
```

这个版本直接针对当前性能问题，同时把改动限制在 `WriteLocalFile` mirror 流量内。
