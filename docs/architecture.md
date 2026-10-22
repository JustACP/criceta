# 架构设计

本文描述 Criceta 的整体结构、核心流程与一致性模型。实现细节以代码为准，文档与代码不一致时
请优先阅读 `pkg/` 下的对应包。

## 1. 设计目标

1. **元数据与数据面分离**：Tracker 只处理小体量的元数据，文件数据由客户端直接与 Storage
   交换，避免所有流量穿过元数据服务。
2. **覆盖写不放大**：大文件按 Chunk 切分，Chunk 内部用追加 Slice + 版本号表达覆盖写，避免
   “改一个字节重写 64 MiB”。
3. **故障可恢复**：节点心跳超时会摘除节点；写租约超时会自动释放；Storage 重启后能从磁盘
   恢复 Chunk 索引。
4. **实现可读**：不引入重量级框架，RPC 使用 Kitex + Thrift，存储格式自定义且带 magic 校验。

## 2. 组件

### Tracker

- 维护 `fileID → FileMeta`、`nodeID → NodeInfo` 两张内存表；
- 分配 Chunk 时选择在线 Storage 节点，并记录文件写租约；
- 客户端提交 Chunk 后更新文件版本，并异步触发副本同步；
- 通过心跳感知 Storage 节点存活，超时（默认 30s）摘除；
- `cluster` 模式下参与 Raft 选举，`standalone` 模式不启动选举。

### Storage

- 管理本机的 Chunk 集合：`chunkID → SliceBasedChunk`；
- Chunk 元数据（`ChunkHead`，定长 256 字节）与 Slice 数据分别落盘；
- 支持推送 / 拉取两种副本同步方式，写入前做 CRC 校验；
- 后台扫描 Slice 数量过多的 Chunk，交给压缩工作池合并。

### Client

- 实现 FUSE 接口（`Getattr` / `Lookup` / `Create` / `Read` / `Write` / `Flush` / `Release` 等）；
- 维护 4 MiB 写缓冲，按连续区间合并写请求；
- 读文件时把 Chunk 列表转换成区间映射，逐段向对应 Storage 读取，空洞补零。

## 3. 数据模型

```
File
 └── Chunk      （副本与校验单位，上限 64 MiB）
      └── Slice （写入单位，带版本号）
```

`ChunkHead` 使用固定长度二进制编码（magic + 字段），便于原地更新：

| 字段 | 说明 |
| --- | --- |
| `Id` / `FileId` | Chunk 与所属文件 |
| `Version` | Chunk 版本，随写入递增 |
| `Offset` | Chunk 在文件中的起始偏移 |
| `Size` / `Range` | 有效数据长度与区间 |
| `CRC` / `Hash` | CRC32 与 BLAKE3 摘要 |
| `WriteIdx` / `ReadIdx` | 顺序读写游标 |
| `Status` | `VALID` / `WRITING` / `SYNCING` 等 |

每个 Slice 也有自己的头部（magic + 区间 + 版本 + CRC），数据紧跟头部之后。

## 4. 写流程

```mermaid
sequenceDiagram
    participant C as Client
    participant T as Tracker
    participant S1 as Storage（主）
    participant S2 as Storage（副本）

    C->>T: CreateFile
    C->>T: AllocateChunk(fileID, offset, size)
    T->>T: 获取写租约（30s）
    T-->>C: ChunkMeta + 目标节点
    C->>S1: WriteChunk(chunkID, offset, data)
    S1->>S1: 追加 Slice + 更新 ChunkHead
    S1-->>C: ChunkMeta（version/CRC/size）
    C->>T: CommitChunk(ChunkMeta)
    T->>T: 更新文件元数据与版本
    T->>S2: SyncChunk(meta + source)
    S2->>S1: ReadChunk（回源拉取）
    S2->>S2: CRC 校验后落盘
    S2-->>T: 同步结果
    T->>T: 回写副本状态 VALID / ERROR
    C->>T: UnlockFile（关闭文件时）
```

要点：

- 写租约在 `AllocateChunk` 时获取、`CommitChunk` 时续期、`UnlockFile` 时释放；
  过期后由 Tracker 的后台协程回收。
- 副本同步是异步的：`CommitChunk` 返回不代表副本已经就绪，客户端可以通过 `GetFile` 查看
  `chunk_replicas` 的 `status`。

## 5. 读流程

```mermaid
sequenceDiagram
    participant C as Client
    participant T as Tracker
    participant S as Storage

    C->>T: GetFile(fileID)
    T-->>C: FileMeta（Chunk 列表 + 副本）
    C->>C: 构建区间映射（按 offset / version）
    loop 每个覆盖的 Chunk
        C->>S: ReadChunk(chunkID, offset, size)
        S->>S: 从 Slice 映射读取最新版本
        S-->>C: data
    end
    C->>C: 空洞补零、拼接结果
```

当前实现总是从 Chunk 的**主节点**读取，副本只用于同步与校验。读副本故障转移在路线图中。

## 6. 副本与一致性

- **副本数**：`minReplicaCount = 2`，由 Tracker 在 `CommitChunk` 时按在线节点选择；
- **同步方式**：拉模式。Tracker 只下发元数据与源地址，目标节点主动回源，避免 Tracker 成为
  数据转发瓶颈；
- **校验**：副本写入前校验 CRC32，失败时拒绝覆盖并回写 `ERROR` 状态；
- **一致性**：单个 Chunk 内以 Slice 版本号决定新旧，高版本覆盖低版本；跨 Chunk 的并发写由
  文件写租约串行化。

## 7. 磁盘布局

Storage 节点在配置的 `chunk_storage.file_path` 下维护两个目录：

```
<file_path>/
  meta/
    chunk_<fileId>_<chunkId>.meta   # ChunkHead（定长 256B）
  chunk/
    slice_<chunkId>_<sliceId>.dat   # SliceHeader + 数据
```

- 启动时扫描 `meta/*.meta`，逐个恢复 Chunk 并挂载其 Slice；
- 单个 Chunk 的元数据使用 `O_EXCL` 创建，已存在的 Chunk 不会被截断；
- 读取时按 Slice 的 `offset` 与 `version` 重建区间映射。

## 8. 故障处理

| 故障 | 处理方式 |
| --- | --- |
| Storage 心跳超时 | Tracker 标记离线并移出节点表；新 Chunk 不再分配给它 |
| 客户端崩溃 | 写租约 30s 后自动过期，其他客户端可以接管文件 |
| 副本同步失败 | 记录 `ERROR` 状态并输出日志；当前不会自动重试 |
| 副本 CRC 不匹配 | 拒绝写入，保留本地已有副本 |
| Storage 重启 | 从 `meta/` 恢复 Chunk 索引与 Slice 映射 |
| Tracker 重启 | **元数据仅在内存，重启后丢失**（见路线图） |

## 9. 已知限制

1. Tracker 元数据没有持久化，也没有接入 Raft 日志复制；
2. 读路径没有版本选择和副本故障转移；
3. 没有容量配额、孤儿 Chunk GC 与后台副本修复；
4. RPC 错误多以 Go `error` 返回，错误码体系尚未统一；
5. 客户端与服务端之间没有 TLS / 鉴权。
