# 配置参考

配置文件使用 YAML，通过 `-config` 参数指定。Tracker 与 Storage 共用一套基础字段，
再叠加各自的扩展字段。

## 示例

- Tracker：[`configs/tracker.yaml`](../configs/tracker.yaml)
- Storage：[`configs/storage.yaml`](../configs/storage.yaml)

## 基础服务器字段

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `server_id` | int64 | 是 | 节点 ID，集群内唯一。Storage 未配置（或为 0）时会向 Tracker 注册并回写 |
| `server_name` | string | 否 | 节点名称，仅用于标识 |
| `server_ip` | string | 否* | 对外广播的地址。Storage 心跳与 Tracker 集群通信依赖它 |
| `port` | int32 | 是 | RPC 监听端口 |
| `raft_port` | int32 | 否 | 预留的 Raft 端口 |
| `memberlist_port` | int32 | 否 | 预留的成员发现端口 |
| `server_type` | string | 是 | `TRACKER` 或 `STORAGE` |

## 日志字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `mode` | string | `STDOUT` / `FILE` / `MIX` |
| `level` | string | `DEBUG` / `INFO` / `WARN` / `ERROR` / `FATAL` |
| `file_path` | string | `FILE` / `MIX` 模式下必填 |

> 当前版本仅解析该配置，日志仍输出到标准输出（见[路线图](roadmap.md)）。

## Tracker 扩展字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `cluster_mode` | string | `standalone`（默认，不启动选举）/ `cluster` / `master-slave` |
| `node_role` | string | `master` / `slave` / `node` |
| `slot_range` | object | 预留的 FileID 分配区间，`{start, end}` |
| `slave_of` | object | `master-slave` 模式下指向主节点 |
| `slaves` | list | `master-slave` 模式下的从节点列表 |
| `cluster_nodes` | list | `cluster` 模式下的 Tracker 成员列表 |

`slave_of`、`slaves`、`cluster_nodes` 的元素与基础服务器字段相同。

## Storage 扩展字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `trackers` | list | Tracker 地址列表，至少一个 |
| `heartbeat_interval` | int64 | 心跳间隔（秒） |
| `chunk_storage` | object | Chunk 存储配置，见下表 |

### chunk_storage

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `file_path` | string | 数据根目录，Chunk 元数据在 `<file_path>/meta`，Slice 在 `<file_path>/chunk` |
| `buffer_size` | int64 | 预留的缓冲区大小（字节） |
| `max_storage_size` | int64 | 节点容量上限（KB），注册时上报给 Tracker |
| `integrity_check_interval` | int64 | 完整性检查间隔（秒） |
| `recycle_interval` | int64 | 孤儿数据回收间隔（秒） |

## 完整示例

```yaml
server_id: 3
server_name: "storage-2"
server_ip: "127.0.0.1"
port: 8910
server_type: "STORAGE"

log:
  mode: "STDOUT"
  level: "INFO"

trackers:
  - server_id: 1
    server_name: "tracker-1"
    server_ip: "127.0.0.1"
    port: 8888
    server_type: "TRACKER"

chunk_storage:
  file_path: "data/chunks"
  buffer_size: 4194304
  max_storage_size: 1073741824
  integrity_check_interval: 3600
  recycle_interval: 1800

heartbeat_interval: 10
```
