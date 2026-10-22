# 路线图

路线图按「先保证正确，再保证可靠，最后补齐运维能力」的顺序推进。

## 已完成

### 正确性

- [x] File → Chunk → Slice 数据模型与覆盖写
- [x] 定长 ChunkHead 编解码与 magic 校验
- [x] Slice 区间映射、空洞补零与压缩合并
- [x] 写入范围、参数完整性与 CRC 校验

### 可靠性

- [x] 写租约（30s 过期）与显式释放，客户端崩溃后自动解锁
- [x] 拉模式副本同步，副本内容逐字节一致
- [x] 副本状态回写（`VALID` / `ERROR`），不再停留在 `SYNCING`
- [x] Storage 重启后从 `<file_path>/meta` 恢复 Chunk 与 Slice
- [x] 重复分配同一 Chunk 时拒绝覆盖已有元数据
- [x] Raft 选举不再自死锁；单节点集群可直接当选；Leader 心跳维持领导权

### 工程化

- [x] 单元测试、`-race`、端到端测试
- [x] GitHub Actions 流水线（格式、构建、vet、单测、端到端）
- [x] 架构 / 配置 / 开发 / 测试文档

## 下一步（P1）

- [x] **Tracker 元数据持久化**：文件、Chunk、副本状态写入 `<data_dir>/tracker_state.json`
      （临时文件 + fsync + rename），启动时恢复
- [ ] **Raft 日志复制**：把元数据变更做成状态机命令，接入成熟的 Raft 实现（如 `hashicorp/raft`）
- [ ] **读路径版本选择与副本故障转移**：主副本不可用时自动切换
- [ ] **后台副本修复**：周期性扫描副本不足的 Chunk 并补齐

## 后续（P2）

- [ ] 存储容量配额、写满保护与孤儿 Chunk 回收
- [ ] 分块/流式 RPC，避免单个 64 MiB Chunk 走一次请求
- [ ] 统一错误码，客户端不再依赖字符串匹配
- [ ] 指标（Prometheus）与结构化日志，接入配置中的 `log` 段
- [ ] 客户端与服务端 TLS、鉴权
- [ ] 基准测试（大文件、小文件、并发）与性能调优

## 长期（P3）

- [ ] 纠删码 / 冷热分层
- [ ] 多 Tracker 分片与水平扩展
- [ ] S3 兼容网关
- [ ] Kubernetes Operator / Helm Chart
