# 测试指南

Criceta 的测试分三层，覆盖的粒度与运行成本依次递增。

> 首次运行前先执行 `make generate`：RPC 代码由 `idl/` 生成，不纳入版本控制。

| 层级 | 位置 | 命令 | 依赖 |
| --- | --- | --- | --- |
| 单元测试 | `pkg/**/*_test.go` | `make test` | 无 |
| 竞争检测 | 同上 | `make test-race` | 无 |
| 端到端 | `test/e2e/e2e_test.go` | `make e2e` | 可执行文件、四个进程、可用端口 |

## 单元测试

单元测试不接触真实网络与固定路径：

- 涉及磁盘的测试使用 `t.TempDir()` 作为数据目录；
- `pkg/chunk` 的测试通过设置包级变量 `SlicePath` / `ChunkMetaPath` 指向临时目录；
- `pkg/node/tracker` 的测试通过 `NewTrackerNodeWithDataDir` 注入临时目录，
  避免在仓库里留下心跳 CSV；元数据持久化测试会在同一目录上重建节点，验证快照恢复。

新增用例时应保持同样的约束：**不写死端口、不依赖仓库相对路径、不依赖执行顺序**。

## 端到端测试

端到端测试的文件带构建标签，默认不参与 `go test ./...`：

```go
//go:build e2e
```

运行方式：

```bash
go test -tags=e2e -v ./test/e2e
# 或者只跑单个用例
go test -tags=e2e -v -run TestClusterWriteReplicateAndRewrite ./test/e2e
```

测试会：

1. 在 `t.TempDir()` 中执行 `go build`，编译 `cmd/tracker` 与 `cmd/storage`；
2. 申请空闲端口，生成 Tracker 与 3 个 Storage 的临时配置（数据目录也在临时目录内）；
3. 启动进程并等待端口就绪、等待 Tracker 看到全部节点；
4. 通过正式客户端 SDK 写入 8 MiB 文件；
5. 校验读回内容与写入一致；
6. 轮询 `GetFile`，直到每个 Chunk 都有 2 个状态为 `VALID` 的副本；
7. 直接向主副本与每个副本节点发送 `ReadChunk`，逐字节比对内容；
8. 用**另一个客户端**重写同一文件，验证写租约确实已释放、覆盖写能读到新内容；
9. 结束时向所有子进程发送 `SIGINT`，超时后强制结束；失败时输出各进程日志尾部。

`go test -short` 会跳过端到端测试。

### 稳定性约定

- 端口通过 `net.Listen("tcp", "127.0.0.1:0")` 动态分配；
- 所有路径位于 `t.TempDir()`，测试结束自动清理；
- 所有等待都带超时（启动 30s、副本 90s），失败会给出上下文；
- 不依赖外部服务、不使用固定路径。

## CI

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) 包含两个 Job，两个 Job 都会先执行
`make generate`：

- **Build, Vet & Unit Tests**：格式检查、构建、vet（含 e2e 标签）、`go test -race ./...`；
- **End-to-End**：`go test -tags=e2e -v ./test/e2e`。

本地提交前建议至少执行：

```bash
make generate
make fmt
make vet
make test-race
make e2e
```
