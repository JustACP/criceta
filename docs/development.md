# 开发指南

## 环境要求

- Go ≥ 1.23（版本以 [`go.mod`](../go.mod) 为准）
- Linux 或 macOS。客户端依赖 [`go-fuse`](https://github.com/hanwen/go-fuse)，Windows 无法编译
- 可选：FUSE 3（仅运行客户端时需要），`kitex` / `thriftgo`（修改 IDL 后重新生成代码时需要）

## 构建与测试

```bash
go build ./...
go vet ./...
go test ./...
go test -race ./pkg/...
go test -tags=e2e -v ./test/e2e
```

如果仓库目录的属主与当前用户不一致，`go build` 可能因为 VCS stamping 报错，可以这样绕过：

```bash
GOFLAGS=-buildvcs=false go build ./...
```

## 本地起一个集群

```bash
# Tracker
go run ./cmd/tracker -config configs/tracker.yaml

# Storage（另开终端；多副本时复制配置，改 server_id / port / chunk_storage.file_path）
go run ./cmd/storage -config configs/storage.yaml

# Client
mkdir -p /tmp/criceta
go run ./cmd/client -mount /tmp/criceta -tracker 127.0.0.1:8888
```

日志输出到标准输出；Tracker 还会把节点心跳记录写入 `data/heartbeats/*.csv`，
便于事后分析节点负载。

## 代码生成（IDL）

RPC 接口定义在 [`idl/`](../idl) 下，生成的代码位于 `kitex_gen/`，**不纳入版本控制**。
克隆仓库或修改 IDL 后，执行：

```bash
make generate
```

该目标会安装固定版本的 `kitex` / `thriftgo`（版本见 [`Makefile`](../Makefile)）并依次生成
`base`、`storage`、`tracker` 三个 IDL。CI 中执行同样的步骤，因此生成代码永远不会与 IDL 脱节。

如果需要手动执行：

```bash
go install github.com/cloudwego/kitex/tool/cmd/kitex@v0.13.1
go install github.com/cloudwego/thriftgo@v0.4.1
kitex -module github.com/JustACP/criceta idl/base.thrift
kitex -module github.com/JustACP/criceta idl/storage.thrift
kitex -module github.com/JustACP/criceta idl/tracker.thrift
```

## 常用命令

| 命令 | 作用 |
| --- | --- |
| `make generate` | 生成 `kitex_gen/`（首次、改 IDL 后） |
| `make build` | 编译所有包 |
| `make test` | 单元测试 |
| `make test-race` | 带数据竞争检测的单元测试 |
| `make e2e` | 端到端测试（1 Tracker + 3 Storage） |
| `make vet` | 静态检查（含 e2e 构建标签） |
| `make fmt` | 列出未格式化文件 |
| `make clean` | 删除生成代码 |

## 代码结构约定

| 目录 | 约定 |
| --- | --- |
| `cmd/` | 只放进程入口：解析参数、初始化配置、启动 RPC，不写业务逻辑 |
| `pkg/node/` | 各角色的服务实现，按角色分包 |
| `pkg/chunk/` | 与 RPC 无关的存储引擎逻辑，可独立单测 |
| `pkg/common/` | 无状态工具（日志、ID、CRC/哈希、指针辅助） |
| `test/e2e/` | 需要真实进程的测试，使用 `//go:build e2e` 隔离 |

风格要求：

- 提交前执行 `gofmt -w ./cmd ./pkg ./test`，CI 会校验格式；
- 注释说明「当前行为与约束」，不要记录修改历史；
- 对外部输入（RPC 参数、文件内容、配置）做显式校验，不要直接解引用可选字段；
- 新增测试使用 `t.TempDir()` 存放数据，不要依赖固定端口或仓库内的相对路径；
- 文件统一使用 LF 换行（见 [`.gitattributes`](../.gitattributes)）。

## 持续集成

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) 在每次 push / pull request 时执行：

1. `make generate`（按固定版本生成 RPC 代码）；
2. `gofmt` 格式检查；
3. `go build ./...` 与 `go vet ./...`（含 `-tags=e2e`）；
4. `go test -race ./...`；
5. `go test -tags=e2e -v ./test/e2e`（真实拉起 1 Tracker + 3 Storage）。

## 调试技巧

- 单测失败时优先看 `pkg/chunk`：Slice 区间映射是最容易出错的部分；
- 端到端失败时测试会自动输出各进程日志尾部；
- Storage 的数据目录可以直接删除，重新启动后会当作空节点；
- 副本是否同步完成可通过 `GetFile` 返回的 `chunk_replicas[].status` 观察。
