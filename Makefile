# Criceta 开发任务
#
# 生成代码（kitex_gen/）不纳入版本控制，克隆仓库或修改 idl/ 后先执行：
#   make generate

GO             ?= go
KITEX_VERSION  ?= v0.13.1
THRIFTGO_VERSION ?= v0.4.1

GOBIN     := $(shell $(GO) env GOPATH)/bin
KITEX     := $(GOBIN)/kitex
GOFMT_DIRS := ./cmd ./pkg ./test

.PHONY: all tools generate build test test-race e2e vet fmt clean

all: build

## tools: 安装与生成代码匹配版本的 IDL 工具链
tools:
	$(GO) install github.com/cloudwego/kitex/tool/cmd/kitex@$(KITEX_VERSION)
	$(GO) install github.com/cloudwego/thriftgo@$(THRIFTGO_VERSION)

## generate: 从 idl/ 重新生成 kitex_gen/
generate: tools
	$(KITEX) -module github.com/JustACP/criceta idl/base.thrift
	$(KITEX) -module github.com/JustACP/criceta idl/storage.thrift
	$(KITEX) -module github.com/JustACP/criceta idl/tracker.thrift

## build: 编译所有包
build:
	$(GO) build ./...

## test: 单元测试
test:
	$(GO) test ./...

## test-race: 带数据竞争检测的单元测试
test-race:
	$(GO) test -race ./pkg/...

## e2e: 端到端测试（会真实拉起 1 Tracker + 3 Storage）
e2e:
	$(GO) test -tags=e2e -v -timeout 10m ./test/e2e

## vet: 静态检查
vet:
	$(GO) vet ./...
	$(GO) vet -tags=e2e ./test/e2e

## fmt: 检查格式（不修改文件）
fmt:
	gofmt -l $(GOFMT_DIRS)

## clean: 删除生成代码与构建产物
clean:
	rm -rf kitex_gen
