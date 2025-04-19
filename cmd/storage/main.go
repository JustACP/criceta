package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/JustACP/criceta/pkg/chunk"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage/storageservice"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker/trackerservice"
	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/JustACP/criceta/pkg/config"
	"github.com/JustACP/criceta/pkg/node/storage"
	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "config", "configs/storage.yaml", "Path to configuration file")
}

func main() {
	flag.Parse()

	// 初始化配置
	configPath, err := filepath.Abs(configFile)
	if err != nil {
		logs.Fatal("Failed to resolve config path: %v", err)
	}
	config.InitInstanceConfig(config.STORAGE, configPath)

	// 获取配置
	rawConfig, err := config.GetRawConfig[config.StorageNodeConfig]()
	if err != nil {
		logs.Fatal("Failed to load config: %v", err)
	}
	chunk.InitChunkSliceMeta()

	// 创建storage node
	node := storage.NewStorageNode()
	if err := node.Init(); err != nil {
		logs.Fatal("Failed to init storage node: %v", err)
	}
	defer node.Close()

	// 创建RPC服务
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", rawConfig.Port))
	if err != nil {
		logs.Fatal("Failed to resolve address: %v", err)
	}

	svr := storageservice.NewServer(
		node,
		server.WithServiceAddr(addr),
		server.WithServerBasicInfo(&rpcinfo.EndpointBasicInfo{
			ServiceName: "storage.criceta",
		}),
	)

	// 启动服务
	go func() {
		if err := svr.Run(); err != nil {
			logs.Fatal("Storage server stopped with error: %v", err)
		}
	}()

	logs.Info("Storage server started on port %d", rawConfig.Port)

	// 连接到Tracker并注册
	if len(rawConfig.Trackers) == 0 {
		logs.Fatal("No tracker configured")
	}
	trackerConfig := rawConfig.Trackers[0]
	trackerAddr := fmt.Sprintf("%s:%d", *trackerConfig.ServerIP, trackerConfig.Port)

	trackerClient, err := trackerservice.NewClient(
		"tracker.criceta",
		client.WithHostPorts(trackerAddr),
	)
	if err != nil {
		logs.Fatal("Failed to create tracker client: %v", err)
	}

	var nodeId int64
	// 检查配置中是否存在有效的ServerId
	if rawConfig.ServerId > 0 {
		// 直接使用ServerId作为NodeId
		nodeId = rawConfig.ServerId
		logs.Info("Using existing server ID as node ID: %d", nodeId)
	} else {
		// 注册到Tracker获取新的NodeId
		registerReq := &tracker.RegisterNodeRequest{
			Base: &base.BaseRequest{
				RequestId: time.Now().UnixNano(),
				Timestamp: time.Now().UnixNano(),
			},
			Address:  fmt.Sprintf("%s:%d", *rawConfig.ServerIP, rawConfig.Port),
			Capacity: *rawConfig.ChunkStorage.MaxStorageSize,
		}

		registerResp, err := trackerClient.RegisterNode(context.Background(), registerReq)
		if err != nil {
			logs.Fatal("Failed to register with tracker: %v", err)
		}
		nodeId = registerResp.NodeId
		logs.Info("Successfully registered with tracker, assigned node ID: %d", nodeId)

		// 更新配置中的ServerId并持久化
		rawConfig.ServerId = nodeId
		if err := config.GetInstanceConfig().SaveConfig(); err != nil {
			logs.Fatal("Failed to save config with new server ID: %v", err)
		}
		logs.Info("Updated config with new server ID")
	}

	// 启动心跳
	heartbeat := storage.NewStorageNodeHeartbeat(
		nodeId,
		*rawConfig.ServerIP,
		int(rawConfig.Port),
		trackerClient,
	)
	// 使用配置的心跳间隔
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	heartbeat.StartHeartbeat(ctx)
	logs.Info("Started heartbeat to tracker with interval %d seconds", rawConfig.HeartbeatInterval)

	// 等待退出信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// 优雅退出
	if err := svr.Stop(); err != nil {
		logs.Error("Failed to stop server: %v", err)
	}
}
