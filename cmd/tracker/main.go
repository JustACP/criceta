package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/JustACP/criceta/kitex_gen/criceta/tracker/trackerservice"
	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/JustACP/criceta/pkg/config"
	"github.com/JustACP/criceta/pkg/node/tracker"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "config", "configs/tracker.yaml", "Path to configuration file")
}

func main() {
	flag.Parse()

	// 初始化配置
	configPath, err := filepath.Abs(configFile)
	if err != nil {
		logs.Fatal("Failed to resolve config path: %v", err)
	}
	config.InitInstanceConfig(config.TRACKER, configPath)

	// 获取配置
	rawConfig, err := config.GetRawConfig[config.TrackerNodeConfig]()
	if err != nil {
		logs.Fatal("Failed to load config: %v", err)
	}

	// 创建tracker node
	node := tracker.NewTrackerNode()
	if err := node.Init(); err != nil {
		logs.Fatal("Failed to init tracker node: %v", err)
	}

	// 启动节点健康检查
	node.StartNodeHealthCheck()

	// 创建RPC服务
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", rawConfig.Port))
	if err != nil {
		logs.Fatal("Failed to resolve address: %v", err)
	}

	svr := trackerservice.NewServer(
		node,
		server.WithServiceAddr(addr),
		server.WithServerBasicInfo(&rpcinfo.EndpointBasicInfo{
			ServiceName: "tracker.criceta",
		}),
	)

	// 启动服务
	go func() {
		if err := svr.Run(); err != nil {
			logs.Fatal("Tracker server stopped with error: %v", err)
		}
	}()

	logs.Info("Tracker server started on port %d", rawConfig.Port)

	// 等待退出信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// 优雅退出
	if err := svr.Stop(); err != nil {
		logs.Error("Failed to stop server: %v", err)
	}
}
