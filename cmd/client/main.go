package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/JustACP/criceta/kitex_gen/criceta/tracker/trackerservice"
	cricetaClient "github.com/JustACP/criceta/pkg/client"
	"github.com/JustACP/criceta/pkg/common/logs"
	kitexClient "github.com/cloudwego/kitex/client"
)

var (
	mountPoint  string
	trackerAddr string
)

func init() {
	flag.StringVar(&mountPoint, "mount", "/tmp/criceta", "Mount point for the filesystem")
	flag.StringVar(&trackerAddr, "tracker", "localhost:8888", "Tracker server address")
}

func main() {
	flag.Parse()

	// 创建 Tracker 客户端
	trackerClient, err := trackerservice.NewClient(
		"tracker.criceta",
		kitexClient.WithHostPorts(trackerAddr),
	)
	if err != nil {
		logs.Error("Failed to create tracker client: %v", err)
		os.Exit(1)
	}

	// 挂载文件系统
	logs.Info("Mounting filesystem at %s", mountPoint)
	server, err := cricetaClient.Mount(mountPoint, trackerClient)
	if err != nil {
		if strings.Contains(err.Error(), "allow_other") {
			logs.Error("Failed to mount filesystem: %v", err)
			logs.Error("To use allow_other option, add 'user_allow_other' to /etc/fuse.conf")
			logs.Error("Or run the client as the user who will access the filesystem")
		} else {
			logs.Error("Failed to mount filesystem: %v", err)
		}
		os.Exit(1)
	}

	logs.Info("Filesystem mounted at %s", mountPoint)
	logs.Info("Press Ctrl+C to unmount and exit")

	// 等待退出信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// 卸载文件系统
	if err := server.Unmount(); err != nil {
		logs.Error("Failed to unmount filesystem: %v", err)
	} else {
		logs.Info("Filesystem unmounted successfully")
	}
}
