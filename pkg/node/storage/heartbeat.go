package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/JustACP/criceta/pkg/common/idgen"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker/trackerservice"
	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/JustACP/criceta/pkg/config"
	"github.com/JustACP/criceta/pkg/meta"
)

const (
	maxRetries = 3 // 最大重试次数
)

// StorageNodeHeartbeat 存储节点结构
type StorageNodeHeartbeat struct {
	nodeId        int64
	host          string
	port          int
	trackerClient trackerservice.Client
}

func NewStorageNodeHeartbeat(nodeId int64, host string, port int, trackerClient trackerservice.Client) *StorageNodeHeartbeat {
	return &StorageNodeHeartbeat{
		nodeId:        nodeId,
		host:          host,
		port:          port,
		trackerClient: trackerClient,
	}
}

// StartHeartbeat 启动心跳发送协程
func (n *StorageNodeHeartbeat) StartHeartbeat(ctx context.Context) {

	rawConfig, err := config.GetRawConfig[config.StorageNodeConfig]()
	if err != nil {
		logs.Error("Failed to get config for heartbeat interval: %v", err)
		return
	}
	heartbeatInterval := time.Duration(rawConfig.HeartbeatInterval) * time.Second

	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		if err := n.sendHeartbeat(); err != nil {
			logs.Error("Failed to send initial heartbeat: %v", err)
		}

		for {
			select {
			case <-ctx.Done():
				logs.Info("Heartbeat goroutine stopped")
				return
			case <-ticker.C:
				if err := n.sendHeartbeat(); err != nil {
					logs.Error("Failed to send heartbeat: %v", err)
				}
			}
		}
	}()
}

// sendHeartbeat 发送心跳包
func (n *StorageNodeHeartbeat) sendHeartbeat() error {

	serverStats, err := n.getServerStats()
	if err != nil {
		return fmt.Errorf("failed to get server stats: %v", err)
	}

	req := &tracker.HeartbeatRequest{
		Base: &base.BaseRequest{
			RequestId: int64(idgen.NextID()),
			Timestamp: time.Now().UnixNano(),
		},
		NodeId:      n.nodeId,
		Host:        n.host,
		Port:        int32(n.port),
		ServerStats: serverStats,
	}

	var resp *tracker.HeartbeatResponse
	var retryCount int
	for retryCount < maxRetries {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err = n.trackerClient.Heartbeat(ctx, req)
		cancel()

		if err == nil && resp.Base.Code == 0 {
			return nil
		}

		retryCount++
		time.Sleep(time.Second * time.Duration(retryCount))
	}

	if resp != nil {
		return fmt.Errorf("heartbeat failed after %d retries, last response: %v", maxRetries, resp.Base)
	}
	return fmt.Errorf("heartbeat failed after %d retries", maxRetries)
}

// getServerStats 获取服务器状态
func (n *StorageNodeHeartbeat) getServerStats() (*base.ServerStats, error) {
	stats := &meta.ServerStats{}
	if err := stats.Stat(); err != nil {
		return nil, err
	}

	return &base.ServerStats{
		Timestamp: time.Now().UnixNano(),
		Cpu: &base.CPUStats{
			UsagePercent: stats.CPU.UsagePercent,
			UserPercent:  stats.CPU.UserPercent,
			SysPercent:   stats.CPU.SysPercent,
			Load1:        stats.CPU.Load1,
			Load5:        stats.CPU.Load5,
			Load15:       stats.CPU.Load15,
			NumCores:     int32(stats.CPU.NumCores),
		},
		Memory: &base.MemoryStats{
			Total:       int64(stats.Memory.Total),
			Used:        int64(stats.Memory.Used),
			Free:        int64(stats.Memory.Free),
			UsedPercent: stats.Memory.UsedPercent,
			SwapTotal:   int64(stats.Memory.SwapTotal),
			SwapUsed:    int64(stats.Memory.SwapUsed),
		},
		Disk: &base.DiskStats{
			Total:       int64(stats.Disk.Total),
			Used:        int64(stats.Disk.Used),
			Free:        int64(stats.Disk.Free),
			UsedPercent: stats.Disk.UsedPercent,
			ReadSpeed:   int64(stats.Disk.ReadSpeed),
			WriteSpeed:  int64(stats.Disk.WriteSpeed),
			Iops:        int64(stats.Disk.IOPS),
		},
		Network: &base.NetworkStats{
			BytesSent:   int64(stats.Network.BytesSent),
			BytesRecv:   int64(stats.Network.BytesRecv),
			PacketsSent: int64(stats.Network.PacketsSent),
			PacketsRecv: int64(stats.Network.PacketsRecv),
			ErrorsIn:    int64(stats.Network.ErrorsIn),
			ErrorsOut:   int64(stats.Network.ErrorsOut),
			DropsIn:     int64(stats.Network.DropsIn),
			DropsOut:    int64(stats.Network.DropsOut),
		},
		SystemInfo: &base.SystemInfo{
			Hostname:    stats.SystemInfo.Hostname,
			Os:          stats.SystemInfo.OS,
			Platform:    stats.SystemInfo.Platform,
			Kernel:      stats.SystemInfo.Kernel,
			Uptime:      int64(stats.SystemInfo.Uptime),
			ProcessId:   int32(stats.SystemInfo.ProcessID),
			ProcessName: stats.SystemInfo.ProcessName,
		},
	}, nil
}
