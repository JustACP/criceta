package tracker

import (
	"sync"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
)

type ClusterConfig struct {
	mu      sync.RWMutex
	Config  *base.ClusterConfig
	Members map[string]*tracker.NodeInfo
}

func (cc *ClusterConfig) ToThrift() *tracker.GetClusterConfigResponse {
	cc.mu.RLock()
	defer cc.mu.RUnlock()

	nodes := make([]*tracker.NodeInfo, 0, len(cc.Members))
	for _, member := range cc.Members {
		nodes = append(nodes, &tracker.NodeInfo{
			NodeId:    member.NodeId,
			Address:   member.Address,
			Status:    member.Status,
			NodeStats: member.NodeStats,
		})
	}

	return &tracker.GetClusterConfigResponse{
		Base: &base.BaseResponse{
			Code:    base.SUCCESS,
			Message: "success",
		},
		Config: &base.ClusterConfig{
			ElectionTimeoutMs:   cc.Config.ElectionTimeoutMs,
			HeartbeatIntervalMs: cc.Config.HeartbeatIntervalMs,
			SnapshotIntervalSec: cc.Config.SnapshotIntervalSec,
			MaxLogSize:          cc.Config.MaxLogSize,
			MaxBatchSize:        cc.Config.MaxBatchSize,
			Sharding:            cc.Config.Sharding,
			Balance:             cc.Config.Balance,
		},
	}
}
