package tracker

import (
	"context"
	"fmt"
	"time"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage/storageservice"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/JustACP/criceta/pkg/meta"
	"github.com/cloudwego/kitex/client"
)

const (
	// inventoryInterval 库存对账周期。对账用于发现“Storage 有数据但 Tracker 不知道”
	// 以及“Tracker 有映射但 Storage 已经没有数据”两类偏差。
	inventoryInterval = 10 * time.Second
	inventoryTimeout  = 30 * time.Second

	// maxTrackedOrphans 孤儿 Chunk 记录的容量上限，避免无界增长。
	maxTrackedOrphans = 10000
)

// nodeReportState 记录单个 Storage 节点上报进度。
type nodeReportState struct {
	lastSeq  int64
	lastSeen time.Time
}

// orphanChunk 是 Storage 上报但 Tracker 没有对应文件映射的 Chunk。
// 这类数据不会进入可读集合，等待后续 GC 或客户端真正提交。
type orphanChunk struct {
	chunkID   uint64
	fileID    uint64
	nodeID    uint64
	version   uint64
	size      uint64
	firstSeen time.Time
}

// ReportChunkEvents 接收 Storage 的变更上报。
//
// 应用规则：
//   - 只有与 Tracker 记录版本一致的 Chunk 才会被标记为有效副本；
//   - 版本落后的副本标记为 ERROR（读路径会跳过）；
//   - 版本超前的 Chunk 视为“未提交的写入”，只记录不采纳；
//   - Tracker 不认识的文件/Chunk 进入孤儿集合，不创建文件映射。
func (t *TrackerNode) ReportChunkEvents(ctx context.Context, req *tracker.ReportChunkEventsRequest) (*tracker.ReportChunkEventsResponse, error) {
	if req.NodeId <= 0 {
		return nil, fmt.Errorf("invalid node id: %d", req.NodeId)
	}

	nodeID := uint64(req.NodeId)
	ackedSeq := int64(0)
	changed := false

	t.fileMux.Lock()
	for _, event := range req.Events {
		if event == nil {
			continue
		}
		if event.Seq > ackedSeq {
			ackedSeq = event.Seq
		}
		if t.applyChunkReportLocked(nodeID, event) {
			changed = true
		}
	}
	t.fileMux.Unlock()

	t.reportMux.Lock()
	state := t.nodeReports[nodeID]
	if state == nil {
		state = &nodeReportState{}
		t.nodeReports[nodeID] = state
	}
	if ackedSeq > state.lastSeq {
		state.lastSeq = ackedSeq
	}
	state.lastSeen = time.Now()
	t.reportMux.Unlock()

	if changed {
		t.markDirty()
	}

	return &tracker.ReportChunkEventsResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		AckedSeq: ackedSeq,
	}, nil
}

// applyChunkReportLocked 应用单条上报（调用方需持有 fileMux 写锁）。
// 返回 true 表示 Tracker 状态发生了变化，需要落盘。
func (t *TrackerNode) applyChunkReportLocked(nodeID uint64, event *tracker.ChunkReport) bool {
	file, ok := t.files[uint64(event.FileId)]
	if !ok {
		t.recordOrphanLocked(nodeID, event)
		return false
	}

	chunkMeta, ok := file.Chunks[uint64(event.ChunkId)]
	if !ok {
		t.recordOrphanLocked(nodeID, event)
		return false
	}

	if event.Op == tracker.ChunkReportOp_DELETE {
		return t.dropReplicaLocked(file, chunkMeta, nodeID)
	}

	switch {
	case uint64(event.Version) > chunkMeta.Version:
		// 超前版本：说明 Storage 上有一次尚未提交的写入，不能直接发布
		logs.Warn("Chunk %d on node %d has uncommitted version %d (tracker=%d), ignored",
			event.ChunkId, nodeID, event.Version, chunkMeta.Version)
		return false
	case uint64(event.Version) < chunkMeta.Version:
		// 落后版本：副本过期，不能作为可读副本
		return t.setReplicaStatusLocked(file, chunkMeta, nodeID, base.ChunkStatus_ERROR)
	default:
		return t.setReplicaStatusLocked(file, chunkMeta, nodeID, base.ChunkStatus_VALID)
	}
}

// setReplicaStatusLocked 更新（或创建）某个节点的副本状态。
func (t *TrackerNode) setReplicaStatusLocked(file *meta.FileMeta, chunkMeta *meta.ChunkMeta, nodeID uint64, status base.ChunkStatus) bool {
	if chunkMeta.NodeId == nodeID {
		// 主副本本身不需要记录在副本列表里
		return false
	}

	if file.ChunkReplicas == nil {
		file.ChunkReplicas = make(map[uint64][]*base.ReplicaInfo)
	}

	for _, replica := range file.ChunkReplicas[chunkMeta.Id] {
		if uint64(replica.NodeId) == nodeID {
			if replica.Status == int32(status) {
				return false
			}
			replica.Status = int32(status)
			replica.SyncTime = time.Now().UnixNano()
			return true
		}
	}

	file.ChunkReplicas[chunkMeta.Id] = append(file.ChunkReplicas[chunkMeta.Id], &base.ReplicaInfo{
		NodeId:   int64(nodeID),
		Status:   int32(status),
		SyncTime: time.Now().UnixNano(),
	})
	return true
}

// dropReplicaLocked 从副本列表中移除某个节点。
func (t *TrackerNode) dropReplicaLocked(file *meta.FileMeta, chunkMeta *meta.ChunkMeta, nodeID uint64) bool {
	replicas := file.ChunkReplicas[chunkMeta.Id]
	if len(replicas) == 0 {
		return false
	}

	remaining := replicas[:0]
	changed := false
	for _, replica := range replicas {
		if uint64(replica.NodeId) == nodeID {
			changed = true
			continue
		}
		remaining = append(remaining, replica)
	}
	file.ChunkReplicas[chunkMeta.Id] = remaining

	return changed
}

// recordOrphanLocked 记录一个 Tracker 未知的 Chunk（不创建文件映射）。
func (t *TrackerNode) recordOrphanLocked(nodeID uint64, event *tracker.ChunkReport) {
	if event.Op == tracker.ChunkReportOp_DELETE {
		return
	}

	if t.orphans == nil {
		t.orphans = make(map[uint64]*orphanChunk)
	}
	if len(t.orphans) >= maxTrackedOrphans {
		return
	}
	if _, exists := t.orphans[uint64(event.ChunkId)]; exists {
		return
	}

	logs.Warn("Record orphan chunk %d (file %d, version %d) reported by node %d",
		event.ChunkId, event.FileId, event.Version, nodeID)

	t.orphans[uint64(event.ChunkId)] = &orphanChunk{
		chunkID:   uint64(event.ChunkId),
		fileID:    uint64(event.FileId),
		nodeID:    nodeID,
		version:   uint64(event.Version),
		size:      uint64(event.Size),
		firstSeen: time.Now(),
	}
}

// runReconciler 周期性拉取各 Storage 节点的库存并修正映射。
func (t *TrackerNode) runReconciler() {
	ticker := time.NewTicker(inventoryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.persistStop:
			return
		case <-ticker.C:
			t.reconcileInventories()
		}
	}
}

// reconcileInventories 拉取所有在线节点的库存摘要并应用。
func (t *TrackerNode) reconcileInventories() {
	t.nodeMux.RLock()
	nodes := make([]*tracker.NodeInfo, 0, len(t.nodes))
	for _, node := range t.nodes {
		if node.Status != base.NodeStatus_ONLINE {
			continue
		}
		copied := *node
		nodes = append(nodes, &copied)
	}
	t.nodeMux.RUnlock()

	for _, node := range nodes {
		inventory, err := fetchInventory(node)
		if err != nil {
			logs.Warn("Fetch inventory from node %d failed: %v", node.NodeId, err)
			continue
		}
		t.applyInventory(node.NodeId, inventory)
	}

	// 对账后顺手补齐副本数不足的 Chunk
	t.repairUnderReplicated()
}

// fetchInventory 返回 chunkID -> version。
func fetchInventory(node *tracker.NodeInfo) (map[uint64]uint64, error) {
	cli, err := storageservice.NewClient(
		fmt.Sprintf("storage-%d", node.NodeId),
		client.WithHostPorts(node.Address),
		client.WithRPCTimeout(inventoryTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("create storage client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), inventoryTimeout)
	defer cancel()

	resp, err := cli.GetInventory(ctx, &storage.GetInventoryRequest{
		Base: &base.BaseRequest{
			RequestId: time.Now().UnixNano(),
			Timestamp: time.Now().UnixNano(),
		},
	})
	if err != nil {
		return nil, err
	}
	if resp.Base != nil && resp.Base.Code != base.SUCCESS {
		return nil, fmt.Errorf("node returned code %d: %s", resp.Base.Code, resp.Base.Message)
	}

	inventory := make(map[uint64]uint64, len(resp.Chunks))
	for _, entry := range resp.Chunks {
		if entry == nil {
			continue
		}
		inventory[uint64(entry.ChunkId)] = uint64(entry.Version)
	}

	return inventory, nil
}

// applyInventory 用节点库存修正 Tracker 的副本视图。
func (t *TrackerNode) applyInventory(nodeID int64, inventory map[uint64]uint64) {
	uid := uint64(nodeID)
	changed := false

	t.fileMux.Lock()
	for _, file := range t.files {
		for chunkID, chunkMeta := range file.Chunks {
			isPrimary := chunkMeta.NodeId == uid
			if !isPrimary && !hasReplica(file, chunkID, uid) {
				continue
			}

			version, found := inventory[chunkID]
			if found && version >= chunkMeta.Version {
				// 数据在且版本不落后：主副本无需处理，副本确认为有效
				if !isPrimary {
					if t.setReplicaStatusLocked(file, chunkMeta, uid, base.ChunkStatus_VALID) {
						changed = true
					}
				}
				continue
			}

			if found && version < chunkMeta.Version && !isPrimary {
				if t.setReplicaStatusLocked(file, chunkMeta, uid, base.ChunkStatus_ERROR) {
					changed = true
				}
				continue
			}

			// 数据缺失或主副本版本落后
			if isPrimary {
				if t.promoteReplicaLocked(file, chunkMeta, uid) {
					changed = true
				}
			} else if t.dropReplicaLocked(file, chunkMeta, uid) {
				changed = true
			}
		}
	}
	t.fileMux.Unlock()

	if changed {
		logs.Warn("Inventory of node %d diverged from tracker metadata, mapping repaired", nodeID)
		t.markDirty()
	}
}

// promoteReplicaLocked 在主副本数据缺失时，把第一个有效副本提升为主副本。
func (t *TrackerNode) promoteReplicaLocked(file *meta.FileMeta, chunkMeta *meta.ChunkMeta, failedNode uint64) bool {
	for _, replica := range file.ChunkReplicas[chunkMeta.Id] {
		if uint64(replica.NodeId) == failedNode || replica.Status != int32(base.ChunkStatus_VALID) {
			continue
		}

		logs.Warn("Promote node %d to primary for chunk %d", replica.NodeId, chunkMeta.Id)
		chunkMeta.NodeId = uint64(replica.NodeId)
		t.dropReplicaLocked(file, chunkMeta, uint64(replica.NodeId))
		return true
	}

	return false
}

func hasReplica(file *meta.FileMeta, chunkID, nodeID uint64) bool {
	for _, replica := range file.ChunkReplicas[chunkID] {
		if uint64(replica.NodeId) == nodeID {
			return true
		}
	}
	return false
}
