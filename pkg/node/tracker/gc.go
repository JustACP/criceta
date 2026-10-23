package tracker

import (
	"context"
	"fmt"
	"time"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage/storageservice"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/JustACP/criceta/pkg/chunk"
	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/JustACP/criceta/pkg/meta"
	"github.com/cloudwego/kitex/client"
)

const (
	// orphanGCInterval 孤儿检查周期
	orphanGCInterval = 5 * time.Second
	// orphanGCBatch 单轮最多清理多少个孤儿，避免一次打爆网络
	orphanGCBatch = 50
)

// runOrphanGC 周期性清理超过 TTL 的孤儿 Chunk。
func (t *TrackerNode) runOrphanGC() {
	ticker := time.NewTicker(orphanGCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.persistStop:
			return
		case <-ticker.C:
			t.collectOrphans()
		}
	}
}

// collectOrphans 向存储节点下发删除指令，成功后才把记录移出孤儿集合。
func (t *TrackerNode) collectOrphans() {
	now := time.Now()

	t.fileMux.Lock()
	expired := make([]*orphanChunk, 0, len(t.orphans))
	for id, orphan := range t.orphans {
		if now.Sub(orphan.firstSeen) < t.orphanTTL {
			continue
		}
		if len(expired) >= orphanGCBatch {
			break
		}
		expired = append(expired, orphan)
		delete(t.orphans, id)
	}
	t.fileMux.Unlock()

	for _, orphan := range expired {
		if err := t.deleteChunkOnNode(orphan); err != nil {
			logs.Warn("Delete orphan chunk %d on node %d failed (will retry): %v",
				orphan.chunkID, orphan.nodeID, err)
			t.fileMux.Lock()
			if _, exists := t.orphans[orphan.chunkID]; !exists {
				t.orphans[orphan.chunkID] = orphan
			}
			t.fileMux.Unlock()
			continue
		}

		logs.Info("Reclaimed orphan chunk %d from node %d", orphan.chunkID, orphan.nodeID)
	}
}

func (t *TrackerNode) deleteChunkOnNode(orphan *orphanChunk) error {
	node := t.getNode(orphan.nodeID)
	if node == nil {
		return nil // 节点已不存在，视为清理完成
	}

	cli, err := storageservice.NewClient(
		fmt.Sprintf("storage-%d", orphan.nodeID),
		client.WithHostPorts(node.Address),
		client.WithRPCTimeout(inventoryTimeout),
	)
	if err != nil {
		return fmt.Errorf("create storage client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), inventoryTimeout)
	defer cancel()

	resp, err := cli.DeleteChunk(ctx, &storage.DeleteChunkRequest{
		Base: &base.BaseRequest{
			RequestId: time.Now().UnixNano(),
			Timestamp: time.Now().UnixNano(),
		},
		ChunkId: int64(orphan.chunkID),
	})
	if err != nil {
		return err
	}
	if resp.Base != nil && resp.Base.Code != base.SUCCESS {
		return fmt.Errorf("node returned code %d: %s", resp.Base.Code, resp.Base.Message)
	}

	return nil
}

// repairUnderReplicated 为副本数不足的 Chunk 触发一次拉模式补齐。
func (t *TrackerNode) repairUnderReplicated() {
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

	if len(nodes) < minReplicaCount {
		return
	}

	type repairJob struct {
		chunk  *meta.ChunkMeta
		source *tracker.NodeInfo
		target *tracker.NodeInfo
	}

	jobs := make([]repairJob, 0)

	t.fileMux.RLock()
	for _, file := range t.files {
		for _, chunkMeta := range file.Chunks {
			if chunkMeta.Status != chunk.VALID {
				continue
			}

			validReplicas := 0
			for _, replica := range file.ChunkReplicas[chunkMeta.Id] {
				if replica.Status == int32(base.ChunkStatus_VALID) {
					validReplicas++
				}
			}
			// 主副本自己算一份
			if validReplicas+1 >= minReplicaCount {
				continue
			}

			source := findNode(nodes, chunkMeta.NodeId)
			if source == nil {
				// 主副本不在线时，用任意有效副本作为回源节点
				for _, replica := range file.ChunkReplicas[chunkMeta.Id] {
					if replica.Status != int32(base.ChunkStatus_VALID) {
						continue
					}
					source = findNode(nodes, uint64(replica.NodeId))
					if source != nil {
						break
					}
				}
			}
			if source == nil {
				continue
			}

			target := selectRepairTarget(file, chunkMeta, nodes)
			if target == nil {
				continue
			}

			jobs = append(jobs, repairJob{chunk: chunkMeta, source: source, target: target})
		}
	}
	t.fileMux.RUnlock()

	for _, job := range jobs {
		if !t.beginRepair(job.chunk.Id) {
			continue
		}
		go func(job repairJob) {
			defer t.endRepair(job.chunk.Id)

			logs.Info("Repairing chunk %d: copy from node %d to node %d",
				job.chunk.Id, job.source.NodeId, job.target.NodeId)

			t.syncReplica(chunkMetaToStorage(job.chunk), job.source, &base.ReplicaInfo{
				NodeId: job.target.NodeId,
				Status: int32(base.ChunkStatus_SYNCING),
			})
		}(job)
	}
}

// selectRepairTarget 选择一个既不在线也不持有该 Chunk 的节点作为修复目标。
func selectRepairTarget(file *meta.FileMeta, chunkMeta *meta.ChunkMeta, nodes []*tracker.NodeInfo) *tracker.NodeInfo {
	for _, node := range nodes {
		if uint64(node.NodeId) == chunkMeta.NodeId {
			continue
		}
		if hasReplica(file, chunkMeta.Id, uint64(node.NodeId)) {
			continue
		}
		return node
	}
	return nil
}

func findNode(nodes []*tracker.NodeInfo, nodeID uint64) *tracker.NodeInfo {
	for _, node := range nodes {
		if uint64(node.NodeId) == nodeID {
			return node
		}
	}
	return nil
}

func (t *TrackerNode) beginRepair(chunkID uint64) bool {
	t.reportMux.Lock()
	defer t.reportMux.Unlock()

	if t.repairing[chunkID] {
		return false
	}
	t.repairing[chunkID] = true
	return true
}

func (t *TrackerNode) endRepair(chunkID uint64) {
	t.reportMux.Lock()
	defer t.reportMux.Unlock()

	delete(t.repairing, chunkID)
}

// chunkMetaToStorage 把 Tracker 的 Chunk 元数据转换成存储侧 RPC 结构。
func chunkMetaToStorage(chunkMeta *meta.ChunkMeta) *storage.ChunkMeta {
	version := int64(chunkMeta.Version)
	size := int64(chunkMeta.Size)
	offset := int64(chunkMeta.Offset)
	nodeID := int64(chunkMeta.NodeId)
	crc := fmt.Sprintf("%d", chunkMeta.CRC)
	createAt := chunkMeta.CreateAt
	modifyAt := chunkMeta.UpdateAt
	status := base.ChunkStatus(chunkMeta.Status)

	return &storage.ChunkMeta{
		Id:       int64(chunkMeta.Id),
		FileId:   int64(chunkMeta.FileId),
		Version:  &version,
		Size:     size,
		Offset:   offset,
		NodeId:   &nodeID,
		Status:   &status,
		Crc:      &crc,
		CreateAt: &createAt,
		ModifyAt: &modifyAt,
	}
}
