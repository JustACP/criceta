package tracker

import (
	"context"
	"testing"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func reportEvent(seq int64, op tracker.ChunkReportOp, chunkID, fileID, version int64) *tracker.ChunkReport {
	return &tracker.ChunkReport{
		Seq:     seq,
		Op:      op,
		ChunkId: chunkID,
		FileId:  fileID,
		Version: version,
		Size:    1024,
	}
}

func reportFrom(t *testing.T, node *TrackerNode, nodeID int64, events ...*tracker.ChunkReport) *tracker.ReportChunkEventsResponse {
	t.Helper()

	resp, err := node.ReportChunkEvents(context.Background(), &tracker.ReportChunkEventsRequest{
		Base:   &base.BaseRequest{RequestId: 1},
		NodeId: nodeID,
		Events: events,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Base)

	return resp
}

func replicaStatus(t *testing.T, node *TrackerNode, fileID, chunkID, nodeID int64) (int32, bool) {
	t.Helper()

	node.fileMux.RLock()
	defer node.fileMux.RUnlock()

	file, ok := node.files[uint64(fileID)]
	if !ok {
		return 0, false
	}
	for _, replica := range file.ChunkReplicas[uint64(chunkID)] {
		if replica.NodeId == nodeID {
			return replica.Status, true
		}
	}
	return 0, false
}

// TestReportChunkEventsMarksReplicaValid 上报可以让副本从 SYNCING 变成 VALID。
func TestReportChunkEventsMarksReplicaValid(t *testing.T) {
	node := newPersistentTestNode(t, t.TempDir())
	fileID := createTestFile(t, node, "report.bin")
	commitTestChunk(t, node, fileID, 501)

	resp := reportFrom(t, node, 3, reportEvent(7, tracker.ChunkReportOp_UPSERT, 501, fileID, 1))
	assert.Equal(t, int64(7), resp.AckedSeq)

	status, ok := replicaStatus(t, node, fileID, 501, 3)
	require.True(t, ok)
	assert.Equal(t, int32(base.ChunkStatus_VALID), status)
}

// TestReportChunkEventsMarksStaleReplicaError 版本落后的副本不能被当作可读副本。
func TestReportChunkEventsMarksStaleReplicaError(t *testing.T) {
	node := newPersistentTestNode(t, t.TempDir())
	fileID := createTestFile(t, node, "stale.bin")
	commitTestChunk(t, node, fileID, 502)

	reportFrom(t, node, 4, reportEvent(1, tracker.ChunkReportOp_UPSERT, 502, fileID, 1))
	reportFrom(t, node, 4, reportEvent(2, tracker.ChunkReportOp_UPSERT, 502, fileID, 0))

	status, ok := replicaStatus(t, node, fileID, 502, 4)
	require.True(t, ok)
	assert.Equal(t, int32(base.ChunkStatus_ERROR), status)
}

// TestReportChunkEventsIgnoresUnknownChunk 未知 Chunk 不创建文件映射，只进孤儿集合。
func TestReportChunkEventsIgnoresUnknownChunk(t *testing.T) {
	node := newPersistentTestNode(t, t.TempDir())

	reportFrom(t, node, 5, reportEvent(1, tracker.ChunkReportOp_UPSERT, 999, 12345, 1))

	node.fileMux.RLock()
	_, exists := node.files[12345]
	_, orphaned := node.orphans[999]
	node.fileMux.RUnlock()
	assert.False(t, exists, "reports must not create file mappings")
	assert.True(t, orphaned)
}

// TestCommitClearsOrphan 提交之后，之前被上报为孤儿的记录必须被清除，
// 避免正常写入被长期误判为孤儿。
func TestCommitClearsOrphan(t *testing.T) {
	node := newPersistentTestNode(t, t.TempDir())

	// 报告先于提交到达：此时 Tracker 还不认识这个 Chunk
	reportFrom(t, node, 3, reportEvent(1, tracker.ChunkReportOp_UPSERT, 777, 0, 1))

	node.fileMux.RLock()
	_, orphaned := node.orphans[777]
	node.fileMux.RUnlock()
	require.True(t, orphaned)

	// 客户端随后创建文件并提交同一个 Chunk
	fileID := createTestFile(t, node, "orphan.bin")
	commitTestChunk(t, node, fileID, 777)

	node.fileMux.RLock()
	_, stillOrphan := node.orphans[777]
	node.fileMux.RUnlock()
	assert.False(t, stillOrphan)
}

// TestApplyInventoryPromotesReplica 主副本数据丢失时，把有效副本提升为主副本。
func TestApplyInventoryPromotesReplica(t *testing.T) {
	node := newPersistentTestNode(t, t.TempDir())
	fileID := createTestFile(t, node, "inventory.bin")
	commitTestChunk(t, node, fileID, 601)
	reportFrom(t, node, 3, reportEvent(1, tracker.ChunkReportOp_UPSERT, 601, fileID, 1))

	// 节点 2（主副本）的库存里没有该 Chunk
	node.applyInventory(2, map[uint64]uint64{})

	node.fileMux.RLock()
	chunkMeta := node.files[uint64(fileID)].Chunks[601]
	node.fileMux.RUnlock()

	assert.Equal(t, uint64(3), chunkMeta.NodeId, "healthy replica should be promoted")
}

// TestApplyInventoryDropsMissingReplica 副本数据缺失时从副本列表移除。
func TestApplyInventoryDropsMissingReplica(t *testing.T) {
	node := newPersistentTestNode(t, t.TempDir())
	fileID := createTestFile(t, node, "drop.bin")
	commitTestChunk(t, node, fileID, 602)
	reportFrom(t, node, 3, reportEvent(1, tracker.ChunkReportOp_UPSERT, 602, fileID, 1))

	// 节点 3 的库存里没有该 Chunk，而主副本还在
	node.applyInventory(3, map[uint64]uint64{})

	_, ok := replicaStatus(t, node, fileID, 602, 3)
	assert.False(t, ok, "missing replica should be removed")
}
