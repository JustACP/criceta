package tracker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func int64Ptr(v int64) *int64 { return &v }

func newPersistentTestNode(t *testing.T, dir string) *TrackerNode {
	t.Helper()

	node := NewTrackerNodeWithDataDir(dir)
	require.NotNil(t, node)
	require.NoError(t, node.Init())

	return node
}

func createTestFile(t *testing.T, node *TrackerNode, name string) int64 {
	t.Helper()

	resp, err := node.CreateFile(context.Background(), &tracker.CreateFileRequest{
		Base:   &base.BaseRequest{RequestId: 1},
		Name:   name,
		Length: 1024,
	})
	require.NoError(t, err)

	return resp.Meta.Id
}

func commitTestChunk(t *testing.T, node *TrackerNode, fileID, chunkID int64) {
	t.Helper()

	version, crc := int64(1), "12345"
	createAt, modifyAt := int64(1000), int64(2000)
	nodeID := int64(2)

	_, err := node.CommitChunk(context.Background(), &tracker.CommitChunkRequest{
		Base: &base.BaseRequest{RequestId: 1},
		Chunk: &storage.ChunkMeta{
			Id:       chunkID,
			FileId:   fileID,
			Version:  &version,
			Offset:   0,
			Size:     1024,
			NodeId:   &nodeID,
			Crc:      &crc,
			CreateAt: &createAt,
			ModifyAt: &modifyAt,
		},
	})
	require.NoError(t, err)
}

// TestTrackerMetadataSurvivesRestart 覆盖 P1-1：Tracker 重启后文件与 Chunk 元数据必须还在。
func TestTrackerMetadataSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	node := newPersistentTestNode(t, dir)
	fileID := createTestFile(t, node, "persist.bin")

	chunkID := int64(9001)
	version, crc := int64(1), "12345"
	createAt, modifyAt := int64(1000), int64(2000)
	nodeID := int64(2)

	_, err := node.CommitChunk(ctx, &tracker.CommitChunkRequest{
		Base: &base.BaseRequest{RequestId: 1},
		Chunk: &storage.ChunkMeta{
			Id:       chunkID,
			FileId:   fileID,
			Version:  &version,
			Offset:   0,
			Size:     1024,
			NodeId:   &nodeID,
			Crc:      &crc,
			CreateAt: &createAt,
			ModifyAt: &modifyAt,
		},
	})
	require.NoError(t, err)

	// 同步落盘后关闭，模拟进程退出
	require.NoError(t, node.flush())
	require.NoError(t, node.Close())

	reopened := newPersistentTestNode(t, dir)
	t.Cleanup(func() { _ = reopened.Close() })

	resp, err := reopened.GetFile(ctx, &tracker.GetFileRequest{FileId: fileID})
	require.NoError(t, err)
	assert.Equal(t, "persist.bin", resp.Meta.Name)
	require.Contains(t, resp.Meta.Chunks, chunkID)
	assert.Equal(t, int64(1024), resp.Meta.Chunks[chunkID].Size)
	assert.Equal(t, int64(1024), resp.Meta.Size)
}

// TestTrackerDeletionSurvivesRestart 删除也要落盘，重启后不能“复活”。
func TestTrackerDeletionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	node := newPersistentTestNode(t, dir)
	fileID := createTestFile(t, node, "gone.bin")

	_, err := node.DeleteFile(ctx, &tracker.DeleteFileRequest{
		Base:   &base.BaseRequest{RequestId: 1},
		FileId: fileID,
	})
	require.NoError(t, err)

	require.NoError(t, node.flush())
	require.NoError(t, node.Close())

	reopened := newPersistentTestNode(t, dir)
	t.Cleanup(func() { _ = reopened.Close() })

	resp, err := reopened.ListFiles(ctx, &tracker.ListFilesRequest{
		Base: &base.BaseRequest{RequestId: 1},
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Files)
}

// TestTrackerSnapshotIsAtomic 验证落盘使用临时文件 + rename，不残留半成品。
func TestTrackerSnapshotIsAtomic(t *testing.T) {
	dir := t.TempDir()
	node := newPersistentTestNode(t, dir)
	t.Cleanup(func() { _ = node.Close() })

	createTestFile(t, node, "atomic.bin")
	require.NoError(t, node.flush())

	_, err := os.Stat(filepath.Join(dir, snapshotFileName))
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(dir, snapshotFileName+snapshotTempSuffix))
	assert.True(t, os.IsNotExist(err), "temp snapshot should be renamed away")
}

// TestTrackerRejectsCorruptSnapshot 快照损坏时启动应报错，而不是静默丢掉全部元数据。
func TestTrackerRejectsCorruptSnapshot(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, snapshotFileName),
		[]byte("{ this is not json"),
		0o644,
	))

	node := NewTrackerNodeWithDataDir(dir)
	require.NotNil(t, node)
	t.Cleanup(func() { _ = node.Close() })

	require.Error(t, node.Init())
}
