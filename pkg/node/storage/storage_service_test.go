package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage"
	"github.com/JustACP/criceta/pkg/chunk"
	"github.com/JustACP/criceta/pkg/common/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStorageNode(t *testing.T) *StorageNode {
	t.Helper()

	dir := t.TempDir()
	chunk.ChunkMetaPath = filepath.Join(dir, "meta")
	chunk.SlicePath = filepath.Join(dir, "slice")

	node := NewStorageNode()
	node.SetNodeID(2)
	return node
}

func int64Ptr(v int64) *int64 { return &v }

func testChunkMeta(id, fileId, size int64) *storage.ChunkMeta {
	crc := fmt.Sprintf("%d", utils.UpdateCRC32(0, nil))
	return &storage.ChunkMeta{
		Id:       id,
		FileId:   fileId,
		Offset:   0,
		Size:     size,
		Version:  int64Ptr(1),
		NodeId:   int64Ptr(2),
		Crc:      &crc,
		CreateAt: int64Ptr(1),
		ModifyAt: int64Ptr(1),
	}
}

// TestSyncChunkPushMode 验证推送式副本同步整块写入，且数据可以原样读回。
func TestSyncChunkPushMode(t *testing.T) {
	s := newTestStorageNode(t)
	ctx := context.Background()

	data := []byte("replica-payload")
	meta := testChunkMeta(1001, 1, int64(len(data)))
	meta.Crc = nil // 未提供 CRC 时不校验

	resp, err := s.SyncChunk(ctx, &storage.SyncChunkRequest{
		Base: &base.BaseRequest{RequestId: 1},
		Meta: meta,
		Data: data,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Meta)
	assert.Equal(t, int64(len(data)), resp.Meta.Size)

	readResp, err := s.ReadChunk(ctx, &storage.ReadChunkRequest{
		Base:    &base.BaseRequest{RequestId: 2},
		ChunkId: meta.Id,
		FileId:  meta.FileId,
		Offset:  0,
		Size:    int64(len(data)),
	})
	require.NoError(t, err)
	assert.Equal(t, data, readResp.Data)
}

// TestSyncChunkRejectsCRCMismatch 验证坏数据不会覆盖本地副本。
func TestSyncChunkRejectsCRCMismatch(t *testing.T) {
	s := newTestStorageNode(t)
	ctx := context.Background()

	data := []byte("good-payload")
	meta := testChunkMeta(1002, 1, int64(len(data)))
	meta.Crc = nil
	if _, err := s.SyncChunk(ctx, &storage.SyncChunkRequest{Meta: meta, Data: data}); err != nil {
		t.Fatalf("setup sync failed: %v", err)
	}

	// 带错误 CRC 的同步必须失败
	badMeta := testChunkMeta(1002, 1, int64(len(data)))
	wrongCRC := "123456"
	badMeta.Crc = &wrongCRC
	_, err := s.SyncChunk(ctx, &storage.SyncChunkRequest{Meta: badMeta, Data: data})
	require.Error(t, err)

	// 本地数据必须保持不变
	readResp, err := s.ReadChunk(ctx, &storage.ReadChunkRequest{
		ChunkId: 1002,
		FileId:  1,
		Offset:  0,
		Size:    int64(len(data)),
	})
	require.NoError(t, err)
	assert.Equal(t, data, readResp.Data)
}

// TestSyncChunkWithoutDataOrSource 验证既无数据也无源节点时返回错误，
// 而不是把 0 字节当成同步成功。
func TestSyncChunkWithoutDataOrSource(t *testing.T) {
	s := newTestStorageNode(t)

	meta := testChunkMeta(1003, 1, 10)
	_, err := s.SyncChunk(context.Background(), &storage.SyncChunkRequest{Meta: meta})
	require.Error(t, err)
}

// TestWriteAndReadChunk 覆盖普通写入/读取路径。
func TestWriteAndReadChunk(t *testing.T) {
	s := newTestStorageNode(t)
	ctx := context.Background()

	data := []byte("hello criceta")
	meta := testChunkMeta(1004, 9, int64(len(data)))
	meta.Crc = nil

	writeResp, err := s.WriteChunk(ctx, &storage.WriteChunkRequest{
		Meta:        meta,
		Data:        data,
		WriteOffset: 0,
		WriteSize:   int64(len(data)),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), writeResp.Meta.Size)

	readResp, err := s.ReadChunk(ctx, &storage.ReadChunkRequest{
		ChunkId: 1004,
		FileId:  9,
		Offset:  0,
		Size:    int64(len(data)),
	})
	require.NoError(t, err)
	assert.Equal(t, data, readResp.Data)
}

func TestParseCRC(t *testing.T) {
	cases := []struct {
		in   string
		want uint32
		ok   bool
	}{
		{"123456", 123456, true},
		{"0x1e240", 123456, true},
		{"1e240", 123456, true},
		{"not-a-crc", 0, false},
	}

	for _, c := range cases {
		got, ok := parseCRC(c.in)
		assert.Equal(t, c.ok, ok, "input: %s", c.in)
		if c.ok {
			assert.Equal(t, c.want, got, "input: %s", c.in)
		}
	}
}
