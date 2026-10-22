package chunk

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordedRange struct {
	chunkID uint64
	offset  uint64
	size    uint64
	isGap   bool
}

type rangeRecorder struct {
	ranges []recordedRange
}

func (r *rangeRecorder) ProcessChunkData(chunk *ChunkMeta, offset uint64, size uint64, isGap bool) error {
	id := uint64(0)
	if chunk != nil {
		id = chunk.Id
	}
	r.ranges = append(r.ranges, recordedRange{chunkID: id, offset: offset, size: size, isGap: isGap})
	return nil
}

// TestChunkManagerPicksHighestVersion 同一区间有多个版本时，读高版本。
func TestChunkManagerPicksHighestVersion(t *testing.T) {
	cm := NewChunkManager(1)
	require.NoError(t, cm.AddChunk(&ChunkMeta{Id: 1, Offset: 0, Size: 1024, Version: 1, Status: VALID}))
	require.NoError(t, cm.AddChunk(&ChunkMeta{Id: 2, Offset: 0, Size: 1024, Version: 5, Status: VALID}))

	rec := &rangeRecorder{}
	require.NoError(t, cm.ProcessRange(0, 1024, rec))

	require.Len(t, rec.ranges, 1)
	assert.Equal(t, uint64(2), rec.ranges[0].chunkID)
	assert.Equal(t, uint64(0), rec.ranges[0].offset)
	assert.Equal(t, uint64(1024), rec.ranges[0].size)
	assert.False(t, rec.ranges[0].isGap)
}

// TestChunkManagerSplitsOverlap 部分重叠时，新版本只覆盖它自己的区间。
func TestChunkManagerSplitsOverlap(t *testing.T) {
	cm := NewChunkManager(1)
	// 旧版本覆盖 [0,100)，新版本覆盖 [50,120)
	require.NoError(t, cm.AddChunk(&ChunkMeta{Id: 10, Offset: 0, Size: 100, Version: 1, Status: VALID}))
	require.NoError(t, cm.AddChunk(&ChunkMeta{Id: 20, Offset: 50, Size: 70, Version: 2, Status: VALID}))

	rec := &rangeRecorder{}
	require.NoError(t, cm.ProcessRange(0, 120, rec))

	require.Len(t, rec.ranges, 2)
	assert.Equal(t, recordedRange{chunkID: 10, offset: 0, size: 50}, rec.ranges[0])
	assert.Equal(t, recordedRange{chunkID: 20, offset: 50, size: 70}, rec.ranges[1])
}

// TestChunkManagerFillsGaps 空洞按零填充处理。
func TestChunkManagerFillsGaps(t *testing.T) {
	cm := NewChunkManager(1)
	require.NoError(t, cm.AddChunk(&ChunkMeta{Id: 1, Offset: 0, Size: 10, Version: 1, Status: VALID}))
	require.NoError(t, cm.AddChunk(&ChunkMeta{Id: 2, Offset: 20, Size: 10, Version: 1, Status: VALID}))

	rec := &rangeRecorder{}
	require.NoError(t, cm.ProcessRange(0, 30, rec))

	require.Len(t, rec.ranges, 3)
	assert.Equal(t, recordedRange{chunkID: 1, offset: 0, size: 10}, rec.ranges[0])
	assert.Equal(t, recordedRange{chunkID: 0, offset: 10, size: 10, isGap: true}, rec.ranges[1])
	assert.Equal(t, recordedRange{chunkID: 2, offset: 20, size: 10}, rec.ranges[2])
}

// TestChunkManagerReplacesOlderSameChunk 同一个 Chunk 的更高版本替换旧版本。
func TestChunkManagerReplacesOlderSameChunk(t *testing.T) {
	cm := NewChunkManager(1)
	require.NoError(t, cm.AddChunk(&ChunkMeta{Id: 7, Offset: 0, Size: 100, Version: 1, Status: VALID}))
	require.NoError(t, cm.AddChunk(&ChunkMeta{Id: 7, Offset: 0, Size: 100, Version: 3, Status: VALID}))

	rec := &rangeRecorder{}
	require.NoError(t, cm.ProcessRange(0, 100, rec))

	require.Len(t, rec.ranges, 1)
	assert.Equal(t, uint64(7), rec.ranges[0].chunkID)
	assert.Equal(t, uint64(3), cm.chunks[0].Version)
}

// TestChunkManagerIgnoresDeletedChunks 状态非 VALID/SYNCING 的 Chunk 不参与读取。
func TestChunkManagerIgnoresDeletedChunks(t *testing.T) {
	cm := NewChunkManager(1)
	require.NoError(t, cm.AddChunk(&ChunkMeta{Id: 1, Offset: 0, Size: 100, Version: 1, Status: DELETED}))

	rec := &rangeRecorder{}
	require.NoError(t, cm.ProcessRange(0, 100, rec))

	require.Len(t, rec.ranges, 1)
	assert.True(t, rec.ranges[0].isGap)
	assert.Equal(t, uint64(0), cm.GetFileSize())
}
