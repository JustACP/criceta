package chunk

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/JustACP/criceta/pkg/common/idgen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeleteRemovesChunkFiles 验证删除 Chunk 会同时清理元数据与全部 Slice 文件。
func TestDeleteRemovesChunkFiles(t *testing.T) {
	initTestPath()
	defer cleanTestDir()

	chunk, err := NewSliceBasedChunk(idgen.NextID(), 42, 0)
	require.NoError(t, err)

	chunkID := chunk.Head.Id
	require.NotZero(t, chunkID)

	_, err = chunk.WriteAt([]byte("payload"), 0)
	require.NoError(t, err)

	metaPath := ChunkMetaFilePath(42, chunkID)
	_, err = os.Stat(metaPath)
	require.NoError(t, err)

	sliceFiles, err := filepath.Glob(filepath.Join(SlicePath, fmt.Sprintf("slice_%d_*.dat", chunkID)))
	require.NoError(t, err)
	require.NotEmpty(t, sliceFiles)

	require.NoError(t, chunk.Delete())

	_, err = os.Stat(metaPath)
	assert.True(t, os.IsNotExist(err), "chunk meta file should be removed")

	sliceFiles, err = filepath.Glob(filepath.Join(SlicePath, fmt.Sprintf("slice_%d_*.dat", chunkID)))
	require.NoError(t, err)
	assert.Empty(t, sliceFiles, "slice files should be removed")
}
