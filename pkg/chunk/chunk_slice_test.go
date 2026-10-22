package chunk

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"path/filepath"

	"github.com/JustACP/criceta/pkg/common/idgen"
	"github.com/JustACP/criceta/pkg/common/utils"
)

func initTestPath() {
	SlicePath = "./slice_test"
	ChunkMetaPath = "./chunk_test"
}

func cleanTestDir() {
	err := os.RemoveAll(SlicePath)
	if err != nil {
		fmt.Println(err.Error())
	}
	err = os.RemoveAll(ChunkMetaPath)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestNewSliceBasedChunk(t *testing.T) {
	initTestPath()
	defer cleanTestDir()

	// 正常情况
	chunk, err := NewSliceBasedChunk(idgen.NextID(), 1, 1)
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}
	defer chunk.Close()
	if chunk.Head.Id == 0 {
		t.Errorf("Chunk ID should not be zero")
	}
	if chunk.Head.FileId != 1 {
		t.Errorf("Chunk FileId should be 1")
	}

	if chunk.Head.CreateAt == 0 {
		t.Errorf("Chunk CreateAt should not be zero")
	}
	if chunk.Head.ModifyAt == 0 {
		t.Errorf("Chunk ModifyAt should not be zero")
	}
	if chunk.Head.Status != VALID {
		t.Errorf("Chunk Status should be VALID")
	}
	if chunk.Head.Version != 1 {
		t.Errorf("Chunk Version should be 1")
	}

	// 异常情况：FileId未设置
	if _, err := NewSliceBasedChunk(idgen.NextID(), 0, 1); err == nil {
		t.Errorf("Expected error when FileId is not set")
	}

	// offset 为 0 是合法值（文件首个 Chunk 的 offset 就是 0）
	offsetZeroChunk, err := NewSliceBasedChunk(idgen.NextID(), 1, 0)
	if err != nil {
		t.Errorf("offset 0 should be valid, got error: %v", err)
	} else {
		defer offsetZeroChunk.Close()
	}

	// 同一个 chunk id 重复创建必须失败：否则 O_TRUNC 会静默丢掉已有 Chunk 数据
	chunkId := idgen.NextID()
	first, err := NewSliceBasedChunk(chunkId, 1, 0)
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}
	defer first.Close()

	if _, err := NewSliceBasedChunk(chunkId, 1, 0); err == nil {
		t.Errorf("Expected error when creating a chunk with an existing id")
	}
}

// TestOpenChunkPreservesData 验证节点重启后能从 meta 文件恢复 Chunk 及其 Slice 数据。
// 这是 Storage 节点重启不丢数据的核心不变量。
func TestOpenChunkPreservesData(t *testing.T) {
	initTestPath()
	defer cleanTestDir()

	chunkId := idgen.NextID()
	fileId := uint64(7)
	created, err := NewSliceBasedChunk(chunkId, fileId, 0)
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}

	data := []byte("criceta-persist-check")
	if _, err := created.WriteAt(data, 0); err != nil {
		t.Fatalf("Failed to write chunk: %v", err)
	}
	crcBefore := created.Head.CRC
	metaPath := ChunkMetaFilePath(fileId, chunkId)

	if err := created.Close(); err != nil {
		t.Fatalf("Failed to close chunk: %v", err)
	}

	// 模拟重启：仅通过 meta 文件路径恢复
	reopened, err := OpenChunkByPath(metaPath)
	if err != nil {
		t.Fatalf("Failed to reopen chunk: %v", err)
	}
	defer reopened.Close()

	if reopened.Head.Id != chunkId || reopened.Head.FileId != fileId {
		t.Errorf("reopened chunk identity mismatch: id=%d file=%d", reopened.Head.Id, reopened.Head.FileId)
	}

	got := make([]byte, len(data))
	if _, err := reopened.ReadAt(got, 0); err != nil && !strings.EqualFold(err.Error(), io.EOF.Error()) {
		t.Fatalf("Failed to read reopened chunk: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("data mismatch after reopen: got %q, want %q", got, data)
	}
	if reopened.Head.CRC != crcBefore {
		t.Errorf("CRC mismatch after reopen: got %d, want %d", reopened.Head.CRC, crcBefore)
	}

	// 重新打开后继续写必须沿用正确的最高版本号
	if _, err := reopened.WriteAt([]byte("-more"), int64(len(data))); err != nil {
		t.Fatalf("Failed to append after reopen: %v", err)
	}
}

func TestOpenSliceBasedChunk(t *testing.T) {
	initTestPath()
	defer cleanTestDir()

	// 创建一个Chunk用于测试
	chunk, err := NewSliceBasedChunk(idgen.NextID(), 1, 1)
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}

	defer chunk.Close()

	// 正常情况
	openedChunk, err := OpenSliceBasedChunk(chunk.Head.Id)
	if err != nil {
		t.Fatalf("Failed to open chunk: %v", err)
	}
	if openedChunk.Head.Id != chunk.Head.Id {
		t.Errorf("Opened chunk ID should match created chunk ID")
	}
	defer openedChunk.Close()

	// 异常情况：Chunk不存在
	_, err = OpenSliceBasedChunk(idgen.NextID())
	if err == nil {
		t.Errorf("Expected error when opening non-existent chunk")
	}

}

func TestWriteAndRead(t *testing.T) {
	initTestPath()
	defer cleanTestDir()

	// 创建一个Chunk用于测试
	chunk, err := NewSliceBasedChunk(idgen.NextID(), 1, 1)
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}
	defer chunk.Close()

	// 测试Write
	data := []byte("Hello, World!")
	n, err := chunk.Write(data)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	if n != len(data) {
		t.Errorf("Written bytes should match data length")
	}

	// 测试Read
	buf := make([]byte, len(data))
	n, err = chunk.Read(buf)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}
	if n != len(data) {
		t.Errorf("Read bytes should match data length")
	}
	if string(buf) != string(data) {
		t.Errorf("Read data should match written data")
	}

	// 测试WriteAt
	offset := int64(10)
	n, err = chunk.WriteAt(data, offset)
	if err != nil {
		t.Fatalf("Failed to write data at offset: %v", err)
	}
	if n != len(data) {
		t.Errorf("Written bytes should match data length")
	}

	// 测试ReadAt
	buf = make([]byte, len(data))
	n, err = chunk.ReadAt(buf, offset)
	if err != nil {
		t.Fatalf("Failed to read data at offset: %v", err)
	}
	if n != len(data) {
		t.Errorf("Read bytes should match data length")
	}
	if string(buf) != string(data) {
		t.Errorf("Read data should match written data")
	}

	// 异常情况：写入超出最大大小
	largeData := make([]byte, MaxChunkSize+1)
	_, err = chunk.Write(largeData)
	if err == nil {
		t.Errorf("Expected error when writing data exceeding max size")
	}

	// 异常情况：读取超出范围
	_, err = chunk.ReadAt(buf, int64(MaxChunkSize)+1)
	if err == nil {
		t.Errorf("Expected error when reading data out of range")
	}
}

func TestClose(t *testing.T) {
	initTestPath()
	defer cleanTestDir()
	// 创建一个Chunk用于测试
	chunk, err := NewSliceBasedChunk(idgen.NextID(), 1, 1)
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}

	// 测试Close
	err = chunk.Close()
	if err != nil {
		t.Fatalf("Failed to close chunk: %v", err)
	}
}

func TestCompact(t *testing.T) {
	initTestPath()
	defer cleanTestDir()

	// 创建一个Chunk用于测试
	chunk, err := NewSliceBasedChunk(idgen.NextID(), 1, 1)
	defer chunk.Close()
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}

	// 写入数据以创建多个Slice
	data := []byte("hello world")

	_, err = chunk.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	_, err = chunk.WriteAt(data, 5)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	_, err = chunk.WriteAt(data, 20)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// real data
	expectData := make([]byte, 32)
	_, err = chunk.Read(expectData)
	if err != nil && !strings.EqualFold(err.Error(), io.EOF.Error()) {
		t.Fatalf("Faild to read chunk content: %v", err)
	}

	// 测试Compact
	err = chunk.Compact()
	if err != nil {
		t.Fatalf("Failed to compact chunk: %v", err)
	}

	//测试数据读写
	testData := make([]byte, 32)
	_, err = chunk.ReadAt(testData, 0)
	if err != nil && !strings.EqualFold(err.Error(), io.EOF.Error()) {
		t.Fatalf("Faild to read chunk content: %v", err)
	}

	for idx, val := range expectData {
		if val != testData[idx] {
			t.Fatal("Compact data not equal to origin data")
		}
	}

	// 检查Slice数量是否减少
	sliceFiles, err := filepath.Glob(filepath.Join(SlicePath, fmt.Sprintf("slice_%d_*.dat", chunk.Head.Id)))
	if err != nil {
		t.Fatalf("Failed to list slice files: %v", err)
	}
	if len(sliceFiles) != 2 {
		t.Errorf("Expected 2 slice file after compact, got %d", len(sliceFiles))
	}
}

func TestFullUpdateCRC(t *testing.T) {

	initTestPath()
	defer cleanTestDir()

	// 创建一个Chunk用于测试
	chunk, err := NewSliceBasedChunk(idgen.NextID(), 1, 1)
	defer chunk.Close()
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}

	// 写入数据
	data := []byte("Hello, World!")
	_, err = chunk.Write(data)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// 计算预期的CRC
	expectedCRC := utils.UpdateCRC32(0, data)

	// 测试fullUpdateCRC
	err = chunk.fullUpdateCRC()
	if err != nil {
		t.Fatalf("Failed to update CRC: %v", err)
	}

	// 检查CRC是否正确
	if chunk.Head.CRC != expectedCRC {
		t.Errorf("CRC mismatch: expected %v, got %v", expectedCRC, chunk.Head.CRC)
	}
}

func TestValidate(t *testing.T) {
	initTestPath()
	defer cleanTestDir()

	// 创建一个Chunk用于测试
	chunk, err := NewSliceBasedChunk(idgen.NextID(), 1, 1)
	defer chunk.Close()
	if err != nil {
		t.Fatalf("Failed to create chunk: %v", err)
	}

	// 写入数据
	data := []byte("Hello, World!")
	_, err = chunk.Write(data)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// 测试Validate
	err = chunk.Validate()
	if err != nil {
		t.Fatalf("Failed to validate chunk: %v", err)
	}

	// 修改数据以使CRC不匹配
	chunk.Head.CRC = 0
	err = chunk.flushHead()
	if err != nil {
		t.Fatalf("Failed to flush head: %v", err)
	}

	// 测试Validate失败
	err = chunk.Validate()
	if err == nil {
		t.Errorf("Expected error when validating chunk with incorrect CRC")
	}
}
