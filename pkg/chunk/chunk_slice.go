package chunk

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JustACP/criceta/pkg/common/idgen"
	"github.com/JustACP/criceta/pkg/common/utils"
	"github.com/JustACP/criceta/pkg/config"
	"lukechampine.com/blake3"
)

const (
	UpdateSteps  uint64 = 4 * 1024 * 1024  // 4 MB
	MaxChunkSize uint64 = 64 * 1024 * 1024 // 64MB
)

// SliceBasedChunk 基于Slice的Chunk实现
type SliceBasedChunk struct {
	Head         *ChunkHead
	meta         *os.File
	SliceManager *SliceManager

	mu sync.RWMutex
}

var ChunkMetaPath string

func initChunkMetaPath() {
	conf, err := config.GetRawConfig[config.StorageNodeConfig]()

	if err != nil {
		panic("current instance config is not storage server")
	}

	SlicePath = filepath.Join(conf.ChunkStorage.FilePath, "chunk")
	ChunkMetaPath = filepath.Join(conf.ChunkStorage.FilePath, "meta")
}

func InitChunkSliceMeta() {
	initChunkMetaPath()
}

// NewSliceBasedChunk 创建一个新的基于Slice的Chunk
// id: the new chunk id
// fileId: the file id of the chunk
// offset: the offset of the chunk in the file
func NewSliceBasedChunk(id, fileId, offset uint64, chOptions ...CHOption) (*SliceBasedChunk, error) {
	// 确保目录存在
	if err := os.MkdirAll(ChunkMetaPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// 创建ChunkHead
	ch := &ChunkHead{
		Id:     id,
		FileId: fileId,
		Offset: offset,
	}
	for _, op := range chOptions {
		op.apply(ch)
	}

	// check or set default val
	if ch.Id == 0 {
		ch.Id = idgen.NextID()
		// return nil, fmt.Errorf("chunk id has not been set")
	}
	if ch.FileId == 0 {
		return nil, fmt.Errorf("chunk file id has not been set")
	}

	if ch.CreateAt == 0 {
		ch.CreateAt = time.Now().UnixNano()
	}

	if ch.ModifyAt == 0 {
		ch.ModifyAt = time.Now().UnixNano()
	}
	if ch.Status == 0 {
		ch.Status = VALID
	}
	if ch.Version == 0 {
		ch.Version = 1
	}

	sliceManager := NewSliceManager(ch.Id, MaxChunkSize)

	chunkFilePath := filepath.Join(ChunkMetaPath, fmt.Sprintf("chunk_%d_%d.meta", ch.FileId, ch.Id))
	chunkFile, err := os.OpenFile(
		chunkFilePath,
		os.O_RDWR|os.O_CREATE|os.O_TRUNC,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create chunk meta file: %w", err)
	}

	headBytes, err := ch.ToBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize chunk head: %w", err)
	}
	if _, err := chunkFile.Write(headBytes); err != nil {
		return nil, fmt.Errorf("failed to write chunk head: %w", err)
	}

	return &SliceBasedChunk{
		Head:         ch,
		SliceManager: sliceManager,
		meta:         chunkFile,
	}, nil
}

// OpenChunkByPath 通过路径打开所有已存在的基于Slice的Chunk
func OpenChunkByPath(path string) (*SliceBasedChunk, error) {
	metaFiles, err := filepath.Glob(path)
	if len(metaFiles) <= 0 || err != nil || len(metaFiles) > 1 {
		return nil, fmt.Errorf("open chunk meta info error")
	}

	pathSplit := strings.Split(path, "_")
	if len(pathSplit) != 2 {
		return nil, fmt.Errorf("chunk path is not valid")
	}
	chunkId, _ := strconv.ParseUint(pathSplit[1], 10, 64)
	chunkFilePath := metaFiles[0]
	chunkFile, err := os.OpenFile(
		chunkFilePath,
		os.O_RDWR,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk meta file: %w", err)
	}
	defer chunkFile.Close()

	// 读取ChunkHead
	headBytes := make([]byte, ChunkHeadSize)
	if _, err := chunkFile.Read(headBytes); err != nil {
		return nil, fmt.Errorf("failed to read chunk head: %w", err)
	}

	ch := &ChunkHead{}
	if err := ch.FromBinary(headBytes); err != nil {
		return nil, fmt.Errorf("failed to parse chunk head: %w", err)
	}

	// 创建SliceManager
	sliceManager := NewSliceManager(ch.Id, MaxChunkSize)

	// 读取所有Slice
	files, _ := filepath.Glob(filepath.Join(SlicePath, fmt.Sprintf("slice_%d_*.dat", chunkId)))
	for _, currSlicePath := range files {
		currSlice, err := OpenSlice(currSlicePath)
		if err != nil && currSlice != nil {
			sliceManager.AppendSlice(currSlice)
		}
	}

	return &SliceBasedChunk{
		Head:         ch,
		SliceManager: sliceManager,
	}, nil
}

// OpenChunk 同OpenSliceBasedChunk
func OpenChunk(chunkId uint64) (*SliceBasedChunk, error) {
	return OpenSliceBasedChunk(chunkId)
}

// OpenSliceBasedChunk 打开一个已存在的基于Slice的Chunk
func OpenSliceBasedChunk(chunkId uint64) (*SliceBasedChunk, error) {
	return OpenChunkByPath(filepath.Join(ChunkMetaPath, fmt.Sprintf("chunk_*_%d.meta", chunkId)))
}

func (sc *SliceBasedChunk) flushHead() error {

	if headBytes, err := sc.Head.ToBinary(); err == nil {
		_, err := sc.meta.WriteAt(headBytes, 0)
		if err != nil {
			return fmt.Errorf("chunk : %v flush head meta info error, err: %v", sc.Head.Id, err.Error())
		}
	}

	return nil
}

func (sc *SliceBasedChunk) Write(p []byte) (n int, err error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.write(p)
}

// Write 写入数据到Chunk
func (sc *SliceBasedChunk) write(p []byte) (n int, err error) {

	n, err = sc.writeAt(p, int64(sc.Head.WriteIdx))
	if n < 0 && err != nil {
		return 0, err
	}

	sc.Head.WriteIdx += uint64(n)
	return n, err
}

func (sc *SliceBasedChunk) WriteAt(p []byte, off int64) (n int, err error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.writeAt(p, off)
}

func (sc *SliceBasedChunk) writeAt(p []byte, off int64) (n int, err error) {
	oldStatus := sc.Head.Status
	sc.Head.Status = WRITING

	// 判断有没有大于MaxChunkSize
	if uint64(len(p))+uint64(off) > MaxChunkSize {
		sc.Head.Status = oldStatus
		return -1, fmt.Errorf("write data out of range")
	}

	_, err = sc.SliceManager.WriteAt(p, off)
	if err != nil {
		sc.Head.Status = oldStatus
		return -1, fmt.Errorf("write chunk: %v data error, err: %v", sc.Head.Id, err.Error())
	}

	err = sc.flushHead()
	if err != nil {
		sc.Head.Status = oldStatus
		return -1, fmt.Errorf("write chunk: %v data error, err: %v", sc.Head.Id, err.Error())
	}

	step, newCRC := make([]byte, UpdateSteps), uint32(0)
	hasher := blake3.New(32, nil)
	totalSize := uint64(0)
	var readErr error

	for readOff := int64(0); readErr == nil || !strings.EqualFold(readErr.Error(), io.EOF.Error()); readOff += int64(UpdateSteps) {
		var readSize int
		readSize, readErr = sc.SliceManager.ReadAt(step, readOff)
		if readErr != nil && !strings.EqualFold(readErr.Error(), io.EOF.Error()) {
			sc.Head.Status = ERROR
			return len(p), fmt.Errorf("chunk :%v read slice manger error, err: %v", sc.Head.Id, err.Error())
		}
		totalSize += uint64(readSize)
		newCRC = utils.UpdateCRC32(newCRC, step[:readSize])
		hasher.Write(step[:readSize])
	}

	sc.Head.CRC = newCRC
	sc.Head.Size = totalSize
	sc.Head.Version = sc.SliceManager.HighestVersion

	sc.Head.Hash = utils.Ptr(hex.EncodeToString(hasher.Sum(nil)))
	sc.Head.Range.Offset, sc.Head.Range.Size = 0, totalSize
	sc.Head.ModifyAt = time.Now().UnixNano()
	sc.Head.Status = VALID

	if err = sc.flushHead(); err != nil {
		sc.Head.Status = ERROR
		return len(p), err
	}

	return len(p), nil

}

func (sc *SliceBasedChunk) ReadAt(p []byte, off int64) (n int, err error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.readAt(p, off)
}

func (sc *SliceBasedChunk) readAt(p []byte, off int64) (n int, err error) {
	return sc.SliceManager.readAt(p, off)
}

func (sc *SliceBasedChunk) Read(p []byte) (n int, err error) {

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return sc.read(p)
}

// Read 从Chunk读取数据
func (sc *SliceBasedChunk) read(p []byte) (n int, err error) {

	n, err = sc.readAt(p, int64(sc.Head.ReadIdx))
	if n < 0 && err != nil {
		return 0, err
	}

	sc.Head.ReadIdx += uint64(n)
	return n, err
}

// Close 关闭Chunk
func (sc *SliceBasedChunk) Close() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 关闭所有slice
	err := sc.SliceManager.close()
	if err != nil {
		return fmt.Errorf("Chunk %v close error when close slice manager, err: %v", sc.Head.Id, err)
	}

	err = sc.flushHead()
	if err != nil {
		return fmt.Errorf("Chunk %v close error when update chunk head, err: %v", sc.Head.Id, err)
	}

	err = sc.meta.Close()
	if err != nil {
		return fmt.Errorf("Chunk %v close error when close chunk head meta file, err: %v", sc.Head.Id, err)
	}

	return nil
}

// Compact 压缩Chunk，合并多个Slice为一个
func (sc *SliceBasedChunk) Compact() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 合并Slice
	err := sc.SliceManager.MergeAllSlices()
	if err != nil {
		return fmt.Errorf("failed to merge slices: %w", err)
	}

	// 更新ChunkHead
	sc.Head.Version = sc.SliceManager.HighestVersion
	sc.Head.ModifyAt = time.Now().UnixNano()

	// 保存ChunkHead
	return sc.flushHead()
}

// UpdateCRC 更新CRC
func (sc *SliceBasedChunk) fullUpdateCRC() error {

	// 重置CRC
	sc.Head.CRC = 0

	step := make([]byte, UpdateSteps)
	var readErr error
	for off := int64(0); readErr == nil || !strings.EqualFold(readErr.Error(), io.EOF.Error()); off += int64(UpdateSteps) {
		var n int
		n, readErr = sc.SliceManager.ReadAt(step, off)
		if readErr != nil && !strings.EqualFold(readErr.Error(), io.EOF.Error()) {
			return fmt.Errorf("chunk :%v read slice manger error, err: %v", sc.Head.Id, readErr.Error())
		}
		sc.Head.CRC = utils.UpdateCRC32(sc.Head.CRC, step[:n])
	}

	// 保存ChunkHead
	return sc.flushHead()
}

// Validate 验证Chunk数据的完整性
func (sc *SliceBasedChunk) Validate() error {
	// 验证ChunkHead
	if err := sc.Head.Validate(); err != nil {
		return err
	}

	// 验证CRC
	oldCRC := sc.Head.CRC
	if err := sc.fullUpdateCRC(); err != nil {
		return fmt.Errorf("failed to update CRC: %w", err)
	}

	if oldCRC != sc.Head.CRC {
		return fmt.Errorf("CRC mismatch")
	}

	return nil
}

// CalculateCRC 计算 chunk 的 CRC 值
func (sc *SliceBasedChunk) CalculateCRC() (string, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	// 重置 CRC
	oldCRC := sc.Head.CRC
	sc.Head.CRC = 0

	step := make([]byte, UpdateSteps)
	var readErr error
	for off := int64(0); readErr == nil || !strings.EqualFold(readErr.Error(), io.EOF.Error()); off += int64(UpdateSteps) {
		var n int
		n, readErr = sc.SliceManager.ReadAt(step, off)
		if readErr != nil && !strings.EqualFold(readErr.Error(), io.EOF.Error()) {
			sc.Head.CRC = oldCRC
			return "", fmt.Errorf("chunk :%v read slice manger error, err: %v", sc.Head.Id, readErr.Error())
		}
		sc.Head.CRC = utils.UpdateCRC32(sc.Head.CRC, step[:n])
	}

	// 将 CRC 转换为字符串
	crcStr := fmt.Sprintf("%08x", sc.Head.CRC)

	// 恢复原始 CRC
	sc.Head.CRC = oldCRC

	return crcStr, nil
}

// GetSliceCount 获取当前 Chunk 的 Slice 数量
func (sc *SliceBasedChunk) GetSliceCount() int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return len(sc.SliceManager.Slices)
}
