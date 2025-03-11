package chunk

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/JustACP/criceta/pkg/common/utils"
	"github.com/JustACP/criceta/pkg/config"
)

// SliceStatus 表示Slice的当前状态
type SliceStatus uint16

const (
	SLICE_VALID SliceStatus = iota + 1
	SLICE_DELETED
	SLICE_READING
	SLICE_WRITING
	SLICE_CORRUPTED
	SLICE_REPAIRING
)

var sliceStatusNames = []string{"VALID", "DELETED", "READING", "WRITING", "CORRUPTED", "REPAIRING"}

var sliceTypeNames = []string{"NORMAL", "INDEX", "META"}

var SliceMagic = []byte("SLCE")

// SliceRange 表示Slice中的有效数据范围
type SliceRange struct {
	Offset uint64 `json:"offset"` // 数据起始位置
	Size   uint64 `json:"size"`   // 数据大小
}

func (sr *SliceRange) Left() uint64 {
	return sr.Offset
}

func (sr *SliceRange) Right() uint64 {
	return sr.Offset + sr.Size
}

// SliceHeader 包含Slice的元数据信息
type SliceHeader struct {
	Id       uint64      `json:"slice_id"`    // SliceID
	ChunkId  uint64      `json:"chunk_id"`    // 所属Chunk ID
	Version  uint64      `json:"version"`     // Slice版本号
	Size     uint64      `json:"size"`        // Slice数据总大小
	Offset   uint64      `json:"offset"`      // Slice位于Chunk中的offset
	Range    SliceRange  `json:"slice_range"` // 有效数据范围
	Status   SliceStatus `json:"status"`      // Slice状态
	CRC      uint32      `json:"crc"`         // Slice CRC
	CreateAt int64       `json:"create_at"`   // 创建时间
	ModifyAt int64       `json:"modify_at"`   // 最后修改时间
}

// parseSliceHeader 从字节数组解析SliceHeader
func (header *SliceHeader) parseSliceHeader(data []byte) error {

	if len(data) < SliceHeaderSize {
		return fmt.Errorf("data too short for slice header")
	}

	// 解析各字段
	start, end := 0, 4
	for idx, val := range data[start:end] {
		if SliceMagic[idx] != val {
			return fmt.Errorf("parase slice header error, err: magic number is invalid")
		}
	}

	start, end = end, end+8
	header.Id = binary.BigEndian.Uint64(data[start:end])

	start, end = end, end+8
	header.ChunkId = binary.BigEndian.Uint64(data[start:end])

	start, end = end, end+8
	header.Version = binary.BigEndian.Uint64(data[start:end])

	start, end = end, end+8
	header.Size = binary.BigEndian.Uint64(data[start:end])

	start, end = end, end+8
	header.Offset = binary.BigEndian.Uint64(data[start:end])

	start, end = end, end+8
	header.Range.Offset = binary.BigEndian.Uint64(data[start:end])

	start, end = end, end+8
	header.Range.Size = binary.BigEndian.Uint64(data[start:end])

	start, end = end, end+2
	header.Status = SliceStatus(binary.BigEndian.Uint16(data[start:end]))

	start, end = end, end+4
	header.CRC = binary.BigEndian.Uint32(data[start:end])

	start, end = end, end+8
	header.CreateAt = int64(binary.BigEndian.Uint64(data[start:end]))

	start, end = end, end+8
	header.ModifyAt = int64(binary.BigEndian.Uint64(data[start:end]))

	return nil
}

// serializeSliceHeader 将SliceHeader序列化为字节数组
func (header *SliceHeader) serializeSliceHeader() []byte {
	data := make([]byte, SliceHeaderSize)

	// 写入各字段
	start, end := 0, 4
	copy(data[start:end], SliceMagic)

	start, end = end, end+8
	binary.BigEndian.PutUint64(data[start:end], header.Id)

	start, end = end, end+8
	binary.BigEndian.PutUint64(data[start:end], header.ChunkId)

	start, end = end, end+8
	binary.BigEndian.PutUint64(data[start:end], header.Version)

	start, end = end, end+8
	binary.BigEndian.PutUint64(data[start:end], header.Size)

	start, end = end, end+8
	binary.BigEndian.PutUint64(data[start:end], header.Offset)

	start, end = end, end+8
	binary.BigEndian.PutUint64(data[start:end], header.Range.Offset)

	start, end = end, end+8
	binary.BigEndian.PutUint64(data[start:end], header.Range.Size)

	start, end = end, end+2
	binary.BigEndian.PutUint16(data[start:end], uint16(header.Status))

	start, end = end, end+4
	binary.BigEndian.PutUint16(data[start:end], uint16(header.CRC))

	start, end = end, end+8
	binary.BigEndian.PutUint64(data[start:end], uint64(header.CreateAt))

	start, end = end, end+8
	binary.BigEndian.PutUint64(data[start:end], uint64(header.ModifyAt))

	return data
}

const SliceHeaderSize = 128

const MaxSliceSize = MaxChunkSize

var SlicePath string

func initSlicePath() {
	instanceConf := config.GetInstanceConfig()
	conf, ok := interface{}(instanceConf.ServerConfig).(config.StorageNodeConfig)
	if !ok {
		panic("current instance config is not storage server")
	}

	SlicePath = filepath.Join(conf.ChunkStorage.FilePath, "slice")

}

// NewSlice 创建一个新的Slice
func NewSlice(id, chunkId, version, offset uint64) (*Slice, error) {

	if err := os.MkdirAll(SlicePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	slicePath := filepath.Join(SlicePath, fmt.Sprintf("slice_%d_%d.dat", chunkId, id))

	file, err := os.OpenFile(slicePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create slice file: %w", err)
	}

	header := &SliceHeader{
		Id:       id,
		ChunkId:  chunkId,
		Version:  version,
		Offset:   offset,
		CreateAt: time.Now().UnixNano(),
		ModifyAt: time.Now().UnixNano(),
		Status:   SLICE_VALID,
		Range:    SliceRange{Offset: 0, Size: 0}, // 初始时没有数据
	}

	slice := &Slice{
		Header:   header,
		Path:     slicePath,
		File:     file,
		DataSize: 0,
	}

	if err := slice.flushHeader(); err != nil {
		file.Close()
		return nil, err
	}

	return slice, nil
}

func OpenSlice(slicePath string) (*Slice, error) {

	file, err := os.OpenFile(slicePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open slice file: %w", err)
	}

	header := &SliceHeader{}
	headerBytes := make([]byte, SliceHeaderSize)

	_, err = file.ReadAt(headerBytes, 0)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read slice header: %w", err)
	}

	if err := header.parseSliceHeader(headerBytes); err != nil {
		file.Close()
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	slice := &Slice{
		Header:   header,
		Path:     slicePath,
		File:     file,
		DataSize: uint64(fileInfo.Size()) - SliceHeaderSize,
	}

	return slice, nil
}

type Slice struct {
	Header   *SliceHeader
	Path     string
	File     *os.File
	mu       sync.RWMutex
	DataSize uint64
}

func (s *Slice) Remove() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.remove()

}

func (s *Slice) remove() error {
	s.Header.Status = SLICE_DELETED
	err := s.flushHeader()
	if err != nil {
		s.Header.Status = SLICE_VALID
		return fmt.Errorf("remove slice file: %v error, change status err: %s", s.Header.Id, err.Error())
	}

	defer s.close()
	fileName := s.File.Name()

	if err := s.close(); err != nil {
		return fmt.Errorf("remove slice file: %v error, file close err: %s", s.Header.Id, err.Error())
	}

	err = os.Remove(fileName)
	if err != nil {
		return fmt.Errorf("remove slice file: %v error, err: %s", s.Header.Id, err.Error())
	}

	return nil

}

func (s *Slice) Close() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.close()
}

func (s *Slice) close() error {
	return s.File.Close()
}

func (s *Slice) flushHeader() error {
	data := s.Header.serializeSliceHeader()
	_, err := s.File.WriteAt(data, 0)
	return err
}

func (s *Slice) WriteData(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 检查是否超出大小限制
	if s.DataSize+uint64(len(data)) > MaxSliceSize {
		return fmt.Errorf("slice size would exceed maximum limit of %d bytes", MaxSliceSize)
	}

	// 设置状态为写入中
	oldStatus := s.Header.Status
	s.Header.Status = SLICE_WRITING

	// 更新slice CRC
	newCRC := utils.UpdateCRC32(s.Header.CRC, data)

	// 写入数据
	offset := int64(SliceHeaderSize) + int64(s.DataSize)
	n, err := s.File.WriteAt(data, offset)
	if err != nil {
		// 恢复状态
		s.Header.Status = oldStatus
		return fmt.Errorf("failed to write data: %w", err)
	}

	// 更新Header
	s.Header.CRC = newCRC
	s.DataSize += uint64(n)
	s.Header.Size += uint64(n)
	s.Header.Range.Size += uint64(n)
	s.Header.Version++ // 增加版本号
	s.Header.ModifyAt = time.Now().UnixNano()
	s.Header.Status = SLICE_VALID

	return s.flushHeader()
}

func (s *Slice) ReadData(offset uint64, size uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 设置状态为写入中
	oldStatus := s.Header.Status
	s.Header.Status = SLICE_READING

	// 检查范围
	if offset+size > s.DataSize {
		s.Header.Status = oldStatus
		return nil, fmt.Errorf("read beyond slice data size")
	}

	// 读取数据
	data := make([]byte, size)
	fileOffset := int64(SliceHeaderSize) + int64(offset)
	_, err := s.File.ReadAt(data, fileOffset)
	if err != nil {
		// 恢复状态
		s.Header.Status = oldStatus
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	s.Header.Status = oldStatus
	s.flushHeader()
	return data, nil
}
