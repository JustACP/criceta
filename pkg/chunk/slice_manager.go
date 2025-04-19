package chunk

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/JustACP/criceta/pkg/common/idgen"
	"github.com/JustACP/criceta/pkg/common/utils"
)

type MappingSlice struct {
	Range SliceRange
	Slice *Slice
}

// SliceManager 管理Chunk的所有Slice
type SliceManager struct {
	ChunkId        uint64
	Slices         []*Slice
	HighestVersion uint64
	MaxSize        uint64            // Chunk最大大小限制
	mapping        [][]*MappingSlice // Slice Content Mapping
	mappingVer     uint64            // mapping snapshot version

	readIdx  uint64
	writeIdx uint64
	mu       sync.RWMutex
}

// Reader
func (sm *SliceManager) Read(p []byte) (n int, err error) {

	sm.mu.RLock()
	defer sm.mu.RUnlock()

	n, err = sm.readAt(p, int64(sm.readIdx))
	sm.readIdx += uint64(n)

	return n, err
}

func (sm *SliceManager) ReadAt(p []byte, off int64) (n int, err error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.readAt(p, off)
}

// readAt
func (sm *SliceManager) readAt(p []byte, off int64) (n int, err error) {

	if off < 0 || len(p) == 0 {
		return 0, io.EOF
	}

	sm.updateMapping()
	pIdx, filledSize, unfilledSize := off, 0, int64(len(p))

	for sgIdx, sliceGroup := range sm.mapping {
		for slcIdx, mappingSlice := range sliceGroup {
			var currSliceRight uint64 = mappingSlice.Range.Right()
			if slcIdx+1 < len(sliceGroup) {
				currSliceRight = utils.Less(sliceGroup[slcIdx+1].Range.Left(), currSliceRight)
			}

			if pIdx >= int64(mappingSlice.Range.Left()) && pIdx < int64(currSliceRight) {

				readSliceOffset := uint64(pIdx - int64(mappingSlice.Range.Left()))
				readSliceSize := utils.Less(pIdx+unfilledSize, int64(currSliceRight)) - utils.More(pIdx, int64(mappingSlice.Range.Left()))

				readBytes, readErr := mappingSlice.Slice.ReadData(readSliceOffset, uint64(readSliceSize))
				if readErr != nil {
					return filledSize, readErr
				}
				copy(p[filledSize:], readBytes)

				pIdx += readSliceSize
				filledSize += int(readSliceSize)
				unfilledSize -= readSliceSize

			}
		}
		// 也就是两个 Slice 之间的 Overlap
		if sgIdx+1 < len(sm.mapping) && len(sm.mapping[sgIdx+1]) > 0 {
			emptySize := utils.Less(int64(sm.mapping[sgIdx+1][0].Range.Left()), pIdx+unfilledSize) - pIdx
			copy(p[pIdx:emptySize+pIdx], bytes.Repeat([]byte{0}, int(emptySize)))

			pIdx += emptySize
			filledSize += int(emptySize)
			unfilledSize -= emptySize
		}

		// 判断读取的内容是不是塞满了
		if pIdx-off >= int64(len(p)) {
			return filledSize, nil
		}
	}
	return filledSize, io.EOF
}

// write
func (sm *SliceManager) Write(p []byte) (n int, err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	n, err = sm.writeAt(p, int64(sm.writeIdx))
	sm.writeIdx += uint64(n)

	return n, err
}

// WriteAt
func (sm *SliceManager) WriteAt(p []byte, off int64) (n int, err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.writeAt(p, off)
}

// writeAt
func (sm *SliceManager) writeAt(p []byte, off int64) (n int, err error) {

	// 使用ID生成器生成新的Slice ID
	sliceId := idgen.NextID()
	version := sm.HighestVersion + 1

	// 创建新Slice
	newSlice, err := NewSlice(sliceId, sm.ChunkId, version, uint64(off))
	if err != nil {
		return 0, fmt.Errorf("SliceManger WriteAt err, err: %v", err.Error())
	}

	err = newSlice.WriteData(p)
	if err != nil {
		newSlice.Remove()
		return 0, fmt.Errorf("SliceManger WriteAt err, err: %v", err.Error())
	}

	sm.appendSlice(newSlice)

	sm.HighestVersion++
	return len(p), nil
}

// NewSliceManager 创建一个新的SliceManager
func NewSliceManager(chunkId uint64, maxSize uint64) *SliceManager {
	return &SliceManager{
		ChunkId:        chunkId,
		Slices:         make([]*Slice, 0),
		MaxSize:        maxSize,
		HighestVersion: 0,
	}

}

func (sm *SliceManager) DeleteSlices(slices []*Slice) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.deleteSlices(slices)
}

func (sm *SliceManager) deleteSlices(slices []*Slice) error {
	var err error
	for _, slice := range slices {
		err = errors.Join(err, slice.Remove())
	}
	return err
}

func (sm *SliceManager) UpdateMapping() {

	sm.mu.RLocker().Lock()
	defer sm.mu.RLocker().Unlock()

	sm.updateMapping()

}

func (sm *SliceManager) updateMapping() {

	if sm.mappingVer != sm.HighestVersion || sm.mappingVer == 0 {
		validSlice := sm.getActiveSlices()
		sm.mapping = mergeMapping(validSlice)
		sm.mappingVer = sm.HighestVersion
	}

}

func mergeMapping(activeSlices []*Slice) [][]*MappingSlice {

	activeSlices = sortSlice(activeSlices)
	mapping := make([][]*MappingSlice, 0)

	var lastSlice *MappingSlice
	for _, currSlice := range activeSlices {
		if len(mapping) != 0 {
			lastSlice = mapping[len(mapping)-1][len(mapping[len(mapping)-1])-1]
		}

		// 没有重合区域的直接 无法Merge
		if len(mapping) == 0 || lastSlice.Range.Offset+lastSlice.Range.Size < currSlice.Header.Offset {
			newWindows := make([]*MappingSlice, 0, 1)
			newWindows = append(newWindows, &MappingSlice{
				Range: SliceRange{
					currSlice.Header.Offset,
					currSlice.Header.Size,
				},
				Slice: currSlice,
			})

			mapping = append(mapping, newWindows)

			// 有重合区域 且 不被包含
		} else if lastSlice.Range.Offset+lastSlice.Range.Size < currSlice.Header.Offset+currSlice.Header.Size {
			mapping[len(mapping)-1] = append(mapping[len(mapping)-1], &MappingSlice{
				Range: SliceRange{
					currSlice.Header.Offset,
					currSlice.Header.Size,
				},
				Slice: currSlice,
			})
		}
	}

	return mapping
}

func (sm *SliceManager) GetActiveSlices() []*Slice {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.getActiveSlices()

}

func sortSlice(slices []*Slice) []*Slice {
	sort.Slice(slices, func(i, j int) bool {
		if slices[i].Header.Offset < slices[j].Header.Offset {
			return true
		}

		if slices[i].Header.Offset == slices[j].Header.Offset &&
			slices[i].Header.Version > slices[j].Header.Version {
			return true
		}

		return false
	})

	return slices
}

// GetActiveSlices 获取当前有效的Slice列表，按照版本排序
// 并且更新最高版本
func (sm *SliceManager) getActiveSlices() []*Slice {

	// 过滤出有效的Slice并按版本排序
	activeSlices := make([]*Slice, 0, len(sm.Slices))
	for _, slice := range sm.Slices {
		if slice.Header.Status == SLICE_VALID {
			activeSlices = append(activeSlices, slice)
		}
		sm.HighestVersion = utils.More(slice.Header.Version, sm.HighestVersion)
	}

	// 方便合并Slice的区间
	activeSlices = sortSlice(activeSlices)

	return activeSlices
}

func (sm *SliceManager) AppendSlice(slice *Slice) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.appendSlice(slice)
}

func (sm *SliceManager) appendSlice(slice *Slice) error {

	slice.mu.RLocker().Lock()
	defer slice.mu.RLocker().Unlock()

	// 检测是否属于对应的chunk
	if slice.Header.ChunkId != sm.ChunkId {
		return fmt.Errorf("this slice(id: %v) not belong to chunk(id: %v)", slice.Header.Id, sm.ChunkId)
	}

	// 检查Slice是否有效
	if slice.Header.Status != SLICE_VALID {
		return fmt.Errorf("this slice(id: %v) is not valid, current status: %v", slice.Header.Id, slice.Header.Status)
	}

	// 添加到管理器
	sm.Slices = append(sm.Slices, slice)
	return nil
}

// CreateNewSlice 创建一个新的Slice
// offset slice offset in chunk
func (sm *SliceManager) CreateNewSlice(offset uint64) (*Slice, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.createNewSlice(offset)
}

func (sm *SliceManager) createNewSlice(offset uint64) (*Slice, error) {

	// 使用ID生成器生成新的Slice ID
	sliceId := idgen.NextID()
	version := uint64(1)

	// 如果已有Slice，则新版本号为最高版本+1
	if len(sm.Slices) > 0 {
		version = sm.HighestVersion + 1
	}

	// 创建新Slice
	slice, err := NewSlice(sliceId, sm.ChunkId, version, offset)
	if err != nil {
		return nil, err
	}

	// 添加到管理器
	sm.Slices = append(sm.Slices, slice)
	sm.HighestVersion++
	return slice, nil

}
func (sm *SliceManager) MergeAllSlices() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.updateMapping()

	for _, smGroup := range sm.mapping {
		if len(smGroup) <= 1 {
			continue
		}

		sliceGroup := make([]*Slice, 0, len(smGroup))
		for _, currSlice := range smGroup {
			sliceGroup = append(sliceGroup, currSlice.Slice)
		}

		newSlice, err := sm.mergeSlices(sliceGroup)
		if err != nil {
			return fmt.Errorf("Chunk: %v merge slices error, err: %s", sm.ChunkId, err.Error())
		}

		for _, currSlice := range sliceGroup {
			err := currSlice.Remove()
			if err != nil {
				return fmt.Errorf("Chunk: %v merge slices error when remove old slice, err: %s", sm.ChunkId, err.Error())
			}
		}

		sm.appendSlice(newSlice)
	}

	return nil
}

func (sm *SliceManager) MergeSlices(slices []*Slice) (*Slice, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.mergeSlices(slices)
}

func (sm *SliceManager) close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	activeSlice := sm.getActiveSlices()

	var closeErrors error
	for _, slice := range activeSlice {
		closeErrors = errors.Join(closeErrors, slice.Close())
	}

	return closeErrors
}

func (sm *SliceManager) mergeSlices(slices []*Slice) (*Slice, error) {

	if len(slices) == 0 {
		return nil, fmt.Errorf("no slices to merge")
	}

	newSliceId := idgen.NextID()

	newSlice, err := NewSlice(newSliceId, sm.ChunkId, sm.HighestVersion+1, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to create new slice: %w", err)
	}

	for idx, slice := range slices {

		// 计算当前相邻Slice重合区域的右边界
		var overlapSize uint64 = 0
		if idx+1 < len(slices) {
			overlapSize = slice.Header.Offset + slice.Header.Size - slices[idx+1].Header.Offset
		}
		realSize := slice.Header.Range.Size - overlapSize

		data, err := slice.ReadData(slice.Header.Range.Offset, realSize)
		if err != nil {
			newSlice.File.Close()
			os.Remove(newSlice.Path)
			return nil, fmt.Errorf("failed to read data from slice: %w", err)
		}

		err = newSlice.WriteData(data)
		if err != nil {
			newSlice.File.Close()
			os.Remove(newSlice.Path)
			return nil, fmt.Errorf("failed to write data to new slice: %w", err)
		}

	}

	sm.HighestVersion++

	return newSlice, nil
}
