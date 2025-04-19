package chunk

import (
	"fmt"
	"sort"
	"sync"

	"github.com/JustACP/criceta/pkg/common/utils"
)

type ChunkMeta struct {
	Id       uint64
	FileId   uint64
	Version  uint64
	Offset   uint64
	Size     uint64
	NodeId   uint64
	Status   ChunkStatus
	CreateAt int64
	UpdateAt int64
	Crc      string
}

type MappingChunk struct {
	Range ChunkRange
	Chunk *ChunkMeta
}

type ChunkManager struct {
	fileId     uint64
	chunks     []*ChunkMeta
	highestVer uint64
	mapping    [][]*MappingChunk
	mu         sync.RWMutex
}

func NewChunkManager(fileId uint64) *ChunkManager {
	return &ChunkManager{
		fileId:     fileId,
		chunks:     make([]*ChunkMeta, 0),
		highestVer: 0,
		mapping:    make([][]*MappingChunk, 0),
	}
}

type ChunkProcessor interface {
	// ProcessChunkData 处理指定范围内的Chunk数据
	// chunk: 当前处理的Chunk元数据，如果是空隙则为nil
	// offset: 当前处理的数据在整个文件中的起始偏移量
	// size: 需要处理的数据大小
	// isGap: 表示当前处理的是否是Chunk之间的空隙
	// return: 处理是否成功
	ProcessChunkData(chunk *ChunkMeta, offset uint64, size uint64, isGap bool) error
}

// 参考SliceManger对于Chunk边界管理
func (cm *ChunkManager) ProcessRange(startOffset uint64, size uint64, processor ChunkProcessor) error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	cm.updateMapping()

	if size == 0 {
		return nil
	}

	pIdx, filledSize, unfilledSize := int64(startOffset), uint64(0), int64(size)

	for groupIdx, chunkGroup := range cm.mapping {
		for slcIdx, mappingChunk := range chunkGroup {
			if unfilledSize <= 0 {
				return nil
			}

			var currChunkRight uint64 = mappingChunk.Range.Right()
			if slcIdx+1 < len(chunkGroup) {
				currChunkRight = utils.Less(chunkGroup[slcIdx+1].Range.Left(), currChunkRight)
			}

			if pIdx >= int64(mappingChunk.Range.Left()) && pIdx < int64(currChunkRight) {
				processSize := utils.Less(uint64(pIdx+unfilledSize), currChunkRight) - utils.More(uint64(pIdx), mappingChunk.Range.Left())
				if err := processor.ProcessChunkData(mappingChunk.Chunk, uint64(pIdx), uint64(processSize), false); err != nil {
					return fmt.Errorf("process chunk data failed: %v", err)
				}

				pIdx += int64(processSize)
				filledSize += uint64(processSize)
				unfilledSize -= int64(processSize)
			}
		}
		if unfilledSize <= 0 {
			return nil
		}
		if groupIdx+1 < len(cm.mapping) && len(cm.mapping[groupIdx+1]) > 0 {
			nextGroupStart := int64(cm.mapping[groupIdx+1][0].Range.Left())
			if pIdx < nextGroupStart && unfilledSize > 0 {
				gapSize := utils.Less(uint64(nextGroupStart-pIdx), uint64(unfilledSize))
				if err := processor.ProcessChunkData(nil, uint64(pIdx), gapSize, true); err != nil {
					return fmt.Errorf("process gap data failed: %v", err)
				}

				pIdx += int64(gapSize)
				filledSize += gapSize
				unfilledSize -= int64(gapSize)
			}
		}

	}

	if unfilledSize <= 0 {
		return nil
	}
	if unfilledSize > 0 {
		if err := processor.ProcessChunkData(nil, uint64(pIdx), uint64(unfilledSize), true); err != nil {
			return fmt.Errorf("process remaining gap data failed: %v", err)
		}
	}

	return nil
}

func (cm *ChunkManager) Read(offset uint64, size uint64) ([]byte, error) {
	result := make([]byte, size)
	processor := &bufferProcessor{buffer: result}

	if err := cm.ProcessRange(offset, size, processor); err != nil {
		return nil, err
	}

	return result, nil
}

// bufferProcessor 默认实现
type bufferProcessor struct {
	buffer  []byte
	written uint64
}

func (bp *bufferProcessor) ProcessChunkData(chunk *ChunkMeta, offset uint64, size uint64, isGap bool) error {
	if isGap {
		// 用0填充间隔
		for i := uint64(0); i < size; i++ {
			bp.buffer[bp.written+i] = 0
		}
	} else {
		for i := uint64(0); i < size; i++ {
			bp.buffer[bp.written+i] = 0
		}
	}
	bp.written += size
	return nil
}

func (cm *ChunkManager) AddChunk(chunk *ChunkMeta) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for _, existingChunk := range cm.chunks {
		if existingChunk.Offset == chunk.Offset && existingChunk.Size == chunk.Size {
			if existingChunk.Version >= chunk.Version {
				return fmt.Errorf("version conflict: existing version %d >= new version %d",
					existingChunk.Version, chunk.Version)
			}
		}
	}
	if chunk.Version > cm.highestVer {
		cm.highestVer = chunk.Version
	}

	cm.chunks = append(cm.chunks, chunk)
	cm.updateMapping()

	return nil
}

func (cm *ChunkManager) updateMapping() {

	activeChunks := cm.getActiveChunks()
	if len(activeChunks) == 0 {
		cm.mapping = make([][]*MappingChunk, 0)
		return
	}

	sort.Slice(activeChunks, func(i, j int) bool {
		return activeChunks[i].Offset < activeChunks[j].Offset
	})

	cm.mapping = cm.mergeChunkMapping(activeChunks)
}

func (cm *ChunkManager) getActiveChunks() []*ChunkMeta {
	activeChunks := make([]*ChunkMeta, 0)
	for _, chunk := range cm.chunks {
		if chunk.Status == VALID || chunk.Status == SYNCING {
			activeChunks = append(activeChunks, chunk)
		}
	}
	return activeChunks
}

func (cm *ChunkManager) mergeChunkMapping(chunks []*ChunkMeta) [][]*MappingChunk {
	if len(chunks) == 0 {
		return make([][]*MappingChunk, 0)
	}

	result := make([][]*MappingChunk, 0)
	currentGroup := make([]*MappingChunk, 0)

	for i := 0; i < len(chunks); i++ {
		chunk := chunks[i]
		mappingChunk := &MappingChunk{
			Range: ChunkRange{
				Offset: chunk.Offset,
				Size:   chunk.Size,
			},
			Chunk: chunk,
		}

		if len(currentGroup) == 0 {
			currentGroup = append(currentGroup, mappingChunk)
			continue
		}
		lastChunk := currentGroup[len(currentGroup)-1]
		if mappingChunk.Range.Left() <= lastChunk.Range.Right() {
			currentGroup = append(currentGroup, mappingChunk)
		} else {
			result = append(result, currentGroup)
			currentGroup = []*MappingChunk{mappingChunk}
		}
	}

	if len(currentGroup) > 0 {
		result = append(result, currentGroup)
	}

	return result
}

func (cm *ChunkManager) GetFileSize() uint64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var maxOffset uint64 = 0
	for _, chunk := range cm.chunks {
		if chunk.Status == VALID {
			chunkEnd := chunk.Offset + chunk.Size
			if chunkEnd > maxOffset {
				maxOffset = chunkEnd
			}
		}
	}
	return maxOffset
}

func (r *ChunkRange) Left() uint64 {
	return r.Offset
}

func (r *ChunkRange) Right() uint64 {
	return r.Offset + r.Size
}
