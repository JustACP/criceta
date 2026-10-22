package chunk

import (
	"fmt"
	"sort"
	"sync"
)

// ChunkMeta 描述一个 Chunk 在文件中的位置、版本与所在节点。
type ChunkMeta struct {
	Id       uint64
	FileId   uint64
	Version  uint64
	Offset   uint64
	Size     uint64
	NodeId   uint64
	Replicas []uint64
	Status   ChunkStatus
	CreateAt int64
	UpdateAt int64
	Crc      string
}

// ChunkProcessor 按区间消费文件数据。
// chunk 为 nil 且 isGap 为 true 时表示该区间没有数据，需要补零。
type ChunkProcessor interface {
	ProcessChunkData(chunk *ChunkMeta, offset uint64, size uint64, isGap bool) error
}

// segment 是经过版本仲裁后的连续区间：该区间内的数据来自同一个 Chunk。
type segment struct {
	start uint64
	end   uint64
	chunk *ChunkMeta
}

// ChunkManager 把一组可能重叠、多版本的 Chunk 整理成不重叠的区间视图。
//
// 版本仲裁规则：同一段数据有多个版本时，版本号大的覆盖版本号小的；
// 因此重建区间时按版本从高到低依次“认领”自己覆盖的范围。
type ChunkManager struct {
	fileId   uint64
	chunks   []*ChunkMeta
	segments []*segment
	size     uint64
	mu       sync.RWMutex
}

func NewChunkManager(fileId uint64) *ChunkManager {
	return &ChunkManager{
		fileId: fileId,
		chunks: make([]*ChunkMeta, 0),
	}
}

// AddChunk 加入一个 Chunk。同一个 Chunk ID 只保留版本更高的那份。
func (cm *ChunkManager) AddChunk(chunk *ChunkMeta) error {
	if chunk == nil {
		return fmt.Errorf("chunk is nil")
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	for i, existing := range cm.chunks {
		if existing.Id == chunk.Id {
			if chunk.Version <= existing.Version {
				return nil
			}
			cm.chunks[i] = chunk
			cm.rebuildLocked()
			return nil
		}
	}

	cm.chunks = append(cm.chunks, chunk)
	cm.rebuildLocked()
	return nil
}

// rebuildLocked 重新计算区间视图与文件大小（调用方需持有写锁）。
func (cm *ChunkManager) rebuildLocked() {
	active := make([]*ChunkMeta, 0, len(cm.chunks))
	for _, c := range cm.chunks {
		if c.Status == VALID || c.Status == SYNCING {
			active = append(active, c)
		}
	}

	// 新版本优先认领区间
	sort.Slice(active, func(i, j int) bool {
		if active[i].Version != active[j].Version {
			return active[i].Version > active[j].Version
		}
		return active[i].Offset < active[j].Offset
	})

	covered := make([]segment, 0, len(active))
	segments := make([]*segment, 0, len(active))

	for _, c := range active {
		start, end := c.Offset, c.Offset+c.Size
		if end <= start {
			continue
		}

		cur := start
		for _, cov := range covered {
			if cov.end <= cur {
				continue
			}
			if cov.start >= end {
				break
			}
			if cov.start > cur {
				segments = append(segments, &segment{start: cur, end: min(cov.start, end), chunk: c})
			}
			if cov.end > cur {
				cur = cov.end
			}
			if cur >= end {
				break
			}
		}
		if cur < end {
			segments = append(segments, &segment{start: cur, end: end, chunk: c})
		}

		covered = append(covered, segment{start: start, end: end})
		sort.Slice(covered, func(i, j int) bool { return covered[i].start < covered[j].start })

		// 合并已认领区间
		merged := covered[:0]
		for _, cov := range covered {
			if n := len(merged); n > 0 && cov.start <= merged[n-1].end {
				if cov.end > merged[n-1].end {
					merged[n-1].end = cov.end
				}
				continue
			}
			merged = append(merged, cov)
		}
		covered = merged
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i].start < segments[j].start })
	cm.segments = segments

	var maxEnd uint64
	for _, c := range cm.chunks {
		if c.Status != VALID {
			continue
		}
		if end := c.Offset + c.Size; end > maxEnd {
			maxEnd = end
		}
	}
	cm.size = maxEnd
}

// ProcessRange 遍历 [startOffset, startOffset+size) 区间，
// 有数据的部分交给 processor 处理，空洞以 isGap=true 调用。
func (cm *ChunkManager) ProcessRange(startOffset, size uint64, processor ChunkProcessor) error {
	if size == 0 {
		return nil
	}

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	end := startOffset + size
	cur := startOffset

	for _, seg := range cm.segments {
		if seg.end <= cur {
			continue
		}
		if seg.start >= end {
			break
		}

		if seg.start > cur {
			gapEnd := min(seg.start, end)
			if err := processor.ProcessChunkData(nil, cur, gapEnd-cur, true); err != nil {
				return fmt.Errorf("process gap at %d: %w", cur, err)
			}
			cur = gapEnd
		}

		segStart := max(seg.start, cur)
		segEnd := min(seg.end, end)
		if segEnd > segStart {
			if err := processor.ProcessChunkData(seg.chunk, segStart, segEnd-segStart, false); err != nil {
				return fmt.Errorf("process chunk %d at %d: %w", seg.chunk.Id, segStart, err)
			}
			cur = segEnd
		}

		if cur >= end {
			break
		}
	}

	if cur < end {
		if err := processor.ProcessChunkData(nil, cur, end-cur, true); err != nil {
			return fmt.Errorf("process trailing gap at %d: %w", cur, err)
		}
	}

	return nil
}

// GetFileSize 返回所有有效 Chunk 覆盖到的最大偏移。
func (cm *ChunkManager) GetFileSize() uint64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return cm.size
}
