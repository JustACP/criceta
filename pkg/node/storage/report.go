package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker/trackerservice"
	"github.com/JustACP/criceta/pkg/chunk"
	"github.com/JustACP/criceta/pkg/common/logs"
)

const (
	reportJournalName = "report.journal"
	reportBatchSize   = 200
	reportInterval    = 1 * time.Second
	reportRPCTimeout  = 15 * time.Second

	reportOpUpsert = "upsert"
	reportOpDelete = "delete"
)

// reportEvent 是 Storage 本地的一条变更记录。
// 先落盘再上报，保证“数据已写入但 Tracker 没收到”时重启还能补发。
type reportEvent struct {
	Seq     int64  `json:"seq"`
	Op      string `json:"op"`
	ChunkID uint64 `json:"chunk_id"`
	FileID  uint64 `json:"file_id"`
	Version uint64 `json:"version"`
	Size    uint64 `json:"size"`
	CRC     string `json:"crc"`
	Time    int64  `json:"timestamp"`
}

// reportJournal 是追加写的变更日志；Tracker 确认后按 seq 裁剪。
type reportJournal struct {
	path string

	mu      sync.Mutex
	seq     int64
	pending []reportEvent
}

func openReportJournal(path string) (*reportJournal, error) {
	journal := &reportJournal{path: path}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return journal, nil
		}
		return nil, fmt.Errorf("read report journal %s: %w", path, err)
	}

	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var event reportEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			logs.Warn("Skip corrupted report journal entry: %v", err)
			continue
		}

		journal.pending = append(journal.pending, event)
		if event.Seq > journal.seq {
			journal.seq = event.Seq
		}
	}

	if len(journal.pending) > 0 {
		logs.Info("Loaded %d pending report event(s) from %s", len(journal.pending), path)
	}

	return journal, nil
}

// append 追加一条事件并 fsync，返回带序号的事件。
func (j *reportJournal) append(event reportEvent) (reportEvent, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.seq++
	event.Seq = j.seq
	event.Time = time.Now().UnixNano()

	line, err := json.Marshal(event)
	if err != nil {
		return reportEvent{}, fmt.Errorf("encode report event: %w", err)
	}

	file, err := os.OpenFile(j.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return reportEvent{}, fmt.Errorf("open report journal: %w", err)
	}
	defer file.Close()

	if _, err := file.Write(append(line, '\n')); err != nil {
		return reportEvent{}, fmt.Errorf("append report journal: %w", err)
	}
	if err := file.Sync(); err != nil {
		return reportEvent{}, fmt.Errorf("sync report journal: %w", err)
	}

	j.pending = append(j.pending, event)
	return event, nil
}

// snapshot 取最多 max 条待上报事件。
func (j *reportJournal) snapshot(max int) []reportEvent {
	j.mu.Lock()
	defer j.mu.Unlock()

	if max <= 0 || max > len(j.pending) {
		max = len(j.pending)
	}

	return append([]reportEvent(nil), j.pending[:max]...)
}

// ack 删除 <= seq 的事件并原子重写日志。
func (j *reportJournal) ack(seq int64) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	remaining := j.pending[:0]
	for _, event := range j.pending {
		if event.Seq > seq {
			remaining = append(remaining, event)
		}
	}
	j.pending = remaining

	return j.rewriteLocked()
}

func (j *reportJournal) rewriteLocked() error {
	path := j.path
	tmpPath := path + ".tmp"

	file, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open report journal temp file: %w", err)
	}

	for _, event := range j.pending {
		line, err := json.Marshal(event)
		if err != nil {
			file.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("encode report event: %w", err)
		}
		if _, err := file.Write(append(line, '\n')); err != nil {
			file.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("write report journal: %w", err)
		}
	}

	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("sync report journal: %w", err)
	}
	if err := file.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close report journal: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("replace report journal: %w", err)
	}

	return nil
}

// StartReporting 启动变更上报协程。必须在拿到 tracker 客户端之后调用。
func (s *StorageNode) StartReporting(trackerCli trackerservice.Client) {
	s.reportMu.Lock()
	s.trackerCli = trackerCli
	s.reportMu.Unlock()

	s.reportOnce.Do(func() {
		go s.reportLoop()
	})
}

func (s *StorageNode) reportLoop() {
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.reportStop:
			return
		case <-ticker.C:
			s.reportPending()
		case <-s.reportCh:
			s.reportPending()
		}
	}
}

// reportPending 把本地待上报事件推给 Tracker，成功后裁剪日志。
func (s *StorageNode) reportPending() {
	if s.report == nil {
		return
	}

	s.reportMu.RLock()
	cli := s.trackerCli
	s.reportMu.RUnlock()
	if cli == nil {
		return
	}

	events := s.report.snapshot(reportBatchSize)
	if len(events) == 0 {
		return
	}

	reports := make([]*tracker.ChunkReport, 0, len(events))
	for _, event := range events {
		reports = append(reports, &tracker.ChunkReport{
			Seq:       event.Seq,
			Op:        reportOpToThrift(event.Op),
			ChunkId:   int64(event.ChunkID),
			FileId:    int64(event.FileID),
			Version:   int64(event.Version),
			Size:      int64(event.Size),
			Crc:       event.CRC,
			Timestamp: event.Time,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), reportRPCTimeout)
	defer cancel()

	resp, err := cli.ReportChunkEvents(ctx, &tracker.ReportChunkEventsRequest{
		Base: &base.BaseRequest{
			RequestId: time.Now().UnixNano(),
			Timestamp: time.Now().UnixNano(),
		},
		NodeId: int64(s.NodeID()),
		Events: reports,
	})
	if err != nil {
		logs.Warn("Report chunk events failed (will retry): %v", err)
		return
	}
	if resp.Base != nil && resp.Base.Code != base.SUCCESS {
		logs.Warn("Report chunk events rejected: %s", resp.Base.Message)
		return
	}

	if err := s.report.ack(resp.AckedSeq); err != nil {
		logs.Error("Trim report journal failed: %v", err)
	}
}

// enqueueReport 记录一次本地变更并触发上报。
func (s *StorageNode) enqueueReport(op string, head chunkHeadSnapshot) {
	if s.report == nil {
		return
	}

	if _, err := s.report.append(reportEvent{
		Op:      op,
		ChunkID: head.id,
		FileID:  head.fileId,
		Version: head.version,
		Size:    head.size,
		CRC:     head.crc,
	}); err != nil {
		logs.Error("Append report journal failed: %v", err)
		return
	}

	select {
	case s.reportCh <- struct{}{}:
	default:
	}
}

// chunkHeadSnapshot 是从 Chunk 中提取的、用于上报的元数据。
type chunkHeadSnapshot struct {
	id      uint64
	fileId  uint64
	version uint64
	size    uint64
	crc     string
}

func headSnapshot(head chunk.ChunkHead) chunkHeadSnapshot {
	return chunkHeadSnapshot{
		id:      head.Id,
		fileId:  head.FileId,
		version: head.Version,
		size:    head.Size,
		crc:     fmt.Sprintf("%d", head.CRC),
	}
}

func reportOpToThrift(op string) tracker.ChunkReportOp {
	if op == reportOpDelete {
		return tracker.ChunkReportOp_DELETE
	}
	return tracker.ChunkReportOp_UPSERT
}

// GetInventory 返回本节点当前持有的全部 Chunk 摘要，供 Tracker 对账。
func (s *StorageNode) GetInventory(ctx context.Context, req *storage.GetInventoryRequest) (*storage.GetInventoryResponse, error) {
	s.chunkMux.RLock()
	defer s.chunkMux.RUnlock()

	entries := make([]*storage.ChunkInventoryEntry, 0, len(s.chunks))
	for _, chk := range s.chunks {
		head := chk.SnapshotHead()
		entries = append(entries, &storage.ChunkInventoryEntry{
			ChunkId: int64(head.Id),
			FileId:  int64(head.FileId),
			Version: int64(head.Version),
			Size:    int64(head.Size),
			Crc:     fmt.Sprintf("%d", head.CRC),
		})
	}

	return &storage.GetInventoryResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		Chunks: entries,
	}, nil
}

// DeleteChunk 删除本地 Chunk 及其全部 Slice 文件，由 Tracker 在孤儿回收时调用。
func (s *StorageNode) DeleteChunk(ctx context.Context, req *storage.DeleteChunkRequest) (*storage.DeleteChunkResponse, error) {
	chunkID := uint64(req.ChunkId)

	s.chunkMux.Lock()
	chk, exists := s.chunks[chunkID]
	if exists {
		delete(s.chunks, chunkID)
	}
	s.chunkMux.Unlock()

	// 幂等：重复删除同样返回成功
	if !exists {
		return &storage.DeleteChunkResponse{
			Base: &base.BaseResponse{
				Code:      base.SUCCESS,
				Message:   "chunk not found",
				Timestamp: time.Now().UnixNano(),
			},
			Success: true,
		}, nil
	}

	head := headSnapshot(chk.SnapshotHead())
	if err := chk.Delete(); err != nil {
		return nil, fmt.Errorf("delete chunk %d: %w", chunkID, err)
	}

	s.enqueueReport(reportOpDelete, head)

	logs.Info("Deleted chunk %d (file %d)", head.id, head.fileId)

	return &storage.DeleteChunkResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		Success: true,
	}, nil
}
