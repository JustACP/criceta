package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage/storageservice"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker/trackerservice"
	"github.com/JustACP/criceta/pkg/chunk"
	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/JustACP/criceta/pkg/common/utils"
	"github.com/JustACP/criceta/pkg/config"
	"github.com/JustACP/criceta/pkg/meta"
	"github.com/cloudwego/kitex/client"
)

const (
	maxCompactWorkers = 10            // 最大压缩工作协程数
	scanInterval      = 1 * time.Hour // Chunk 扫描间隔
	maxSliceCount     = 3             // 触发压缩的最大 Slice 数量
)

// CompactTask 表示一个压缩任务
type CompactTask struct {
	chunkId uint64
	chunk   *chunk.SliceBasedChunk
}

type StorageNode struct {
	nodeId         uint64
	chunks         map[uint64]*chunk.SliceBasedChunk
	chunkMux       sync.RWMutex
	compactTasks   chan CompactTask // 压缩任务通道
	compactWorkers sync.WaitGroup   // 压缩工作协程等待组
	stopChan       chan struct{}    // 停止信号通道

	// 变更上报：本地日志 + 向 Tracker 推送
	report         *reportJournal
	trackerCli     trackerservice.Client
	reportCh       chan struct{}
	reportStop     chan struct{}
	reportOnce     sync.Once
	reportStopOnce sync.Once
	reportMu       sync.RWMutex
}

func NewStorageNode() *StorageNode {
	return &StorageNode{
		chunks:       make(map[uint64]*chunk.SliceBasedChunk),
		compactTasks: make(chan CompactTask, maxCompactWorkers*2), // 任务通道缓冲区大小为工作协程数的2倍
		stopChan:     make(chan struct{}),
		reportCh:     make(chan struct{}, 1),
		reportStop:   make(chan struct{}),
	}
}

func (s *StorageNode) Init() error {
	// 从配置文件读取nodeId
	rawConfig, err := config.GetRawConfig[config.StorageNodeConfig]()
	if err != nil {
		return fmt.Errorf("GetRawConfig error: %v", err)
	}

	s.nodeId = uint64(rawConfig.ServerId)

	// 加载已有的chunk数据
	chunkConfig := rawConfig.ChunkStorage
	dataDir := chunkConfig.FilePath
	// Chunk 元数据落在 <file_path>/meta 下，Slice 数据落在 <file_path>/slice 下
	chunkDir := filepath.Join(dataDir, "meta")

	// 确保目录存在
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		return fmt.Errorf("failed to create chunk directory: %v", err)
	}

	// 遍历目录加载chunk
	entries, err := os.ReadDir(chunkDir)
	if err != nil {
		return fmt.Errorf("failed to read chunk directory: %v", err)
	}

	s.chunkMux.Lock()
	defer s.chunkMux.Unlock()

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".meta") {
			continue
		}

		// 加载chunk
		chk, err := chunk.OpenChunkByPath(filepath.Join(chunkDir, entry.Name()))
		if err != nil {
			logs.Error("Failed to load chunk %v: %v", entry.Name(), err)
			continue
		}

		s.chunks[chk.Head.Id] = chk
		logs.Info("Loaded chunk %d (file %d, %d slices)", chk.Head.Id, chk.Head.FileId, chk.GetSliceCount())
	}

	// 启动扫描器
	go s.startChunkScanner()
	// 启动压缩工作池
	go s.startCompactWorkerPool()

	// 打开变更日志（未上报的事件会在拿到 Tracker 客户端后补发）
	journal, err := openReportJournal(filepath.Join(dataDir, reportJournalName))
	if err != nil {
		return fmt.Errorf("open report journal: %v", err)
	}
	s.report = journal

	return nil
}

// SetNodeID 更新本节点 ID。
// 节点首次向 Tracker 注册后会拿到新的 NodeId，必须同步到这里，
// 否则 Read/Write 响应里携带的 NodeId 会与 Tracker 记录的不一致。
func (s *StorageNode) SetNodeID(nodeId uint64) {
	s.chunkMux.Lock()
	defer s.chunkMux.Unlock()
	s.nodeId = nodeId
}

// NodeID 返回本节点 ID
func (s *StorageNode) NodeID() uint64 {
	s.chunkMux.RLock()
	defer s.chunkMux.RUnlock()
	return s.nodeId
}

func (s *StorageNode) Close() error {
	s.reportStopOnce.Do(func() {
		close(s.reportStop)
	})

	// 发送停止信号
	close(s.stopChan)
	// 等待所有压缩工作完成
	s.compactWorkers.Wait()
	// 关闭任务通道
	close(s.compactTasks)
	s.chunkMux.Lock()
	defer s.chunkMux.Unlock()

	// 关闭所有chunk
	for _, c := range s.chunks {
		if err := c.Close(); err != nil {
			logs.Error("Failed to close chunk %d: %v", c.Head.Id, err)
		}
	}
	return nil
}

// startChunkScanner 启动 Chunk 扫描器
func (s *StorageNode) startChunkScanner() {
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.scanChunks()
		}
	}
}

// scanChunks 扫描所有 Chunk
func (s *StorageNode) scanChunks() {
	s.chunkMux.RLock()
	defer s.chunkMux.RUnlock()

	for chunkId, ch := range s.chunks {
		// 检查 Chunk 的 Slice 数量
		sliceCount := ch.GetSliceCount()
		if sliceCount > maxSliceCount {
			// 创建压缩任务
			task := CompactTask{
				chunkId: chunkId,
				chunk:   ch,
			}

			// 尝试发送压缩任务，如果通道已满则跳过
			select {
			case s.compactTasks <- task:
				logs.Info("Created compact task for chunk %d with %d slices", chunkId, sliceCount)
			default:
				logs.Warn("Compact task channel is full, skipping chunk %d", chunkId)
			}
		}
	}
}

// startCompactWorkerPool 启动压缩工作池
func (s *StorageNode) startCompactWorkerPool() {
	// 启动固定数量的工作协程
	for i := 0; i < maxCompactWorkers; i++ {
		s.compactWorkers.Add(1)
		go s.compactWorker(i)
	}
}

// compactWorker 压缩工作协程
func (s *StorageNode) compactWorker(workerId int) {
	defer s.compactWorkers.Done()

	for {
		select {
		case <-s.stopChan:
			return
		case task, ok := <-s.compactTasks:
			if !ok {
				return
			}

			// 执行压缩任务
			logs.Info("Worker %d starting to compact chunk %d", workerId, task.chunkId)
			if err := task.chunk.Compact(); err != nil {
				logs.Error("Worker %d failed to compact chunk %d: %v", workerId, task.chunkId, err)
			} else {
				logs.Info("Worker %d successfully compacted chunk %d", workerId, task.chunkId)
			}
		}
	}
}

func (s *StorageNode) WriteChunk(ctx context.Context, req *storage.WriteChunkRequest) (*storage.WriteChunkResponse, error) {
	// 只在查找/创建 Chunk 时持有全局锁，真正的写入由 Chunk 自身的锁保护，
	// 避免一个大 Chunk 的写入阻塞整个节点。
	s.chunkMux.Lock()
	chk, err := s.getOrCreateChunk(req.Meta)
	s.chunkMux.Unlock()
	if err != nil {
		return nil, fmt.Errorf("get or create chunk failed: %v", err)
	}

	// 验证写入范围
	if req.WriteOffset < 0 || req.WriteSize <= 0 || req.WriteOffset+req.WriteSize > int64(chunk.MaxChunkSize) {
		return nil, fmt.Errorf("invalid write range: offset=%d, size=%d, chunk_size=%d",
			req.WriteOffset, req.WriteSize, chk.Head.Size)
	}

	// 写入数据
	n, err := chk.WriteAt(req.Data, req.WriteOffset)
	if err != nil {
		return nil, fmt.Errorf("write chunk data failed: %v", err)
	}

	if int64(n) != req.WriteSize {
		return nil, fmt.Errorf("write size mismatch: expected=%d, actual=%d", req.WriteSize, n)
	}

	// 更新chunk状态
	chk.Head.ModifyAt = time.Now().UnixNano()
	s.enqueueReport(reportOpUpsert, headSnapshot(chk.SnapshotHead()))

	return &storage.WriteChunkResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		Meta: &storage.ChunkMeta{
			Id:       int64(chk.Head.Id),
			FileId:   int64(chk.Head.FileId),
			Version:  utils.Ptr(int64(chk.Head.Version)),
			Offset:   int64(chk.Head.Offset),
			Size:     int64(chk.Head.Range.Size),
			NodeId:   utils.Ptr(int64(s.nodeId)),
			Status:   utils.Ptr(base.ChunkStatus(chk.Head.Status)),
			Crc:      utils.Ptr(fmt.Sprintf("%d", chk.Head.CRC)),
			CreateAt: &chk.Head.CreateAt,
			ModifyAt: &chk.Head.ModifyAt,
		},
	}, nil
}

func (s *StorageNode) ReadChunk(ctx context.Context, req *storage.ReadChunkRequest) (*storage.ReadChunkResponse, error) {
	// 获取chunk
	s.chunkMux.RLock()
	chk, ok := s.chunks[uint64(req.ChunkId)]
	s.chunkMux.RUnlock()
	if !ok {
		return nil, fmt.Errorf("chunk not found: %d", req.ChunkId)
	}

	// 验证请求参数
	if chk.Head.FileId != uint64(req.FileId) {
		return nil, fmt.Errorf("file id mismatch: expected %d, got %d", chk.Head.FileId, req.FileId)
	}
	// 读取数据
	data := make([]byte, req.Size)
	if _, err := chk.ReadAt(data, int64(req.Offset)); err != nil {
		return nil, fmt.Errorf("read chunk data failed: %v", err)
	}

	return &storage.ReadChunkResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Meta: &storage.ChunkMeta{
			Id:       int64(chk.Head.Id),
			FileId:   int64(chk.Head.FileId),
			Version:  utils.Ptr(int64(chk.Head.Version)),
			Offset:   int64(chk.Head.Offset),
			Size:     int64(chk.Head.Range.Size),
			NodeId:   utils.Ptr(int64(s.nodeId)),
			Status:   utils.Ptr(base.ChunkStatus(chk.Head.Status)),
			CreateAt: &chk.Head.CreateAt,
			ModifyAt: &chk.Head.ModifyAt,
		},
		Data: data,
	}, nil
}

// SyncChunk 处理副本同步请求。
//
// 支持两种模式：
//   - 推送：请求里带 Data，直接落盘；
//   - 拉取：请求只带 Meta + SourceNode，由本节点去源节点把整块数据拉回来。
//
// 数据总是整块写入 Chunk 内偏移 0；提供 CRC 时会先校验再落盘，
// 校验失败不会覆盖本地已有的副本。
func (s *StorageNode) SyncChunk(ctx context.Context, req *storage.SyncChunkRequest) (*storage.SyncChunkResponse, error) {
	if req.Meta == nil {
		return nil, fmt.Errorf("sync chunk: meta is nil")
	}

	// 创建或获取目标chunk
	s.chunkMux.Lock()
	chk, err := s.getOrCreateChunk(req.Meta)
	s.chunkMux.Unlock()
	if err != nil {
		return nil, fmt.Errorf("failed to create target chunk: %v", err)
	}

	// 验证chunk元数据
	if chk.Head.FileId != uint64(req.Meta.FileId) {
		return nil, fmt.Errorf("file id mismatch: expected %d, got %d", chk.Head.FileId, req.Meta.FileId)
	}

	data := req.Data
	if len(data) == 0 {
		if req.SourceNode == nil {
			return nil, fmt.Errorf("sync chunk: neither data nor source node provided")
		}
		data, err = s.fetchChunkFromSource(ctx, req.Meta, req.SourceNode)
		if err != nil {
			return nil, fmt.Errorf("fetch chunk %d from source node %d failed: %w", req.Meta.Id, req.SourceNode.NodeId, err)
		}
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("sync chunk: empty payload for chunk %d", req.Meta.Id)
	}

	// 校验 CRC：只信任通过校验的数据，且必须在落盘前完成，
	// 避免坏数据把本地已有的好副本覆盖掉。
	if req.Meta.Crc != nil && *req.Meta.Crc != "" {
		expected, ok := parseCRC(*req.Meta.Crc)
		if !ok {
			return nil, fmt.Errorf("invalid crc in chunk meta: %s", *req.Meta.Crc)
		}
		actual := utils.UpdateCRC32(0, data)
		if actual != expected {
			return nil, fmt.Errorf("crc mismatch for chunk %d: expected=%08x, actual=%08x", req.Meta.Id, expected, actual)
		}
	}

	// 副本总是整块写入：偏移固定为 Chunk 内 0
	if _, err := chk.WriteAt(data, 0); err != nil {
		return nil, fmt.Errorf("write sync data failed: %v", err)
	}

	version, modifyAt := uint64(0), int64(0)
	if req.Meta.Version != nil {
		version = uint64(*req.Meta.Version)
	}
	if req.Meta.ModifyAt != nil {
		modifyAt = *req.Meta.ModifyAt
	}
	if err := chk.ApplyMeta(version, modifyAt); err != nil {
		return nil, fmt.Errorf("update chunk meta failed: %v", err)
	}
	s.enqueueReport(reportOpUpsert, headSnapshot(chk.SnapshotHead()))

	return &storage.SyncChunkResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		Meta: &storage.ChunkMeta{
			Id:       int64(chk.Head.Id),
			FileId:   int64(chk.Head.FileId),
			Version:  utils.Ptr(int64(chk.Head.Version)),
			Offset:   int64(chk.Head.Offset),
			Size:     int64(chk.Head.Size),
			NodeId:   utils.Ptr(int64(s.nodeId)),
			Status:   utils.Ptr(base.ChunkStatus(chk.Head.Status)),
			Crc:      utils.Ptr(fmt.Sprintf("%d", chk.Head.CRC)),
			CreateAt: &chk.Head.CreateAt,
			ModifyAt: &chk.Head.ModifyAt,
		},
	}, nil
}

// fetchChunkFromSource 从源存储节点拉取整个 Chunk 的数据
func (s *StorageNode) fetchChunkFromSource(ctx context.Context, meta *storage.ChunkMeta, source *storage.NodeInfo) ([]byte, error) {
	if source == nil || source.Address == "" {
		return nil, fmt.Errorf("invalid source node")
	}

	sourceCli, err := storageservice.NewClient(
		fmt.Sprintf("storage-%d", source.NodeId),
		client.WithHostPorts(source.Address),
		client.WithRPCTimeout(time.Minute),
	)
	if err != nil {
		return nil, fmt.Errorf("create source client failed: %w", err)
	}

	version := int64(0)
	if meta.Version != nil {
		version = *meta.Version
	}

	readCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	resp, err := sourceCli.ReadChunk(readCtx, &storage.ReadChunkRequest{
		Base: &base.BaseRequest{
			RequestId: time.Now().UnixNano(),
			Timestamp: time.Now().UnixNano(),
		},
		ChunkId: meta.Id,
		Version: version,
		FileId:  meta.FileId,
		Offset:  0, // chunk 内偏移
		Size:    meta.Size,
	})
	if err != nil {
		return nil, fmt.Errorf("read chunk from source failed: %w", err)
	}
	if resp.Base != nil && resp.Base.Code != base.SUCCESS {
		return nil, fmt.Errorf("source returned error: %s", resp.Base.Message)
	}

	return resp.Data, nil
}

// parseCRC 兼容十进制与十六进制两种 CRC 字符串表示
func parseCRC(crc string) (uint32, bool) {
	crc = strings.TrimSpace(crc)
	if v, err := strconv.ParseUint(crc, 10, 32); err == nil {
		return uint32(v), true
	}
	if v, err := strconv.ParseUint(strings.TrimPrefix(strings.ToLower(crc), "0x"), 16, 32); err == nil {
		return uint32(v), true
	}
	return 0, false
}

func (s *StorageNode) getOrCreateChunk(meta *storage.ChunkMeta) (*chunk.SliceBasedChunk, error) {
	// 调用方必须持有 s.chunkMux
	if meta == nil {
		return nil, fmt.Errorf("chunk meta is nil")
	}

	// 检查是否已存在
	if chk, ok := s.chunks[uint64(meta.Id)]; ok {
		return chk, nil
	}

	version, size := uint64(0), uint64(0)
	if meta.Version != nil {
		version = uint64(*meta.Version)
	}
	if meta.Size > 0 {
		size = uint64(meta.Size)
	}

	// 创建新的chunk
	chk, err := chunk.NewSliceBasedChunk(
		uint64(meta.Id),
		uint64(meta.FileId),
		uint64(meta.Offset),
		chunk.WithVersion(version),
		chunk.WithRangeSize(size),
	)
	if err != nil {
		// 元数据已存在（例如节点重启后首次写入）时改为打开已有 Chunk，绝不覆盖
		existing, openErr := chunk.OpenChunkByPath(chunk.ChunkMetaFilePath(uint64(meta.FileId), uint64(meta.Id)))
		if openErr == nil {
			s.chunks[uint64(meta.Id)] = existing
			return existing, nil
		}
		return nil, fmt.Errorf("create chunk failed: %v (open existing chunk also failed: %v)", err, openErr)
	}

	s.chunks[uint64(meta.Id)] = chk
	return chk, nil
}

func (s *StorageNode) BackupChunks(ctx context.Context, req *storage.BackupChunksRequest) (*storage.BackupChunksResponse, error) {
	s.chunkMux.RLock()
	defer s.chunkMux.RUnlock()

	backupPaths := make(map[int64]string)
	totalSize := int64(0)

	for _, chunkMeta := range req.Chunks {
		chunk, exists := s.chunks[uint64(chunkMeta.Id)]
		if !exists {
			return nil, fmt.Errorf("chunk not found: %d", chunkMeta.Id)
		}

		// 构建备份文件路径
		backupPath := filepath.Join(req.BackupPath, fmt.Sprintf("chunk_%d.bak", chunkMeta.Id))

		// 创建备份目录
		if err := os.MkdirAll(filepath.Dir(backupPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create backup directory: %v", err)
		}

		// 创建备份文件
		backupFile, err := os.Create(backupPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create backup file: %v", err)
		}
		defer backupFile.Close()

		// 读取 chunk 数据
		data := make([]byte, chunk.Head.Range.Size)
		size, err := chunk.ReadAt(data, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk data: %v", err)
		}

		// 写入备份文件
		if _, err := backupFile.Write(data[:size]); err != nil {
			return nil, fmt.Errorf("failed to write backup file: %v", err)
		}

		backupPaths[chunkMeta.Id] = backupPath
		totalSize += int64(size)
	}

	return &storage.BackupChunksResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		BackupSize: totalSize,
		ChunkPaths: backupPaths,
	}, nil
}

func (s *StorageNode) CreateSnapshot(ctx context.Context, req *storage.CreateSnapshotRequest) (*storage.CreateSnapshotResponse, error) {
	s.chunkMux.RLock()
	defer s.chunkMux.RUnlock()

	// 获取配置
	rawConfig, err := config.GetRawConfig[config.StorageNodeConfig]()
	if err != nil {
		return nil, fmt.Errorf("failed to get config: %v", err)
	}

	// 构建快照路径
	snapshotPath := filepath.Join(rawConfig.ChunkStorage.FilePath, fmt.Sprintf("snapshot_%d_%d", req.Meta.Index, req.Meta.Term))
	if err := os.MkdirAll(snapshotPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %v", err)
	}

	// 创建快照元数据
	snapshotMeta := struct {
		Meta       *base.SnapshotMeta          `json:"meta"`
		ChunkHeads map[uint64]*chunk.ChunkHead `json:"chunk_heads"` // chunk_id -> chunk_metadata
	}{
		Meta:       req.Meta,
		ChunkHeads: make(map[uint64]*chunk.ChunkHead),
	}

	// 只记录 chunk 的元数据信息
	for chunkId, chunk := range s.chunks {
		snapshotMeta.ChunkHeads[chunkId] = chunk.Head
	}

	// 更新快照元数据
	snapshotMeta.Meta.Size = int64(len(s.chunks))
	snapshotMeta.Meta.Timestamp = time.Now().UnixNano()

	// 写入元数据文件
	metaPath := filepath.Join(snapshotPath, "meta.json")
	metaData, err := json.Marshal(snapshotMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %v", err)
	}

	if err := os.WriteFile(metaPath, metaData, 0644); err != nil {
		return nil, fmt.Errorf("failed to write metadata file: %v", err)
	}

	return &storage.CreateSnapshotResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Meta:         snapshotMeta.Meta,
		SnapshotPath: snapshotPath,
	}, nil
}

func (s *StorageNode) GetSnapshot(ctx context.Context, req *storage.GetSnapshotRequest) (*storage.GetSnapshotResponse, error) {
	// 获取配置
	rawConfig, err := config.GetRawConfig[config.StorageNodeConfig]()
	if err != nil {
		return nil, fmt.Errorf("failed to get config: %v", err)
	}

	// 构建快照路径
	snapshotPath := filepath.Join(rawConfig.ChunkStorage.FilePath, fmt.Sprintf("snapshot_%d_%d", req.Index, req.Term))
	metaPath := filepath.Join(snapshotPath, "meta.json")

	// 读取元数据文件
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %v", err)
	}

	// 解析元数据
	var snapshotMeta struct {
		Meta       *base.SnapshotMeta          `json:"meta"`
		ChunkHeads map[uint64]*chunk.ChunkHead `json:"chunk_heads"`
	}
	if err := json.Unmarshal(metaData, &snapshotMeta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %v", err)
	}

	return &storage.GetSnapshotResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Meta:    snapshotMeta.Meta,
		HasMore: false,
	}, nil
}

func (s *StorageNode) RestoreChunks(ctx context.Context, req *storage.RestoreChunksRequest) (*storage.RestoreChunksResponse, error) {
	s.chunkMux.Lock()
	defer s.chunkMux.Unlock()

	restoredChunks := make(map[int64]*storage.ChunkMeta)

	for _, chunkMeta := range req.Chunks {
		// 构建备份文件路径
		backupPath := filepath.Join(req.BackupPath, fmt.Sprintf("chunk_%d.bak", chunkMeta.Id))

		// 读取备份文件
		backupData, err := os.ReadFile(backupPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read backup file: %v", err)
		}

		// 创建或获取chunk
		chunk, err := s.getOrCreateChunk(chunkMeta)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunk: %v", err)
		}

		// 写入数据
		if _, err := chunk.WriteAt(backupData, 0); err != nil {
			return nil, fmt.Errorf("failed to write chunk data: %v", err)
		}

		// 更新元数据
		restoredChunks[chunkMeta.Id] = &storage.ChunkMeta{
			Id:       int64(chunk.Head.Id),
			FileId:   int64(chunk.Head.FileId),
			Version:  utils.Ptr(int64(chunk.Head.Version)),
			Offset:   int64(chunk.Head.Offset),
			Size:     int64(chunk.Head.Range.Size),
			NodeId:   utils.Ptr(int64(s.nodeId)),
			Status:   utils.Ptr(base.ChunkStatus(chunk.Head.Status)),
			CreateAt: &chunk.Head.CreateAt,
			ModifyAt: &chunk.Head.ModifyAt,
		}
	}

	return &storage.RestoreChunksResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		RestoredChunks: restoredChunks,
	}, nil
}

func (s *StorageNode) GetNodeLoad(ctx context.Context, req *storage.GetNodeLoadRequest) (*storage.GetNodeLoadResponse, error) {
	s.chunkMux.RLock()
	defer s.chunkMux.RUnlock()

	// 计算总大小和总chunks数
	var totalSize int64
	for _, chk := range s.chunks {
		totalSize += int64(chk.Head.Range.Size)
	}

	// 获取系统状态
	stats := &meta.ServerStats{}
	if err := stats.Stat(); err != nil {
		return nil, fmt.Errorf("failed to stat server: %v", err)
	}

	return &storage.GetNodeLoadResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		LoadInfo: &storage.NodeLoadInfo{
			TotalChunks:     int64(len(s.chunks)),
			TotalSize:       totalSize,
			ReadIops:        int64(stats.Disk.IOPS),
			WriteIops:       int64(stats.Disk.IOPS),
			ReadThroughput:  int64(stats.Disk.ReadSpeed),
			WriteThroughput: int64(stats.Disk.WriteSpeed),
			CpuUsage:        stats.CPU.UsagePercent,
			MemoryUsage:     stats.Memory.UsedPercent,
			DiskUsage:       stats.Disk.UsedPercent,
			NetworkUsage:    float64(stats.Network.BytesSent+stats.Network.BytesRecv) / 1024 / 1024, // MB/s
		},
	}, nil
}

func (s *StorageNode) GetChunkStatus(ctx context.Context, req *storage.GetChunkStatusRequest) (*storage.GetChunkStatusResponse, error) {
	s.chunkMux.RLock()
	defer s.chunkMux.RUnlock()

	chunkStatus := make(map[int64]*storage.ChunkStatusInfo)
	for _, chunkId := range req.ChunkIds {
		chunk, exists := s.chunks[uint64(chunkId)]
		if !exists {
			continue
		}

		chunkStatus[chunkId] = &storage.ChunkStatusInfo{
			ChunkId:    chunkId,
			Status:     base.ChunkStatus(chunk.Head.Status),
			Version:    int64(chunk.Head.Version),
			Crc:        fmt.Sprintf("%d", chunk.Head.CRC),
			ModifyTime: chunk.Head.ModifyAt,
		}
	}

	return &storage.GetChunkStatusResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		ChunkStatus: chunkStatus,
	}, nil
}

func (s *StorageNode) MigrateChunk(ctx context.Context, req *storage.MigrateChunkRequest) (*storage.MigrateChunkResponse, error) {
	s.chunkMux.Lock()
	defer s.chunkMux.Unlock()

	// 检查源chunk是否存在
	chunk, exists := s.chunks[uint64(req.Meta.Id)]
	if !exists {
		return nil, fmt.Errorf("chunk not found: %d", req.Meta.Id)
	}

	// 读取chunk数据
	data := make([]byte, chunk.Head.Range.Size)
	size, err := chunk.ReadAt(data, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk data: %v", err)
	}

	// 创建迁移请求
	syncReq := &storage.SyncChunkRequest{
		Base: &base.BaseRequest{
			RequestId: time.Now().UnixNano(),
			Timestamp: time.Now().UnixNano(),
		},
		Meta: req.Meta,
		Data: data[:size],
	}

	// 连接目标节点
	targetClient, err := storageservice.NewClient(
		fmt.Sprintf("storage-%d", req.TargetNodeId),
		client.WithHostPorts(fmt.Sprintf("%s:%d", req.TargetAddress, req.TargetPort)),
		client.WithRPCTimeout(time.Second*30),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create target node client: %v", err)
	}

	// 发送同步请求
	syncResp, err := targetClient.SyncChunk(ctx, syncReq)
	if err != nil {
		return nil, fmt.Errorf("failed to sync chunk to target node: %v", err)
	}

	// 迁移成功后删除本地chunk
	head := headSnapshot(chunk.SnapshotHead())
	delete(s.chunks, uint64(req.Meta.Id))
	if err := chunk.Close(); err != nil {
		logs.Error("Failed to close chunk %d: %v", req.Meta.Id, err)
	}
	s.enqueueReport(reportOpDelete, head)

	return &storage.MigrateChunkResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Success: true,
		Meta:    syncResp.Meta,
	}, nil
}

// RecoverChunk 从源节点恢复chunk数据
func (s *StorageNode) RecoverChunk(ctx context.Context, req *storage.RecoverChunkRequest) (*storage.RecoverChunkResponse, error) {
	// 创建源节点的客户端
	sourceClient, err := storageservice.NewClient(
		fmt.Sprintf("storage-%d", req.SourceNodeId),
		client.WithHostPorts(fmt.Sprintf("%s:%d", req.SourceAddress, req.SourcePort)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create source node client: %v", err)
	}

	// 从源节点读取chunk数据
	readResp, err := sourceClient.ReadChunk(ctx, &storage.ReadChunkRequest{
		Base: &base.BaseRequest{
			RequestId: time.Now().UnixNano(),
			Timestamp: time.Now().UnixNano(),
		},
		ChunkId: req.Meta.Id,
		Version: *req.Meta.Version,
		FileId:  req.Meta.FileId,
		Offset:  req.Meta.Offset,
		Size:    req.Meta.Size,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk from source node: %v", err)
	}

	// 在本地创建chunk
	s.chunkMux.Lock()
	chunk, err := s.getOrCreateChunk(req.Meta)
	s.chunkMux.Unlock()
	if err != nil {
		return nil, fmt.Errorf("failed to create local chunk: %v", err)
	}

	// 写入数据
	if n, err := chunk.Write(readResp.Data); err != nil {
		return nil, fmt.Errorf("failed to write chunk data: %v", err)
	} else if int64(n) != req.Meta.Size {
		return nil, fmt.Errorf("incomplete write: wrote %d bytes, expected %d bytes", n, req.Meta.Size)
	}

	return &storage.RecoverChunkResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		Success: true,
		Meta:    req.Meta,
	}, nil
}

// VerifyReplica 验证副本的完整性和一致性
func (s *StorageNode) VerifyReplica(ctx context.Context, req *storage.VerifyReplicaRequest) (*storage.VerifyReplicaResponse, error) {
	s.chunkMux.RLock()
	defer s.chunkMux.RUnlock()

	// 构造基础响应
	resp := &storage.VerifyReplicaResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		IsValid: false,
		Status:  base.ChunkStatus_VALID,
	}

	// 查找对应的 chunk
	chunk, exists := s.chunks[uint64(req.ChunkId)]
	if !exists {
		resp.Base.Code = base.ERR_NOT_FOUND
		resp.Base.Message = "chunk not found"
		resp.Status = base.ChunkStatus_ERROR
		return resp, nil
	}

	// 计算 CRC
	crc, err := chunk.CalculateCRC()
	if err != nil {
		resp.Base.Code = base.ERR_INTERNAL
		resp.Base.Message = fmt.Sprintf("failed to calculate CRC: %v", err)
		resp.Status = base.ChunkStatus_ERROR
		return resp, nil
	}

	resp.Crc = crc
	resp.IsValid = true
	return resp, nil
}
