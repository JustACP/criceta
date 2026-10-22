package tracker

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

	"github.com/JustACP/criceta/pkg/chunk"
	"github.com/JustACP/criceta/pkg/common/utils"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage/storageservice"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/JustACP/criceta/pkg/common/idgen"
	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/JustACP/criceta/pkg/config"
	"github.com/JustACP/criceta/pkg/meta"
	"github.com/cloudwego/kitex/client"
)

type TrackerNode struct {
	nodeId          int64
	files           map[uint64]*meta.FileMeta
	nodes           map[uint64]*tracker.NodeInfo
	fileMux         sync.RWMutex
	nodeMux         sync.RWMutex
	writeLock       map[uint64]*fileWriteLease // fileId -> 写租约
	lockMux         sync.RWMutex
	raftState       *RaftState
	heartbeatLog    *HeartbeatRecorder
	healthCheckOnce sync.Once

	// dataDir 下保存元数据快照与心跳记录
	dataDir         string
	persistCh       chan struct{}
	persistStop     chan struct{}
	persistOnce     sync.Once
	persistStopOnce sync.Once
	persistMux      sync.Mutex
}

// fileWriteLease 文件写租约。
// 租约带过期时间：客户端崩溃后文件会自动解锁，不会被永久占用。
type fileWriteLease struct {
	clientId uint64
	expireAt time.Time
}

// writeLeaseTTL 写租约有效期：客户端崩溃后最多这么久文件即可被其他客户端接管
const writeLeaseTTL = 30 * time.Second

// NewTrackerNode 按配置中的数据目录创建 Tracker。
func NewTrackerNode() *TrackerNode {
	dataDir := defaultTrackerDataDir
	if cfg, err := config.GetRawConfig[config.TrackerNodeConfig](); err == nil && cfg.DataDir != "" {
		dataDir = cfg.DataDir
	}
	return newTrackerNode(dataDir)
}

// NewTrackerNodeWithDataDir 使用指定的数据目录创建 Tracker（测试用，避免污染工作区）
func NewTrackerNodeWithDataDir(dataDir string) *TrackerNode {
	return newTrackerNode(dataDir)
}

func newTrackerNode(dataDir string) *TrackerNode {
	if dataDir == "" {
		dataDir = defaultTrackerDataDir
	}

	heartbeatLog, err := NewHeartbeatRecorder(filepath.Join(dataDir, "heartbeats"), 0)
	if err != nil {
		logs.Error("Failed to create heartbeat recorder: %v", err)
		return nil
	}

	node := &TrackerNode{
		files:        make(map[uint64]*meta.FileMeta),
		nodes:        make(map[uint64]*tracker.NodeInfo),
		writeLock:    make(map[uint64]*fileWriteLease),
		heartbeatLog: heartbeatLog,
		dataDir:      dataDir,
		persistCh:    make(chan struct{}, 1),
		persistStop:  make(chan struct{}),
	}
	node.initRaft()
	return node
}

// SetNodeID 设置本 Tracker 节点 ID（用于 Raft 投票身份）
func (t *TrackerNode) SetNodeID(nodeId int64) {
	t.nodeId = nodeId
}

// acquireWriteLease 获取（或续期）文件写租约。
// 同一客户端重复获取视为续期；其他客户端持有未过期租约时返回锁冲突。
func (t *TrackerNode) acquireWriteLease(fileId, clientId uint64) error {
	t.lockMux.Lock()
	defer t.lockMux.Unlock()

	now := time.Now()
	if lease, ok := t.writeLock[fileId]; ok {
		if lease.clientId != clientId && now.Before(lease.expireAt) {
			return fmt.Errorf("file is locked by other client")
		}
	}

	t.writeLock[fileId] = &fileWriteLease{
		clientId: clientId,
		expireAt: now.Add(writeLeaseTTL),
	}
	return nil
}

// renewWriteLease 续期租约；若租约已不属于该客户端返回 false
func (t *TrackerNode) renewWriteLease(fileId, clientId uint64) bool {
	t.lockMux.Lock()
	defer t.lockMux.Unlock()

	lease, ok := t.writeLock[fileId]
	if !ok || lease.clientId != clientId {
		return false
	}
	lease.expireAt = time.Now().Add(writeLeaseTTL)
	return true
}

// checkWriteLease 仅检查文件是否被其他客户端以未过期租约占用
func (t *TrackerNode) checkWriteLease(fileId, clientId uint64) error {
	t.lockMux.RLock()
	defer t.lockMux.RUnlock()

	lease, ok := t.writeLock[fileId]
	if !ok {
		return nil
	}
	if lease.clientId != clientId && time.Now().Before(lease.expireAt) {
		return fmt.Errorf("file is locked by other client")
	}
	return nil
}

// releaseWriteLease 释放租约。
// 文件本来就没有锁、或锁已过期时视为成功（幂等）。
func (t *TrackerNode) releaseWriteLease(fileId, clientId uint64) error {
	t.lockMux.Lock()
	defer t.lockMux.Unlock()

	lease, ok := t.writeLock[fileId]
	if !ok {
		return nil
	}
	if lease.clientId != clientId && time.Now().Before(lease.expireAt) {
		return fmt.Errorf("file is locked by other client")
	}
	delete(t.writeLock, fileId)
	return nil
}

// cleanupExpiredLeases 清理过期写租约
func (t *TrackerNode) cleanupExpiredLeases() {
	t.lockMux.Lock()
	defer t.lockMux.Unlock()

	now := time.Now()
	for fileId, lease := range t.writeLock {
		if now.After(lease.expireAt) {
			logs.Warn("Write lease of file %d (client %d) expired, releasing", fileId, lease.clientId)
			delete(t.writeLock, fileId)
		}
	}
}

func (t *TrackerNode) CreateFile(ctx context.Context, req *tracker.CreateFileRequest) (*tracker.CreateFileResponse, error) {
	if req.Name == "" {
		logs.Error("CreateFile failed: empty file name")
		return nil, fmt.Errorf("file name cannot be empty")
	}

	t.fileMux.Lock()
	defer t.fileMux.Unlock()

	for _, file := range t.files {
		if file.Name == req.Name {
			logs.Error("CreateFile failed: file name already exists: %s", req.Name)
			return nil, fmt.Errorf("file name already exists: %s", req.Name)
		}
	}

	fileId := idgen.NextID()
	fileMeta := &meta.FileMeta{
		Id:            int64(fileId),
		Name:          req.Name,
		Length:        uint64(req.Length),
		Status:        meta.VALID,
		Version:       1,
		CreatAt:       time.Now().UnixNano(),
		UpdateAt:      time.Now().UnixNano(),
		Chunks:        make(map[uint64]*meta.ChunkMeta),
		ChunkReplicas: make(map[uint64][]*base.ReplicaInfo),
	}

	t.files[fileId] = fileMeta
	t.markDirty()
	logs.Info("File created successfully: id=%d, name=%s", fileId, req.Name)

	return &tracker.CreateFileResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Meta: &tracker.FileMeta{
			Id:       fileMeta.Id,
			Name:     fileMeta.Name,
			Length:   int64(fileMeta.Length),
			Status:   base.FileStatus(fileMeta.Status),
			CreateAt: fileMeta.CreatAt,
			UpdateAt: fileMeta.UpdateAt,
		},
	}, nil
}

func (t *TrackerNode) AllocateChunk(ctx context.Context, req *tracker.AllocateChunkRequest) (*tracker.AllocateChunkResponse, error) {
	if req.FileId <= 0 {
		logs.Error("AllocateChunk failed: invalid file id: %d", req.FileId)
		return nil, fmt.Errorf("invalid file id: %d", req.FileId)
	}

	// 检查文件是否存在
	t.fileMux.RLock()
	fileMeta, ok := t.files[uint64(req.FileId)]
	t.fileMux.RUnlock()
	if !ok {
		logs.Error("AllocateChunk failed: file not found: %d", req.FileId)
		return nil, fmt.Errorf("file not found: %d", req.FileId)
	}

	// 检查写锁（带租约，客户端崩溃后自动过期）
	if err := t.acquireWriteLease(uint64(req.FileId), requestClientId(req.Base)); err != nil {
		logs.Error("AllocateChunk failed: file %d is locked: %v", req.FileId, err)
		return nil, err
	}

	// 选择存储节点
	t.nodeMux.RLock()
	var selectedNode *tracker.NodeInfo
	for _, node := range t.nodes {
		if node.Status == base.NodeStatus_ONLINE {
			selectedNode = node
			break
		}
	}
	t.nodeMux.RUnlock()

	if selectedNode == nil {
		logs.Error("AllocateChunk failed: no available storage node")
		return nil, fmt.Errorf("no available storage node")
	}

	// 创建Chunk元数据
	chunkId := idgen.NextID()
	chunkMeta := &storage.ChunkMeta{
		Id:       int64(chunkId),
		FileId:   req.FileId,
		Version:  utils.Ptr(int64(fileMeta.LatestChunkVersion() + 1)),
		Offset:   req.Offset,
		Size:     req.Size,
		NodeId:   &selectedNode.NodeId,
		Status:   utils.Ptr(base.ChunkStatus(meta.VALID)),
		CreateAt: utils.Ptr(time.Now().UnixNano()),
		ModifyAt: utils.Ptr(time.Now().UnixNano()),
	}

	logs.Info("Chunk allocated successfully: id=%d, fileId=%d, nodeId=%d", chunkId, req.FileId, selectedNode.NodeId)

	return &tracker.AllocateChunkResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Chunk: chunkMeta,
		Node:  selectedNode,
	}, nil
}

func (t *TrackerNode) CommitChunk(ctx context.Context, req *tracker.CommitChunkRequest) (*tracker.CommitChunkResponse, error) {
	if req.Chunk == nil {
		return nil, fmt.Errorf("commit chunk: chunk meta is nil")
	}
	// 这些字段是存储节点写入后必须回传的权威元数据，缺任何一个都说明请求非法
	if req.Chunk.Crc == nil || req.Chunk.Version == nil || req.Chunk.NodeId == nil ||
		req.Chunk.CreateAt == nil || req.Chunk.ModifyAt == nil {
		return nil, fmt.Errorf("commit chunk: incomplete chunk meta")
	}

	t.fileMux.Lock()
	defer t.fileMux.Unlock()

	file, ok := t.files[uint64(req.Chunk.FileId)]
	if !ok {
		return nil, fmt.Errorf("file not found: %d", req.Chunk.FileId)
	}

	// 续期写租约；若租约已被其他客户端接管，拒绝提交，避免两个客户端交错写同一文件
	commitClientId := requestClientId(req.Base)
	if !t.renewWriteLease(uint64(req.Chunk.FileId), commitClientId) {
		if err := t.acquireWriteLease(uint64(req.Chunk.FileId), commitClientId); err != nil {
			logs.Error("CommitChunk failed: file %d is locked: %v", req.Chunk.FileId, err)
			return nil, err
		}
	}

	chunkId := uint64(req.Chunk.Id)
	updateCRC, err := strconv.ParseUint(*req.Chunk.Crc, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("chunk id: %v convert crc val to uint32 error, crc val: %v", req.Chunk.Crc, err)
	}

	var currChunkMeta *meta.ChunkMeta

	if existingChunk, ok := file.Chunks[chunkId]; ok {

		currChunkMeta = existingChunk
	} else {

		currChunkMeta = &meta.ChunkMeta{
			ChunkHead: chunk.ChunkHead{
				Id:      uint64(req.Chunk.Id),
				FileId:  uint64(req.Chunk.FileId),
				Version: uint64(*req.Chunk.Version),
				Offset:  uint64(req.Chunk.Offset),
				Size:    uint64(req.Chunk.Size),
				CRC:     uint32(updateCRC),
			},
			NodeId:   uint64(*req.Chunk.NodeId),
			CreateAt: *req.Chunk.CreateAt,
			UpdateAt: *req.Chunk.ModifyAt,
		}
		file.Chunks[chunkId] = currChunkMeta
	}

	currChunkMeta.Status = chunk.VALID
	currChunkMeta.CreateAt = *req.Chunk.CreateAt
	currChunkMeta.UpdateAt = *req.Chunk.ModifyAt
	currChunkMeta.Version = uint64(*req.Chunk.Version)
	currChunkMeta.NodeId = uint64(*req.Chunk.NodeId)
	currChunkMeta.CRC = uint32(updateCRC)
	currChunkMeta.Size = uint64(req.Chunk.Size)

	currChunkMeta.ChunkHead.CRC = currChunkMeta.CRC
	currChunkMeta.ChunkHead.Version = currChunkMeta.Version
	currChunkMeta.ChunkHead.Status = chunk.ChunkStatus(currChunkMeta.Status)
	currChunkMeta.ChunkHead.Size = currChunkMeta.Size

	var totalSize int64 = 0
	for _, chk := range file.Chunks {
		if !(chk.Status == chunk.VALID || chk.Status == chunk.SYNCING) {
			continue
		}

		chunkEnd := chk.Offset + chk.Size
		if int64(chunkEnd) > totalSize {
			totalSize = int64(chunkEnd)
		}

	}

	// 更新文件元数据
	file.Size = totalSize
	file.Length = uint64(file.Size)
	file.ModifyTime = time.Now().UnixNano()
	file.Version++
	t.files[uint64(req.Chunk.FileId)] = file
	t.markDirty()

	// 选择副本节点（当前策略：取在线节点中最靠前的若干个）
	// TODO 后续根据节点负载/网络拓扑选择
	sourceNodeId := *req.Chunk.NodeId
	replicas := make([]*base.ReplicaInfo, 0, minReplicaCount)

	t.nodeMux.RLock()
	sourceNode, sourceExists := t.nodes[uint64(sourceNodeId)]
	for _, node := range t.nodes {
		if node.NodeId == sourceNodeId || node.Status != base.NodeStatus_ONLINE {
			continue
		}
		replicas = append(replicas, &base.ReplicaInfo{
			NodeId: node.NodeId,
			Status: int32(base.ChunkStatus_SYNCING),
		})
		if len(replicas) >= minReplicaCount {
			break
		}
	}
	t.nodeMux.RUnlock()

	file.ChunkReplicas[chunkId] = replicas

	if len(replicas) < minReplicaCount {
		logs.Warn("Chunk %d of file %d only got %d replica target(s), want %d",
			chunkId, req.Chunk.FileId, len(replicas), minReplicaCount)
	}

	// 异步触发副本同步：Tracker 只下发元数据 + 源节点地址，
	// 由目标节点主动回源拉取整块数据（拉模式）。
	if sourceExists {
		for _, replica := range replicas {
			metaCopy := *req.Chunk
			go t.syncReplica(&metaCopy, sourceNode, replica)
		}
	} else {
		logs.Error("Source node %d not found, skip replica sync of chunk %d", sourceNodeId, chunkId)
	}

	return &tracker.CommitChunkResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Chunk:    req.Chunk,
		Replicas: replicas,
	}, nil
}

// syncReplica 让目标节点从源节点拉取 Chunk 数据，并把结果回写到 ChunkReplicas。
func (t *TrackerNode) syncReplica(chunkMeta *storage.ChunkMeta, sourceNode *tracker.NodeInfo, replica *base.ReplicaInfo) {
	chunkId := chunkMeta.Id

	targetNode := t.getNode(uint64(replica.NodeId))
	if targetNode == nil {
		logs.Error("Target node %d not found, skip replica sync of chunk %d", replica.NodeId, chunkId)
		t.setChunkReplicaStatus(chunkMeta.FileId, chunkId, replica.NodeId, base.ChunkStatus_ERROR)
		return
	}

	targetClient, err := storageservice.NewClient(
		fmt.Sprintf("storage-%d", targetNode.NodeId),
		client.WithHostPorts(targetNode.Address),
		client.WithRPCTimeout(replicaSyncTimeout),
	)
	if err != nil {
		logs.Error("Failed to create storage client for node %d: %v", replica.NodeId, err)
		t.setChunkReplicaStatus(chunkMeta.FileId, chunkId, replica.NodeId, base.ChunkStatus_ERROR)
		return
	}

	syncCtx, cancel := context.WithTimeout(context.Background(), replicaSyncTimeout)
	defer cancel()

	syncResp, err := targetClient.SyncChunk(syncCtx, &storage.SyncChunkRequest{
		Base: &base.BaseRequest{
			RequestId: time.Now().UnixNano(),
			Timestamp: time.Now().UnixNano(),
		},
		Meta: chunkMeta,
		SourceNode: &storage.NodeInfo{
			NodeId:  sourceNode.NodeId,
			Address: sourceNode.Address,
		},
	})
	if err != nil {
		logs.Error("Failed to sync chunk %d to node %d: %v", chunkId, replica.NodeId, err)
		t.setChunkReplicaStatus(chunkMeta.FileId, chunkId, replica.NodeId, base.ChunkStatus_ERROR)
		return
	}

	if syncResp.Base == nil || syncResp.Base.Code != base.SUCCESS {
		msg := "unknown error"
		if syncResp.Base != nil {
			msg = syncResp.Base.Message
		}
		logs.Error("Failed to sync chunk %d to node %d: %s", chunkId, replica.NodeId, msg)
		t.setChunkReplicaStatus(chunkMeta.FileId, chunkId, replica.NodeId, base.ChunkStatus_ERROR)
		return
	}

	t.setChunkReplicaStatus(chunkMeta.FileId, chunkId, replica.NodeId, base.ChunkStatus_VALID)
	logs.Info("Replica of chunk %d synced to node %d", chunkId, replica.NodeId)
}

// setChunkReplicaStatus 更新 Chunk 某个副本的状态
func (t *TrackerNode) setChunkReplicaStatus(fileId, chunkId, nodeId int64, status base.ChunkStatus) {
	t.fileMux.Lock()
	defer t.fileMux.Unlock()

	file, ok := t.files[uint64(fileId)]
	if !ok {
		return
	}

	for _, replica := range file.ChunkReplicas[uint64(chunkId)] {
		if replica.NodeId == nodeId {
			replica.Status = int32(status)
			t.markDirty()
			return
		}
	}
}

// getNode 返回节点快照；不存在时返回 nil
func (t *TrackerNode) getNode(nodeId uint64) *tracker.NodeInfo {
	t.nodeMux.RLock()
	defer t.nodeMux.RUnlock()

	node, ok := t.nodes[nodeId]
	if !ok {
		return nil
	}

	copied := *node
	return &copied
}

func (t *TrackerNode) GetFile(ctx context.Context, req *tracker.GetFileRequest) (*tracker.GetFileResponse, error) {
	t.fileMux.RLock()
	defer t.fileMux.RUnlock()

	fileMeta, ok := t.files[uint64(req.FileId)]
	if !ok {
		return nil, fmt.Errorf("file not found: %d", req.FileId)
	}

	chunks := make(map[int64]*storage.ChunkMeta)
	for id, chunk := range fileMeta.Chunks {
		chunks[int64(id)] = &storage.ChunkMeta{
			Id:       int64(chunk.Id),
			FileId:   int64(chunk.FileId),
			Version:  utils.Ptr(int64(chunk.Version)),
			Offset:   int64(chunk.Offset),
			Size:     int64(chunk.Size),
			NodeId:   utils.Ptr(int64(chunk.NodeId)),
			Status:   utils.Ptr(base.ChunkStatus(chunk.Status)),
			Crc:      utils.Ptr(fmt.Sprintf("%d", chunk.CRC)),
			CreateAt: utils.Ptr(chunk.CreateAt),
			ModifyAt: utils.Ptr(chunk.ModifyAt),
		}
	}

	chunkReplicas := make(map[int64][]*base.ReplicaInfo, len(fileMeta.ChunkReplicas))
	for chunkId, replicas := range fileMeta.ChunkReplicas {
		converted := make([]*base.ReplicaInfo, 0, len(replicas))
		for _, replica := range replicas {
			converted = append(converted, &base.ReplicaInfo{
				NodeId:   replica.NodeId,
				Status:   replica.Status,
				SyncTime: replica.SyncTime,
				Crc:      replica.Crc,
			})
		}
		chunkReplicas[int64(chunkId)] = converted
	}

	return &tracker.GetFileResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Meta: &tracker.FileMeta{
			Id:            fileMeta.Id,
			Name:          fileMeta.Name,
			Length:        int64(fileMeta.Length),
			Status:        base.FileStatus(fileMeta.Status),
			Size:          fileMeta.Size,
			Version:       int64(fileMeta.Version),
			CreateAt:      fileMeta.CreatAt,
			UpdateAt:      fileMeta.UpdateAt,
			ModifyTime:    fileMeta.ModifyTime,
			Chunks:        chunks,
			LockInfo:      convertLockInfoToThrift(fileMeta.LockInfo),
			ChunkReplicas: chunkReplicas,
		},
	}, nil
}

const (
	nodeHeartbeatTimeout = 30 * time.Second
	nodeCleanupInterval  = 10 * time.Second

	// minReplicaCount 设计文档要求的最小副本数
	minReplicaCount = 2
	// replicaSyncTimeout 单次副本同步的总超时
	replicaSyncTimeout = 5 * time.Minute
)

func (t *TrackerNode) Init() error {
	// 先恢复上次运行留下的元数据，再对外提供服务
	if err := t.loadSnapshot(); err != nil {
		return err
	}

	t.startPersister()
	t.StartNodeHealthCheck()
	return nil
}

// StartNodeHealthCheck 启动节点健康检查与租约清理协程。
// 使用 sync.Once 保证重复调用（如 main 里再调一次）不会启动多个清理协程。
func (t *TrackerNode) StartNodeHealthCheck() {
	t.healthCheckOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(nodeCleanupInterval)
			defer ticker.Stop()

			for range ticker.C {
				t.cleanupInactiveNodes()
				t.cleanupExpiredLeases()
			}
		}()
	})
}

func (t *TrackerNode) cleanupInactiveNodes() {
	t.nodeMux.Lock()
	defer t.nodeMux.Unlock()

	now := time.Now()
	for id, node := range t.nodes {
		if now.Sub(time.Unix(0, node.LastHeartbeat)) > nodeHeartbeatTimeout {
			logs.Warn("Node is inactive, marking as offline:%v", node.NodeId)
			node.Status = base.NodeStatus(base.NodeStatus_OFFLINE)
			delete(t.nodes, id)
		}
	}
}

func (t *TrackerNode) UpdateNodeHeartbeat(ctx context.Context, req *tracker.HeartbeatRequest) (*tracker.HeartbeatResponse, error) {
	t.nodeMux.Lock()
	defer t.nodeMux.Unlock()

	node, exists := t.nodes[uint64(req.NodeId)]
	if !exists {
		node = &tracker.NodeInfo{
			NodeId:  req.NodeId,
			Address: fmt.Sprintf("%s:%d", req.Host, req.Port),
			Status:  base.NodeStatus_ONLINE,
		}
		t.nodes[uint64(req.NodeId)] = node
	}

	node.LastHeartbeat = time.Now().UnixNano()
	node.ServerStats = req.ServerStats
	return &tracker.HeartbeatResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
	}, nil
}

func (t *TrackerNode) DeleteFile(ctx context.Context, req *tracker.DeleteFileRequest) (*tracker.DeleteFileResponse, error) {
	t.fileMux.Lock()
	defer t.fileMux.Unlock()

	fileMeta, ok := t.files[uint64(req.FileId)]
	if !ok {
		logs.Error("DeleteFile failed: file not found: %d", req.FileId)
		return nil, fmt.Errorf("file not found: %d", req.FileId)
	}

	if err := t.checkWriteLease(uint64(req.FileId), requestClientId(req.Base)); err != nil {
		logs.Error("DeleteFile failed: file %d is locked: %v", req.FileId, err)
		return nil, err
	}

	fileMeta.Status = meta.DELETED
	fileMeta.UpdateAt = time.Now().UnixNano()

	delete(t.files, uint64(req.FileId))
	t.markDirty()
	// 删除成功后释放租约，避免死锁残留
	if err := t.releaseWriteLease(uint64(req.FileId), requestClientId(req.Base)); err != nil {
		logs.Warn("DeleteFile: release lease of file %d failed: %v", req.FileId, err)
	}

	logs.Info("File deleted successfully: id=%d, name=%s", req.FileId, fileMeta.Name)

	return &tracker.DeleteFileResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
	}, nil
}

func (t *TrackerNode) UnlockFile(ctx context.Context, req *tracker.UnlockFileRequest) (*tracker.UnlockFileResponse, error) {
	if err := t.releaseWriteLease(uint64(req.FileId), requestClientId(req.Base)); err != nil {
		logs.Error("UnlockFile failed: file %d: %v", req.FileId, err)
		return nil, err
	}

	logs.Info("File unlocked successfully: id=%d", req.FileId)

	return &tracker.UnlockFileResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
	}, nil
}

func (t *TrackerNode) RegisterNode(ctx context.Context, req *tracker.RegisterNodeRequest) (*tracker.RegisterNodeResponse, error) {
	t.nodeMux.Lock()
	defer t.nodeMux.Unlock()

	nodeId := idgen.NextID()

	node := &tracker.NodeInfo{
		NodeId:        int64(nodeId),
		Address:       req.Address,
		Status:        base.NodeStatus_ONLINE,
		Capacity:      req.Capacity,
		LastHeartbeat: time.Now().UnixNano(),
	}

	t.nodes[nodeId] = node

	logs.Info("Node registered successfully: id=%d, address=%s", nodeId, req.Address)

	return &tracker.RegisterNodeResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		NodeId: int64(nodeId),
	}, nil
}

func (t *TrackerNode) ListFiles(ctx context.Context, req *tracker.ListFilesRequest) (*tracker.ListFilesResponse, error) {
	t.fileMux.RLock()
	defer t.fileMux.RUnlock()

	files := make([]*tracker.FileMeta, 0)
	for _, file := range t.files {

		if req.Status != 0 && int32(file.Status) != req.Status {
			continue
		}

		if req.NamePattern != "" {
			if !strings.Contains(file.Name, req.NamePattern) {
				continue
			}
		}

		chunks := make(map[int64]*storage.ChunkMeta)
		for id, chunk := range file.Chunks {
			chunks[int64(id)] = &storage.ChunkMeta{
				Id:       int64(chunk.Id),
				FileId:   int64(chunk.FileId),
				Version:  utils.Ptr(int64(chunk.Version)),
				Offset:   int64(chunk.Offset),
				Size:     int64(chunk.Range.Size),
				NodeId:   utils.Ptr(int64(chunk.NodeId)),
				Status:   utils.Ptr(base.ChunkStatus(chunk.Status)),
				CreateAt: &chunk.CreateAt,
				ModifyAt: &chunk.ModifyAt,
			}
		}

		files = append(files, &tracker.FileMeta{
			Id:       file.Id,
			Name:     file.Name,
			Length:   int64(file.Length),
			Status:   base.FileStatus(file.Status),
			CreateAt: file.CreatAt,
			UpdateAt: file.UpdateAt,
			Chunks:   chunks,
		})
	}

	return &tracker.ListFilesResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Files: files,
	}, nil
}

// Heartbeat 处理存储节点的心跳请求
func (t *TrackerNode) Heartbeat(ctx context.Context, req *tracker.HeartbeatRequest) (*tracker.HeartbeatResponse, error) {
	t.nodeMux.Lock()
	defer t.nodeMux.Unlock()

	node, exists := t.nodes[uint64(req.NodeId)]
	if !exists {

		node = &tracker.NodeInfo{
			NodeId:        req.NodeId,
			Address:       req.Host + ":" + fmt.Sprint(req.Port),
			Status:        base.NodeStatus_ONLINE,
			ServerStats:   req.ServerStats,
			LastHeartbeat: time.Now().UnixNano(),
		}
		t.nodes[uint64(req.NodeId)] = node
		logs.Info("Auto registered node during heartbeat: %d", req.NodeId)
	} else {

		node.Address = req.Host + ":" + fmt.Sprint(req.Port)
		node.Status = base.NodeStatus_ONLINE
		node.ServerStats = req.ServerStats
		node.LastHeartbeat = time.Now().UnixNano()
	}

	if t.heartbeatLog != nil {
		err := t.heartbeatLog.Record(req.NodeId, req.Host, int32(req.Port), req.ServerStats)
		if err != nil {
			logs.Error("Failed to record heartbeat: %v", err)
		}
	}

	return &tracker.HeartbeatResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "Heartbeat received",
			Timestamp: time.Now().UnixNano(),
		},
	}, nil
}

// AcquireLock 获取文件锁
func (t *TrackerNode) AcquireLock(ctx context.Context, req *tracker.AcquireLockRequest) (*tracker.AcquireLockResponse, error) {
	clientId := requestClientId(req.Base)

	// 需要修改 FileMeta.LockInfo，必须持写锁（原实现持读锁写入，属于数据竞争）
	t.fileMux.Lock()
	defer t.fileMux.Unlock()

	fileMeta, ok := t.files[uint64(req.FileId)]
	if !ok {
		logs.Error("AcquireLock failed: file not found: %d", req.FileId)
		return nil, fmt.Errorf("file not found: %d", req.FileId)
	}

	if fileMeta.LockInfo != nil {
		// AcquireTime 是纳秒时间戳，LeaseTimeout 也必须是纳秒，
		// 原实现写死 30000 并直接比较纳秒差，等于没有租约。
		lease := time.Duration(fileMeta.LockInfo.LeaseTimeout)
		expired := time.Since(time.Unix(0, fileMeta.LockInfo.AcquireTime)) >= lease
		if !expired && fileMeta.LockInfo.ClientId != clientId {
			return &tracker.AcquireLockResponse{
				Base: &base.BaseResponse{
					Code:    base.ERR_LOCK_CONFLICT,
					Message: "file is locked by other client",
				},
				Success:     false,
				CurrentLock: convertLockInfoToThrift(fileMeta.LockInfo),
			}, nil
		}
	}

	lockInfo := &base.LockInfo{
		ClientId:     int64(clientId),
		LockType:     req.LockType,
		AcquireTime:  time.Now().UnixNano(),
		LeaseTimeout: int64(writeLeaseTTL), // 纳秒
		LockedChunks: req.Chunks,
	}

	// 更新文件的锁信息
	fileMeta.LockInfo = convertLockInfoFromThrift(lockInfo)

	return &tracker.AcquireLockResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Success:     true,
		CurrentLock: lockInfo,
	}, nil
}

// SyncReplica 处理副本同步请求
func (t *TrackerNode) SyncReplica(ctx context.Context, req *tracker.SyncReplicaRequest) (*tracker.SyncReplicaResponse, error) {
	t.fileMux.Lock()
	defer t.fileMux.Unlock()

	var chunkMeta *meta.ChunkMeta
	var fileMeta *meta.FileMeta
	for _, file := range t.files {
		if chunk, ok := file.Chunks[uint64(req.ChunkId)]; ok {
			chunkMeta = chunk
			fileMeta = file
			break
		}
	}

	if chunkMeta == nil || fileMeta == nil {
		logs.Error("SyncReplica failed: chunk not found: %d", req.ChunkId)
		return nil, fmt.Errorf("chunk not found: %d", req.ChunkId)
	}

	metaReplicaInfo := &meta.ReplicaInfo{
		NodeId:   uint64(req.NodeId),
		Status:   meta.VALID,
		SyncTime: time.Now().UnixNano(),
		Crc:      req.Crc,
	}

	if chunkMeta.Replicas == nil {
		chunkMeta.Replicas = make(map[uint64]*meta.ReplicaInfo)
	}
	chunkMeta.Replicas[uint64(req.NodeId)] = metaReplicaInfo

	replicaInfo := convertReplicaInfoToThrift(metaReplicaInfo)

	logs.Info("Replica sync successful: chunkId=%d, nodeId=%d", req.ChunkId, req.NodeId)

	return &tracker.SyncReplicaResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Success:     true,
		ReplicaInfo: replicaInfo,
	}, nil
}

func convertLockInfoToThrift(info *meta.LockInfo) *base.LockInfo {
	if info == nil {
		return nil
	}
	return &base.LockInfo{
		ClientId:     int64(info.ClientId),
		LockType:     base.LockType(info.LockType),
		AcquireTime:  info.AcquireTime,
		LeaseTimeout: info.LeaseTimeout,
		LockedChunks: info.LockedChunks,
	}
}

func convertLockInfoFromThrift(info *base.LockInfo) *meta.LockInfo {
	if info == nil {
		return nil
	}
	return &meta.LockInfo{
		ClientId:     uint64(info.ClientId),
		LockType:     meta.LockType(info.LockType),
		AcquireTime:  info.AcquireTime,
		LeaseTimeout: info.LeaseTimeout,
		LockedChunks: info.LockedChunks,
	}
}

// requestClientId 统一从 BaseRequest 中取客户端 ID。
// 目前客户端把 clientId 放在 RequestId 里传递，这里集中处理以兼容 Base 为空的情况。
func requestClientId(baseReq *base.BaseRequest) uint64 {
	if baseReq == nil {
		return 0
	}
	return uint64(baseReq.RequestId)
}

func convertReplicaInfoToThrift(info *meta.ReplicaInfo) *base.ReplicaInfo {
	if info == nil {
		return nil
	}
	return &base.ReplicaInfo{
		NodeId:   int64(info.NodeId),
		Status:   int32(info.Status),
		SyncTime: info.SyncTime,
		Crc:      info.Crc,
	}
}

func (t *TrackerNode) CreateBackup(ctx context.Context, req *tracker.CreateBackupRequest) (*tracker.CreateBackupResponse, error) {
	t.fileMux.RLock()
	defer t.fileMux.RUnlock()

	targetFiles := make([]*meta.FileMeta, 0)
	if len(req.FileIds) == 0 {
		for _, file := range t.files {
			targetFiles = append(targetFiles, file)
		}
	} else {

		for _, fileId := range req.FileIds {
			file, exists := t.files[uint64(fileId)]
			if !exists {
				return nil, fmt.Errorf("file not found: %d", fileId)
			}
			targetFiles = append(targetFiles, file)
		}
	}

	backupPath := req.Location
	if err := os.MkdirAll(backupPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create backup directory: %v", err)
	}

	backupMeta := &base.BackupMeta{
		Id:         time.Now().UnixNano(),
		Timestamp:  time.Now().UnixNano(),
		Location:   backupPath,
		FileChunks: make(map[int64][]int64),
	}

	totalSize := int64(0)

	for _, file := range targetFiles {
		chunkIds := make([]int64, 0)

		for chunkId := range file.Chunks {
			chunkIds = append(chunkIds, int64(chunkId))
			totalSize += int64(file.Chunks[chunkId].Size)
		}

		backupMeta.FileChunks[file.Id] = chunkIds
	}

	backupMeta.Size = totalSize

	metaPath := filepath.Join(backupPath, "backup_meta.json")
	metaData, err := json.Marshal(backupMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal backup metadata: %v", err)
	}

	if err := os.WriteFile(metaPath, metaData, 0644); err != nil {
		return nil, fmt.Errorf("failed to write backup metadata: %v", err)
	}

	return &tracker.CreateBackupResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Meta: backupMeta,
	}, nil
}

func (t *TrackerNode) GetAlerts(ctx context.Context, req *tracker.GetAlertsRequest) (*tracker.GetAlertsResponse, error) {
	// TODO 后续处理警告逻辑
	return &tracker.GetAlertsResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Alerts: make([]*base.AlertInfo, 0),
	}, nil
}

func (t *TrackerNode) GetClusterConfig(ctx context.Context, req *tracker.GetClusterConfigRequest) (*tracker.GetClusterConfigResponse, error) {

	rawConfig, err := config.GetRawConfig[config.TrackerNodeConfig]()
	if err != nil {
		return nil, fmt.Errorf("failed to get config: %v", err)
	}

	return &tracker.GetClusterConfigResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Config: &base.ClusterConfig{
			ElectionTimeoutMs:   int32(electionTimeout.Milliseconds()),
			HeartbeatIntervalMs: int32(heartbeatInterval.Milliseconds()),
			SnapshotIntervalSec: 3600,
			MaxLogSize:          1024 * 1024 * 100,
			MaxBatchSize:        1000,
			Sharding: &base.ShardingConfig{
				MaxChunkSize:     1024 * 1024 * 64,
				MinChunksPerFile: 1,
				MaxChunksPerFile: 1024,
				TargetChunkSize:  1024 * 1024 * 32,
			},
			Balance: &base.BalanceConfig{
				Strategy:           base.BalanceStrategy_HYBRID,
				CapacityThreshold:  0.85,
				IoThreshold:        0.75,
				NetworkThreshold:   0.75,
				MinFreeSpaceGb:     10,
				BalanceIntervalSec: 300,
			},
			Extra: map[string]string{
				"cluster_mode": rawConfig.ClusterMode,
				"node_role":    rawConfig.NodeRole,
			},
		},
	}, nil
}

func (t *TrackerNode) GetClusterStats(ctx context.Context, req *tracker.GetClusterStatsRequest) (*tracker.GetClusterStatsResponse, error) {
	t.nodeMux.RLock()
	defer t.nodeMux.RUnlock()

	resp := &tracker.GetClusterStatsResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		TotalNodes:    int32(len(t.nodes)),
		TotalCapacity: 0,
		UsedCapacity:  0,
		NodeStats:     make(map[int64]*tracker.NodeInfo),
	}

	for _, node := range t.nodes {
		resp.NodeStats[node.NodeId] = node
		resp.TotalCapacity += node.Capacity
		if node.ServerStats != nil && node.ServerStats.Disk != nil {
			resp.UsedCapacity += int64(float64(node.Capacity) * node.ServerStats.Disk.UsedPercent / 100)
		}
	}

	resp.BalanceConfig = &base.BalanceConfig{
		Strategy:           base.BalanceStrategy_HYBRID,
		CapacityThreshold:  85.0,
		IoThreshold:        80.0,
		NetworkThreshold:   75.0,
		MinFreeSpaceGb:     10,
		BalanceIntervalSec: 300,
	}

	return resp, nil
}

// ReleaseLock 释放文件锁
func (t *TrackerNode) ReleaseLock(ctx context.Context, req *tracker.UnlockFileRequest) (*base.BaseResponse, error) {
	resp, err := t.UnlockFile(ctx, req)
	if err != nil {
		return &base.BaseResponse{
			Code:      base.ERR_INTERNAL,
			Message:   err.Error(),
			Timestamp: time.Now().UnixNano(),
		}, err
	}
	return resp.Base, nil
}

// HealthCheck 健康检查
func (t *TrackerNode) HealthCheck(ctx context.Context, req *tracker.HealthCheckRequest) (*tracker.HealthCheckResponse, error) {
	t.nodeMux.RLock()
	defer t.nodeMux.RUnlock()

	node, exists := t.nodes[uint64(req.NodeId)]
	if !exists {
		return &tracker.HealthCheckResponse{
			Base: &base.BaseResponse{
				Code:      base.ERR_NOT_FOUND,
				Message:   "node not found",
				Timestamp: time.Now().UnixNano(),
			},
		}, nil
	}

	isHealthy := node.Status == base.NodeStatus_ONLINE &&
		time.Since(time.Unix(0, node.LastHeartbeat)) < nodeHeartbeatTimeout

	resp := &tracker.HealthCheckResponse{
		Base: &base.BaseResponse{
			Code:      0,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		Result_: &base.HealthCheckResult_{
			IsHealthy:           isHealthy,
			LastCheckTime:       time.Now().UnixNano(),
			ConsecutiveFailures: 0,
		},
	}

	return resp, nil
}

func (t *TrackerNode) GetNodes(ctx context.Context, req *tracker.GetNodesRequest) (*tracker.GetNodesResponse, error) {
	t.nodeMux.RLock()
	defer t.nodeMux.RUnlock()

	nodes := make([]*tracker.NodeInfo, 0, len(t.nodes))
	for _, node := range t.nodes {

		if req.Status != nil && node.Status != *req.Status {
			continue
		}

		if req.Role != nil && node.NodeStats != nil && node.NodeStats.Role != *req.Role {
			continue
		}
		nodes = append(nodes, node)
	}

	return &tracker.GetNodesResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		Nodes: nodes,
	}, nil
}

// UpdateNodeStatus 更新节点状态
func (t *TrackerNode) UpdateNodeStatus(ctx context.Context, req *tracker.UpdateNodeStatusRequest) (*tracker.UpdateNodeStatusResponse, error) {
	t.nodeMux.Lock()
	defer t.nodeMux.Unlock()

	node, exists := t.nodes[uint64(req.NodeId)]
	if !exists {
		return nil, fmt.Errorf("node not found: %d", req.NodeId)
	}

	node.Status = req.Status
	if req.Reason != nil {
		logs.Info("Node %d status updated to %v: %s", req.NodeId, req.Status, *req.Reason)
	} else {
		logs.Info("Node %d status updated to %v", req.NodeId, req.Status)
	}

	return &tracker.UpdateNodeStatusResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		Node: node,
	}, nil
}

// UpdateBalanceConfig 更新负载均衡配置
func (t *TrackerNode) UpdateBalanceConfig(ctx context.Context, req *tracker.UpdateBalanceConfigRequest) (*tracker.UpdateBalanceConfigResponse, error) {
	return &tracker.UpdateBalanceConfigResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		Success: true,
	}, nil
}

// UpdateShardingConfig 更新分片配置
func (t *TrackerNode) UpdateShardingConfig(ctx context.Context, req *tracker.UpdateShardingConfigRequest) (*tracker.UpdateShardingConfigResponse, error) {
	return &tracker.UpdateShardingConfigResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		Success: true,
	}, nil
}

// UpdateClusterConfig 更新集群配置
func (t *TrackerNode) UpdateClusterConfig(ctx context.Context, req *tracker.UpdateClusterConfigRequest) (*tracker.UpdateClusterConfigResponse, error) {
	return &tracker.UpdateClusterConfigResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		Success: true,
	}, nil
}

// ReportMetrics 上报监控指标
func (t *TrackerNode) ReportMetrics(ctx context.Context, req *tracker.ReportMetricsRequest) (*tracker.ReportMetricsResponse, error) {
	return &tracker.ReportMetricsResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
	}, nil
}

// GetMetrics 获取监控指标
func (t *TrackerNode) GetMetrics(ctx context.Context, req *tracker.GetMetricsRequest) (*tracker.GetMetricsResponse, error) {
	return &tracker.GetMetricsResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		Metrics: make(map[string][]*base.MetricValue),
	}, nil
}

// RestoreFromBackup 从备份恢复
func (t *TrackerNode) RestoreFromBackup(ctx context.Context, req *tracker.RestoreFromBackupRequest) (*tracker.RestoreFromBackupResponse, error) {
	return &tracker.RestoreFromBackupResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		RestoredFiles: make([]*tracker.FileMeta, 0),
	}, nil
}

// InstallSnapshot 安装快照
func (t *TrackerNode) InstallSnapshot(ctx context.Context, req *tracker.InstallSnapshotRequest) (*tracker.InstallSnapshotResponse, error) {
	return &tracker.InstallSnapshotResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		Term:        t.currentTerm(),
		BytesStored: req.Meta.Size,
	}, nil
}

// ProposeConfigChange 提议配置变更
func (t *TrackerNode) ProposeConfigChange(ctx context.Context, req *tracker.ProposeConfigChangeRequest) (*tracker.ProposeConfigChangeResponse, error) {
	return &tracker.ProposeConfigChangeResponse{
		Base: &base.BaseResponse{
			Code:      base.SUCCESS,
			Message:   "success",
			Timestamp: time.Now().UnixNano(),
		},
		Accepted: true,
		LogIndex: int64(t.raftLogCount()),
	}, nil
}

func (t *TrackerNode) Close() error {
	t.stopRaft()
	// 关闭前做一次同步落盘，避免丢失最近一次元数据变更
	t.stopPersister()
	if t.heartbeatLog != nil {
		if err := t.heartbeatLog.Close(); err != nil {
			logs.Error("Failed to close heartbeat recorder: %v", err)
		}
	}
	return nil
}
