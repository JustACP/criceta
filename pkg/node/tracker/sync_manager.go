package tracker

//import (
//	"context"
//	"sync"
//	"time"
//
//	"github.com/JustACP/criceta/kitex_gen/criceta/base"
//	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
//	"github.com/JustACP/criceta/kitex_gen/criceta/tracker/trackerservice"
//	"github.com/JustACP/criceta/pkg/common/logs"
//	"github.com/JustACP/criceta/pkg/meta"
//	"github.com/cloudwego/kitex/client"
//)
//
//type SyncManager struct {
//	node           *TrackerNode
//	masterClient   trackerservice.Client
//	syncInterval   time.Duration
//	lastSyncTime   time.Time
//	syncInProgress bool
//	stopChan       chan struct{}
//	mu             sync.RWMutex
//}
//
//func NewSyncManager(node *TrackerNode, masterAddr string) (*SyncManager, error) {
//	masterClient, err := trackerservice.NewClient(
//		"tracker.master",
//		client.WithHostPorts(masterAddr),
//	)
//	if err != nil {
//		return nil, err
//	}
//
//	return &SyncManager{
//		node:         node,
//		masterClient: masterClient,
//		syncInterval: 5 * time.Second,
//		stopChan:     make(chan struct{}),
//	}, nil
//}
//
//func (sm *SyncManager) Start() {
//	go sm.syncLoop()
//}
//
//func (sm *SyncManager) Stop() {
//	close(sm.stopChan)
//}
//
//func (sm *SyncManager) syncLoop() {
//	ticker := time.NewTicker(sm.syncInterval)
//	defer ticker.Stop()
//
//	for {
//		select {
//		case <-sm.stopChan:
//			return
//		case <-ticker.C:
//			if err := sm.syncWithMaster(); err != nil {
//				logs.Error("同步主节点数据失败: %v", err)
//			}
//		}
//	}
//}
//
//func (sm *SyncManager) syncWithMaster() error {
//	sm.mu.Lock()
//	if sm.syncInProgress {
//		sm.mu.Unlock()
//		return nil
//	}
//	sm.syncInProgress = true
//	sm.mu.Unlock()
//	defer func() {
//		sm.mu.Lock()
//		sm.syncInProgress = false
//		sm.mu.Unlock()
//	}()
//
//	// 1. 同步文件元数据
//	if err := sm.syncFileMeta(); err != nil {
//		return err
//	}
//
//	// 2. 同步节点信息
//	if err := sm.syncNodeInfo(); err != nil {
//		return err
//	}
//
//	// 3. 同步集群配置
//	if err := sm.syncClusterConfig(); err != nil {
//		return err
//	}
//
//	sm.lastSyncTime = time.Now()
//	return nil
//}
//
//func (sm *SyncManager) syncFileMeta() error {
//	ctx := context.Background()
//	req := &tracker.ListFilesRequest{
//		Base: &base.BaseRequest{
//			RequestId: time.Now().UnixNano(),
//			Timestamp: time.Now().UnixNano(),
//		},
//		Status: int32(base.FileStatus_VALID),
//	}
//
//	resp, err := sm.masterClient.ListFiles(ctx, req)
//	if err != nil {
//		return err
//	}
//
//	sm.node.fileMux.Lock()
//	defer sm.node.fileMux.Unlock()
//
//	// 更新文件元数据
//	for _, file := range resp.Files {
//		sm.node.files[uint64(file.Id)] = &meta.FileMeta{
//			Id:         file.Id,
//			Name:       file.Name,
//			Length:     uint64(file.Length),
//			Status:     meta.FileStatus(file.Status),
//			Version:    uint64(file.Version),
//			CreatAt:    file.CreateAt,
//			UpdateAt:   file.UpdateAt,
//			Size:       file.Size,
//			ModifyTime: file.ModifyTime,
//		}
//	}
//
//	return nil
//}
//
//func (sm *SyncManager) syncNodeInfo() error {
//	ctx := context.Background()
//	req := &tracker.GetNodesRequest{
//		Base: &base.BaseRequest{
//			RequestId: time.Now().UnixNano(),
//			Timestamp: time.Now().UnixNano(),
//		},
//	}
//
//	resp, err := sm.masterClient.GetNodes(ctx, req)
//	if err != nil {
//		return err
//	}
//
//	sm.node.nodeMux.Lock()
//	defer sm.node.nodeMux.Unlock()
//
//	// 更新节点信息
//	for _, node := range resp.Nodes {
//		sm.node.nodes[uint64(node.NodeId)] = node
//	}
//
//	return nil
//}
//
//func (sm *SyncManager) syncClusterConfig() error {
//	ctx := context.Background()
//	req := &tracker.GetClusterConfigRequest{
//		Base: &base.BaseRequest{
//			RequestId: time.Now().UnixNano(),
//			Timestamp: time.Now().UnixNano(),
//		},
//	}
//
//	resp, err := sm.masterClient.GetClusterConfig(ctx, req)
//	if err != nil {
//		return err
//	}
//
//	// 更新集群配置
//	sm.node.clusterConfig = resp.Config
//	return nil
//}
