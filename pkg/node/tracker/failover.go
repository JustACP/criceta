package tracker

//import (
//	"context"
//	"fmt"
//	"sync"
//	"time"
//
//	"github.com/JustACP/criceta/kitex_gen/criceta/base"
//	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
//	"github.com/JustACP/criceta/kitex_gen/criceta/tracker/trackerservice"
//	"github.com/JustACP/criceta/pkg/common/logs"
//	"github.com/JustACP/criceta/pkg/config"
//	"github.com/cloudwego/kitex/client"
//)
//
//type FailoverManager struct {
//	node              *TrackerNode
//	failoverInProcess bool
//	lastFailoverTime  time.Time
//	mu                sync.RWMutex
//}
//
//func NewFailoverManager(node *TrackerNode) *FailoverManager {
//	return &FailoverManager{
//		node: node,
//	}
//}
//
//// TryFailover 尝试进行主从切换
//func (fm *FailoverManager) TryFailover() error {
//	fm.mu.Lock()
//	if fm.failoverInProcess {
//		fm.mu.Unlock()
//		return fmt.Errorf("failover already in progress")
//	}
//	fm.failoverInProcess = true
//	fm.mu.Unlock()
//
//	defer func() {
//		fm.mu.Lock()
//		fm.failoverInProcess = false
//		fm.mu.Unlock()
//	}()
//
//	// 1. 检查是否满足切换条件
//	if !fm.shouldFailover() {
//		return nil
//	}
//
//	// 2. 停止从主节点同步
//	if fm.node.syncManager != nil {
//		fm.node.syncManager.Stop()
//	}
//
//	// 3. 提升为主节点
//	if err := fm.promoteToMaster(); err != nil {
//		return fmt.Errorf("failed to promote to master: %v", err)
//	}
//
//	fm.lastFailoverTime = time.Now()
//	return nil
//}
//
//func (fm *FailoverManager) shouldFailover() bool {
//	// 如果已经是主节点，不需要切换
//	if fm.node.isMaster {
//		return false
//	}
//
//	// 检查距离上次切换是否已经超过5分钟
//	if time.Since(fm.lastFailoverTime) < 5*time.Minute {
//		return false
//	}
//
//	// 检查主节点是否无法访问
//	if err := fm.checkMasterHealth(); err != nil {
//		logs.Error("主节点健康检查失败: %v", err)
//		return true
//	}
//
//	return false
//}
//
//func (fm *FailoverManager) checkMasterHealth() error {
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//
//	req := &tracker.HealthCheckRequest{
//		Base: &base.BaseRequest{
//			RequestId: time.Now().UnixNano(),
//			Timestamp: time.Now().UnixNano(),
//		},
//	}
//
//	if fm.node.syncManager == nil || fm.node.syncManager.masterClient == nil {
//		return fmt.Errorf("sync manager or master client not initialized")
//	}
//
//	_, err := fm.node.syncManager.masterClient.HealthCheck(ctx, req)
//	return err
//}
//
//func (fm *FailoverManager) promoteToMaster() error {
//	// 1. 更新节点状态
//	fm.node.isMaster = true
//	fm.node.masterAddr = ""
//
//	// 2. 更新配置
//	rawConfig, err := config.GetRawConfig[config.TrackerNodeConfig]()
//	if err != nil {
//		return fmt.Errorf("failed to load config: %v", err)
//	}
//
//	rawConfig.NodeRole = "master"
//	rawConfig.SlaveOf = nil
//
//	if err := config.GetInstanceConfig().SaveConfig(); err != nil {
//		return fmt.Errorf("failed to save config: %v", err)
//	}
//
//	// 3. 通知其他从节点
//	if err := fm.notifySlaves(); err != nil {
//		logs.Error("通知从节点失败: %v", err)
//	}
//
//	logs.Info("节点已成功提升为主节点")
//	return nil
//}
//
//func (fm *FailoverManager) notifySlaves() error {
//	ctx := context.Background()
//	req := &tracker.UpdateNodeStatusRequest{
//		Base: &base.BaseRequest{
//			RequestId: time.Now().UnixNano(),
//			Timestamp: time.Now().UnixNano(),
//		},
//		NodeId: fm.node.nodeId,
//		Status: base.NodeStatus_ONLINE,
//	}
//
//	fm.node.nodeMux.RLock()
//	defer fm.node.nodeMux.RUnlock()
//
//	for _, node := range fm.node.nodes {
//		if node.NodeId == fm.node.nodeId {
//			continue
//		}
//
//		// 异步通知每个从节点
//		go func(nodeInfo *tracker.NodeInfo) {
//			client, err := trackerservice.NewClient(
//				fmt.Sprintf("tracker-%d", nodeInfo.NodeId),
//				client.WithHostPorts(nodeInfo.Address),
//			)
//			if err != nil {
//				logs.Error("创建从节点客户端失败: %v", err)
//				return
//			}
//
//			_, err = client.UpdateNodeStatus(ctx, req)
//			if err != nil {
//				logs.Error("通知从节点失败 [%d]: %v", nodeInfo.NodeId, err)
//			}
//		}(node)
//	}
//
//	return nil
//}
