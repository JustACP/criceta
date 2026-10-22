package tracker

//import (
//	"encoding/json"
//	"fmt"
//	"os"
//	"path/filepath"
//	"time"
//
//	"github.com/JustACP/criceta/kitex_gen/criceta/base"
//	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
//	"github.com/JustACP/criceta/pkg/common/logs"
//	"github.com/JustACP/criceta/pkg/meta"
//)
//
//const (
//	persistInterval = 5 * time.Minute
//	filesDataFile   = "files.json"
//	nodesDataFile   = "nodes.json"
//	raftDataFile    = "raft.json"
//)
//
//type persistentData struct {
//	Files     map[uint64]*meta.FileMeta    `json:"files"`
//	Nodes     map[uint64]*tracker.NodeInfo `json:"nodes"`
//	RaftState *RaftState                   `json:"raft_state"`
//	Term      int64                        `json:"term"`
//	Role      base.NodeRole                `json:"role"`
//}
//
//// startPersistence 启动定期持久化
//func (t *TrackerNode) startPersistence() {
//	t.persistTicker = time.NewTicker(persistInterval)
//	go func() {
//		for range t.persistTicker.C {
//			if err := t.persistData(); err != nil {
//				logs.Error("持久化数据失败: %v", err)
//			}
//		}
//	}()
//}
//
//// persistData 将数据持久化到本地
//func (t *TrackerNode) persistData() error {
//	t.fileMux.RLock()
//	t.nodeMux.RLock()
//	t.raftMux.RLock()
//	defer func() {
//		t.raftMux.RUnlock()
//		t.nodeMux.RUnlock()
//		t.fileMux.RUnlock()
//	}()
//
//	data := &persistentData{
//		Files:     t.files,
//		Nodes:     t.nodes,
//		RaftState: t.raftState,
//		Term:      t.term,
//		Role:      t.role,
//	}
//
//	jsonData, err := json.Marshal(data)
//	if err != nil {
//		return fmt.Errorf("序列化数据失败: %v", err)
//	}
//
//	dataFile := filepath.Join(t.dataDir, "tracker_data.json")
//	if err := os.WriteFile(dataFile, jsonData, 0644); err != nil {
//		return fmt.Errorf("写入数据文件失败: %v", err)
//	}
//
//	logs.Info("数据已成功持久化到: %s", dataFile)
//	return nil
//}
//
//// loadPersistedData 从本地加载持久化的数据
//func (t *TrackerNode) loadPersistedData() error {
//	dataFile := filepath.Join(t.dataDir, "tracker_data.json")
//	jsonData, err := os.ReadFile(dataFile)
//	if err != nil {
//		if os.IsNotExist(err) {
//			logs.Info("没有找到持久化数据文件，将使用空数据初始化")
//			return nil
//		}
//		return fmt.Errorf("读取数据文件失败: %v", err)
//	}
//
//	var data persistentData
//	if err := json.Unmarshal(jsonData, &data); err != nil {
//		return fmt.Errorf("解析数据文件失败: %v", err)
//	}
//
//	t.fileMux.Lock()
//	t.nodeMux.Lock()
//	t.raftMux.Lock()
//	defer func() {
//		t.raftMux.Unlock()
//		t.nodeMux.Unlock()
//		t.fileMux.Unlock()
//	}()
//
//	t.files = data.Files
//	t.nodes = data.Nodes
//	t.raftState = data.RaftState
//	t.term = data.Term
//	t.role = data.Role
//
//	logs.Info("成功加载持久化数据")
//	return nil
//}
//
//// stopPersistence 停止定期持久化
//func (t *TrackerNode) stopPersistence() {
//	if t.persistTicker != nil {
//		t.persistTicker.Stop()
//	}
//
//	// 最后执行一次持久化
//	if err := t.persistData(); err != nil {
//		logs.Error("最终数据持久化失败: %v", err)
//	}
//}
