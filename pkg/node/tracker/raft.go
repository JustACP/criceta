package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker/trackerservice"
	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/cloudwego/kitex/client"
)

const (
	electionTimeout    = 150 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
	maxElectionTimeout = 300 * time.Millisecond
)

type RaftState struct {
	currentTerm int64
	votedFor    int64
	logs        []*base.RaftLogEntry
	commitIndex int64
	lastApplied int64
	nextIndex   map[int64]int64
	matchIndex  map[int64]int64
	role        base.NodeRole
	leaderId    int64
	mu          sync.RWMutex
}

func (t *TrackerNode) initRaft() {
	t.raftState = &RaftState{
		currentTerm: 0,
		votedFor:    -1,
		logs:        make([]*base.RaftLogEntry, 0),
		commitIndex: -1,
		lastApplied: -1,
		nextIndex:   make(map[int64]int64),
		matchIndex:  make(map[int64]int64),
		role:        base.NodeRole_FOLLOWER,
	}
	go t.runElectionTimer()
}

func (t *TrackerNode) runElectionTimer() {
	for {
		timeout := time.Duration(electionTimeout + time.Duration(t.nodeId%10)*10*time.Millisecond)
		timer := time.NewTimer(timeout)
		<-timer.C

		t.raftState.mu.Lock()
		if t.raftState.role == base.NodeRole_FOLLOWER {
			t.startElection()
		}
		t.raftState.mu.Unlock()
	}
}

func (t *TrackerNode) startElection() {
	t.raftState.mu.Lock()
	defer t.raftState.mu.Unlock()

	t.raftState.role = base.NodeRole_CANDIDATE
	t.raftState.currentTerm++
	t.raftState.votedFor = int64(t.nodeId)

	voteChan := make(chan bool, len(t.nodes))
	votes := 1
	majority := len(t.nodes)/2 + 1

	// 发送RequestVote RPC
	for nodeId, node := range t.nodes {
		if int64(nodeId) == int64(t.nodeId) {
			continue
		}

		go func(nodeId int64, node *tracker.NodeInfo) {
			lastLogIndex := int64(-1)
			lastLogTerm := int64(0)
			if len(t.raftState.logs) > 0 {
				lastLogIndex = int64(len(t.raftState.logs) - 1)
				lastLogTerm = t.raftState.logs[lastLogIndex].Term
			}

			req := &tracker.RequestVoteRequest{
				Base: &base.BaseRequest{
					RequestId: time.Now().UnixNano(),
					Timestamp: time.Now().UnixNano(),
				},
				Term:         t.raftState.currentTerm,
				CandidateId:  int64(t.nodeId),
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			// 创建节点客户端
			client, err := trackerservice.NewClient(
				fmt.Sprintf("tracker-%d", nodeId),
				client.WithHostPorts(node.Address),
				client.WithRPCTimeout(time.Second*5),
			)
			if err != nil {
				logs.Error("Failed to create client for node %d: %v", nodeId, err)
				voteChan <- false
				return
			}

			// 发送投票请求
			resp, err := client.RequestVote(context.Background(), req)
			if err != nil {
				logs.Error("Failed to send RequestVote to node %d: %v", nodeId, err)
				voteChan <- false
				return
			}

			voteChan <- resp.VoteGranted
		}(int64(nodeId), node)
	}

	// 等待投票结果
	go func() {
		for i := 0; i < len(t.nodes)-1; i++ {
			if <-voteChan {
				votes++
				if votes >= majority {
					t.raftState.mu.Lock()
					if t.raftState.role == base.NodeRole_CANDIDATE {
						t.raftState.role = base.NodeRole_LEADER
						// 初始化leader状态
						for nodeId := range t.nodes {
							t.raftState.nextIndex[int64(nodeId)] = int64(len(t.raftState.logs))
							t.raftState.matchIndex[int64(nodeId)] = -1
						}
					}
					t.raftState.mu.Unlock()
					return
				}
			}
		}
		// 如果没有获得足够的票数，保持follower状态
		t.raftState.mu.Lock()
		if t.raftState.role == base.NodeRole_CANDIDATE {
			t.raftState.role = base.NodeRole_FOLLOWER
		}
		t.raftState.mu.Unlock()
	}()
}

func (t *TrackerNode) RequestVote(ctx context.Context, req *tracker.RequestVoteRequest) (*tracker.RequestVoteResponse, error) {
	t.raftState.mu.Lock()
	defer t.raftState.mu.Unlock()

	resp := &tracker.RequestVoteResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Term:        t.raftState.currentTerm,
		VoteGranted: false,
	}

	if req.Term < t.raftState.currentTerm {
		return resp, nil
	}

	if req.Term > t.raftState.currentTerm {
		t.raftState.currentTerm = req.Term
		t.raftState.role = base.NodeRole_FOLLOWER
		t.raftState.votedFor = -1
	}

	lastLogIndex := int64(-1)
	lastLogTerm := int64(0)
	if len(t.raftState.logs) > 0 {
		lastLogIndex = int64(len(t.raftState.logs) - 1)
		lastLogTerm = t.raftState.logs[lastLogIndex].Term
	}

	if (t.raftState.votedFor == -1 || t.raftState.votedFor == req.CandidateId) &&
		(req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)) {
		t.raftState.votedFor = req.CandidateId
		resp.VoteGranted = true
	}

	return resp, nil
}

func (t *TrackerNode) AppendEntries(ctx context.Context, req *tracker.AppendEntriesRequest) (*tracker.AppendEntriesResponse, error) {
	t.raftState.mu.Lock()
	defer t.raftState.mu.Unlock()

	resp := &tracker.AppendEntriesResponse{
		Base: &base.BaseResponse{
			Code:    0,
			Message: "success",
		},
		Term:    t.raftState.currentTerm,
		Success: false,
	}

	if req.Term < t.raftState.currentTerm {
		return resp, nil
	}

	// 重置选举计时器
	t.raftState.role = base.NodeRole_FOLLOWER
	t.raftState.leaderId = req.LeaderId

	// 日志一致性检查
	if req.PrevLogIndex >= 0 {
		if req.PrevLogIndex >= int64(len(t.raftState.logs)) {
			return resp, nil
		}
		if t.raftState.logs[req.PrevLogIndex].Term != req.PrevLogTerm {
			return resp, nil
		}
	}

	// 追加或覆盖日志条目
	for i, entry := range req.Entries {
		index := req.PrevLogIndex + int64(i) + 1
		if index < int64(len(t.raftState.logs)) {
			if t.raftState.logs[index].Term != entry.Term {
				// 删除该位置及之后的所有日志
				t.raftState.logs = t.raftState.logs[:index]
			} else {
				continue
			}
		}
		t.raftState.logs = append(t.raftState.logs, entry)
	}

	// 更新提交索引
	if req.LeaderCommit > t.raftState.commitIndex {
		t.raftState.commitIndex = min(req.LeaderCommit, int64(len(t.raftState.logs)-1))
		// 应用日志到状态机
		go t.applyLogs()
	}

	resp.Success = true
	resp.MatchIndex = int64(len(t.raftState.logs) - 1)
	return resp, nil
}

// applyLogs 应用已提交的日志到状态机
func (t *TrackerNode) applyLogs() {
	t.raftState.mu.Lock()
	defer t.raftState.mu.Unlock()

	for t.raftState.lastApplied < t.raftState.commitIndex {
		t.raftState.lastApplied++
		entry := t.raftState.logs[t.raftState.lastApplied]

		// 根据日志类型执行相应的操作
		switch entry.Type {
		case base.RaftLogType_NORMAL:
			// 普通操作日志
			t.applyNormalLog(entry)
		case base.RaftLogType_CONFIG:
			// 配置变更日志
			t.applyConfigLog(entry)
		case base.RaftLogType_SNAPSHOT:
			// 快照日志
			t.applySnapshotLog(entry)
		}
	}
}

// applyNormalLog 应用普通操作日志
func (t *TrackerNode) applyNormalLog(entry *base.RaftLogEntry) {
	// TODO: 解析日志数据并执行相应的操作
	logs.Info("Applying normal log: index=%d, term=%d", entry.Index, entry.Term)
}

// applyConfigLog 应用配置变更日志
func (t *TrackerNode) applyConfigLog(entry *base.RaftLogEntry) {
	// 解析配置变更并应用
	var configChange base.RaftConfigChange
	if err := json.Unmarshal(entry.Data, &configChange); err != nil {
		logs.Error("Failed to unmarshal config change: %v", err)
		return
	}

	switch configChange.Type {
	case base.RaftConfigChangeType_ADD_NODE:
		// 添加节点
		t.addNode(&configChange)
	case base.RaftConfigChangeType_REMOVE_NODE:
		// 移除节点
		t.removeNode(&configChange)
	case base.RaftConfigChangeType_UPDATE_NODE:
		// 更新节点
		t.updateNode(&configChange)
	}
}

// applySnapshotLog 应用快照日志
func (t *TrackerNode) applySnapshotLog(entry *base.RaftLogEntry) {
	// 解析快照元数据并应用
	var snapshotMeta base.SnapshotMeta
	if err := json.Unmarshal(entry.Data, &snapshotMeta); err != nil {
		logs.Error("Failed to unmarshal snapshot meta: %v", err)
		return
	}

	logs.Info("Applying snapshot: index=%d, term=%d", snapshotMeta.Index, entry.Term)
}

// 节点管理相关方法
func (t *TrackerNode) addNode(config *base.RaftConfigChange) {
	t.nodeMux.Lock()
	defer t.nodeMux.Unlock()

	t.nodes[uint64(config.NodeId)] = &tracker.NodeInfo{
		NodeId:  config.NodeId,
		Address: config.Address,
		Status:  base.NodeStatus_ONLINE,
	}
}

func (t *TrackerNode) removeNode(config *base.RaftConfigChange) {
	t.nodeMux.Lock()
	defer t.nodeMux.Unlock()

	delete(t.nodes, uint64(config.NodeId))
}

func (t *TrackerNode) updateNode(config *base.RaftConfigChange) {
	t.nodeMux.Lock()
	defer t.nodeMux.Unlock()

	if node, exists := t.nodes[uint64(config.NodeId)]; exists {
		node.Address = config.Address
	}
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
