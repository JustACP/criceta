package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker/trackerservice"
	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/JustACP/criceta/pkg/config"
	"github.com/cloudwego/kitex/client"
)

const (
	electionTimeout    = 150 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
	maxElectionTimeout = 300 * time.Millisecond
	// voteRequestTimeout 一轮选举最多等待多久，避免 goroutine 永久阻塞
	voteRequestTimeout = 2 * time.Second
)

// RaftState Raft 节点状态。
//
// 注意：这里只实现了「选举 + 心跳 + 日志追加」的最小闭环，
// 日志复制（Leader 主动 send/commit）仍未实现，Tracker 的元数据也还没有
// 走状态机。真正接入 Raft 之前，standalone 模式下不会启动选举。
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
	lastContact time.Time

	// peers 是 Tracker 集群成员（来自配置的 cluster_nodes / slave_of / slaves），
	// 不是 Storage 节点列表。
	peers []*tracker.NodeInfo

	// enabled=false 时完全不参与选举（standalone 模式）
	enabled bool

	stopCh   chan struct{}
	stopOnce sync.Once

	mu sync.RWMutex
}

// initRaft 根据集群模式初始化 Raft 状态。
// standalone（默认）下只保留状态对象，不启动选举协程，避免无意义的空转。
func (t *TrackerNode) initRaft() {
	enabled, peers := raftClusterFromConfig()

	t.raftState = &RaftState{
		currentTerm: 0,
		votedFor:    -1,
		logs:        make([]*base.RaftLogEntry, 0),
		commitIndex: -1,
		lastApplied: -1,
		nextIndex:   make(map[int64]int64),
		matchIndex:  make(map[int64]int64),
		role:        base.NodeRole_FOLLOWER,
		peers:       peers,
		enabled:     enabled,
		stopCh:      make(chan struct{}),
		lastContact: time.Now(),
	}

	if enabled {
		logs.Info("Raft enabled with %d peer(s)", len(peers))
		go t.runElectionTimer()
	}
}

// raftClusterFromConfig 从配置中解析集群模式与 Tracker 成员。
// 配置不可用（例如单测环境）时按 standalone 处理。
func raftClusterFromConfig() (bool, []*tracker.NodeInfo) {
	rawConfig, err := config.GetRawConfig[config.TrackerNodeConfig]()
	if err != nil {
		return false, nil
	}

	switch rawConfig.ClusterMode {
	case "cluster":
		return true, trackerPeerInfos(rawConfig.ClusterNodes)
	case "master-slave":
		slaves := make([]config.ServerConfig, 0, len(rawConfig.Slaves)+1)
		if rawConfig.SlaveOf != nil {
			slaves = append(slaves, *rawConfig.SlaveOf)
		}
		slaves = append(slaves, rawConfig.Slaves...)
		return true, trackerPeerInfos(slaves)
	default: // standalone
		return false, nil
	}
}

func trackerPeerInfos(servers []config.ServerConfig) []*tracker.NodeInfo {
	peers := make([]*tracker.NodeInfo, 0, len(servers))
	for _, srv := range servers {
		if srv.ServerIP == nil {
			continue
		}
		peers = append(peers, &tracker.NodeInfo{
			NodeId:  srv.ServerId,
			Address: fmt.Sprintf("%s:%d", *srv.ServerIP, srv.Port),
			Status:  base.NodeStatus_ONLINE,
		})
	}
	return peers
}

func (t *TrackerNode) runElectionTimer() {
	for {
		timer := time.NewTimer(randomElectionTimeout())
		select {
		case <-t.raftState.stopCh:
			timer.Stop()
			return
		case <-timer.C:
		}

		if t.shouldStartElection() {
			t.startElection()
		}
	}
}

func randomElectionTimeout() time.Duration {
	jitter := time.Duration(rand.Int63n(int64(maxElectionTimeout - electionTimeout)))
	return electionTimeout + jitter
}

func (t *TrackerNode) shouldStartElection() bool {
	t.raftState.mu.RLock()
	defer t.raftState.mu.RUnlock()

	if t.raftState.role == base.NodeRole_LEADER {
		return false
	}
	return time.Since(t.raftState.lastContact) >= electionTimeout
}

// startElection 拉票并尝试成为 Leader。
//
// 选举在独立的 goroutine 中运行，有两个约束：
//  1. 调用方不得持有 raftState.mu（sync.Mutex 不可重入）；
//  2. 单节点集群直接当选，避免空集群永远选不出 Leader。
func (t *TrackerNode) startElection() {
	t.raftState.mu.Lock()
	if t.raftState.role == base.NodeRole_LEADER {
		t.raftState.mu.Unlock()
		return
	}

	t.raftState.role = base.NodeRole_CANDIDATE
	t.raftState.currentTerm++
	term := t.raftState.currentTerm
	t.raftState.votedFor = t.nodeId
	t.raftState.lastContact = time.Now()

	lastLogIndex := int64(len(t.raftState.logs) - 1)
	lastLogTerm := int64(0)
	if lastLogIndex >= 0 {
		lastLogTerm = t.raftState.logs[lastLogIndex].Term
	}
	peers := append([]*tracker.NodeInfo(nil), t.raftState.peers...)
	t.raftState.mu.Unlock()

	// 单节点集群直接当选
	if len(peers) == 0 {
		t.becomeLeader(term)
		return
	}

	voteCh := make(chan bool, len(peers))
	for _, peer := range peers {
		go t.requestVote(peer, term, lastLogIndex, lastLogTerm, voteCh)
	}

	votes := 1
	majority := len(peers)/2 + 1
	deadline := time.NewTimer(voteRequestTimeout)
	defer deadline.Stop()

	for votes < majority {
		select {
		case granted := <-voteCh:
			if granted {
				votes++
			}
		case <-deadline.C:
			logs.Warn("Election for term %d timed out with %d/%d votes", term, votes, majority)
			t.stepDownIfCandidate(term)
			return
		case <-t.raftState.stopCh:
			return
		}
	}

	t.becomeLeader(term)
}

// requestVote 向单个 peer 拉票，结果写入 voteCh
func (t *TrackerNode) requestVote(peer *tracker.NodeInfo, term, lastLogIndex, lastLogTerm int64, voteCh chan<- bool) {
	cli, err := trackerservice.NewClient(
		fmt.Sprintf("tracker-%d", peer.NodeId),
		client.WithHostPorts(peer.Address),
		client.WithRPCTimeout(voteRequestTimeout),
	)
	if err != nil {
		logs.Error("Failed to create tracker client for node %d: %v", peer.NodeId, err)
		voteCh <- false
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), voteRequestTimeout)
	defer cancel()

	resp, err := cli.RequestVote(ctx, &tracker.RequestVoteRequest{
		Base: &base.BaseRequest{
			RequestId: time.Now().UnixNano(),
			Timestamp: time.Now().UnixNano(),
		},
		Term:         term,
		CandidateId:  t.nodeId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	})
	if err != nil {
		voteCh <- false
		return
	}

	// 对方任期更高，立刻退回 follower
	if resp.Term > term {
		t.stepDownIfCandidate(term)
	}

	voteCh <- resp.VoteGranted
}

func (t *TrackerNode) becomeLeader(term int64) {
	t.raftState.mu.Lock()
	if t.raftState.currentTerm != term || t.raftState.role != base.NodeRole_CANDIDATE {
		t.raftState.mu.Unlock()
		return
	}

	t.raftState.role = base.NodeRole_LEADER
	t.raftState.leaderId = t.nodeId
	t.raftState.lastContact = time.Now()
	for _, peer := range t.raftState.peers {
		t.raftState.nextIndex[peer.NodeId] = int64(len(t.raftState.logs))
		t.raftState.matchIndex[peer.NodeId] = -1
	}
	t.raftState.mu.Unlock()

	logs.Info("Tracker node %d became leader for term %d", t.nodeId, term)
	go t.runLeaderHeartbeat(term)
}

// stepDownIfCandidate 若仍是该任期的候选者则退回 follower
func (t *TrackerNode) stepDownIfCandidate(term int64) {
	t.raftState.mu.Lock()
	defer t.raftState.mu.Unlock()

	if t.raftState.role == base.NodeRole_CANDIDATE && t.raftState.currentTerm == term {
		t.raftState.role = base.NodeRole_FOLLOWER
	}
}

func (t *TrackerNode) isLeader(term int64) bool {
	t.raftState.mu.RLock()
	defer t.raftState.mu.RUnlock()
	return t.raftState.role == base.NodeRole_LEADER && t.raftState.currentTerm == term
}

// runLeaderHeartbeat 定期向所有 peer 发送空 AppendEntries，维持领导权并刷新 follower 的选举计时
func (t *TrackerNode) runLeaderHeartbeat(term int64) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.raftState.stopCh:
			return
		case <-ticker.C:
		}

		if !t.isLeader(term) {
			return
		}
		t.broadcastAppendEntries(term)
	}
}

func (t *TrackerNode) broadcastAppendEntries(term int64) {
	t.raftState.mu.RLock()
	peers := append([]*tracker.NodeInfo(nil), t.raftState.peers...)
	prevLogIndex := int64(len(t.raftState.logs) - 1)
	prevLogTerm := int64(0)
	if prevLogIndex >= 0 {
		prevLogTerm = t.raftState.logs[prevLogIndex].Term
	}
	leaderCommit := t.raftState.commitIndex
	t.raftState.mu.RUnlock()

	for _, peer := range peers {
		go func(peer *tracker.NodeInfo) {
			cli, err := trackerservice.NewClient(
				fmt.Sprintf("tracker-%d", peer.NodeId),
				client.WithHostPorts(peer.Address),
				client.WithRPCTimeout(voteRequestTimeout),
			)
			if err != nil {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), voteRequestTimeout)
			defer cancel()

			resp, err := cli.AppendEntries(ctx, &tracker.AppendEntriesRequest{
				Base: &base.BaseRequest{
					RequestId: time.Now().UnixNano(),
					Timestamp: time.Now().UnixNano(),
				},
				Term:         term,
				LeaderId:     t.nodeId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil,
				LeaderCommit: leaderCommit,
			})
			if err != nil {
				return
			}

			if resp.Term > term {
				t.raftState.mu.Lock()
				if resp.Term > t.raftState.currentTerm {
					t.raftState.role = base.NodeRole_FOLLOWER
					t.raftState.currentTerm = resp.Term
					t.raftState.votedFor = -1
					t.raftState.lastContact = time.Now()
				}
				t.raftState.mu.Unlock()
			}
		}(peer)
	}
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

	if !t.raftState.enabled {
		return resp, nil
	}

	if req.Term < t.raftState.currentTerm {
		return resp, nil
	}

	if req.Term > t.raftState.currentTerm {
		t.raftState.currentTerm = req.Term
		t.raftState.role = base.NodeRole_FOLLOWER
		t.raftState.votedFor = -1
	}

	lastLogIndex := int64(len(t.raftState.logs) - 1)
	lastLogTerm := int64(0)
	if lastLogIndex >= 0 {
		lastLogTerm = t.raftState.logs[lastLogIndex].Term
	}

	if (t.raftState.votedFor == -1 || t.raftState.votedFor == req.CandidateId) &&
		(req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)) {
		t.raftState.votedFor = req.CandidateId
		t.raftState.lastContact = time.Now()
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
	t.raftState.lastContact = time.Now()
	if req.Term > t.raftState.currentTerm {
		t.raftState.currentTerm = req.Term
		t.raftState.votedFor = -1
	}

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
	// TODO: 解析日志数据并执行相应的操作（元数据状态机尚未接入 Raft）
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

// stopRaft 停止选举/心跳协程
func (t *TrackerNode) stopRaft() {
	if t.raftState == nil {
		return
	}
	t.raftState.stopOnce.Do(func() {
		close(t.raftState.stopCh)
	})
}

func (t *TrackerNode) currentTerm() int64 {
	t.raftState.mu.RLock()
	defer t.raftState.mu.RUnlock()
	return t.raftState.currentTerm
}

func (t *TrackerNode) raftLogCount() int {
	t.raftState.mu.RLock()
	defer t.raftState.mu.RUnlock()
	return len(t.raftState.logs)
}
