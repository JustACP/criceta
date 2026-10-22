package tracker

import (
	"context"
	"testing"
	"time"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestTrackerNode(t *testing.T) *TrackerNode {
	t.Helper()
	node := NewTrackerNodeWithDataDir(t.TempDir())
	require.NotNil(t, node)
	t.Cleanup(func() { _ = node.Close() })
	return node
}

func TestTrackerNode_CreateFile(t *testing.T) {
	node := newTestTrackerNode(t)
	ctx := context.Background()

	// 测试创建文件
	req := &tracker.CreateFileRequest{
		Base:   &base.BaseRequest{RequestId: 1},
		Name:   "test.txt",
		Length: 1024,
	}

	resp, err := node.CreateFile(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "test.txt", resp.Meta.Name)

	// 测试重复创建
	_, err = node.CreateFile(ctx, req)
	assert.Error(t, err)
}

func TestTrackerNode_ListFiles(t *testing.T) {
	node := newTestTrackerNode(t)
	ctx := context.Background()

	// 创建测试文件
	files := []string{"test1.txt", "test2.txt", "other.txt"}
	for _, name := range files {
		_, err := node.CreateFile(ctx, &tracker.CreateFileRequest{
			Base: &base.BaseRequest{RequestId: 1},
			Name: name,
		})
		assert.NoError(t, err)
	}

	// 测试列表查询
	resp, err := node.ListFiles(ctx, &tracker.ListFilesRequest{
		Base:        &base.BaseRequest{RequestId: 1},
		NamePattern: "test",
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(resp.Files))
}

func TestTrackerNode_NodeHeartbeat(t *testing.T) {
	node := newTestTrackerNode(t)
	ctx := context.Background()

	// 测试节点心跳
	req := &tracker.HeartbeatRequest{
		Base:   &base.BaseRequest{RequestId: 1},
		NodeId: 1,
	}

	_, err := node.UpdateNodeHeartbeat(ctx, req)
	assert.NoError(t, err)

	// 验证节点状态
	node.nodeMux.RLock()
	storageNode, exists := node.nodes[1]
	node.nodeMux.RUnlock()

	require.True(t, exists)
	assert.Equal(t, base.NodeStatus_ONLINE, storageNode.Status)

	// 模拟心跳超时（不改动全局超时，避免测试等待 30s）
	node.nodeMux.Lock()
	node.nodes[1].LastHeartbeat = time.Now().Add(-nodeHeartbeatTimeout - time.Second).UnixNano()
	node.nodeMux.Unlock()

	node.cleanupInactiveNodes()

	node.nodeMux.RLock()
	_, exists = node.nodes[1]
	node.nodeMux.RUnlock()
	assert.False(t, exists)
}

// TestWriteLeaseExpires 验证客户端崩溃后写租约会自动过期，文件不会永久锁死。
func TestWriteLeaseExpires(t *testing.T) {
	node := newTestTrackerNode(t)
	ctx := context.Background()

	created, err := node.CreateFile(ctx, &tracker.CreateFileRequest{
		Base: &base.BaseRequest{RequestId: 1},
		Name: "lease.txt",
	})
	require.NoError(t, err)
	fileId := created.Meta.Id

	// 客户端 1 拿到写租约
	_, err = node.AllocateChunk(ctx, &tracker.AllocateChunkRequest{
		Base:   &base.BaseRequest{RequestId: 100},
		FileId: fileId,
		Offset: 0,
		Size:   1024,
	})
	// 没有存储节点，分配会失败；但租约已经建立，这正是我们要验证的
	assert.Error(t, err)

	// 客户端 2 在租约有效期内必须被拒绝
	err = node.acquireWriteLease(uint64(fileId), 200)
	assert.Error(t, err)

	// 把租约改成已过期，模拟客户端崩溃
	node.lockMux.Lock()
	node.writeLock[uint64(fileId)].expireAt = time.Now().Add(-time.Second)
	node.lockMux.Unlock()

	// 过期后客户端 2 可以接管
	assert.NoError(t, node.acquireWriteLease(uint64(fileId), 200))

	// 清理协程也能回收过期租约
	node.lockMux.Lock()
	node.writeLock[uint64(fileId)].expireAt = time.Now().Add(-time.Second)
	node.lockMux.Unlock()
	node.cleanupExpiredLeases()

	node.lockMux.RLock()
	_, stillLocked := node.writeLock[uint64(fileId)]
	node.lockMux.RUnlock()
	assert.False(t, stillLocked)
}

// TestUnlockFileReleasesLease 验证 UnlockFile 能立即释放租约（客户端正常关闭文件）。
func TestUnlockFileReleasesLease(t *testing.T) {
	node := newTestTrackerNode(t)
	ctx := context.Background()

	created, err := node.CreateFile(ctx, &tracker.CreateFileRequest{
		Base: &base.BaseRequest{RequestId: 1},
		Name: "unlock.txt",
	})
	require.NoError(t, err)
	fileId := created.Meta.Id

	require.NoError(t, node.acquireWriteLease(uint64(fileId), 100))
	assert.Error(t, node.acquireWriteLease(uint64(fileId), 200))

	_, err = node.UnlockFile(ctx, &tracker.UnlockFileRequest{
		Base:   &base.BaseRequest{RequestId: 100},
		FileId: fileId,
	})
	require.NoError(t, err)

	// 重复释放是幂等的（此时文件已经没有租约）
	_, err = node.UnlockFile(ctx, &tracker.UnlockFileRequest{
		Base:   &base.BaseRequest{RequestId: 100},
		FileId: fileId,
	})
	assert.NoError(t, err)

	// 释放后其他客户端可以写入
	assert.NoError(t, node.acquireWriteLease(uint64(fileId), 200))

	// 但他人不能释放不属于自己的有效租约
	err = node.releaseWriteLease(uint64(fileId), 100)
	assert.Error(t, err)
}

// TestAcquireLockLease 验证文件锁的租约单位正确（纳秒），且过期后可被接管。
func TestAcquireLockLease(t *testing.T) {
	node := newTestTrackerNode(t)
	ctx := context.Background()

	created, err := node.CreateFile(ctx, &tracker.CreateFileRequest{
		Base: &base.BaseRequest{RequestId: 1},
		Name: "lock.txt",
	})
	require.NoError(t, err)

	resp, err := node.AcquireLock(ctx, &tracker.AcquireLockRequest{
		Base:     &base.BaseRequest{RequestId: 100},
		FileId:   created.Meta.Id,
		LockType: base.LockType_WRITE,
	})
	require.NoError(t, err)
	require.True(t, resp.Success)

	// 另一个客户端立刻抢锁应冲突
	resp, err = node.AcquireLock(ctx, &tracker.AcquireLockRequest{
		Base:     &base.BaseRequest{RequestId: 200},
		FileId:   created.Meta.Id,
		LockType: base.LockType_WRITE,
	})
	require.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, int32(base.ERR_LOCK_CONFLICT), resp.Base.Code)

	// 等租约过期（用极短租约模拟）
	node.fileMux.Lock()
	node.files[uint64(created.Meta.Id)].LockInfo.AcquireTime = time.Now().Add(-2 * writeLeaseTTL).UnixNano()
	node.fileMux.Unlock()

	resp, err = node.AcquireLock(ctx, &tracker.AcquireLockRequest{
		Base:     &base.BaseRequest{RequestId: 200},
		FileId:   created.Meta.Id,
		LockType: base.LockType_WRITE,
	})
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

// TestRaftElectionDoesNotDeadlock 验证选举路径不会自死锁，
// 且单节点集群能正常当选、后续 Raft RPC 不会被卡住的锁阻塞。
func TestRaftElectionDoesNotDeadlock(t *testing.T) {
	node := newTestTrackerNode(t)
	node.SetNodeID(1)

	// 单测环境没有配置实例，initRaft 会按 standalone 关闭选举；
	// 这里手动打开，验证选举链路本身。
	node.raftState.mu.Lock()
	node.raftState.enabled = true
	node.raftState.lastContact = time.Now().Add(-time.Second)
	node.raftState.mu.Unlock()

	done := make(chan struct{})
	go func() {
		node.runElectionTimer()
		close(done)
	}()
	t.Cleanup(func() { node.stopRaft() })

	// 必须在 3s 内完成单节点当选
	require.Eventually(t, func() bool {
		node.raftState.mu.RLock()
		defer node.raftState.mu.RUnlock()
		return node.raftState.role == base.NodeRole_LEADER
	}, 3*time.Second, 20*time.Millisecond, "single node cluster should become leader")

	// 如果锁被死锁的 goroutine 持有，这里会超时
	rpcDone := make(chan struct{})
	go func() {
		_, _ = node.AppendEntries(context.Background(), &tracker.AppendEntriesRequest{
			Base:         &base.BaseRequest{RequestId: 1},
			Term:         1,
			LeaderId:     2,
			PrevLogIndex: -1,
		})
		_, _ = node.RequestVote(context.Background(), &tracker.RequestVoteRequest{
			Base:        &base.BaseRequest{RequestId: 2},
			Term:        1,
			CandidateId: 2,
		})
		close(rpcDone)
	}()

	select {
	case <-rpcDone:
	case <-time.After(2 * time.Second):
		t.Fatal("raft RPC blocked: election path deadlocked")
	}
}
