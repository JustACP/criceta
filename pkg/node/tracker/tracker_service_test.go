package tracker

import (
	"context"
	"testing"
	"time"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/JustACP/criceta/pkg/meta"
	"github.com/stretchr/testify/assert"
)

func TestTrackerNode_CreateFile(t *testing.T) {
	node := NewTrackerNode()
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
	node := NewTrackerNode()
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
	node := NewTrackerNode()
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

	assert.True(t, exists)
	assert.Equal(t, int32(meta.VALID), storageNode.Status)

	// 测试节点清理
	time.Sleep(nodeHeartbeatTimeout + time.Second)
	node.cleanupInactiveNodes()

	node.nodeMux.RLock()
	_, exists = node.nodes[1]
	node.nodeMux.RUnlock()
	assert.False(t, exists)
}
