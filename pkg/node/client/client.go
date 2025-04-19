package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage/storageservice"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker/trackerservice"
	"github.com/JustACP/criceta/pkg/chunk"
	"github.com/JustACP/criceta/pkg/common/idgen"
	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/connpool"
)

const (
	DefaultChunkMaxSize = 64 * 1024 * 1024 // 64MB
	WriteBufferSize     = 4 * 1024 * 1024  // 4MB
)

// WriteBuffer 写缓冲区
type WriteBuffer struct {
	fileId    int64
	offset    int64
	data      []byte
	size      int
	lastFlush time.Time
}

type Client struct {
	clientId    uint64
	trackerCli  trackerservice.Client
	storageClis map[uint64]storageservice.Client
	cliMux      sync.RWMutex

	// 写缓冲区相关
	writeBuffers map[int64]*WriteBuffer // fileId -> WriteBuffer
	bufferMux    sync.RWMutex
}

func NewClient(trackerCli trackerservice.Client) *Client {
	return &Client{
		clientId:     idgen.NextID(),
		trackerCli:   trackerCli,
		storageClis:  make(map[uint64]storageservice.Client),
		writeBuffers: make(map[int64]*WriteBuffer),
	}
}

// CreateFile 创建新文件
func (c *Client) CreateFile(ctx context.Context, name string, length int64) error {
	resp, err := c.trackerCli.CreateFile(ctx, &tracker.CreateFileRequest{
		Base: &base.BaseRequest{
			RequestId: int64(c.clientId),
			Timestamp: time.Now().UnixNano(),
		},
		Name:   name,
		Length: length,
	})
	if err != nil {
		return fmt.Errorf("create file failed: %v", err)
	}
	if resp.Base.Code != 0 {
		return fmt.Errorf("create file failed: %s", resp.Base.Message)
	}
	return nil
}

// WriteFile 写入文件
func (c *Client) WriteFile(ctx context.Context, fileId int64, offset int64, data []byte) error {

	c.bufferMux.Lock()
	wb, exists := c.writeBuffers[fileId]
	if !exists || wb.offset+int64(wb.size) != offset {
		// 如缓冲区不存在或者写入位置不连续，先刷新缓冲区
		if wb != nil {
			c.bufferMux.Unlock()
			if err := c.flushBuffer(ctx, wb); err != nil {
				return fmt.Errorf("flush buffer failed: %v", err)
			}
			c.bufferMux.Lock()
		}

		wb = &WriteBuffer{
			fileId:    fileId,
			offset:    offset,
			data:      make([]byte, 0, WriteBufferSize),
			size:      0,
			lastFlush: time.Now(),
		}
		c.writeBuffers[fileId] = wb
	}

	remainingData := data
	for len(remainingData) > 0 {
		// 计算可以写入当前缓冲区的数据量
		spaceLeft := WriteBufferSize - wb.size
		writeSize := len(remainingData)
		if writeSize > spaceLeft {
			writeSize = spaceLeft
		}

		// 写入数据到缓冲区
		wb.data = append(wb.data, remainingData[:writeSize]...)
		wb.size += writeSize

		// 如果缓冲区已满，需要刷新
		if wb.size >= WriteBufferSize {
			c.bufferMux.Unlock()
			if err := c.flushBuffer(ctx, wb); err != nil {
				return fmt.Errorf("flush buffer failed: %v", err)
			}
			c.bufferMux.Lock()

			// 创建新的缓冲区
			wb = &WriteBuffer{
				fileId:    fileId,
				offset:    offset + int64(len(data)) - int64(len(remainingData)) + int64(writeSize),
				data:      make([]byte, 0, WriteBufferSize),
				size:      0,
				lastFlush: time.Now(),
			}
			c.writeBuffers[fileId] = wb
		}

		remainingData = remainingData[writeSize:]
	}
	c.bufferMux.Unlock()

	return nil
}

// Flush 刷新指定文件的写缓冲区
func (c *Client) Flush(ctx context.Context, fileId int64) error {
	c.bufferMux.Lock()
	defer c.bufferMux.Unlock()

	wb, exists := c.writeBuffers[fileId]
	if !exists {
		return nil
	}

	err := c.flushBuffer(ctx, wb)
	if err != nil {
		logs.Error("fileId: %v flush error, err: %v", err)
		return err
	}

	delete(c.writeBuffers, fileId)

	return nil
}

// flushBuffer 将缓冲区数据写入存储节点
func (c *Client) flushBuffer(ctx context.Context, wb *WriteBuffer) error {
	if wb.size == 0 {
		return nil
	}

	fileResp, err := c.trackerCli.GetFile(ctx, &tracker.GetFileRequest{
		Base: &base.BaseRequest{
			RequestId: int64(c.clientId),
			Timestamp: time.Now().UnixNano(),
		},
		FileId: wb.fileId,
	})
	if err != nil {
		return fmt.Errorf("get file failed: %v", err)
	}

	var targetChunk *storage.ChunkMeta
	for _, chunk := range fileResp.Meta.Chunks {
		chunkStart := chunk.Offset
		chunkEnd := chunk.Offset + chunk.Size
		if wb.offset >= chunkStart && wb.offset < chunkEnd {
			targetChunk = chunk
			break
		}
	}

	if targetChunk != nil {
		chunkStart := targetChunk.Offset
		writeOffset := wb.offset - chunkStart
		chunkRemainSpace := targetChunk.Size - writeOffset
		writeSize := int64(wb.size)
		if writeSize > chunkRemainSpace {
			writeSize = chunkRemainSpace
		}

		primaryCli, err := c.getStorageClient(*targetChunk.NodeId)
		if err != nil {
			return fmt.Errorf("get primary storage client failed: %v", err)
		}

		// 写入数据到主节点
		writeResp, err := primaryCli.WriteChunk(ctx, &storage.WriteChunkRequest{
			Base: &base.BaseRequest{
				RequestId: int64(c.clientId),
				Timestamp: time.Now().UnixNano(),
			},
			Meta:        targetChunk,
			Data:        wb.data[:writeSize],
			WriteOffset: writeOffset,
			WriteSize:   writeSize,
		})
		if err != nil {
			return fmt.Errorf("write chunk to primary node failed: %v", err)
		}

		// 提交Chunk更新
		_, err = c.trackerCli.CommitChunk(ctx, &tracker.CommitChunkRequest{
			Base: &base.BaseRequest{
				RequestId: int64(c.clientId),
				Timestamp: time.Now().UnixNano(),
			},
			Chunk: writeResp.Meta,
		})
		if err != nil {
			return fmt.Errorf("commit chunk failed: %v", err)
		}

		// 如果还有剩余数据，需要创建新的Chunk
		if writeSize < int64(wb.size) {
			remainingData := wb.data[writeSize:]
			remainingOffset := wb.offset + writeSize

			// 分配新的Chunk
			allocResp, err := c.trackerCli.AllocateChunk(ctx, &tracker.AllocateChunkRequest{
				Base: &base.BaseRequest{
					RequestId: int64(c.clientId),
					Timestamp: time.Now().UnixNano(),
				},
				FileId: wb.fileId,
				Offset: remainingOffset,
				Size:   int64(len(remainingData)),
			})
			if err != nil {
				return fmt.Errorf("allocate chunk failed: %v", err)
			}

			// 获取新chunk的storage client
			primaryCli, err = c.getStorageClient(*allocResp.Chunk.NodeId)
			if err != nil {
				return fmt.Errorf("get primary storage client failed: %v", err)
			}

			// 写入剩余数据
			writeResp, err = primaryCli.WriteChunk(ctx, &storage.WriteChunkRequest{
				Base: &base.BaseRequest{
					RequestId: int64(c.clientId),
					Timestamp: time.Now().UnixNano(),
				},
				Meta:        allocResp.Chunk,
				Data:        remainingData,
				WriteOffset: 0,
				WriteSize:   int64(len(remainingData)),
			})
			if err != nil {
				return fmt.Errorf("write chunk to primary node failed: %v", err)
			}

			// 提交新Chunk
			_, err = c.trackerCli.CommitChunk(ctx, &tracker.CommitChunkRequest{
				Base: &base.BaseRequest{
					RequestId: int64(c.clientId),
					Timestamp: time.Now().UnixNano(),
				},
				Chunk: writeResp.Meta,
			})
			if err != nil {
				return fmt.Errorf("commit chunk failed: %v", err)
			}
		}
	} else {
		// 如果没有找到目标chunk，创建新的chunk
		allocResp, err := c.trackerCli.AllocateChunk(ctx, &tracker.AllocateChunkRequest{
			Base: &base.BaseRequest{
				RequestId: int64(c.clientId),
				Timestamp: time.Now().UnixNano(),
			},
			FileId: wb.fileId,
			Offset: wb.offset,
			Size:   int64(wb.size),
		})
		if err != nil {
			return fmt.Errorf("allocate chunk failed: %v", err)
		}

		// 获取主节点的storage client
		primaryCli, err := c.getStorageClient(*allocResp.Chunk.NodeId)
		if err != nil {
			return fmt.Errorf("get primary storage client failed: %v", err)
		}

		// 写入数据到主节点
		writeResp, err := primaryCli.WriteChunk(ctx, &storage.WriteChunkRequest{
			Base: &base.BaseRequest{
				RequestId: int64(c.clientId),
				Timestamp: time.Now().UnixNano(),
			},
			Meta:        allocResp.Chunk,
			Data:        wb.data,
			WriteOffset: 0,
			WriteSize:   int64(wb.size),
		})
		if err != nil {
			return fmt.Errorf("write chunk to primary node failed: %v", err)
		}

		// 提交新Chunk
		_, err = c.trackerCli.CommitChunk(ctx, &tracker.CommitChunkRequest{
			Base: &base.BaseRequest{
				RequestId: int64(c.clientId),
				Timestamp: time.Now().UnixNano(),
			},
			Chunk: writeResp.Meta,
		})
		if err != nil {
			return fmt.Errorf("commit chunk failed: %v", err)
		}
	}

	return nil
}

type ReadProcessor struct {
	client      *Client
	ctx         context.Context
	result      []byte
	written     uint64
	storageClis map[uint64]storageservice.Client
}

func NewReadProcessor(ctx context.Context, client *Client, size uint64) *ReadProcessor {
	return &ReadProcessor{
		client:      client,
		ctx:         ctx,
		result:      make([]byte, size),
		storageClis: make(map[uint64]storageservice.Client),
	}
}

func (rp *ReadProcessor) ProcessChunkData(chunk *chunk.ChunkMeta, offset uint64, size uint64, isGap bool) error {
	if isGap {
		copy(rp.result[rp.written:rp.written+size], make([]byte, size))
		rp.written += size
		return nil
	}

	// 从存储节点读取数据
	cli, err := rp.getStorageClient(chunk.NodeId)
	if err != nil {
		return fmt.Errorf("get storage client failed: %v", err)
	}

	resp, err := cli.ReadChunk(rp.ctx, &storage.ReadChunkRequest{
		Base: &base.BaseRequest{
			RequestId: time.Now().UnixNano(),
			Timestamp: time.Now().UnixNano(),
		},
		ChunkId: int64(chunk.Id),
		Version: int64(chunk.Version),
		FileId:  int64(chunk.FileId),
		Offset:  int64(offset) - int64(chunk.Offset), // 转换为chunk内的偏移量
		Size:    int64(size),
	})
	if err != nil {
		return fmt.Errorf("read chunk failed: %v", err)
	}

	copy(rp.result[rp.written:], resp.Data)
	rp.written += size
	return nil
}

func (rp *ReadProcessor) getStorageClient(nodeId uint64) (storageservice.Client, error) {
	if cli, ok := rp.storageClis[nodeId]; ok {
		return cli, nil
	}

	cli, err := rp.client.getStorageClient(int64(nodeId))
	if err != nil {
		return nil, err
	}

	rp.storageClis[nodeId] = cli
	return cli, nil
}

func (rp *ReadProcessor) GetResult() []byte {
	return rp.result
}

func (c *Client) ReadFile(ctx context.Context, fileId int64, offset int64, size int64) ([]byte, error) {

	c.Flush(ctx, fileId)

	fileResp, err := c.trackerCli.GetFile(ctx, &tracker.GetFileRequest{
		Base: &base.BaseRequest{
			RequestId: int64(c.clientId),
			Timestamp: time.Now().UnixNano(),
		},
		FileId: fileId,
	})
	if err != nil {
		return nil, fmt.Errorf("get file failed: %v", err)
	}

	cm := chunk.NewChunkManager(uint64(fileId))

	for _, chunkMeta := range fileResp.Meta.Chunks {
		cm.AddChunk(&chunk.ChunkMeta{
			Id:       uint64(chunkMeta.Id),
			FileId:   uint64(chunkMeta.FileId),
			Version:  uint64(*chunkMeta.Version),
			Offset:   uint64(chunkMeta.Offset),
			Size:     uint64(chunkMeta.Size),
			NodeId:   uint64(*chunkMeta.NodeId),
			Status:   chunk.ChunkStatus(*chunkMeta.Status),
			CreateAt: *chunkMeta.CreateAt,
			UpdateAt: *chunkMeta.ModifyAt,
		})
	}

	processor := NewReadProcessor(ctx, c, uint64(size))

	if err := cm.ProcessRange(uint64(offset), uint64(size), processor); err != nil {
		return nil, fmt.Errorf("process read range failed: %v", err)
	}

	return processor.GetResult(), nil
}

// DeleteFile 删除文件
func (c *Client) DeleteFile(ctx context.Context, fileId int64) error {
	resp, err := c.trackerCli.DeleteFile(ctx, &tracker.DeleteFileRequest{
		Base: &base.BaseRequest{
			RequestId: int64(c.clientId),
			Timestamp: time.Now().UnixNano(),
		},
		FileId: fileId,
	})
	if err != nil {
		return fmt.Errorf("delete file failed: %v", err)
	}
	if resp.Base.Code != 0 {
		return fmt.Errorf("delete file failed: %s", resp.Base.Message)
	}
	return nil
}

func (c *Client) getStorageClient(nodeId int64) (storageservice.Client, error) {
	c.cliMux.RLock()
	if cli, ok := c.storageClis[uint64(nodeId)]; ok {
		c.cliMux.RUnlock()
		return cli, nil
	}
	c.cliMux.RUnlock()

	c.cliMux.Lock()
	defer c.cliMux.Unlock()

	if cli, ok := c.storageClis[uint64(nodeId)]; ok {
		return cli, nil
	}

	resp, err := c.trackerCli.GetNodes(context.Background(), &tracker.GetNodesRequest{
		Base: &base.BaseRequest{
			RequestId: int64(idgen.NextID()),
			Timestamp: time.Now().UnixNano(),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes from tracker: %v", err)
	}

	var targetNode *tracker.NodeInfo
	for _, node := range resp.Nodes {
		if node.NodeId == nodeId {
			targetNode = node
			break
		}
	}
	if targetNode == nil {
		return nil, fmt.Errorf("node not found: %d", nodeId)
	}

	if targetNode.Status != base.NodeStatus_ONLINE {
		return nil, fmt.Errorf("node is not online: %d", nodeId)
	}

	cli, err := storageservice.NewClient(
		fmt.Sprintf("storage-%d", nodeId),
		client.WithHostPorts(targetNode.Address),
		client.WithRPCTimeout(time.Second*5),
		client.WithConnectTimeout(time.Second*1),
		client.WithLongConnection(connpool.IdleConfig{
			MaxIdlePerAddress: 100,
			MaxIdleGlobal:     1000,
			MaxIdleTimeout:    time.Minute * 10,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %v", err)
	}

	c.storageClis[uint64(nodeId)] = cli

	return cli, nil
}
