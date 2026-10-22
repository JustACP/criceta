package tracker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/pkg/chunk"
	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/JustACP/criceta/pkg/meta"
)

const (
	// snapshotFormatVersion 快照格式版本。修改持久化结构时递增，并在 loadSnapshot 中做兼容处理。
	snapshotFormatVersion = 1
	snapshotFileName      = "tracker_state.json"
	snapshotTempSuffix    = ".tmp"

	// defaultTrackerDataDir 未配置 data_dir 时的默认数据目录。
	defaultTrackerDataDir = "data"
)

// snapshot 是 Tracker 元数据的持久化格式。
//
// 只持久化「文件 + Chunk 映射 + 副本状态」这类需要跨重启保留的数据；
// 节点心跳、写租约、读锁都属于易失状态，重启后由节点重新注册、客户端重新获取。
type snapshot struct {
	FormatVersion int                      `json:"format_version"`
	SavedAt       int64                    `json:"saved_at"`
	Files         map[uint64]*fileSnapshot `json:"files"`
}

type fileSnapshot struct {
	Id            int64                        `json:"id"`
	Name          string                       `json:"name"`
	Length        uint64                       `json:"length"`
	Size          int64                        `json:"size"`
	Status        uint16                       `json:"status"`
	Version       uint64                       `json:"version"`
	CreateAt      int64                        `json:"create_at"`
	UpdateAt      int64                        `json:"update_at"`
	ModifyTime    int64                        `json:"modify_time"`
	Chunks        map[uint64]*chunkSnapshot    `json:"chunks"`
	ChunkReplicas map[uint64][]replicaSnapshot `json:"chunk_replicas,omitempty"`
}

type chunkSnapshot struct {
	Id       uint64 `json:"id"`
	FileId   uint64 `json:"file_id"`
	Version  uint64 `json:"version"`
	Offset   uint64 `json:"offset"`
	Size     uint64 `json:"size"`
	NodeId   uint64 `json:"node_id"`
	Status   uint16 `json:"status"`
	CRC      uint32 `json:"crc"`
	CreateAt int64  `json:"create_at"`
	UpdateAt int64  `json:"update_at"`
}

type replicaSnapshot struct {
	NodeId   int64  `json:"node_id"`
	Status   int32  `json:"status"`
	SyncTime int64  `json:"sync_time"`
	Crc      string `json:"crc,omitempty"`
}

// snapshotPath 返回快照文件路径
func (t *TrackerNode) snapshotPath() string {
	return filepath.Join(t.dataDir, snapshotFileName)
}

// loadSnapshot 从磁盘恢复元数据。文件不存在时视为全新集群。
func (t *TrackerNode) loadSnapshot() error {
	path := t.snapshotPath()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			logs.Info("No tracker snapshot found at %s, starting with empty metadata", path)
			return nil
		}
		return fmt.Errorf("read tracker snapshot %s: %w", path, err)
	}

	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("decode tracker snapshot %s: %w", path, err)
	}
	if snap.FormatVersion != snapshotFormatVersion {
		return fmt.Errorf("unsupported tracker snapshot format %d (want %d)", snap.FormatVersion, snapshotFormatVersion)
	}

	files := make(map[uint64]*meta.FileMeta, len(snap.Files))
	for id, fs := range snap.Files {
		files[id] = fs.toMeta()
	}

	t.fileMux.Lock()
	t.files = files
	t.fileMux.Unlock()

	logs.Info("Restored %d file(s) from tracker snapshot %s", len(files), path)
	return nil
}

// markDirty 通知后台协程尽快落盘。重复通知会被合并。
func (t *TrackerNode) markDirty() {
	select {
	case t.persistCh <- struct{}{}:
	default:
	}
}

// startPersister 启动后台持久化协程
func (t *TrackerNode) startPersister() {
	t.persistOnce.Do(func() {
		go func() {
			for {
				select {
				case <-t.persistCh:
					if err := t.flush(); err != nil {
						logs.Error("Failed to persist tracker metadata: %v", err)
					}
				case <-t.persistStop:
					return
				}
			}
		}()
	})
}

// stopPersister 停止后台协程并做一次同步落盘
func (t *TrackerNode) stopPersister() {
	t.persistStopOnce.Do(func() {
		close(t.persistStop)
	})

	if err := t.flush(); err != nil {
		logs.Error("Failed to persist tracker metadata on shutdown: %v", err)
	}
}

// flush 同步地把当前元数据写入快照。
// 先持有读锁复制一份快照，再在锁外完成序列化与落盘，避免阻塞正常请求。
func (t *TrackerNode) flush() error {
	t.persistMux.Lock()
	defer t.persistMux.Unlock()

	snap := t.buildSnapshot()
	return t.writeSnapshot(snap)
}

func (t *TrackerNode) buildSnapshot() *snapshot {
	t.fileMux.RLock()
	defer t.fileMux.RUnlock()

	files := make(map[uint64]*fileSnapshot, len(t.files))
	for id, file := range t.files {
		fs := &fileSnapshot{
			Id:         file.Id,
			Name:       file.Name,
			Length:     file.Length,
			Size:       file.Size,
			Status:     uint16(file.Status),
			Version:    file.Version,
			CreateAt:   file.CreatAt,
			UpdateAt:   file.UpdateAt,
			ModifyTime: file.ModifyTime,
			Chunks:     make(map[uint64]*chunkSnapshot, len(file.Chunks)),
		}

		for chunkId, chk := range file.Chunks {
			fs.Chunks[chunkId] = &chunkSnapshot{
				Id:       chk.Id,
				FileId:   chk.FileId,
				Version:  chk.Version,
				Offset:   chk.Offset,
				Size:     chk.Size,
				NodeId:   chk.NodeId,
				Status:   uint16(chk.Status),
				CRC:      chk.CRC,
				CreateAt: chk.CreateAt,
				UpdateAt: chk.UpdateAt,
			}
		}

		if len(file.ChunkReplicas) > 0 {
			fs.ChunkReplicas = make(map[uint64][]replicaSnapshot, len(file.ChunkReplicas))
			for chunkId, replicas := range file.ChunkReplicas {
				list := make([]replicaSnapshot, 0, len(replicas))
				for _, replica := range replicas {
					list = append(list, replicaSnapshot{
						NodeId:   replica.NodeId,
						Status:   replica.Status,
						SyncTime: replica.SyncTime,
						Crc:      replica.Crc,
					})
				}
				fs.ChunkReplicas[chunkId] = list
			}
		}

		files[id] = fs
	}

	return &snapshot{
		FormatVersion: snapshotFormatVersion,
		SavedAt:       time.Now().UnixNano(),
		Files:         files,
	}
}

// writeSnapshot 原子写入快照：先写临时文件并 fsync，再 rename 覆盖。
func (t *TrackerNode) writeSnapshot(snap *snapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("encode tracker snapshot: %w", err)
	}

	if err := os.MkdirAll(t.dataDir, 0o755); err != nil {
		return fmt.Errorf("create tracker data dir %s: %w", t.dataDir, err)
	}

	path := t.snapshotPath()
	tmpPath := path + snapshotTempSuffix

	tmp, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open snapshot temp file %s: %w", tmpPath, err)
	}

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write snapshot temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("sync snapshot temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close snapshot temp file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("replace snapshot %s: %w", path, err)
	}

	// 同步目录项，确保 rename 之后即使断电也能看到新文件
	if dir, err := os.Open(t.dataDir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	return nil
}

func (fs *fileSnapshot) toMeta() *meta.FileMeta {
	file := &meta.FileMeta{
		Id:            fs.Id,
		Name:          fs.Name,
		Length:        fs.Length,
		Size:          fs.Size,
		Status:        meta.FileStatus(fs.Status),
		Version:       fs.Version,
		CreatAt:       fs.CreateAt,
		UpdateAt:      fs.UpdateAt,
		ModifyTime:    fs.ModifyTime,
		Chunks:        make(map[uint64]*meta.ChunkMeta, len(fs.Chunks)),
		ChunkReplicas: make(map[uint64][]*base.ReplicaInfo, len(fs.ChunkReplicas)),
	}

	for chunkId, chk := range fs.Chunks {
		file.Chunks[chunkId] = &meta.ChunkMeta{
			ChunkHead: chunk.ChunkHead{
				Id:      chk.Id,
				FileId:  chk.FileId,
				Version: chk.Version,
				Offset:  chk.Offset,
				Size:    chk.Size,
				CRC:     chk.CRC,
				Status:  chunk.ChunkStatus(chk.Status),
			},
			NodeId:   chk.NodeId,
			Status:   chunk.ChunkStatus(chk.Status),
			CreateAt: chk.CreateAt,
			UpdateAt: chk.UpdateAt,
		}
	}

	for chunkId, replicas := range fs.ChunkReplicas {
		list := make([]*base.ReplicaInfo, 0, len(replicas))
		for _, replica := range replicas {
			list = append(list, &base.ReplicaInfo{
				NodeId:   replica.NodeId,
				Status:   replica.Status,
				SyncTime: replica.SyncTime,
				Crc:      replica.Crc,
			})
		}
		file.ChunkReplicas[chunkId] = list
	}

	return file
}
