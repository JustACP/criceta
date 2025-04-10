package meta

import (
	"fmt"
	"strings"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/pkg/common/utils"

	"github.com/JustACP/criceta/pkg/chunk"
	"github.com/JustACP/criceta/pkg/common"
)

type FileStatus uint16

const (
	INVALID FileStatus = iota + 1
	VALID
	DELETED
	READING
	WRITING
	SYNCING
)

var fileStatusNames = []string{"INVALID", "VALID", "DELETED", "READING", "WRITING", "SYNCING"}

func (fs FileStatus) String() string {
	fsIdx := int(fs) - 1
	if len(fileStatusNames) <= fsIdx || fsIdx < 0 {
		return "UNKNOWN"
	}
	return fileStatusNames[fsIdx]
}

func (fs FileStatus) FromString(s string) (common.Enum, error) {
	fsName := strings.ToUpper(s)
	for idx, currCSName := range fileStatusNames {
		if strings.Compare(fsName, currCSName) == 0 {
			return FileStatus(idx + 1), nil
		}
	}

	return FileStatus(0), fmt.Errorf("invalid log mode")
}

type FileMeta struct {
	Id            int64                          `json:"file_id"`
	Name          string                         `json:"name"`
	Length        uint64                         `json:"length"`
	Status        FileStatus                     `json:"status"`
	Version       uint64                         `json:"version"`
	CreatAt       int64                          `json:"create_at"`
	UpdateAt      int64                          `json:"update_at"`
	Size          int64                          `json:"size"`
	ModifyTime    int64                          `json:"modify_time"`
	Chunks        map[uint64]*ChunkMeta          `json:"-"`
	LockInfo      *LockInfo                      `json:"lock_info,omitempty"`
	ChunkReplicas map[uint64][]*base.ReplicaInfo `json:"chunk_replicas,omitempty"`
}

// LockInfo 文件锁信息
type LockInfo struct {
	ClientId     uint64   `json:"client_id"`
	LockType     LockType `json:"lock_type"`
	AcquireTime  int64    `json:"acquire_time"`
	LeaseTimeout int64    `json:"lease_timeout"`
}

// LockType 锁类型
type LockType uint8

const (
	NONE LockType = iota
	READ
	WRITE
)

func (f *FileMeta) LatestChunkVersion() uint64 {
	latestVersion := uint64(0)
	for _, currChunk := range f.Chunks {
		latestVersion = utils.More(latestVersion, currChunk.Version)
	}
	return latestVersion
}

type ChunkMeta struct {
	chunk.ChunkHead
	NodeId   uint64                  `json:"node_id"`
	Status   chunk.ChunkStatus       `json:"status"`
	CreateAt int64                   `json:"create_at"`
	UpdateAt int64                   `json:"update_at"`
	Replicas map[uint64]*ReplicaInfo `json:"replicas,omitempty"`
}

// ReplicaInfo 副本信息
type ReplicaInfo struct {
	NodeId   uint64     `json:"node_id"`
	Status   FileStatus `json:"status"`
	SyncTime int64      `json:"sync_time"`
	Crc      string     `json:"crc"`
}
