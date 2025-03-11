package meta

import (
	"fmt"
	"strings"
	"sync"

	"github.com/JustACP/criceta/pkg/chunk"
	"github.com/JustACP/criceta/pkg/common"
)

type FileStatus int16

const (
	VALID FileStatus = iota + 1
	DELETED
	READING
	WRITING
	SYNCING
)

var fileStatusNames = []string{"VALID", "DELETED", "READING", "WRITING", "SYNCING"}

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

type File struct {
	Id       int64             `json:"file_id"`
	CRC      uint32            `json:"crc"`
	Hash     *string           `json:"hash"`
	Length   uint64            `json:"length"`
	Status   FileStatus        `json:"status"`
	CreatAt  int64             `json:"CreateAt"`
	UpdateAt int64             `json:"UpdateAt"`
	Chunks   map[uint64]*Chunk `json:"-"`
	rw       sync.RWMutex      `json:"-"`
}

type Chunk struct {
	chunk.ChunkHead
	rw sync.RWMutex `json:"-"`
}
