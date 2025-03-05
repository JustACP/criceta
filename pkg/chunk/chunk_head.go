package chunk

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/JustACP/criceta/pkg/common"
	"github.com/JustACP/criceta/pkg/common/constant"
	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/JustACP/criceta/pkg/common/utils"
)

const ChunkHeadSize uint64 = 128

type ChunkStatus uint16

const (
	VALID ChunkStatus = iota + 1
	DELETED
	READING
	WRITING
	SYNCING
)

var chunkStatusNames = []string{"VALID", "DELETED", "READING", "WRITING", "SYNCING"}

func (cs ChunkStatus) String() string {
	csIdx := int(cs) - 1
	if len(chunkStatusNames) <= csIdx || csIdx < 0 {
		return "UNKNOWN"
	}
	return chunkStatusNames[csIdx]
}

func (cs ChunkStatus) FromString(s string) (common.Enum, error) {
	csName := strings.ToUpper(s)
	for idx, currCSName := range chunkStatusNames {
		if strings.Compare(csName, currCSName) == 0 {
			return ChunkStatus(idx + 1), nil
		}
	}

	return ChunkStatus(0), fmt.Errorf("invalid log mode")
}

type CHOption interface {
	apply(*ChunkHead)
}

type chFuncOption struct {
	f func(*ChunkHead)
}

func (c chFuncOption) apply(ch *ChunkHead) {
	c.f(ch)
}

func NewCHOption(f func(*ChunkHead)) CHOption {
	return &chFuncOption{
		f: f,
	}
}

func WithId(id uint64) CHOption {
	return NewCHOption(func(ch *ChunkHead) {
		ch.Id = id
	})
}

func WithFileId(fileId uint64) CHOption {
	return NewCHOption(func(ch *ChunkHead) {
		ch.FileId = fileId
	})
}

func WithVersion(ver uint64) CHOption {
	return NewCHOption(func(ch *ChunkHead) {
		ch.Version = ver
	})
}

func WithPos(pos uint64) CHOption {
	return NewCHOption(func(ch *ChunkHead) {
		ch.Pos = pos
	})
}

func WithOff(off uint64) CHOption {
	return NewCHOption(func(ch *ChunkHead) {
		ch.Off = off
	})
}

func WithStatus(status ChunkStatus) CHOption {
	return NewCHOption(func(ch *ChunkHead) {
		ch.Status = status
	})
}

func WithCreatAt(createAt int64) CHOption {
	return NewCHOption(func(ch *ChunkHead) {
		ch.CreateAt = createAt
	})
}

func WithModifyAt(modifyAt int64) CHOption {
	return NewCHOption(func(ch *ChunkHead) {
		ch.ModifyAt = modifyAt
	})
}

type ChunkHead struct {
	Id       uint64      `json:"chunk_id"`
	FileId   uint64      `json:"file_id"`
	CRC      uint32      `json:"crc"`
	Hash     *string     `json:"hash"`
	Size     uint64      `json:"size"`
	Version  uint64      `json:"version"`
	Pos      uint64      `json:"pos"` // chunk position in file
	Off      uint64      `json:"off"`
	CreateAt int64       `json:"create_at"`
	ModifyAt int64       `json:"modify_at"`
	Status   ChunkStatus `json:"status"`
}

func (ch *ChunkHead) SetStatus(newStatus ChunkStatus) {
	ch.Status = newStatus
}

func (ch *ChunkHead) ToBinary() ([]byte, error) {
	chBytes := make([]byte, ChunkHeadSize)

	startIdx, endIdx := 0, 7
	// APP NAME 7 Bytes
	copy(chBytes[startIdx:endIdx], []byte(constant.APP_NAME))

	// Chunk Id 8 Bytes
	startIdx, endIdx = endIdx, endIdx+8
	binary.BigEndian.PutUint64(chBytes[startIdx:endIdx], uint64(ch.Id))

	// Chunk FileId 8 Bytes
	startIdx, endIdx = endIdx, endIdx+8
	binary.BigEndian.PutUint64(chBytes[startIdx:endIdx], uint64(ch.FileId))

	// Chunk CRC 4 Bytes
	startIdx, endIdx = endIdx, endIdx+4

	if ch.Hash != nil {
		binary.BigEndian.PutUint32(chBytes[startIdx:endIdx], uint32(ch.CRC))
	}

	// Chunk Blake3 Hash 32 Bytes
	startIdx, endIdx = endIdx, endIdx+32
	if ch.Hash != nil {
		hashBytes, err := hex.DecodeString(*ch.Hash)
		if err != nil {
			logs.Error("Chunk Head decode hash hex string as bytes error, err: %s", err.Error())
			return nil, err
		}
		copy(chBytes[startIdx:endIdx], hashBytes)
	}

	// Chunk File Size 8 Bytes
	startIdx, endIdx = endIdx, endIdx+8
	binary.BigEndian.PutUint64(chBytes[startIdx:endIdx], uint64(ch.Size))

	// Chunk Version 8 Bytes
	startIdx, endIdx = endIdx, endIdx+8
	binary.BigEndian.PutUint64(chBytes[startIdx:endIdx], uint64(ch.Version))

	// Chunk Pos 8 Bytes
	startIdx, endIdx = endIdx, endIdx+8
	binary.BigEndian.PutUint64(chBytes[startIdx:endIdx], uint64(ch.Pos))

	// Chunk Off 8 Bytes
	startIdx, endIdx = endIdx, endIdx+8
	binary.BigEndian.PutUint64(chBytes[startIdx:endIdx], uint64(ch.Off))

	// Chunk CreateAt 8 Bytes
	startIdx, endIdx = endIdx, endIdx+8
	binary.BigEndian.PutUint64(chBytes[startIdx:endIdx], uint64(ch.CreateAt))

	// Chunk ModifyAt 8 Bytes
	startIdx, endIdx = endIdx, endIdx+8
	binary.BigEndian.PutUint64(chBytes[startIdx:endIdx], uint64(ch.ModifyAt))

	// Chunk Status 2 Bytes
	startIdx = endIdx
	endIdx += 2
	binary.BigEndian.PutUint16(chBytes[startIdx:endIdx], uint16(ch.Status))

	return chBytes, nil
}

func (ch *ChunkHead) FromBinary(input []byte) error {
	if len(input) < int(ChunkHeadSize) {
		err := fmt.Errorf("invalid input length: %d, expected at least %d", len(input), ChunkHeadSize)
		logs.Error("Read chunk head error, err: %v", err.Error())
		return err
	}

	// check app name
	appName := string(input[0:7])
	if strings.Compare(appName, constant.APP_NAME) != 0 {
		err := fmt.Errorf("invalid app name %s, app name should be %s", appName, constant.APP_NAME)
		logs.Error("Read chunk head error, err: %v", err.Error())
		return err
	}

	startIdx, endIdx := 7, 7+8
	ch.Id = binary.BigEndian.Uint64(input[startIdx:endIdx])

	startIdx, endIdx = endIdx, endIdx+8
	ch.Id = binary.BigEndian.Uint64(input[startIdx:endIdx])

	startIdx, endIdx = endIdx, endIdx+4
	ch.CRC = binary.BigEndian.Uint32(input[startIdx:endIdx])

	startIdx, endIdx = endIdx, endIdx+32
	hash := input[startIdx:endIdx]
	validFlag := 0
	for _, val := range hash {
		if val > 0 {
			validFlag += 1
			break
		}
	}
	if validFlag > 0 {
		ch.Hash = utils.Ptr(hex.EncodeToString(hash))
	}

	startIdx, endIdx = endIdx, endIdx+8
	ch.Size = binary.BigEndian.Uint64(input[startIdx:endIdx])

	startIdx, endIdx = endIdx, endIdx+8
	ch.Version = binary.BigEndian.Uint64(input[startIdx:endIdx])

	startIdx, endIdx = endIdx, endIdx+8
	ch.Pos = binary.BigEndian.Uint64(input[startIdx:endIdx])

	startIdx, endIdx = endIdx, endIdx+8
	ch.Off = binary.BigEndian.Uint64(input[startIdx:endIdx])

	startIdx, endIdx = endIdx, endIdx+8
	ch.CreateAt = int64(binary.BigEndian.Uint64(input[startIdx:endIdx]))

	startIdx, endIdx = endIdx, endIdx+8
	ch.ModifyAt = int64(binary.BigEndian.Uint64(input[startIdx:endIdx]))

	startIdx, endIdx = endIdx, endIdx+2
	ch.Status = ChunkStatus(binary.BigEndian.Uint16(input[startIdx:endIdx]))
	return nil
}

// Validate checks if the ChunkHead contains valid data
func (ch *ChunkHead) Validate() error {
	if ch.Id == 0 {
		return fmt.Errorf("invalid chunk id: %d", ch.Id)
	}

	if ch.Hash == nil || len(*ch.Hash) != 32 {
		return fmt.Errorf("invalid hash length")
	}

	if ch.Size == 0 {
		return fmt.Errorf("invalid chunk size: %d", ch.Size)
	}

	return nil
}

// Equal compares two ChunkHead structs
func (ch *ChunkHead) Equal(other *ChunkHead) bool {
	if ch == nil || other == nil {
		return ch == nil && other == nil
	}

	return ch.Id == other.Id &&
		ch.FileId == other.FileId &&
		ch.CRC == other.CRC &&
		((ch.Hash == nil && other.Hash == nil) ||
			(ch.Hash != nil && other.Hash != nil && *ch.Hash == *other.Hash)) &&
		ch.Size == other.Size &&
		ch.Pos == other.Pos &&
		ch.Status == other.Status &&
		ch.Off == other.Off
}
