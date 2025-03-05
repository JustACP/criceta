package chunk

import (
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/JustACP/criceta/pkg/common/utils"
	"github.com/edsrzf/mmap-go"
	"lukechampine.com/blake3"
)

// Update Option
type UpdateOption func(cf *ChunkFile, append []byte) error

type ForceUpdateOption func(cf *ChunkFile, append []byte, once *sync.Once) error

type UpdateFunc func(cf *ChunkFile, data []byte) error
type PrepareFunc func(cf *ChunkFile)

type UpdateHandler struct {
	prepareOnce bool
	done        bool
	prepares    []PrepareFunc
	updates     []UpdateFunc
}

func NewUpdateHandler(prepareOnce bool, prepares []PrepareFunc, updates []UpdateFunc) *UpdateHandler {
	return &UpdateHandler{
		prepareOnce: prepareOnce,
		prepares:    prepares,
		updates:     updates,
	}
}

func (h *UpdateHandler) doPrepares(cf *ChunkFile) error {
	if h.prepareOnce && h.done {
		return nil
	}

	for _, prepare := range h.prepares {
		prepare(cf)
	}

	h.done = true
	return nil
}

func (h *UpdateHandler) Apply(cf *ChunkFile, data []byte) error {
	// 执行所有Prepare
	h.doPrepares(cf)

	// 执行所有Update
	for _, update := range h.updates {
		if err := update(cf, data); err != nil {
			return err
		}
	}
	return nil
}

type ChunkFile struct {
	Head   *ChunkHead
	File   *os.File
	mmap   mmap.MMap
	m      *mmap.MMap
	offset uint64
	mu     sync.RWMutex
	hasher *blake3.Hasher
	writer io.WriterAt
}

const (
	UpdateSteps  uint64 = 4 * 1024 * 1024  // 4 MB
	MaxChunkSize uint64 = 64 * 1024 * 1024 // 64MB
)

func initChunk(path string) (*os.File, error) {
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		return nil, fmt.Errorf("invalid path format")
	}

	// 1. 确保目录存在
	if err := os.MkdirAll(path[:idx], 0764); err != nil {
		return nil, fmt.Errorf("create directory failed: %w", err)
	}

	file, err := os.OpenFile(
		path,
		os.O_RDWR|os.O_CREATE|os.O_TRUNC,
		0764,
	)
	if err != nil {
		return nil, fmt.Errorf("open file failed: %w", err)
	}

	if err := file.Truncate(int64(ChunkHeadSize)); err != nil {
		file.Close()
		return nil, fmt.Errorf("preallocate space failed: %w", err)
	}

	return file, nil
}

// New create a new chunk file and returen a chunk file ptr
// when the chunk file is already existed, it will open as R&W and append mode.
func New(path string, chOptions ...CHOption) (*ChunkFile, error) {

	file, err := initChunk(path)

	if err != nil {
		logs.Error("new chunk file error, err: %s", err.Error())
		return nil, fmt.Errorf("new chunk file error, err: %s", err.Error())
	}

	ch := &ChunkHead{}
	for _, op := range chOptions {
		op.apply(ch)
	}

	cf := &ChunkFile{
		Head:   ch,
		File:   file,
		offset: 0,
		hasher: blake3.New(32, nil),
		writer: file,
	}

	mmapContent, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		err = fmt.Errorf("chunk file mmap init error, err: %s", err.Error())
		logs.Error("%s", err.Error())
		return nil, err
	}
	cf.mmap = mmapContent
	cf.m = &mmapContent

	// init chunk head bytes
	cf.flushChunkHead()

	return cf, nil
}

func Open(path string) (*ChunkFile, error) {

	crcPrepare, crcUpdate := UpdateCRC(true)
	hashPrepare, hashUpdate := UpdateHash(true)
	sizePrepare, sizeUpdate := UpdateSize(true)

	updateCH := NewUpdateHandler(
		true,
		[]PrepareFunc{crcPrepare, hashPrepare, sizePrepare},
		[]UpdateFunc{crcUpdate, hashUpdate, sizeUpdate},
	)

	return OpenChunkFile(path, updateCH)
}

// OpenChunkFile only try to open chunk file
// if the chunk file not exists, it will return error.
func OpenChunkFile(path string, h *UpdateHandler) (*ChunkFile, error) {

	file, err := os.OpenFile(
		path,
		os.O_RDWR,
		0764,
	)
	if err != nil {
		logs.Error("open chunk file error, err: %s", err.Error())
		return nil, fmt.Errorf("open chunk file error, err: %s", err.Error())
	}

	mmapContent, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		err = fmt.Errorf("chunk file mmap init error, err: %s", err.Error())
		logs.Error("%s", err.Error())

		mmapContent.Unmap()
		file.Close()

		return nil, err
	}

	ch := &ChunkHead{}

	err = ch.FromBinary(mmapContent[:ChunkHeadSize])
	if err != nil {
		logs.Error("read chunk head error, err: %v", err.Error())

		mmapContent.Unmap()
		file.Close()
		return nil, err
	}

	cf := &ChunkFile{
		Head:   ch,
		File:   file,
		mmap:   mmapContent,
		m:      &mmapContent,
		offset: 0,
		hasher: blake3.New(32, nil),
		writer: file,
	}

	updateErr := cf.forceUpdateOptions(h)
	if updateErr != nil {
		logs.Error("open chunk file occured errors when force update, errs: %v", updateErr.Error())
		return nil, updateErr
	}

	return cf, nil

}

func (cf *ChunkFile) flushChunkHead() error {
	if cf.Head == nil {
		return fmt.Errorf("chunk head is nil")
	}

	heads, err := cf.Head.ToBinary()
	if err != nil {
		return fmt.Errorf("chunk head to binary error, err: %v", err.Error())
	}

	idx, err := cf.File.WriteAt(heads, 0)
	if idx != len(heads) || err != nil {
		return fmt.Errorf("chunk head flushed error")
	}

	return nil
}

func UpdateCRC(force bool) (PrepareFunc, UpdateFunc) {
	return func(cf *ChunkFile) {
			if force {
				cf.Head.CRC = 0
			}
		},
		func(cf *ChunkFile, data []byte) error {
			cf.Head.CRC = crc32.Update(cf.Head.CRC, crc32.IEEETable, data)
			return nil
		}
}

func UpdateHash(force bool) (PrepareFunc, UpdateFunc) {
	return func(cf *ChunkFile) {
			if force {
				cf.Head.Hash = nil
			}
		},
		func(cf *ChunkFile, data []byte) error {
			if cf.hasher == nil {
				return fmt.Errorf("chunk file hasher is nil")
			}
			_, err := cf.hasher.Write(data)
			if err != nil {
				return err
			}
			hashBytes := cf.hasher.Sum(nil)
			cf.Head.Hash = utils.Ptr(hex.EncodeToString(hashBytes))
			return nil
		}
}

func UpdateSize(force bool) (PrepareFunc, UpdateFunc) {

	return func(cf *ChunkFile) {
			if force {
				cf.Head.Size = 0
			}
		},
		func(cf *ChunkFile, data []byte) error {
			if cf.Head == nil {
				return fmt.Errorf("chunk file Head is nil")
			}
			cf.Head.Size += uint64(len(data))
			return nil
		}
}

func UpdateVer(force bool) (PrepareFunc, UpdateFunc) {

	return func(cf *ChunkFile) {
			if force {
				cf.Head.Version = 0
			}
		},
		func(cf *ChunkFile, data []byte) error {
			if cf.Head == nil {
				return fmt.Errorf("chunk file Head is nil")
			}
			cf.Head.Version++
			return nil
		}
}

func UpdateModifyAt(force bool) (PrepareFunc, UpdateFunc) {

	return func(cf *ChunkFile) {
			if force {
				cf.Head.ModifyAt = 0
			}
		},
		func(cf *ChunkFile, data []byte) error {
			if cf.Head == nil {
				return fmt.Errorf("chunk file Head is nil")
			}
			cf.Head.ModifyAt = time.Now().UnixNano()
			return nil
		}
}

func (cf *ChunkFile) forceUpdateOptions(h *UpdateHandler) error {

	if cf == nil || cf.File == nil || cf.Head == nil {
		nilChunk := fmt.Errorf("chunk file is nil")
		logs.Error("%s", nilChunk.Error())
		return nilChunk
	}

	fileSize, fileSizeErr := cf.checkSizeValid()
	if fileSizeErr != nil {
		logs.Error("force update chunk file stat errors, err: %v", fileSizeErr.Error())
		return fileSizeErr
	}

	contentSize := fileSize - ChunkHeadSize

	for idx := uint64(0); idx < contentSize; {
		readSize := utils.Less(idx+UpdateSteps, contentSize-idx)
		cf.remap(idx, utils.Less(readSize, UpdateSteps))
		err := h.Apply(cf, cf.mmap[idx:idx+readSize])
		if err != nil {
			logs.Error("full force update error,err: %v", err)
			return err
		}

		idx += readSize
	}

	cf.flushChunkHead()

	return nil
}

func (cf *ChunkFile) Close() error {

	cf.mu.Lock()
	defer cf.mu.Unlock()
	err := errors.Join(cf.m.Unmap(), cf.File.Close())

	if err != nil {
		logs.Error("chunk file close error, err: %v", err.Error())
		return err
	}

	return nil
}

func (cf *ChunkFile) checkCRCValid() (uint32, error) {

	var checkCRC uint32 = 0

	for idx := ChunkHeadSize; idx < ChunkHeadSize+cf.Head.Size; idx += uint64(UpdateSteps) {
		crc32.Update(checkCRC, crc32.IEEETable, cf.mmap[idx:idx+UpdateSteps])
	}

	if cf.Head.CRC != checkCRC {
		crcNotEqual := fmt.Errorf("chunk head crc is not eqaul to real crc")
		logs.Error("%v", crcNotEqual.Error())
		return checkCRC, crcNotEqual
	}

	return checkCRC, nil
}

// Stat Get Chunk File Stat
// You can use update options to force refresh chunk stat via update options.
// if you don't want to force refresh chunk stat, you can ignore params.
func (cf *ChunkFile) Stat(h *UpdateHandler) (*ChunkHead, error) {
	if cf == nil || cf.Head == nil {
		nilChunk := fmt.Errorf("currrent chunk file is nil")
		logs.Error("%s", nilChunk.Error())
		return nil, nilChunk
	}

	if h == nil {
		return cf.Head, nil
	}

	updateOptionErrs := cf.forceUpdateOptions(h)

	if updateOptionErrs != nil {
		logs.Error("force update chunk file stat errors, err: %v", updateOptionErrs.Error())
		return nil, updateOptionErrs
	}

	return cf.Head, nil

}

// checkSizeValid to check chunk head size + chunk file size < file total size
// first param return the chunk file total size
// second param return error when chunk head size + chunk file size > file total size or other error occured
func (cf *ChunkFile) checkSizeValid() (uint64, error) {
	if cf.File == nil {
		fileNotExist := fmt.Errorf("chunk file is not opened")
		logs.Error("%s", fileNotExist.Error())
		return 0, fileNotExist
	}

	chunkStat, err := cf.File.Stat()
	if err != nil {
		logs.Error("read chunk file stat error, err: %s", err.Error())
		return 0, err
	}
	cfSize := uint64(chunkStat.Size())
	if cfSize < ChunkHeadSize+cf.Head.Size {
		sizeInvalid := fmt.Errorf("chunk file size is invalid")
		return cfSize, sizeInvalid
	}

	return cfSize, nil

}

func (cf *ChunkFile) ReadAt(p []byte, off uint64) (n int, err error) {
	cf.mu.RLock()
	defer cf.mu.Unlock()

	cf.Head.SetStatus(READING)
	defer cf.Head.SetStatus(VALID)

	for {
		if off >= cf.offset && off+uint64(len(p)) <= cf.offset+UpdateSteps {
			// 当前窗口命中
			copy(p, cf.mmap[off-cf.offset:])
			return len(p), nil
		}

		// 移动窗口

		newOffset := (off / UpdateSteps) * UpdateSteps
		if err := cf.remap(newOffset, UpdateSteps); err != nil {
			return 0, err
		}
	}
}

// remap newOffset is the offset for the chunk content after excluding the chunk head size
func (cf *ChunkFile) remap(newOffset uint64, stepSize uint64) error {
	if err := cf.unmapIfNeeded(); err != nil {
		return err
	}

	pageOffset, offDiff := cf.calculatePageOffset(newOffset)

	size, err := cf.calculateRemapSize(newOffset, stepSize, offDiff)
	if err != nil {
		return err
	}

	return cf.doRemap(newOffset, size, pageOffset, offDiff)
}

func (cf *ChunkFile) unmapIfNeeded() error {
	if cf.mmap != nil {
		if err := cf.mmap.Unmap(); err != nil {
			return err
		}
	}
	return nil
}

func (cf *ChunkFile) calculateRemapSize(newOffset uint64, stepSize uint64, offDiff uint64) (int, error) {
	chunkStat, err := cf.File.Stat()
	if err != nil {
		logs.Error("failed to get chunk file stat, file: %s, err: %v", cf.File.Name(), err)
		return 0, fmt.Errorf("get chunk file stat failed: %w", err)
	}

	fileSize := uint64(chunkStat.Size())
	capSize := fileSize - ChunkHeadSize

	size := stepSize
	if newOffset+UpdateSteps > capSize {
		size = capSize - newOffset + offDiff
	}
	return int(size), nil
}

func (cf *ChunkFile) calculatePageOffset(newOffset uint64) (int64, uint64) {
	pageSize := syscall.Getpagesize()
	pageOffset := (newOffset + ChunkHeadSize) / uint64(pageSize) * uint64(pageSize)
	offDiff := newOffset + ChunkHeadSize - pageOffset
	return int64(pageOffset), offDiff
}

func (cf *ChunkFile) doRemap(newOffset uint64, size int, pageOffset int64, offDiff uint64) error {
	mmap, err := mmap.MapRegion(cf.File, size, mmap.RDWR, 0, pageOffset)
	if err != nil {
		return err
	}

	cf.mmap = mmap[offDiff:]
	cf.m = &mmap
	cf.offset = newOffset
	return nil
}

func (cf *ChunkFile) Append(data []byte) (int, error) {
	_, updateCRC := UpdateCRC(false)
	_, updateSize := UpdateSize(false)
	_, updateHash := UpdateHash(false)
	_, updateVer := UpdateVer(false)
	_, UpdateModifyAt := UpdateModifyAt(false)
	appendCh := NewUpdateHandler(false, []PrepareFunc{},
		[]UpdateFunc{
			updateCRC,
			updateSize,
			updateHash,
			updateVer,
			UpdateModifyAt,
		},
	)
	return cf.append(data, appendCh)
}

func (cf *ChunkFile) append(data []byte, h *UpdateHandler) (int, error) {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	cf.Head.SetStatus(WRITING)
	defer cf.Head.SetStatus(VALID)

	appendSize := uint64(len(data))

	// TODO 待确认是从Config中读取还是另行处理
	// 超出最大限制进行截断
	if appendSize+cf.Head.Size > MaxChunkSize {
		data = data[:MaxChunkSize-cf.Head.Size]
	}
	appendSize = uint64(len(data))

	for idx := uint64(0); idx < appendSize; {
		writeSize := utils.Less(idx+UpdateSteps, appendSize-idx)
		_, err := cf.writer.WriteAt(data[idx:writeSize], int64(ChunkHeadSize+cf.Head.Size+idx))
		if err != nil {
			logs.Warn("chunk file append data err: %v", err.Error())
			return -1, err
		}

		idx += writeSize
	}

	updateErrs := h.Apply(cf, data)
	if updateErrs != nil {
		logs.Error("append data occured error when do update options, err: %v", updateErrs.Error())
		return -1, nil
	}

	cf.Head.SetStatus(VALID)
	cf.flushChunkHead()
	return len(data), nil
}
