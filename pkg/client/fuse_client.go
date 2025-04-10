package client

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker/trackerservice"
	"github.com/JustACP/criceta/pkg/common/logs"
	nodeclient "github.com/JustACP/criceta/pkg/node/client"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type CricetaFS struct {
	fs.Inode

	client        *nodeclient.Client
	trackerClient trackerservice.Client
	root          string
	fileCache     map[string]*CricetaFile
	cacheMutex    sync.RWMutex
}

type CricetaFile struct {
	fs.Inode

	fs       *CricetaFS
	fileId   int64
	name     string
	size     int64
	data     []byte
	modified bool
	mu       sync.RWMutex
}

func NewCricetaFS(trackerClient trackerservice.Client, root string) *CricetaFS {
	return &CricetaFS{
		client:        nodeclient.NewClient(trackerClient),
		trackerClient: trackerClient,
		root:          root,
		fileCache:     make(map[string]*CricetaFile),
	}
}

func (fs *CricetaFS) OnMount(ctx context.Context) {
	// 加载根目录下的文件列表
	req := &tracker.ListFilesRequest{
		Base: &base.BaseRequest{
			RequestId: time.Now().UnixNano(),
			Timestamp: time.Now().UnixNano(),
		},
		Status: int32(base.FileStatus_VALID), // 获取有效的文件
	}

	resp, err := fs.trackerClient.ListFiles(ctx, req)
	if err != nil {
		logs.Error("获取文件列表失败: %v", err)
		return
	}

	fs.cacheMutex.Lock()
	defer fs.cacheMutex.Unlock()

	fs.fileCache = make(map[string]*CricetaFile)

	for _, file := range resp.Files {
		cricetaFile := &CricetaFile{
			fs:     fs,
			fileId: file.Id,
			name:   file.Name,
			size:   file.Size,
		}
		fs.fileCache[file.Name] = cricetaFile
		child := fs.NewPersistentInode(ctx, cricetaFile, fs.StableAttr())
		fs.AddChild(file.Name, child, true)
	}
}

func (fs *CricetaFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fs.cacheMutex.RLock()
	file, exists := fs.fileCache[name]
	fs.cacheMutex.RUnlock()

	if !exists {
		req := &tracker.ListFilesRequest{
			Base: &base.BaseRequest{
				RequestId: time.Now().UnixNano(),
				Timestamp: time.Now().UnixNano(),
			},
			NamePattern: name,
			Status:      int32(base.FileStatus_VALID),
		}

		resp, err := fs.trackerClient.ListFiles(ctx, req)
		if err != nil {
			logs.Error("查找文件失败: %v", err)
			return nil, syscall.EIO
		}

		var fileMeta *tracker.FileMeta
		for _, f := range resp.Files {
			if f.Name == name {
				fileMeta = f
				break
			}
		}

		if fileMeta == nil {
			return nil, syscall.ENOENT
		}

		fs.cacheMutex.Lock()
		file = &CricetaFile{
			fs:     fs,
			fileId: fileMeta.Id,
			name:   fileMeta.Name,
			size:   fileMeta.Size,
		}
		fs.fileCache[name] = file
		fs.cacheMutex.Unlock()
	}

	out.Size = uint64(file.size)
	out.Mode = fuse.S_IFREG | 0644
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	now := time.Now()
	out.SetTimes(&now, &now, &now)

	child := fs.NewInode(ctx, file, fs.StableAttr())
	return child, 0
}

func (fs *CricetaFS) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	fs.cacheMutex.Lock()
	defer fs.cacheMutex.Unlock()

	if _, exists := fs.fileCache[name]; exists {
		return nil, nil, 0, syscall.EEXIST
	}

	err := fs.client.CreateFile(ctx, name, 0)
	if err != nil {
		logs.Error("Failed to create file: %v", err)
		return nil, nil, 0, syscall.EIO
	}

	resp, err := fs.trackerClient.ListFiles(ctx, &tracker.ListFilesRequest{
		Base: &base.BaseRequest{
			RequestId: time.Now().UnixNano(),
			Timestamp: time.Now().UnixNano(),
		},
		NamePattern: name,
	})
	if err != nil {
		logs.Error("Failed to get file metadata: %v", err)
		return nil, nil, 0, syscall.EIO
	}

	var fileMeta *tracker.FileMeta
	for _, file := range resp.Files {
		if file.Name == name {
			fileMeta = file
			break
		}
	}

	if fileMeta == nil {
		logs.Error("File metadata not found after creation")
		return nil, nil, 0, syscall.EIO
	}

	file := &CricetaFile{
		fs:     fs,
		fileId: fileMeta.Id,
		name:   name,
		size:   0,
		data:   make([]byte, 0),
	}

	fs.fileCache[name] = file

	stableAttr := fs.StableAttr()
	stableAttr.Mode = fuse.S_IFREG | 0777

	child := fs.NewPersistentInode(ctx, file, stableAttr)
	fs.AddChild(name, child, true)

	out.Mode = fuse.S_IFREG | 0777
	out.Size = 0
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())

	return child, file, fuse.FOPEN_DIRECT_IO, 0
}

func (fs *CricetaFS) Unlink(ctx context.Context, name string) syscall.Errno {
	fs.cacheMutex.Lock()
	defer fs.cacheMutex.Unlock()

	file, exists := fs.fileCache[name]
	if !exists {
		return syscall.ENOENT
	}

	err := fs.client.DeleteFile(ctx, file.fileId)
	if err != nil {
		logs.Error("Failed to delete file: %v", err)
		return syscall.EIO
	}

	delete(fs.fileCache, name)
	fs.RmChild(name)
	return 0
}

func (f *CricetaFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	f.mu.RLock()
	defer f.mu.RUnlock()

	out.Size = uint64(f.size)
	out.Mode = fuse.S_IFREG | 0777
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	out.SetTimes(nil, nil, nil)
	return 0
}

func (f *CricetaFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if off >= f.size {
		return fuse.ReadResultData([]byte{}), 0
	}

	end := off + int64(len(dest))
	if end > f.size {
		end = f.size
	}

	data, err := f.fs.client.ReadFile(ctx, f.fileId, off, end-off)
	// 排除EOF
	if err != nil && !strings.Contains(err.Error(), "EOF") {
		logs.Error("Failed to read file: %v", err)
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(data), 0
}

func (f *CricetaFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()

	newSize := off + int64(len(data))

	err := f.fs.client.WriteFile(ctx, f.fileId, off, data)
	if err != nil {
		logs.Error("Failed to write file: %v", err)
		return 0, syscall.EIO
	}

	if newSize > f.size {
		f.size = newSize
	}

	return uint32(len(data)), 0
}

func (f *CricetaFile) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.modified {
		return 0
	}

	// Chunk的分配和写入入口
	err := f.fs.client.WriteFile(ctx, f.fileId, 0, f.data)
	if err != nil {
		logs.Error("Failed to write file: %v", err)
		return syscall.EIO
	}

	f.modified = false
	return 0
}

func (f *CricetaFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()

	if size, ok := in.GetSize(); ok {
		f.size = int64(size)
		if size == 0 {
			f.data = make([]byte, 0)
		} else if int64(len(f.data)) < f.size {
			newData := make([]byte, f.size)
			copy(newData, f.data)
			f.data = newData
		}
	}

	if mode, ok := in.GetMode(); ok {
		out.Mode = mode
	}

	out.Size = uint64(f.size)
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())

	return 0
}

func (f *CricetaFile) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return f, fuse.FOPEN_DIRECT_IO, 0
}

func (f *CricetaFile) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {

	if f.modified {
		return f.Flush(ctx, fh)
	}
	return 0
}

func Mount(mountPoint string, trackerClient trackerservice.Client) (*fuse.Server, error) {
	if err := os.MkdirAll(mountPoint, 0777); err != nil {
		return nil, err
	}

	cricetaFS := NewCricetaFS(trackerClient, mountPoint)
	server, err := fs.Mount(mountPoint, cricetaFS, &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:         true,
			FsName:        "criceta",
			DisableXAttrs: true,
			AllowOther:    true,
			Options:       []string{"default_permissions"},
			DirectMount:   true,
			EnableLocks:   true,
		},
		FirstAutomaticIno: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("mount failed: %v", err)
	}

	return server, nil
}
