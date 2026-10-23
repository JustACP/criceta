//go:build e2e

// Package e2e spins up a real Criceta cluster (one tracker and several storage
// nodes) and exercises the public RPC surface end to end.
//
// Run it with:
//
//	go test -tags=e2e -v ./test/e2e
package e2e

import (
	"context"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage"
	"github.com/JustACP/criceta/kitex_gen/criceta/storage/storageservice"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker"
	"github.com/JustACP/criceta/kitex_gen/criceta/tracker/trackerservice"
	nodeclient "github.com/JustACP/criceta/pkg/node/client"
	"github.com/cloudwego/kitex/client"
	"github.com/stretchr/testify/require"
)

const (
	// fileSize is larger than the client write buffer, so the file is split
	// into several chunks and the replication path is exercised more than once.
	fileSize        = 8 << 20
	replicasPerFile = 2
	storageNodeNum  = 3
	startupTimeout  = 30 * time.Second
	replicaTimeout  = 90 * time.Second
)

type cluster struct {
	t             *testing.T
	root          string
	workDir       string
	trackerAddr   string
	trackerBin    string
	trackerConfig string
	trackerProc   *exec.Cmd
	storageAddr   map[int64]string
	storageProcs  map[int64]*exec.Cmd
	procs         []*exec.Cmd
	logFiles      map[string]string
}

func TestClusterWriteReplicateAndRewrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end test in short mode")
	}

	c := startCluster(t, storageNodeNum)
	trackerCli := newTrackerClient(t, c.trackerAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// 1. Create a file and write 8 MiB through the client SDK.
	_, err := trackerCli.CreateFile(ctx, &tracker.CreateFileRequest{
		Base:   newBaseRequest(),
		Name:   "e2e.bin",
		Length: fileSize,
	})
	require.NoError(t, err)

	file := waitForFile(ctx, t, trackerCli, "e2e.bin")
	writer := nodeclient.NewClient(trackerCli)
	data := payload(fileSize, 0x00)

	require.NoError(t, writer.WriteFile(ctx, file.Id, 0, data))
	require.NoError(t, writer.ReleaseFile(ctx, file.Id))

	readBack, err := writer.ReadFile(ctx, file.Id, 0, fileSize)
	require.NoError(t, err)
	requireSameBytes(t, data, readBack, "initial write")

	// 2. Every chunk must end up with the configured number of valid replicas.
	var meta *tracker.FileMeta
	require.Eventually(t, func() bool {
		meta = getFile(ctx, t, trackerCli, file.Id)
		if len(meta.Chunks) == 0 {
			return false
		}
		for chunkID := range meta.Chunks {
			replicas := meta.ChunkReplicas[chunkID]
			if len(replicas) < replicasPerFile {
				return false
			}
			for _, replica := range replicas {
				if replica.Status != int32(base.ChunkStatus_VALID) {
					return false
				}
			}
		}
		return true
	}, replicaTimeout, 500*time.Millisecond, "replicas did not become valid")

	// 3. Replica content must be byte-identical to the primary.
	for chunkID, chunk := range meta.Chunks {
		primary := c.readChunk(ctx, t, *chunk.NodeId, chunk)
		require.Equal(t, chunk.Size, int64(len(primary)), "chunk %d size on primary node", chunkID)

		for _, replica := range meta.ChunkReplicas[chunkID] {
			replicaData := c.readChunk(ctx, t, replica.NodeId, chunk)
			requireSameBytes(t, primary, replicaData,
				fmt.Sprintf("chunk %d replica on node %d", chunkID, replica.NodeId))
		}
	}

	// 4. Storage change reports must be acknowledged and trimmed: a non-empty
	//    journal means the tracker is not converging with the data plane.
	require.Eventually(t, c.reportsAcknowledged, 30*time.Second, 500*time.Millisecond,
		"storage report journals were not acknowledged by the tracker")

	// 4. A different client must be able to rewrite the file. If the previous
	//    writer still held its lease, the commit would be rejected.
	rewriter := nodeclient.NewClient(trackerCli)
	updated := payload(fileSize, 0x5a)

	require.NoError(t, rewriter.WriteFile(ctx, file.Id, 0, updated))
	require.NoError(t, rewriter.ReleaseFile(ctx, file.Id))

	readBack, err = rewriter.ReadFile(ctx, file.Id, 0, fileSize)
	require.NoError(t, err)
	requireSameBytes(t, updated, readBack, "rewrite")

	// 5. Restarting the tracker must not lose metadata: the file and its
	//    content have to survive a full process restart.
	c.restartTracker()

	trackerCli = newTrackerClient(t, c.trackerAddr)
	reader := nodeclient.NewClient(trackerCli)

	reloaded := getFile(ctx, t, trackerCli, file.Id)
	require.Equal(t, file.Id, reloaded.Id)
	require.Equal(t, "e2e.bin", reloaded.Name)

	afterRestart, err := reader.ReadFile(ctx, file.Id, 0, fileSize)
	require.NoError(t, err)
	requireSameBytes(t, updated, afterRestart, "after tracker restart")

	// 6. Orphan chunks must be reclaimed after their TTL. Write a chunk that no
	//    file references (as if the client crashed before committing it) and
	//    wait for the tracker to delete it from the storage node.
	orphanChunkID := int64(991001)
	orphanFileID := int64(424242)
	orphanData := []byte("orphan-payload")
	orphanCRC := strconv.FormatUint(uint64(crc32.ChecksumIEEE(orphanData)), 10)
	orphanNode := int64(2)

	storageCli := c.storageClient(t, orphanNode)
	_, err = storageCli.SyncChunk(ctx, &storage.SyncChunkRequest{
		Base: newBaseRequest(),
		Meta: &storage.ChunkMeta{
			Id:      orphanChunkID,
			FileId:  orphanFileID,
			Version: int64Ptr(1),
			Offset:  0,
			Size:    int64(len(orphanData)),
			NodeId:  &orphanNode,
			Crc:     &orphanCRC,
		},
		Data: orphanData,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, readErr := storageCli.ReadChunk(ctx, &storage.ReadChunkRequest{
			Base:    newBaseRequest(),
			ChunkId: orphanChunkID,
			Version: 1,
			FileId:  orphanFileID,
			Offset:  0,
			Size:    int64(len(orphanData)),
		})
		return readErr != nil
	}, 30*time.Second, 500*time.Millisecond, "orphan chunk was not reclaimed")

	// 7. Losing the primary of a chunk must not break reads: the client has to
	//    fall back to a healthy replica.
	victim := int64(0)
	for _, chunk := range meta.Chunks {
		victim = *chunk.NodeId
		break
	}
	require.NotZero(t, victim)
	c.stopStorage(victim)

	degraded, err := reader.ReadFile(ctx, file.Id, 0, fileSize)
	require.NoError(t, err)
	requireSameBytes(t, updated, degraded, "read after primary failure")
}

func int64Ptr(v int64) *int64 { return &v }

func (c *cluster) storageClient(t *testing.T, nodeID int64) storageservice.Client {
	t.Helper()

	addr, ok := c.storageAddr[nodeID]
	require.True(t, ok, "unknown storage node %d", nodeID)

	cli, err := storageservice.NewClient(
		fmt.Sprintf("storage-%d", nodeID),
		client.WithHostPorts(addr),
		client.WithRPCTimeout(2*time.Minute),
	)
	require.NoError(t, err)

	return cli
}

// stopStorage 停止指定 Storage 进程，模拟节点宕机。
func (c *cluster) stopStorage(nodeID int64) {
	c.t.Helper()

	cmd, ok := c.storageProcs[nodeID]
	require.True(c.t, ok, "storage node %d is not running", nodeID)

	require.NoError(c.t, cmd.Process.Signal(os.Interrupt))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = cmd.Process.Wait()
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		c.t.Fatalf("storage node %d did not stop within 15s", nodeID)
	}

	delete(c.storageProcs, nodeID)
}

func newTrackerClient(t *testing.T, addr string) trackerservice.Client {
	t.Helper()

	cli, err := trackerservice.NewClient(
		"tracker.criceta",
		client.WithHostPorts(addr),
		client.WithRPCTimeout(2*time.Minute),
	)
	require.NoError(t, err)

	return cli
}

// reportsAcknowledged 检查所有 Storage 的变更日志是否已被 Tracker 确认并裁剪。
// 日志为空说明“本地已落盘 → 上报 → ACK → 裁剪”这条链路走通了。
func (c *cluster) reportsAcknowledged() bool {
	for i := 1; i <= storageNodeNum; i++ {
		path := filepath.Join(c.workDir, fmt.Sprintf("storage-%d", i), "report.journal")

		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return false
		}

		for _, line := range strings.Split(string(data), "\n") {
			if strings.TrimSpace(line) != "" {
				return false
			}
		}
	}

	return true
}

// startCluster builds the binaries, writes configs into a temporary directory
// and launches one tracker plus n storage nodes.
func startCluster(t *testing.T, n int) *cluster {
	t.Helper()

	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)

	c := &cluster{
		t:            t,
		root:         root,
		workDir:      t.TempDir(),
		storageAddr:  make(map[int64]string),
		storageProcs: make(map[int64]*exec.Cmd),
		logFiles:     make(map[string]string),
	}

	for _, dir := range []string{"bin", "logs"} {
		require.NoError(t, os.MkdirAll(filepath.Join(c.workDir, dir), 0o755))
	}

	trackerBin := c.build("tracker", "./cmd/tracker")
	storageBin := c.build("storage", "./cmd/storage")

	trackerPort := freePort(t)
	c.trackerAddr = fmt.Sprintf("127.0.0.1:%d", trackerPort)
	c.trackerBin = trackerBin

	trackerDataDir := filepath.Join(c.workDir, "tracker")
	require.NoError(t, os.MkdirAll(trackerDataDir, 0o755))

	c.trackerConfig = filepath.Join(c.workDir, "tracker.yaml")
	require.NoError(t, os.WriteFile(c.trackerConfig, []byte(trackerConfigYAML(trackerPort, trackerDataDir)), 0o644))

	c.start("tracker", trackerBin, "-config", c.trackerConfig)
	waitForPort(t, c.trackerAddr)

	for i := 0; i < n; i++ {
		nodeID := int64(i + 2)
		port := freePort(t)
		dataDir := filepath.Join(c.workDir, fmt.Sprintf("storage-%d", i+1))
		require.NoError(t, os.MkdirAll(dataDir, 0o755))

		configPath := filepath.Join(c.workDir, fmt.Sprintf("storage-%d.yaml", i+1))
		require.NoError(t, os.WriteFile(
			configPath,
			[]byte(storageConfigYAML(nodeID, port, trackerPort, dataDir)),
			0o644,
		))

		c.start(fmt.Sprintf("storage-%d", i+1), storageBin, "-config", configPath)
		c.storageProcs[nodeID] = c.procs[len(c.procs)-1]
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		c.storageAddr[nodeID] = addr
		waitForPort(t, addr)
	}

	t.Cleanup(func() {
		c.stop()
		if t.Failed() {
			c.dumpLogs()
		}
	})

	waitForNodes(t, c.trackerAddr, n)

	return c
}

func (c *cluster) build(name, pkg string) string {
	c.t.Helper()

	out := filepath.Join(c.workDir, "bin", name)
	cmd := exec.Command("go", "build", "-buildvcs=false", "-o", out, pkg)
	cmd.Dir = c.root
	output, err := cmd.CombinedOutput()
	require.NoError(c.t, err, "go build %s failed:\n%s", pkg, output)

	return out
}

func (c *cluster) start(name, bin string, args ...string) {
	c.t.Helper()

	logPath := filepath.Join(c.workDir, "logs", name+".log")
	logFile, err := os.Create(logPath)
	require.NoError(c.t, err)
	defer logFile.Close()

	cmd := exec.Command(bin, args...)
	cmd.Dir = c.root
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(c.t, cmd.Start())

	c.procs = append(c.procs, cmd)
	c.logFiles[name] = logPath

	if name == "tracker" {
		c.trackerProc = cmd
	}
}

// restartTracker 停止 Tracker 并用同一份配置重新启动，
// 用于验证元数据快照能让文件在进程重启后继续可见。
func (c *cluster) restartTracker() {
	c.t.Helper()

	require.NotNil(c.t, c.trackerProc)

	require.NoError(c.t, c.trackerProc.Process.Signal(os.Interrupt))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.trackerProc.Process.Wait()
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		_ = c.trackerProc.Process.Kill()
		c.t.Fatal("tracker did not stop within 15s")
	}

	c.start("tracker", c.trackerBin, "-config", c.trackerConfig)
	waitForPort(c.t, c.trackerAddr)
	waitForNodes(c.t, c.trackerAddr, storageNodeNum)
}

func (c *cluster) stop() {
	deadline := time.Now().Add(10 * time.Second)

	for _, cmd := range c.procs {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(os.Interrupt)
		}
	}

	for _, cmd := range c.procs {
		done := make(chan struct{})
		go func(cmd *exec.Cmd) {
			defer close(done)
			_ = cmd.Wait()
		}(cmd)

		select {
		case <-done:
		case <-time.After(time.Until(deadline)):
			_ = cmd.Process.Kill()
			<-done
		}
	}
}

func (c *cluster) dumpLogs() {
	for name, path := range c.logFiles {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		c.t.Logf("---- %s ----\n%s", name, tail(string(data), 100))
	}
}

func (c *cluster) readChunk(ctx context.Context, t *testing.T, nodeID int64, chunk *storage.ChunkMeta) []byte {
	t.Helper()

	addr, ok := c.storageAddr[nodeID]
	require.True(t, ok, "unknown storage node %d", nodeID)

	cli, err := storageservice.NewClient(
		fmt.Sprintf("storage-%d", nodeID),
		client.WithHostPorts(addr),
		client.WithRPCTimeout(2*time.Minute),
	)
	require.NoError(t, err)

	version := int64(0)
	if chunk.Version != nil {
		version = *chunk.Version
	}

	resp, err := cli.ReadChunk(ctx, &storage.ReadChunkRequest{
		Base:    newBaseRequest(),
		ChunkId: chunk.Id,
		Version: version,
		FileId:  chunk.FileId,
		Offset:  0,
		Size:    chunk.Size,
	})
	require.NoError(t, err)

	return resp.Data
}

func freePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}

func waitForPort(t *testing.T, addr string) {
	t.Helper()

	deadline := time.Now().Add(startupTimeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("%s did not start listening within %s", addr, startupTimeout)
}

func waitForNodes(t *testing.T, trackerAddr string, want int) {
	t.Helper()

	trackerCli, err := trackerservice.NewClient(
		"tracker.criceta",
		client.WithHostPorts(trackerAddr),
		client.WithRPCTimeout(10*time.Second),
	)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, err := trackerCli.GetNodes(context.Background(), &tracker.GetNodesRequest{
			Base: newBaseRequest(),
		})
		if err != nil {
			return false
		}
		return len(resp.Nodes) >= want
	}, startupTimeout, 200*time.Millisecond, "storage nodes were not registered")
}

func waitForFile(ctx context.Context, t *testing.T, trackerCli trackerservice.Client, name string) *tracker.FileMeta {
	t.Helper()

	var file *tracker.FileMeta
	require.Eventually(t, func() bool {
		resp, err := trackerCli.ListFiles(ctx, &tracker.ListFilesRequest{
			Base:        newBaseRequest(),
			NamePattern: name,
		})
		if err != nil {
			return false
		}
		for _, candidate := range resp.Files {
			if candidate.Name == name {
				file = candidate
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "file %q was not created", name)

	return file
}

func getFile(ctx context.Context, t *testing.T, trackerCli trackerservice.Client, fileID int64) *tracker.FileMeta {
	t.Helper()

	resp, err := trackerCli.GetFile(ctx, &tracker.GetFileRequest{
		Base:   newBaseRequest(),
		FileId: fileID,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Meta)

	return resp.Meta
}

func requireSameBytes(t *testing.T, want, got []byte, msg string) {
	t.Helper()

	require.Equal(t, len(want), len(got), "%s: length mismatch", msg)
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("%s: first mismatch at byte %d", msg, i)
		}
	}
}

func payload(size int, seed byte) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*31+7) ^ seed
	}
	return data
}

func newBaseRequest() *base.BaseRequest {
	return &base.BaseRequest{
		RequestId: time.Now().UnixNano(),
		Timestamp: time.Now().UnixNano(),
	}
}

func tail(s string, lines int) string {
	parts := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(parts) <= lines {
		return s
	}
	return strings.Join(parts[len(parts)-lines:], "\n")
}

func trackerConfigYAML(port int, dataDir string) string {
	return fmt.Sprintf(`server_id: 1
server_name: "tracker-1"
server_ip: "127.0.0.1"
port: %d
server_type: "TRACKER"

log:
  mode: "STDOUT"
  level: "INFO"

data_dir: "%s"
orphan_ttl: 2

cluster_mode: "standalone"
node_role: "master"

slot_range:
  start: 0
  end: 1000000
`, port, dataDir)
}

func storageConfigYAML(nodeID int64, port, trackerPort int, dataDir string) string {
	return fmt.Sprintf(`server_id: %d
server_name: "storage-%d"
server_ip: "127.0.0.1"
port: %d
server_type: "STORAGE"

log:
  mode: "STDOUT"
  level: "INFO"

trackers:
  - server_id: 1
    server_name: "tracker-1"
    server_ip: "127.0.0.1"
    port: %d
    server_type: "TRACKER"

chunk_storage:
  file_path: "%s"
  buffer_size: 4194304
  max_storage_size: 1073741824
  integrity_check_interval: 3600
  recycle_interval: 1800

heartbeat_interval: 1
`, nodeID, nodeID, port, trackerPort, dataDir)
}
