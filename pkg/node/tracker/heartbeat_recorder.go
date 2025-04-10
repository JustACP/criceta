package tracker

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/JustACP/criceta/kitex_gen/criceta/base"
	"github.com/JustACP/criceta/pkg/common/logs"
)

const (
	defaultHeartbeatDir = "data/heartbeats"
	defaultMaxFileSize  = 100 * 1024 * 1024
	csvHeaders          = "timestamp,node_id,host,port," +
		// CPU stats
		"cpu_usage,cpu_user,cpu_sys,cpu_load1,cpu_load5,cpu_load15,cpu_cores," +
		// Memory stats
		"mem_total,mem_used,mem_free,mem_used_percent,mem_swap_total,mem_swap_used," +
		// Disk stats
		"disk_total,disk_used,disk_free,disk_used_percent,disk_read_speed,disk_write_speed,disk_iops," +
		// Network stats
		"net_bytes_sent,net_bytes_recv,net_packets_sent,net_packets_recv," +
		"net_errors_in,net_errors_out,net_drops_in,net_drops_out," +
		// System info
		"sys_hostname,sys_os,sys_platform,sys_kernel,sys_uptime,sys_pid,sys_process_name"
)

type HeartbeatRecorder struct {
	baseDir     string
	maxFileSize int64
	currentFile *os.File
	csvWriter   *csv.Writer
	mu          sync.Mutex

	currentFileName string
	currentSize     int64
}

func NewHeartbeatRecorder(baseDir string, maxFileSize int64) (*HeartbeatRecorder, error) {
	if baseDir == "" {
		baseDir = defaultHeartbeatDir
	}
	if maxFileSize <= 0 {
		maxFileSize = defaultMaxFileSize
	}

	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create heartbeat directory: %v", err)
	}

	recorder := &HeartbeatRecorder{
		baseDir:     baseDir,
		maxFileSize: maxFileSize,
	}

	if err := recorder.rotateFile(); err != nil {
		return nil, err
	}

	return recorder, nil
}

func (r *HeartbeatRecorder) Record(nodeId int64, host string, port int32, stats *base.ServerStats) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentSize >= r.maxFileSize {
		if err := r.rotateFile(); err != nil {
			return err
		}
	}

	record := []string{
		fmt.Sprintf("%d", time.Now().Unix()),
		fmt.Sprintf("%d", nodeId),
		host,
		fmt.Sprintf("%d", port),

		// CPU stats
		fmt.Sprintf("%.2f", stats.Cpu.UsagePercent),
		fmt.Sprintf("%.2f", stats.Cpu.UserPercent),
		fmt.Sprintf("%.2f", stats.Cpu.SysPercent),
		fmt.Sprintf("%.2f", stats.Cpu.Load1),
		fmt.Sprintf("%.2f", stats.Cpu.Load5),
		fmt.Sprintf("%.2f", stats.Cpu.Load15),
		fmt.Sprintf("%d", stats.Cpu.NumCores),

		// Memory stats
		fmt.Sprintf("%d", stats.Memory.Total),
		fmt.Sprintf("%d", stats.Memory.Used),
		fmt.Sprintf("%d", stats.Memory.Free),
		fmt.Sprintf("%.2f", stats.Memory.UsedPercent),
		fmt.Sprintf("%d", stats.Memory.SwapTotal),
		fmt.Sprintf("%d", stats.Memory.SwapUsed),

		// Disk stats
		fmt.Sprintf("%d", stats.Disk.Total),
		fmt.Sprintf("%d", stats.Disk.Used),
		fmt.Sprintf("%d", stats.Disk.Free),
		fmt.Sprintf("%.2f", stats.Disk.UsedPercent),
		fmt.Sprintf("%d", stats.Disk.ReadSpeed),
		fmt.Sprintf("%d", stats.Disk.WriteSpeed),
		fmt.Sprintf("%d", stats.Disk.Iops),

		// Network stats
		fmt.Sprintf("%d", stats.Network.BytesSent),
		fmt.Sprintf("%d", stats.Network.BytesRecv),
		fmt.Sprintf("%d", stats.Network.PacketsSent),
		fmt.Sprintf("%d", stats.Network.PacketsRecv),
		fmt.Sprintf("%d", stats.Network.ErrorsIn),
		fmt.Sprintf("%d", stats.Network.ErrorsOut),
		fmt.Sprintf("%d", stats.Network.DropsIn),
		fmt.Sprintf("%d", stats.Network.DropsOut),

		// System info
		stats.SystemInfo.Hostname,
		stats.SystemInfo.Os,
		stats.SystemInfo.Platform,
		stats.SystemInfo.Kernel,
		fmt.Sprintf("%d", stats.SystemInfo.Uptime),
		fmt.Sprintf("%d", stats.SystemInfo.ProcessId),
		stats.SystemInfo.ProcessName,
	}

	if err := r.csvWriter.Write(record); err != nil {
		return fmt.Errorf("failed to write record: %v", err)
	}
	r.csvWriter.Flush()

	fileInfo, err := r.currentFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %v", err)
	}
	r.currentSize = fileInfo.Size()

	return nil
}

func (r *HeartbeatRecorder) rotateFile() error {

	if r.currentFile != nil {
		r.csvWriter.Flush()
		r.currentFile.Close()
	}

	timestamp := time.Now().Format("2006-01-02_15-04-05")
	fileName := fmt.Sprintf("heartbeat_%s.csv", timestamp)
	filePath := filepath.Join(r.baseDir, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create new file: %v", err)
	}

	writer := csv.NewWriter(file)
	if err := writer.Write(strings.Split(csvHeaders, ",")); err != nil {
		file.Close()
		return fmt.Errorf("failed to write headers: %v", err)
	}
	writer.Flush()

	r.currentFile = file
	r.csvWriter = writer
	r.currentFileName = fileName
	r.currentSize = 0

	logs.Info("Rotated heartbeat log to new file: %s", fileName)
	return nil
}

func (r *HeartbeatRecorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentFile != nil {
		r.csvWriter.Flush()
		return r.currentFile.Close()
	}
	return nil
}
