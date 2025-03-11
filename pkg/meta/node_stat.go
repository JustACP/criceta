package meta

import (
	"fmt"
	"os"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
	"github.com/shirou/gopsutil/process"
)

type Stats interface {
	Stat() error
}

// 通用资源统计结构体
type ServerStats struct {
	Timestamp  time.Time     `json:"timestamp"`   // 统计时间戳
	CPU        *CPUStats     `json:"cpu"`         // CPU相关统计
	Memory     *MemoryStats  `json:"memory"`      // 内存相关统计
	Disk       *DiskStats    `json:"disk"`        // 磁盘相关统计
	Network    *NetworkStats `json:"network"`     // 网络相关统计
	SystemInfo *SystemInfo   `json:"system_info"` // 系统信息
}

func (s *ServerStats) Stat() error {
	s.Timestamp = time.Now()

	if s.CPU == nil {
		s.CPU = &CPUStats{}
	}
	if err := s.CPU.Stat(); err != nil {
		return err
	}

	if s.Memory == nil {
		s.Memory = &MemoryStats{}
	}
	if err := s.Memory.Stat(); err != nil {
		return err
	}

	if s.Disk == nil {
		s.Disk = &DiskStats{}
	}
	if err := s.Disk.Stat(); err != nil {
		return err
	}

	if s.Network == nil {
		s.Network = &NetworkStats{}
	}
	if err := s.Network.Stat(); err != nil {
		return err
	}

	if s.SystemInfo == nil {
		s.SystemInfo = &SystemInfo{}
	}
	if err := s.SystemInfo.Stat(); err != nil {
		return err
	}

	return nil
}

// CPU相关统计
type CPUStats struct {
	UsagePercent float64 `json:"usage_percent"` // CPU使用率（0-100）
	UserPercent  float64 `json:"user_percent"`  // 用户态CPU占比
	SysPercent   float64 `json:"sys_percent"`   // 内核态CPU占比
	Load1        float64 `json:"load1"`         // 1分钟平均负载
	Load5        float64 `json:"load5"`         // 5分钟平均负载
	Load15       float64 `json:"load15"`        // 15分钟平均负载
	NumCores     int     `json:"num_cores"`     // CPU核心数
}

func (c *CPUStats) Stat() error {
	if err := c.setCPUUsage(); err != nil {
		return err
	}

	if err := c.setCPUTimes(); err != nil {
		return err
	}

	if err := c.setLoadAvg(); err != nil {
		return err
	}

	if err := c.setNumCores(); err != nil {
		return err
	}

	return nil
}

func (c *CPUStats) setCPUUsage() error {
	cpuPercents, err := cpu.Percent(0, false)
	if err != nil {
		return err
	}
	if len(cpuPercents) == 0 {
		return fmt.Errorf("no CPU usage data available")
	}
	c.UsagePercent = cpuPercents[0]
	return nil
}

func (c *CPUStats) setCPUTimes() error {
	cpuTimes, err := cpu.Times(false)
	if err != nil {
		return err
	}
	if len(cpuTimes) == 0 {
		return fmt.Errorf("no CPU times data available")
	}
	c.UserPercent = cpuTimes[0].User
	c.SysPercent = cpuTimes[0].System
	return nil
}

func (c *CPUStats) setLoadAvg() error {
	load, err := load.Avg()
	if err != nil {
		return err
	}
	c.Load1 = load.Load1
	c.Load5 = load.Load5
	c.Load15 = load.Load15
	return nil
}

func (c *CPUStats) setNumCores() error {
	cores, err := cpu.Counts(true)
	if err != nil {
		return err
	}
	c.NumCores = cores
	return nil
}

// 内存相关统计
type MemoryStats struct {
	Total       uint64  `json:"total"`        // 总内存（字节）
	Used        uint64  `json:"used"`         // 已用内存（字节）
	Free        uint64  `json:"free"`         // 空闲内存（字节）
	UsedPercent float64 `json:"used_percent"` // 内存使用率（0-100）
	SwapTotal   uint64  `json:"swap_total"`   // 交换区总大小
	SwapUsed    uint64  `json:"swap_used"`    // 交换区已用大小
}

func (m *MemoryStats) Stat() error {
	if err := m.setMemoryUsage(); err != nil {
		return err
	}

	if err := m.setSwapUsage(); err != nil {
		return err
	}

	return nil
}

func (m *MemoryStats) setMemoryUsage() error {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return err
	}

	m.Total = memInfo.Total
	m.Used = memInfo.Used
	m.Free = memInfo.Free
	m.UsedPercent = memInfo.UsedPercent
	return nil
}

func (m *MemoryStats) setSwapUsage() error {
	swapInfo, err := mem.SwapMemory()
	if err != nil {
		return err
	}

	m.SwapTotal = swapInfo.Total
	m.SwapUsed = swapInfo.Used
	return nil
}

// 磁盘相关统计
type DiskStats struct {
	Total       uint64  `json:"total"`        // 磁盘总空间（字节）
	Used        uint64  `json:"used"`         // 已用空间（字节）
	Free        uint64  `json:"free"`         // 可用空间（字节）
	UsedPercent float64 `json:"used_percent"` // 使用率（0-100）
	ReadSpeed   uint64  `json:"read_speed"`   // 读取速度（字节/秒）
	WriteSpeed  uint64  `json:"write_speed"`  // 写入速度（字节/秒）
	IOPS        uint64  `json:"iops"`         // 每秒IO操作数
}

func (d *DiskStats) Stat() error {
	if err := d.setDiskUsage(); err != nil {
		return err
	}

	if err := d.setDiskIO(); err != nil {
		return err
	}

	return nil
}

func (d *DiskStats) setDiskUsage() error {
	usage, err := disk.Usage("/")
	if err != nil {
		return err
	}

	d.Total = usage.Total
	d.Used = usage.Used
	d.Free = usage.Free
	d.UsedPercent = usage.UsedPercent
	return nil
}

func (d *DiskStats) setDiskIO() error {
	io1, err := disk.IOCounters()
	if err != nil {
		return err
	}

	time.Sleep(1 * time.Second)

	io2, err := disk.IOCounters()
	if err != nil {
		return err
	}

	for name, counter1 := range io1 {
		if counter2, ok := io2[name]; ok {
			d.ReadSpeed = counter2.ReadBytes - counter1.ReadBytes
			d.WriteSpeed = counter2.WriteBytes - counter1.WriteBytes
			d.IOPS = counter2.ReadCount - counter1.ReadCount + counter2.WriteCount - counter1.WriteCount
			break
		}
	}

	return nil
}

// 网络相关统计
type NetworkStats struct {
	BytesSent   uint64 `json:"bytes_sent"`   // 累计发送字节数
	BytesRecv   uint64 `json:"bytes_recv"`   // 累计接收字节数
	PacketsSent uint64 `json:"packets_sent"` // 累计发送数据包数
	PacketsRecv uint64 `json:"packets_recv"` // 累计接收数据包数
	ErrorsIn    uint64 `json:"errors_in"`    // 接收错误数
	ErrorsOut   uint64 `json:"errors_out"`   // 发送错误数
	DropsIn     uint64 `json:"drops_in"`     // 接收丢包数
	DropsOut    uint64 `json:"drops_out"`    // 发送丢包数
}

func (n *NetworkStats) Stat() error {
	if err := n.setNetworkUsage(); err != nil {
		return err
	}

	return nil
}

func (n *NetworkStats) setNetworkUsage() error {
	netIO, err := net.IOCounters(false)
	if err != nil {
		return err
	}

	if len(netIO) > 0 {
		n.BytesSent = netIO[0].BytesSent
		n.BytesRecv = netIO[0].BytesRecv
		n.PacketsSent = netIO[0].PacketsSent
		n.PacketsRecv = netIO[0].PacketsRecv
		n.ErrorsIn = netIO[0].Errin
		n.ErrorsOut = netIO[0].Errout
		n.DropsIn = netIO[0].Dropin
		n.DropsOut = netIO[0].Dropout
	}

	return nil
}

type SystemInfo struct {
	Hostname    string `json:"hostname"`     // 主机名
	OS          string `json:"os"`           // 操作系统
	Platform    string `json:"platform"`     // 平台信息
	Kernel      string `json:"kernel"`       // 内核版本
	Uptime      uint64 `json:"uptime"`       // 系统运行时间（秒）
	ProcessID   int    `json:"process_id"`   // 当前进程ID
	ProcessName string `json:"process_name"` // 当前进程名
}

func (s *SystemInfo) Stat() error {
	if err := s.setHostInfo(); err != nil {
		return err
	}

	if err := s.setProcessInfo(); err != nil {
		return err
	}

	return nil
}

func (s *SystemInfo) setHostInfo() error {
	hostInfo, err := host.Info()
	if err != nil {
		return err
	}

	s.Hostname = hostInfo.Hostname
	s.OS = hostInfo.OS
	s.Platform = hostInfo.Platform
	s.Kernel = hostInfo.KernelVersion
	s.Uptime = hostInfo.Uptime
	return nil
}

func (s *SystemInfo) setProcessInfo() error {
	pid := os.Getpid()
	proc, err := process.NewProcess(int32(pid))
	if err != nil {
		return err
	}

	s.ProcessID = pid
	name, err := proc.Name()
	if err != nil {
		return err
	}
	s.ProcessName = name

	return nil
}
