namespace go criceta.base

// 文件状态枚举
enum FileStatus {
    INVALID = 1,     // 无效状态
    VALID = 2,       // 有效状态
    DELETED = 3,     // 已删除
    READING = 4,     // 读取中
    WRITING = 5,     // 写入中
    SYNCING = 6,     // 同步中
}

// 节点状态枚举
enum NodeStatus {
    ONLINE = 0,      // 在线
    OFFLINE = 1,     // 离线
    ABNORMAL = 2,    // 异常
    SYNCING = 3,     // 同步中
}

// 锁类型枚举
enum LockType {
    NONE = 0,        // 无锁
    READ = 1,        // 读锁
    WRITE = 2,       // 写锁
}

// 系统常量配置
const i32 MIN_REPLICA_COUNT = 2      // 最小副本数
const i32 MAX_RETRY_COUNT = 3        // 最大重试次数
const i64 LOCK_LEASE_TIMEOUT = 30000 // 锁租约超时时间(毫秒)

// 错误码定义
const i32 SUCCESS = 0                    // 成功
const i32 ERR_INTERNAL = 1000           // 内部错误
const i32 ERR_INVALID_PARAM = 1001      // 参数错误
const i32 ERR_NOT_FOUND = 1002          // 资源不存在
const i32 ERR_ALREADY_EXISTS = 1003     // 资源已存在
const i32 ERR_NO_AVAILABLE_NODE = 1004  // 无可用节点
const i32 ERR_LOCK_CONFLICT = 1005      // 锁冲突
const i32 ERR_VERSION_CONFLICT = 1006   // 版本冲突
const i32 ERR_REPLICA_FAILED = 1007     // 副本同步失败
const i32 ERR_CRC_MISMATCH = 1008      // CRC校验失败
const i32 ERR_TIMEOUT = 1009           // 操作超时
const i32 ERR_NODE_OFFLINE = 1010      // 节点离线
const i32 ERR_INSUFFICIENT_SPACE = 1011 // 空间不足

// Chunk状态枚举
enum ChunkStatus {
    VALID = 1,        // 有效
    DELETED = 2,      // 已删除
    READING = 3,      // 读取中
    WRITING = 4,      // 写入中
    SYNCING = 5,      // 同步中
    ERROR = 6,        // 错误状态
}

// 节点角色枚举
enum NodeRole {
    UNKNOWN = 0,     // 未知
    LEADER = 1,      // 主节点
    FOLLOWER = 2,    // 从节点
    CANDIDATE = 3,   // 候选节点
}

// 节点健康检查结果
struct HealthCheckResult {
    1: bool is_healthy           // 是否健康
    2: i64 last_check_time      // 最后检查时间
    3: i32 consecutive_failures  // 连续失败次数
    4: string error_message     // 错误信息
    5: map<string, double> metrics  // 健康指标
}

struct BaseRequest {
    1: i64 request_id
    2: i64 timestamp
    3: map<string, string> metadata
}

struct BaseResponse {
    1: i32 code
    2: string message
    3: i64 timestamp
    4: map<string, string> metadata
}

// CPU相关统计
struct CPUStats {
    1: double usage_percent  // CPU使用率（0-100）
    2: double user_percent   // 用户态CPU占比
    3: double sys_percent    // 内核态CPU占比
    4: double load1         // 1分钟平均负载
    5: double load5         // 5分钟平均负载
    6: double load15        // 15分钟平均负载
    7: i32 num_cores        // CPU核心数
}

// 内存相关统计
struct MemoryStats {
    1: i64 total           // 总内存（字节）
    2: i64 used            // 已用内存（字节）
    3: i64 free            // 空闲内存（字节）
    4: double used_percent  // 内存使用率（0-100）
    5: i64 swap_total      // 交换区总大小
    6: i64 swap_used       // 交换区已用大小
}

// 磁盘相关统计
struct DiskStats {
    1: i64 total           // 磁盘总空间（字节）
    2: i64 used            // 已用空间（字节）
    3: i64 free            // 可用空间（字节）
    4: double used_percent  // 使用率（0-100）
    5: i64 read_speed      // 读取速度（字节/秒）
    6: i64 write_speed     // 写入速度（字节/秒）
    7: i64 iops            // 每秒IO操作数
}

// 网络相关统计
struct NetworkStats {
    1: i64 bytes_sent      // 累计发送字节数
    2: i64 bytes_recv      // 累计接收字节数
    3: i64 packets_sent    // 累计发送数据包数
    4: i64 packets_recv    // 累计接收数据包数
    5: i64 errors_in       // 接收错误数
    6: i64 errors_out      // 发送错误数
    7: i64 drops_in        // 接收丢包数
    8: i64 drops_out       // 发送丢包数
}

// 系统信息
struct SystemInfo {
    1: string hostname     // 主机名
    2: string os          // 操作系统
    3: string platform    // 平台信息
    4: string kernel      // 内核版本
    5: i64 uptime        // 系统运行时间（秒）
    6: i32 process_id    // 当前进程ID
    7: string process_name // 当前进程名
}

// 通用资源统计结构体
struct ServerStats {
    1: i64 timestamp      // 统计时间戳
    2: CPUStats cpu       // CPU相关统计
    3: MemoryStats memory // 内存相关统计
    4: DiskStats disk     // 磁盘相关统计
    5: NetworkStats network // 网络相关统计
    6: SystemInfo system_info // 系统信息
}

// Chunk锁定信息
struct ChunkLockInfo {
    1: i64 chunk_id        // Chunk ID
    2: i64 node_id         // 所在节点ID
    3: string node_address // 节点地址
    4: i32 node_port      // 节点端口
    5: i64 version        // Chunk版本
}

// 添加锁信息结构体
struct LockInfo {
    1: i64 client_id        // 持有锁的客户端ID
    2: LockType lock_type   // 锁类型
    3: i64 acquire_time     // 获取锁的时间戳
    4: i64 lease_timeout    // 租约超时时间
    5: list<ChunkLockInfo> locked_chunks  // 被锁定的chunk列表及其节点信息
}

// 添加副本信息结构体
struct ReplicaInfo {
    1: i64 node_id         // 副本所在节点ID
    2: i32 status          // 副本状态
    3: i64 sync_time       // 最后同步时间
    4: string crc          // 数据校验和
}

// Raft 日志条目类型
enum RaftLogType {
    NORMAL = 0,      // 普通日志
    CONFIG = 1,      // 配置变更
    SNAPSHOT = 2,    // 快照
}

// Raft 配置变更类型
enum RaftConfigChangeType {
    ADD_NODE = 0,    // 添加节点
    REMOVE_NODE = 1, // 移除节点
    UPDATE_NODE = 2, // 更新节点
}

// 负载均衡策略
enum BalanceStrategy {
    CAPACITY = 0,    // 按容量均衡
    IO_LOAD = 1,     // 按IO负载均衡
    NETWORK = 2,     // 按网络负载均衡
    HYBRID = 3,      // 混合策略
}

// Raft 日志条目
struct RaftLogEntry {
    1: i64 index            // 日志索引
    2: i64 term            // 任期号
    3: RaftLogType type    // 日志类型
    4: binary data         // 日志数据
    5: i64 timestamp      // 时间戳
}

// Raft 配置变更
struct RaftConfigChange {
    1: RaftConfigChangeType type  // 变更类型
    2: i64 node_id               // 目标节点ID
    3: string address           // 节点地址
    4: i32 port                // 节点端口
    5: map<string, string> metadata  // 元数据
}

// 分片策略配置
struct ShardingConfig {
    1: i64 max_chunk_size        // 最大chunk大小
    2: i32 min_chunks_per_file   // 每个文件最少chunk数
    3: i32 max_chunks_per_file   // 每个文件最大chunk数
    4: i32 target_chunk_size     // 目标chunk大小
}

// 负载均衡配置
struct BalanceConfig {
    1: BalanceStrategy strategy          // 均衡策略
    2: double capacity_threshold        // 容量阈值
    3: double io_threshold             // IO负载阈值
    4: double network_threshold        // 网络负载阈值
    5: i32 min_free_space_gb          // 最小剩余空间(GB)
    6: i32 balance_interval_sec       // 均衡检查间隔(秒)
}

// 更新 NodeStats 结构体
struct NodeStats {
    1: NodeRole role                    // 节点角色
    2: i64 leader_term                  // 当前任期（Raft）
    3: HealthCheckResult health         // 健康检查结果
    4: i64 total_chunks                 // 总Chunk数
    5: i64 syncing_chunks              // 同步中的Chunk数
    6: i64 corrupted_chunks            // 损坏的Chunk数
    7: map<i64, i32> replica_status    // 副本状态统计
    8: i64 last_log_index             // 最后日志索引
    9: i64 commit_index               // 已提交日志索引
    10: i64 last_applied_index        // 最后应用日志索引
    11: map<i64, i64> match_index     // 节点日志匹配索引
}

// 快照类型
enum SnapshotType {
    FULL = 0,        // 全量快照
    INCREMENTAL = 1, // 增量快照
    METADATA = 2,    // 元数据快照
}

// 告警级别
enum AlertLevel {
    INFO = 0,
    WARNING = 1,
    ERROR = 2,
    CRITICAL = 3,
}

// 告警类型
enum AlertType {
    NODE_DOWN = 0,        // 节点离线
    DISK_FULL = 1,        // 磁盘空间不足
    HIGH_LOAD = 2,        // 负载过高
    REPLICA_INSUFFICIENT = 3, // 副本数不足
    DATA_CORRUPTED = 4,   // 数据损坏
    NETWORK_PARTITION = 5, // 网络分区
}

// 快照元数据
struct SnapshotMeta {
    1: i64 index           // 快照对应的日志索引
    2: i64 term           // 快照对应的任期号
    3: SnapshotType type  // 快照类型
    4: i64 timestamp     // 创建时间
    5: i64 size          // 快照大小
    6: string crc        // 校验和
}

// 集群配置
struct ClusterConfig {
    1: i32 election_timeout_ms     // 选举超时时间
    2: i32 heartbeat_interval_ms   // 心跳间隔
    3: i32 snapshot_interval_sec   // 快照间隔
    4: i64 max_log_size           // 最大日志大小
    5: i32 max_batch_size         // 最大批处理大小
    6: ShardingConfig sharding    // 分片配置
    7: BalanceConfig balance      // 均衡配置
    8: map<string, string> extra  // 扩展配置
}

// 告警信息
struct AlertInfo {
    1: AlertLevel level           // 告警级别
    2: AlertType type            // 告警类型
    3: i64 timestamp            // 告警时间
    4: string target            // 告警目标
    5: string message           // 告警信息
    6: map<string, string> tags // 告警标签
}

// 监控指标
struct MetricValue {
    1: string name              // 指标名称
    2: double value            // 指标值
    3: i64 timestamp          // 时间戳
    4: map<string, string> labels // 标签
}

// 备份元数据
struct BackupMeta {
    1: i64 id                 // 备份ID
    2: i64 timestamp         // 备份时间
    3: i64 size             // 备份大小
    4: string location      // 备份位置
    5: string crc          // 校验和
    6: map<i64, list<i64>> file_chunks // 文件-Chunk映射
}

// 心跳信息结构体
struct HeartbeatInfo {
    1: i64 node_id         // 节点ID
    2: string host         // 主机地址
    3: i32 port           // 端口
    4: ServerStats stats   // 服务器状态
    5: i64 timestamp      // 心跳时间戳
    6: NodeRole role      // 节点角色
    7: i64 capacity       // 节点容量
    8: NodeStats node_stats // 节点统计信息
}