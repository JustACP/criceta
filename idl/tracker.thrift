namespace go criceta.tracker

include "base.thrift"
include "storage.thrift"

struct FileMeta {
    1: i64 id
    2: string name
    3: i64 length
    4: base.FileStatus status
    5: i64 createAt
    6: i64 updateAt
    7: map<i64, storage.ChunkMeta> chunks
    8: i64 version
    9: base.LockInfo lock_info
    10: map<i64, list<base.ReplicaInfo>> chunk_replicas
    11: i64 size                  // 文件的实际大小
    12: i64 modify_time          // 文件的最后修改时间
}

struct NodeInfo {
    1: i64 node_id
    2: string address
    3: base.NodeStatus status
    4: i64 last_heartbeat
    5: base.ServerStats server_stats
    6: i64 capacity
    7: base.NodeStats node_stats
}

struct CreateFileRequest {
    1: base.BaseRequest base
    2: string name
    3: i64 length
}

struct CreateFileResponse {
    1: base.BaseResponse base
    2: FileMeta meta
}

struct AllocateChunkRequest {
    1: base.BaseRequest base
    2: i64 fileId
    3: i64 offset
    4: i64 size
}

struct AllocateChunkResponse {
    1: base.BaseResponse base
    2: storage.ChunkMeta chunk
    3: NodeInfo node
}

struct CommitChunkRequest {
    1: base.BaseRequest base
    2: storage.ChunkMeta chunk
}

struct CommitChunkResponse {
    1: base.BaseResponse base
    2: storage.ChunkMeta chunk
    3: list<base.ReplicaInfo> replicas
}

struct GetFileRequest {
    1: base.BaseRequest base
    2: i64 fileId
}

struct GetFileResponse {
    1: base.BaseResponse base
    2: FileMeta meta
}

struct HeartbeatRequest {
    1: base.BaseRequest base
    2: i64 nodeId
    3: string host
    4: i32 port
    5: base.ServerStats serverStats
}

struct HeartbeatResponse {
    1: base.BaseResponse base
}

struct DeleteFileRequest {
    1: base.BaseRequest base
    2: i64 fileId
}

struct DeleteFileResponse {
    1: base.BaseResponse base
}

struct UnlockFileRequest {
    1: base.BaseRequest base
    2: i64 fileId
}

struct UnlockFileResponse {
    1: base.BaseResponse base
}

struct RegisterNodeRequest {
    1: base.BaseRequest base
    2: string address
    3: i64 capacity
}

struct RegisterNodeResponse {
    1: base.BaseResponse base
    2: i64 nodeId
}

struct ListFilesRequest {
    1: base.BaseRequest base
    2: i32 status
    3: string namePattern
}

struct ListFilesResponse {
    1: base.BaseResponse base
    2: list<FileMeta> files
}

struct AcquireLockRequest {
    1: base.BaseRequest base
    2: i64 fileId
    3: base.LockType lockType
    4: list<base.ChunkLockInfo> chunks
}

struct AcquireLockResponse {
    1: base.BaseResponse base
    2: bool success
    3: base.LockInfo current_lock
}

struct SyncReplicaRequest {
    1: base.BaseRequest base
    2: i64 chunkId
    3: i64 nodeId
    4: string crc
}

struct SyncReplicaResponse {
    1: base.BaseResponse base
    2: bool success
    3: base.ReplicaInfo replica_info
}

// 节点健康检查请求
struct HealthCheckRequest {
    1: base.BaseRequest base
    2: i64 nodeId
}

struct HealthCheckResponse {
    1: base.BaseResponse base
    2: base.HealthCheckResult result
}

// 获取节点列表请求
struct GetNodesRequest {
    1: base.BaseRequest base
    2: optional base.NodeStatus status  // 按状态过滤
    3: optional base.NodeRole role      // 按角色过滤
}

struct GetNodesResponse {
    1: base.BaseResponse base
    2: list<NodeInfo> nodes
}

// 更新节点状态请求
struct UpdateNodeStatusRequest {
    1: base.BaseRequest base
    2: i64 nodeId
    3: base.NodeStatus status
    4: optional string reason
}

struct UpdateNodeStatusResponse {
    1: base.BaseResponse base
    2: NodeInfo node
}

// Raft 投票请求
struct RequestVoteRequest {
    1: base.BaseRequest base
    2: i64 term                // 候选人的任期号
    3: i64 candidate_id        // 候选人ID
    4: i64 last_log_index     // 候选人最后日志条目的索引值
    5: i64 last_log_term      // 候选人最后日志条目的任期号
}

struct RequestVoteResponse {
    1: base.BaseResponse base
    2: i64 term               // 当前任期号
    3: bool vote_granted      // 是否投票给候选人
}

// Raft 日志复制请求
struct AppendEntriesRequest {
    1: base.BaseRequest base
    2: i64 term                // 领导人的任期号
    3: i64 leader_id           // 领导人ID
    4: i64 prev_log_index     // 新的日志条目之前的索引值
    5: i64 prev_log_term      // prevLogIndex条目的任期号
    6: list<base.RaftLogEntry> entries  // 准备存储的日志条目
    7: i64 leader_commit      // 领导人已经提交的日志的索引值
}

struct AppendEntriesResponse {
    1: base.BaseResponse base
    2: i64 term               // 当前任期号
    3: bool success           // 如果跟随者包含匹配prevLogIndex和prevLogTerm的日志则为真
    4: i64 match_index       // 已匹配的最高日志索引
}

// 配置变更请求
struct ProposeConfigChangeRequest {
    1: base.BaseRequest base
    2: base.RaftConfigChange change
}

struct ProposeConfigChangeResponse {
    1: base.BaseResponse base
    2: bool accepted
    3: i64 log_index
}

// 负载均衡相关请求
struct GetClusterStatsRequest {
    1: base.BaseRequest base
}

struct GetClusterStatsResponse {
    1: base.BaseResponse base
    2: i32 total_nodes
    3: i64 total_capacity
    4: i64 used_capacity
    5: map<i64, NodeInfo> node_stats
    6: base.BalanceConfig balance_config
}

struct UpdateBalanceConfigRequest {
    1: base.BaseRequest base
    2: base.BalanceConfig config
}

struct UpdateBalanceConfigResponse {
    1: base.BaseResponse base
    2: bool success
}

// 分片策略相关请求
struct UpdateShardingConfigRequest {
    1: base.BaseRequest base
    2: base.ShardingConfig config
}

struct UpdateShardingConfigResponse {
    1: base.BaseResponse base
    2: bool success
}

// 快照相关请求
struct InstallSnapshotRequest {
    1: base.BaseRequest base
    2: i64 term                // 当前任期号
    3: i64 leader_id           // 领导者ID
    4: base.SnapshotMeta meta  // 快照元数据
    5: binary data             // 快照数据块
    6: i64 offset             // 数据块偏移
    7: bool done              // 是否最后一块
}

struct InstallSnapshotResponse {
    1: base.BaseResponse base
    2: i64 term               // 当前任期号
    3: i64 bytes_stored       // 已存储字节数
}

// 集群配置管理
struct GetClusterConfigRequest {
    1: base.BaseRequest base
}

struct GetClusterConfigResponse {
    1: base.BaseResponse base
    2: base.ClusterConfig config
}

struct UpdateClusterConfigRequest {
    1: base.BaseRequest base
    2: base.ClusterConfig config
}

struct UpdateClusterConfigResponse {
    1: base.BaseResponse base
    2: bool success
}

// 监控和告警
struct ReportMetricsRequest {
    1: base.BaseRequest base
    2: i64 node_id
    3: list<base.MetricValue> metrics
}

struct ReportMetricsResponse {
    1: base.BaseResponse base
}

struct GetMetricsRequest {
    1: base.BaseRequest base
    2: list<string> metric_names
    3: i64 start_time
    4: i64 end_time
    5: map<string, string> labels
}

struct GetMetricsResponse {
    1: base.BaseResponse base
    2: map<string, list<base.MetricValue>> metrics
}

struct GetAlertsRequest {
    1: base.BaseRequest base
    2: optional base.AlertLevel min_level
    3: optional i64 start_time
    4: optional i64 end_time
}

struct GetAlertsResponse {
    1: base.BaseResponse base
    2: list<base.AlertInfo> alerts
}

// 备份管理
struct CreateBackupRequest {
    1: base.BaseRequest base
    2: list<i64> file_ids     // 要备份的文件ID列表，空表示全量备份
    3: string location        // 备份位置
}

struct CreateBackupResponse {
    1: base.BaseResponse base
    2: base.BackupMeta meta
}

struct RestoreFromBackupRequest {
    1: base.BaseRequest base
    2: i64 backup_id
    3: optional list<i64> file_ids  // 要恢复的文件ID列表，空表示全量恢复
}

struct RestoreFromBackupResponse {
    1: base.BaseResponse base
    2: list<FileMeta> restored_files
}

service TrackerService {
    CreateFileResponse CreateFile(1: CreateFileRequest req)
    AllocateChunkResponse AllocateChunk(1: AllocateChunkRequest req)
    CommitChunkResponse CommitChunk(1: CommitChunkRequest req)
    GetFileResponse GetFile(1: GetFileRequest req)
    HeartbeatResponse Heartbeat(1: HeartbeatRequest req)
    DeleteFileResponse DeleteFile(1: DeleteFileRequest req)
    UnlockFileResponse UnlockFile(1: UnlockFileRequest req)
    RegisterNodeResponse RegisterNode(1: RegisterNodeRequest req)
    ListFilesResponse ListFiles(1: ListFilesRequest req)
    AcquireLockResponse AcquireLock(1: AcquireLockRequest req)
    base.BaseResponse ReleaseLock(1: UnlockFileRequest req)
    SyncReplicaResponse SyncReplica(1: SyncReplicaRequest req)
    HealthCheckResponse HealthCheck(1: HealthCheckRequest req)
    GetNodesResponse GetNodes(1: GetNodesRequest req)
    UpdateNodeStatusResponse UpdateNodeStatus(1: UpdateNodeStatusRequest req)
    
    // Raft 共识相关接口
    RequestVoteResponse RequestVote(1: RequestVoteRequest req)
    AppendEntriesResponse AppendEntries(1: AppendEntriesRequest req)
    ProposeConfigChangeResponse ProposeConfigChange(1: ProposeConfigChangeRequest req)
    
    // 负载均衡相关接口
    GetClusterStatsResponse GetClusterStats(1: GetClusterStatsRequest req)
    UpdateBalanceConfigResponse UpdateBalanceConfig(1: UpdateBalanceConfigRequest req)
    UpdateShardingConfigResponse UpdateShardingConfig(1: UpdateShardingConfigRequest req)
    
    // 快照相关接口
    InstallSnapshotResponse InstallSnapshot(1: InstallSnapshotRequest req)
    
    // 集群配置管理接口
    GetClusterConfigResponse GetClusterConfig(1: GetClusterConfigRequest req)
    UpdateClusterConfigResponse UpdateClusterConfig(1: UpdateClusterConfigRequest req)
    
    // 监控和告警接口
    ReportMetricsResponse ReportMetrics(1: ReportMetricsRequest req)
    GetMetricsResponse GetMetrics(1: GetMetricsRequest req)
    GetAlertsResponse GetAlerts(1: GetAlertsRequest req)
    
    // 备份管理接口
    CreateBackupResponse CreateBackup(1: CreateBackupRequest req)
    RestoreFromBackupResponse RestoreFromBackup(1: RestoreFromBackupRequest req)
}