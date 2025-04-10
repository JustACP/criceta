namespace go criceta.storage

include "base.thrift"

struct ChunkMeta {
    1: i64 id
    2: i64 fileId
    3: optional i64 version
    4: i64 offset        // Chunk在文件中的偏移量
    5: i64 size         // Chunk的总大小
    6: optional i64 nodeId
    7: optional base.ChunkStatus status
    8: optional i64 createAt
    9: optional i64 modifyAt
    10: optional string crc
    11: optional i32 replica_count
}

struct WriteChunkRequest {
    1: base.BaseRequest base
    2: ChunkMeta meta
    3: binary data
    4: i64 write_offset  // 数据在Chunk内的写入偏移量
    5: i64 write_size    // 要写入的数据大小
}

struct WriteChunkResponse {
    1: base.BaseResponse base
    2: ChunkMeta meta
}

struct ReadChunkRequest {
    1: base.BaseRequest base
    2: i64 chunkId
    3: i64 version
    4: i64 fileId
    5: i64 offset
    6: i64 size
}

struct ReadChunkResponse {
    1: base.BaseResponse base
    2: ChunkMeta meta
    3: binary data
}

struct SyncChunkRequest {
    1: base.BaseRequest base
    2: ChunkMeta meta
    3: binary data
}

struct SyncChunkResponse {
    1: base.BaseResponse base
    2: ChunkMeta meta
}

// 副本校验请求
struct VerifyReplicaRequest {
    1: base.BaseRequest base
    2: i64 chunkId
    3: i64 version
}

struct VerifyReplicaResponse {
    1: base.BaseResponse base
    2: bool is_valid
    3: string crc
    4: base.ChunkStatus status
}

// 副本恢复请求
struct RecoverChunkRequest {
    1: base.BaseRequest base
    2: ChunkMeta meta
    3: i64 source_node_id    // 源节点ID
    4: string source_address // 源节点地址
    5: i32 source_port      // 源节点端口
}

struct RecoverChunkResponse {
    1: base.BaseResponse base
    2: bool success
    3: ChunkMeta meta
}

// 获取Chunk状态请求
struct GetChunkStatusRequest {
    1: base.BaseRequest base
    2: list<i64> chunk_ids
}

struct ChunkStatusInfo {
    1: i64 chunk_id
    2: base.ChunkStatus status
    3: i64 version
    4: string crc
    5: i64 modify_time
}

struct GetChunkStatusResponse {
    1: base.BaseResponse base
    2: map<i64, ChunkStatusInfo> chunk_status
}

// 迁移 Chunk 请求
struct MigrateChunkRequest {
    1: base.BaseRequest base
    2: ChunkMeta meta
    3: i64 target_node_id      // 目标节点ID
    4: string target_address   // 目标节点地址
    5: i32 target_port        // 目标节点端口
}

struct MigrateChunkResponse {
    1: base.BaseResponse base
    2: bool success
    3: ChunkMeta meta
}

// 获取节点负载信息
struct GetNodeLoadRequest {
    1: base.BaseRequest base
}

struct NodeLoadInfo {
    1: i64 total_chunks
    2: i64 total_size
    3: i64 read_iops
    4: i64 write_iops
    5: i64 read_throughput
    6: i64 write_throughput
    7: double cpu_usage
    8: double memory_usage
    9: double disk_usage
    10: double network_usage
}

struct GetNodeLoadResponse {
    1: base.BaseResponse base
    2: NodeLoadInfo load_info
}

// 快照相关请求
struct CreateSnapshotRequest {
    1: base.BaseRequest base
    2: base.SnapshotMeta meta
}

struct CreateSnapshotResponse {
    1: base.BaseResponse base
    2: base.SnapshotMeta meta
    3: string snapshot_path
}

struct GetSnapshotRequest {
    1: base.BaseRequest base
    2: i64 index
    3: i64 term
}

struct GetSnapshotResponse {
    1: base.BaseResponse base
    2: base.SnapshotMeta meta
    3: binary data
    4: bool has_more
}

// 备份相关请求
struct BackupChunksRequest {
    1: base.BaseRequest base
    2: list<ChunkMeta> chunks
    3: string backup_path
}

struct BackupChunksResponse {
    1: base.BaseResponse base
    2: i64 backup_size
    3: map<i64, string> chunk_paths  // chunk_id -> backup_path
}

struct RestoreChunksRequest {
    1: base.BaseRequest base
    2: list<ChunkMeta> chunks
    3: string backup_path
}

struct RestoreChunksResponse {
    1: base.BaseResponse base
    2: map<i64, ChunkMeta> restored_chunks
}

service StorageService {
    WriteChunkResponse WriteChunk(1: WriteChunkRequest req)
    ReadChunkResponse ReadChunk(1: ReadChunkRequest req)
    SyncChunkResponse SyncChunk(1: SyncChunkRequest req)
    VerifyReplicaResponse VerifyReplica(1: VerifyReplicaRequest req)
    RecoverChunkResponse RecoverChunk(1: RecoverChunkRequest req)
    GetChunkStatusResponse GetChunkStatus(1: GetChunkStatusRequest req)
    MigrateChunkResponse MigrateChunk(1: MigrateChunkRequest req)
    GetNodeLoadResponse GetNodeLoad(1: GetNodeLoadRequest req)
    
    // 快照相关接口
    CreateSnapshotResponse CreateSnapshot(1: CreateSnapshotRequest req)
    GetSnapshotResponse GetSnapshot(1: GetSnapshotRequest req)
    
    // 备份相关接口
    BackupChunksResponse BackupChunks(1: BackupChunksRequest req)
    RestoreChunksResponse RestoreChunks(1: RestoreChunksRequest req)
}