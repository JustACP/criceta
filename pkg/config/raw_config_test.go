package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testStorageYAML = `
server_id: 2
server_name: "storage-test"
server_ip: "127.0.0.1"
port: 18889
server_type: "STORAGE"

log:
  mode: "STDOUT"
  level: "INFO"

trackers:
  - server_id: 1
    server_name: "tracker-test"
    server_ip: "127.0.0.1"
    port: 18888
    server_type: "TRACKER"

chunk_storage:
  file_path: "/tmp/criceta-test"
  buffer_size: 4194304
  max_storage_size: 1073741824
  integrity_check_interval: 3600
  recycle_interval: 1800

heartbeat_interval: 10
`

// TestGetRawConfigReturnsLiveInstance 验证 GetRawConfig 返回全局实例指针：
// 对返回值的修改必须立即对其他调用方可见，而不是修改一个值拷贝。
func TestGetRawConfigReturnsLiveInstance(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "storage.yaml")
	require.NoError(t, os.WriteFile(path, []byte(testStorageYAML), 0644))

	prev := instance
	t.Cleanup(func() { instance = prev })

	instance = NewInstanceConfig(STORAGE, path)
	instance.GetConfig()

	conf, err := GetRawConfig[StorageNodeConfig]()
	require.NoError(t, err)
	require.NotNil(t, conf)
	assert.Equal(t, int64(2), conf.ServerId)

	// 修改必须落回全局实例
	conf.ServerId = 4242
	again, err := GetRawConfig[StorageNodeConfig]()
	require.NoError(t, err)
	assert.Same(t, conf, again)
	assert.Equal(t, int64(4242), instance.ServerConfig.(*StorageNodeConfig).ServerId)

	// 类型不匹配必须报错，而不是静默返回零值
	_, err = GetRawConfig[TrackerNodeConfig]()
	assert.Error(t, err)
}
