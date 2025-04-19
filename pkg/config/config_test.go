package config_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/JustACP/criceta/pkg/common/utils"
	"github.com/JustACP/criceta/pkg/config"
	"github.com/stretchr/testify/assert"
)

var exampleServerConfig = &config.ServerConfig{
	ServerId:   1,
	ServerName: utils.Ptr("Boots"),
	ServerIP:   utils.Ptr("1.1.1.1"),
	Port:       6084,
	ServerType: config.STORAGE,
}

var exampleLogConfig = &config.LogConfig{
	Mode:     config.MIX,
	Level:    logs.DEBUG,
	FilePath: utils.Ptr("./logs/test.log"),
}

var exampleTrackers = []*config.ServerConfig{
	&config.ServerConfig{
		ServerId:   2,
		ServerName: utils.Ptr("Tracker0"),
		ServerIP:   utils.Ptr("1.1.1.2"),
		Port:       6084,
		ServerType: config.TRACKER,
	},
	&config.ServerConfig{
		ServerId:   3,
		ServerName: utils.Ptr("Tracker1"),
		ServerIP:   utils.Ptr("1.1.1.3"),
		Port:       6084,
		ServerType: config.TRACKER,
	},
}

var exampleChunkStorage = &config.ChunkStorageConfig{
	FilePath:               "/",
	BufferSize:             2048,
	MaxStorageSize:         utils.Ptr[int64](0),
	IntegrityCheckInterval: 1,
	RecycleInterval:        2,
}

var exampleStorageNodeConfig = &config.StorageNodeConfig{
	ServerConfig:      exampleServerConfig,
	Log:               exampleLogConfig,
	Trackers:          exampleTrackers,
	ChunkStorage:      exampleChunkStorage,
	HeartbeatInterval: 5,
}

func TestSaveStorageNodeConfig(t *testing.T) {
	saveConfig := exampleStorageNodeConfig.SaveConfig()

	exampleJSONBytes, err := json.MarshalIndent(saveConfig, "", "  ")
	assert.NoError(t, err, "marshal storage node config error")
	t.Log(string(exampleJSONBytes))

	inputStorageNodeConfig := new(config.StorageNodeConfig)
	readConfig, err := inputStorageNodeConfig.ReadConfig(saveConfig)
	assert.NoError(t, err, "read storage node config error")

	checkConfig := readConfig.SaveConfig()
	checkConfigJSONBytes, err := json.MarshalIndent(checkConfig, "", "  ")
	assert.NoError(t, err, "marshal storage node config error")
	t.Log(string(checkConfigJSONBytes))

	diff := strings.Compare(string(checkConfigJSONBytes), string(exampleJSONBytes))
	assert.Equal(t, diff, 0, "read storage node config error")
}

func TestReadStorageNodeConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]any
		wantErr bool
	}{
		{
			name: "valid config with all fields",
			config: map[string]any{
				"server_id":   int64(1),
				"server_name": "test",
				"server_ip":   "1.1.1.1",
				"port":        int32(6084),
				"server_type": "storage",
				"log": map[string]any{
					"mode":      "MIX",
					"level":     "DEBUG",
					"file_path": "./logs/test.log",
				},
				"trackers": []map[string]any{
					{
						"server_id":   int64(2),
						"server_name": "Tracker0",
						"server_ip":   "1.1.1.2",
						"port":        int32(6084),
						"server_type": "tracker",
					},
				},
				"chunk_storage": map[string]any{
					"file_path":                "/",
					"buffer_size":              int64(2048),
					"max_storage_size":         0,
					"integrity_check_interval": int64(1),
					"recycle_interval":         int64(1),
				},
				"heartbeat_interval": int64(5),
			},
			wantErr: false,
		},
		{
			name: "invalid field type",
			config: map[string]any{
				"server_id":   "invalid", // Should be int
				"server_name": "test",
				"server_ip":   "1.1.1.1",
				"port":        "6084", // Should be int
				"server_type": "storage",
			},
			wantErr: true,
		},
		{
			name: "invalid nested config type",
			config: map[string]any{
				"server_id":   int64(1),
				"server_name": "test",
				"server_ip":   "1.1.1.1",
				"port":        6084,
				"server_type": config.STORAGE,
				"log":         "invalid", // Should be map[string]any
			},
			wantErr: true,
		},
		{
			name: "invalid trackers type",
			config: map[string]any{
				"server_id":   int64(1),
				"server_name": "test",
				"server_ip":   "1.1.1.1",
				"port":        6084,
				"server_type": config.STORAGE,
				"trackers":    "invalid", // Should be []map[string]any
			},
			wantErr: true,
		},
	}

	for idx, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := new(config.StorageNodeConfig)
			_, err := config.ReadConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("StorageNodeConfig.ReadConfig() error = %v, wantErr %v, idx: %d", err, tt.wantErr, idx)
			}
		})
	}
}
