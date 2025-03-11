package config_test

import (
	"encoding/json"
	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/JustACP/criceta/pkg/common/utils"
	"github.com/JustACP/criceta/pkg/config"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

var exampleServerConfig = &config.ServerConfig{
	ServerId:   1,
	ServerName: utils.Ptr[string]("Boots"),
	ServerIP:   utils.Ptr[string]("1.1.1.1"),
	Port:       6084,
	ServerType: config.STORAGE,
}

var exampleLogConfig = &config.LogConfig{
	Mode:     config.MIX,
	Level:    logs.DEBUG,
	FilePath: utils.Ptr[string]("./logs/test.log"),
}

var exampleTrackers = []*config.ServerConfig{
	&config.ServerConfig{
		ServerId:   2,
		ServerName: utils.Ptr[string]("Tracker0"),
		ServerIP:   utils.Ptr[string]("1.1.1.2"),
		Port:       6084,
		ServerType: config.TRACKER,
	},
	&config.ServerConfig{
		ServerId:   3,
		ServerName: utils.Ptr[string]("Tracker1"),
		ServerIP:   utils.Ptr[string]("1.1.1.3"),
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
