package config

import (
	"fmt"

	"github.com/JustACP/criceta/pkg/common/logs"
)

// ChunkStorageConfig Storage Node Save File Config
// FilePath is the path where the chunk files save.
// MaxStorageSize is the storage node max storage size.
// IntegrityCheckInterval How often to perform an integrity check.
// RecycleInterval How often to recovery orphan chunk files.
type ChunkStorageConfig struct {
	FilePath               string `json:"file_path"`
	BufferSize             int64  `json:"buffer_size"`              // Bytes
	MaxStorageSize         *int64 `json:"max_storage_size"`         // KBytes
	IntegrityCheckInterval int64  `json:"integrity_check_interval"` // seconds
	RecycleInterval        int64  `json:"recycle_interval"`         // seconds
}

func (csc *ChunkStorageConfig) SaveConfig() map[string]any {
	output := map[string]any{
		"file_path":                csc.FilePath,
		"buffer_size":              csc.BufferSize,
		"integrity_check_interval": csc.IntegrityCheckInterval,
		"recycle_interval":         csc.RecycleInterval,
	}

	if csc.MaxStorageSize != nil {
		output["max_storage_size"] = *csc.MaxStorageSize
	}

	return output
}

func (csc *ChunkStorageConfig) ReadConfig(input map[string]any) (Config, error) {
	var err error

	// Required fields
	csc.FilePath, err = getParam[string](input, "file_path")
	if err != nil {
		err = fmt.Errorf("read file_path error: %v", err)
		logs.Error("%s", err.Error())
		return nil, err
	}

	csc.BufferSize, err = getParam[int64](input, "buffer_size")
	if err != nil {
		err = fmt.Errorf("read buffer_size error: %v", err)
		logs.Error("%s", err.Error())
		return nil, err
	}

	csc.IntegrityCheckInterval, err = getParam[int64](input, "integrity_check_interval")
	if err != nil {
		err = fmt.Errorf("read integrity_check_interval error: %v", err)
		logs.Error("%s", err.Error())
		return nil, err
	}

	csc.RecycleInterval, err = getParam[int64](input, "recycle_interval")
	if err != nil {
		err = fmt.Errorf("read recycle_interval error: %v", err)
		logs.Error("%s", err.Error())
		return nil, err
	}

	// Optional field
	if maxStorageSize, err := getParam[int64](input, "max_storage_size"); err == nil {
		csc.MaxStorageSize = &maxStorageSize
	}

	return csc, nil
}

// StorageNodeConfig Storage server boot config.
// Trackers the trackers list in your cluster.
// HeartbeatInterval how often to send a heartbeat signal.
type StorageNodeConfig struct {
	*ServerConfig
	Log               *LogConfig          `json:"log"`
	Trackers          []*ServerConfig     `json:"trackers"`
	ChunkStorage      *ChunkStorageConfig `json:"chunk_storage"`
	HeartbeatInterval int64               `json:"heartbeat_interval"` // seconds
}

func (snc *StorageNodeConfig) SaveConfig() map[string]any {
	output := snc.ServerConfig.SaveConfig()

	trackersOutput := make([]map[string]any, len(snc.Trackers))
	for idx, currTracker := range snc.Trackers {
		trackersOutput[idx] = currTracker.SaveConfig()
	}

	output["trackers"] = trackersOutput

	output["log"] = snc.Log.SaveConfig()

	output["chunk_storage"] = snc.ChunkStorage.SaveConfig()

	output["heartbeat_interval"] = snc.HeartbeatInterval

	return output
}

func (snc *StorageNodeConfig) readTrackersConfig(input map[string]any) (Config, error) {

	trackersInput, err := getParam[[]map[string]any](input, "trackers")
	if err != nil {
		return nil, fmt.Errorf("trackers config doesn't exist")
	}

	snc.Trackers = make([]*ServerConfig, 0, len(trackersInput))
	for _, currTrackerInput := range trackersInput {
		tracker := new(ServerConfig)
		_, err := tracker.ReadConfig(currTrackerInput)
		if err != nil {
			return nil, fmt.Errorf("convert tracker config err, err: %s", err.Error())
		}

		if tracker.ServerIP == nil {
			return nil, fmt.Errorf("tracker config lacks ServerIP parameter")
		}
		snc.Trackers = append(snc.Trackers, tracker)
	}

	return snc, nil

}

func (snc *StorageNodeConfig) ReadConfig(input map[string]any) (Config, error) {
	var err error

	if snc.ServerConfig == nil {
		snc.ServerConfig = new(ServerConfig)
	}
	_, err = snc.ServerConfig.ReadConfig(input)
	if err != nil {
		err = fmt.Errorf("storage Node Config read server config err, err: %s", err.Error())
		logs.Error("%s", err.Error())
		return nil, err
	}

	logInput, err := getParam[map[string]any](input, "log")
	if err != nil {
		err = fmt.Errorf("storage Node Config read Log config err, err: %s", err.Error())
		logs.Error("%s", err.Error())
		return nil, err
	}

	if snc.Log == nil {
		snc.Log = new(LogConfig)
	}
	_, err = snc.Log.ReadConfig(logInput)
	if err != nil {
		err = fmt.Errorf("storage Node Config read Log config err, err: %s", err.Error())
		logs.Error("%s", err.Error())
		return nil, err
	}

	_, err = snc.readTrackersConfig(input)
	if err != nil {
		err = fmt.Errorf("storage Node Config read Trackers config err, err: %s", err.Error())
		logs.Error("%s", err.Error())
		return nil, err
	}

	chunkStorageInput, err := getParam[map[string]any](input, "chunk_storage")
	if err != nil {
		err = fmt.Errorf("storage Node Config read Chunk Storage config err, err: %s", err.Error())
		logs.Error("%s", err.Error())
		return nil, err
	}

	if snc.ChunkStorage == nil {
		snc.ChunkStorage = new(ChunkStorageConfig)
	}
	_, err = snc.ChunkStorage.ReadConfig(chunkStorageInput)
	if err != nil {
		err = fmt.Errorf("storage Node Config read Chunk Storage config err, err: %s", err.Error())
		logs.Error("%s", err.Error())
		return nil, err
	}

	snc.HeartbeatInterval, err = getParam[int64](input, "heartbeat_interval")
	if err != nil {
		err = fmt.Errorf("storage Node Config read Hertbeat Interval err, err: %v", err.Error())
		logs.Error("%s", err.Error())
		return nil, err
	}

	return snc, nil
}
