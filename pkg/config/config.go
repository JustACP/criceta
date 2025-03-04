package config

import (
	"fmt"
	"strings"

	"github.com/JustACP/criceta/pkg/common"
	"github.com/JustACP/criceta/pkg/common/logs"
)

type ServerType int16

const (
	TRACKER = ServerType(1)
	STORAGE = ServerType(2)
)

var serverTypeNames = []string{"TRACKER", "STORAGE"}

func (st ServerType) String() string {
	stIdx := st - 1
	if len(serverTypeNames) <= int(stIdx) || int(stIdx) < 0 {
		return "UNKNOWN"
	}
	return serverTypeNames[stIdx]
}

func (st ServerType) FromString(s string) (common.Enum, error) {
	stName := strings.ToUpper(s)
	for idx, currServerTypeName := range serverTypeNames {
		if strings.Compare(currServerTypeName, stName) == 0 {
			return ServerType(idx + 1), nil
		}
	}

	return ServerType(0), fmt.Errorf("invalid server type")
}

type LogMode int16

const (
	STDOUT = 1
	FILE   = 2
	MIX    = 3
)

var logModeNames = []string{"STDOUT", "FILE", "MIX"}

func (lm LogMode) String() string {
	lmIdx := int(lm) - 1
	if len(logModeNames) <= lmIdx || lmIdx < 0 {
		return "UNKNOWN"
	}
	return logModeNames[lmIdx]
}

func (lm LogMode) FromString(s string) (common.Enum, error) {
	lmName := strings.ToUpper(s)
	for idx, currLogModeName := range logModeNames {
		if strings.Compare(lmName, currLogModeName) == 0 {
			return LogMode(idx + 1), nil
		}
	}

	return LogMode(0), fmt.Errorf("invalid log mode")
}

type Config interface {
	ReadConfig(input map[string]any) (Config, error)
	SaveConfig() map[string]any
}

type LogConfig struct {
	Mode     LogMode       `json:"Mode"`
	Level    logs.LogLevel `json:"Level"`
	FilePath *string       `json:"FilePath,omitempty"`
}

// getParam : get param and convert
func getParam[T any](input map[string]any, paramName string) (T, error) {

	var paramVal T

	val, exists := input[paramName]
	if !exists {
		return paramVal, fmt.Errorf("cannot found param %s", paramName)
	}

	paramVal, ok := val.(T)
	if !ok {
		return paramVal, fmt.Errorf("the param %s is not of type %T, real type: %T", paramName, paramVal, val)
	}

	return paramVal, nil
}

func convertEnumParam[T common.Enum](input map[string]any, paramName string) (T, error) {

	var enumVal T

	enumName, err := getParam[string](input, paramName)
	if err != nil {
		return enumVal, err
	}

	enumInstance, err := enumVal.FromString(enumName)
	if err != nil {
		return enumVal, err
	}

	enumVal, ok := enumInstance.(T)
	if !ok {
		return enumVal, fmt.Errorf("the param %s assert error, target type %T, real type: %T", paramName, enumVal, enumInstance)
	}

	return enumVal, nil

}

func (lc *LogConfig) ReadConfig(input map[string]any) (Config, error) {
	var err error

	// Read Mode
	lc.Mode, err = convertEnumParam[LogMode](input, "Mode")
	if err != nil {
		logs.Error("LogConfig read Mode param err, err: %v", err)
		return nil, err
	}

	// Read Level
	lc.Level, err = convertEnumParam[logs.LogLevel](input, "Level")
	if err != nil {
		logs.Error("LogConfig read Level param err, err: %v", err)
		return nil, err
	}

	// Read optional FilePath
	if lc.Mode == FILE || lc.Mode == MIX {
		var filePath string
		filePath, err = getParam[string](input, "FilePath")
		if err != nil {
			logs.Error("LogConfig read FilePath param err, err: %v", err)
			return nil, err
		}
		lc.FilePath = &filePath
	}

	return lc, nil
}

func (lc *LogConfig) SaveConfig() map[string]any {
	logOutput := map[string]any{
		"Mode":  lc.Mode.String(),
		"Level": lc.Level.String(),
	}

	if lc != nil {
		logOutput["FilePath"] = *lc.FilePath
	}

	return logOutput
}

// ServerConfig Common server config
// ServerName is an optional param, it is just used to note your services.
// ServerIP you have to configure it only when you to connect another server or connect to cluster.
// Port local service network port.
type ServerConfig struct {
	ServerId   int64      `json:"ServerId"`
	ServerName *string    `json:"ServerName,omitempty"`
	ServerIP   *string    `json:"ServerIP,omitempty"`
	Port       int32      `json:"Port"`
	ServerType ServerType `json:"ServerType"`
}

func (sc *ServerConfig) ReadConfig(input map[string]any) (Config, error) {
	var err error

	sc.ServerId, err = getParam[int64](input, "ServerId")
	if err != nil {
		logs.Error("ServerConfig read ServerId param err, err: %v", err)
		return nil, err
	}

	var serverName string
	serverName, _ = getParam[string](input, "ServerName")
	sc.ServerName = &serverName

	var serverIP string
	serverIP, _ = getParam[string](input, "ServerIP")
	sc.ServerIP = &serverIP

	sc.Port, err = getParam[int32](input, "Port")
	if err != nil {
		logs.Error("ServerConfig read Port param err, err: %v", err)
		return nil, err
	}

	sc.ServerType, err = convertEnumParam[ServerType](input, "ServerType")
	if err != nil {
		logs.Error("ServerConfig read ServerTyep param err, err: %v", err)
		return nil, err
	}

	return sc, nil
}

func (sc *ServerConfig) SaveConfig() map[string]any {
	config := map[string]any{
		"ServerId":   sc.ServerId,
		"Port":       sc.Port,
		"ServerType": sc.ServerType.String(),
	}

	if sc.ServerName != nil {
		config["ServerName"] = *sc.ServerName
	}
	if sc.ServerIP != nil {
		config["ServerIP"] = *sc.ServerIP
	}

	return config
}

// ChunkStorageConfig Storage Node Save File Config
// FilePath is the path where the chunk files save.
// MaxStorageSize is the storage node max storage size.
// IntegrityCheckInterval How often to perform an integrity check.
// RecycleInterval How often to recovery orphan chunk files.
type ChunkStorageConfig struct {
	FilePath               string `json:"FilePath"`
	BufferSize             int64  `json:"BufferSize"`             // Bytes
	MaxStorageSize         *int64 `json:"MaxStorageUsage"`        // KBytes
	IntegrityCheckInterval int64  `json:"IntegrityCheckInterval"` // seconds
	RecycleInterval        int64  `json:"RecycleInterval"`        // seconds
}

func (csc *ChunkStorageConfig) SaveConfig() map[string]any {
	output := map[string]any{
		"FilePath":               csc.FilePath,
		"BufferSize":             csc.BufferSize,
		"IntegrityCheckInterval": csc.IntegrityCheckInterval,
		"RecycleInterval":        csc.RecycleInterval,
	}

	if csc.MaxStorageSize != nil {
		output["MaxStorageUsage"] = *csc.MaxStorageSize
	}

	return output
}

func (csc *ChunkStorageConfig) ReadConfig(input map[string]any) (Config, error) {
	var err error

	// Required fields
	csc.FilePath, err = getParam[string](input, "FilePath")
	if err != nil {
		err = fmt.Errorf("read FilePath error: %v", err)
		logs.Error("%s", err.Error())
		return nil, err
	}

	csc.BufferSize, err = getParam[int64](input, "BufferSize")
	if err != nil {
		err = fmt.Errorf("read BufferSize error: %v", err)
		logs.Error("%s", err.Error())
		return nil, err
	}

	csc.IntegrityCheckInterval, err = getParam[int64](input, "IntegrityCheckInterval")
	if err != nil {
		err = fmt.Errorf("read IntegrityCheckInterval error: %v", err)
		logs.Error("%s", err.Error())
		return nil, err
	}

	csc.RecycleInterval, err = getParam[int64](input, "RecycleInterval")
	if err != nil {
		err = fmt.Errorf("read RecycleInterval error: %v", err)
		logs.Error("%s", err.Error())
		return nil, err
	}

	// Optional field
	if maxStorageSize, err := getParam[int64](input, "MaxStorageUsage"); err == nil {
		csc.MaxStorageSize = &maxStorageSize
	}

	return csc, nil
}

// StorageNodeConfig Storage server boot config.
// Trackers the trackers list in your cluster.
// HeartbeatInterval how often to send a heartbeat signal.
type StorageNodeConfig struct {
	*ServerConfig
	Log               *LogConfig          `json:"Log"`
	Trackers          []*ServerConfig     `json:"Trackers"`
	ChunkStorage      *ChunkStorageConfig `json:"ChunkStorage"`
	HeartbeatInterval int64               `json:"HeartbeatInterval"` // seconds
}

func (snc *StorageNodeConfig) SaveConfig() map[string]any {
	output := snc.ServerConfig.SaveConfig()

	trackersOutput := make([]map[string]any, len(snc.Trackers))
	for idx, currTracker := range snc.Trackers {
		trackersOutput[idx] = currTracker.SaveConfig()
	}

	output["Trackers"] = trackersOutput

	output["Log"] = snc.Log.SaveConfig()

	output["ChunkStorage"] = snc.ChunkStorage.SaveConfig()

	output["HeartbeatInterval"] = snc.HeartbeatInterval

	return output
}

func (snc *StorageNodeConfig) readTrackersConfig(input map[string]any) (Config, error) {

	trackersInput, err := getParam[[]map[string]any](input, "Trackers")
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

	logInput, err := getParam[map[string]any](input, "Log")
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

	chunkStorageInput, err := getParam[map[string]any](input, "ChunkStorage")
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

	snc.HeartbeatInterval, err = getParam[int64](input, "HeartbeatInterval")
	if err != nil {
		err = fmt.Errorf("storage Node Config read Hertbeat Interval err, err: %v", err.Error())
		logs.Error("%s", err.Error())
		return nil, err
	}

	return snc, nil
}
