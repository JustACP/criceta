package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/JustACP/criceta/pkg/common/utils"

	"github.com/JustACP/criceta/pkg/common"
	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/spf13/viper"
)

type ServerType int16

const (
	TRACKER = ServerType(1)
	STORAGE = ServerType(2)
)

type InstanceConfig struct {
	ServerConfig Config
	cf           *os.File
	v            *viper.Viper
	rw           sync.RWMutex
}

func (ic *InstanceConfig) GetConfig() {
	ic.rw.RLocker().Lock()
	defer ic.rw.RLocker().Unlock()

	if ic.v == nil {
		panic("instance config viper reader is nil")
	}

	err := ic.v.ReadInConfig()
	if err != nil {
		panic(fmt.Sprintf("read config error, err: %v", err.Error()))
	}

	_, err = ic.ServerConfig.ReadConfig(ic.v.AllSettings())
	if err != nil {
		panic(fmt.Sprintf("read config error, err: %v", err.Error()))
	}
}

func (ic *InstanceConfig) SaveConfig() error {
	ic.rw.Lock()
	defer ic.rw.Unlock()

	mapConf := ic.ServerConfig.SaveConfig()

	for k, v := range mapConf {
		ic.v.Set(k, v)
	}

	err := ic.v.WriteConfig()
	if err != nil {
		return err
	}

	return nil
}

const defaultConfigPath = "/etc/criceta"
const configName = "config"
const configType = "yaml"

func newViperReader(configPaths []string) *viper.Viper {
	v := viper.New()
	for _, path := range configPaths {
		v.SetConfigFile(path)
	}
	return v
}

func NewInstanceConfig(serverType ServerType, configPaths ...string) *InstanceConfig {
	instanceConf := &InstanceConfig{}

	switch serverType {
	case TRACKER:
		instanceConf.ServerConfig = new(TrackerNodeConfig)
	case STORAGE:
		instanceConf.ServerConfig = new(StorageNodeConfig)
	default:
		panic("No Server Type")
	}

	if len(configPaths) == 0 {
		configPaths = []string{filepath.Join(defaultConfigPath, fmt.Sprintf("%s.%s", configName, configType))}
	}

	var cf *os.File
	var err error
	var configFile string

	for _, path := range configPaths {
		if utils.FileExists(path) {
			cf, err = os.OpenFile(path, os.O_RDWR, 0764)
			if err == nil {
				configFile = path
				break
			}
		}
	}

	if cf == nil {
		panicMsg := fmt.Sprintf("No config was found, please check config file in: %v", configPaths)
		panic(panicMsg)
	}

	instanceConf.cf = cf
	instanceConf.v = newViperReader([]string{configFile})

	return instanceConf
}

var (
	instance          *InstanceConfig
	currentServerType ServerType
	once              sync.Once
)

func InitInstanceConfig(serverType ServerType, configPath ...string) {
	once.Do(func() {
		instance = NewInstanceConfig(serverType, configPath...)
		instance.GetConfig()
	})
}

func GetInstanceConfig() *InstanceConfig {

	return instance
}

func GetRawConfig[T any]() (*T, error) {
	instanceConf := GetInstanceConfig()
	if instanceConf == nil {
		return nil, fmt.Errorf("failed to get config instance")
	}

	// 获取 ServerConfig 的实际类型
	serverConfigType := reflect.TypeOf(instanceConf.ServerConfig)
	var conf T
	// 根据 T 的类型进行断言
	switch any((*T)(nil)).(type) {
	case *StorageNodeConfig:
		tmpConf, ok := instanceConf.ServerConfig.(*StorageNodeConfig)
		if !ok {
			return nil, fmt.Errorf("failed to get raw config: expected *StorageNodeConfig, got %s", serverConfigType)
		}
		conf, ok = interface{}(*tmpConf).(T)
		if !ok {
			return nil, fmt.Errorf("failed to convert config to type %T", (*T)(nil))
		}
	case *TrackerNodeConfig:
		tmpConf, ok := instanceConf.ServerConfig.(*TrackerNodeConfig)
		if !ok {
			return nil, fmt.Errorf("failed to get raw config: expected *TrackerNodeConfig, got %s", serverConfigType)
		}
		conf, ok = interface{}(*tmpConf).(T)
		if !ok {
			return nil, fmt.Errorf("failed to convert config to type %T", (*T)(nil))
		}
	default:
		return nil, fmt.Errorf("unsupported type %T", any((*T)(nil)))
	}

	return &conf, nil
}

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
	Mode     LogMode       `json:"mode"`
	Level    logs.LogLevel `json:"level"`
	FilePath *string       `json:"file_path,omitempty"`
}

func (lc *LogConfig) ReadConfig(input map[string]any) (Config, error) {
	var err error

	// Read Mode
	lc.Mode, err = convertEnumParam[LogMode](input, "mode")
	if err != nil {
		logs.Error("LogConfig read mode param err, err: %v", err)
		return nil, err
	}

	// Read Level
	lc.Level, err = convertEnumParam[logs.LogLevel](input, "level")
	if err != nil {
		logs.Error("LogConfig read level param err, err: %v", err)
		return nil, err
	}

	// Read optional FilePath
	if lc.Mode == FILE || lc.Mode == MIX {
		var filePath string
		filePath, err = getParam[string](input, "file_path")
		if err != nil {
			logs.Error("LogConfig read file_path param err, err: %v", err)
			return nil, err
		}
		lc.FilePath = &filePath
	}

	return lc, nil
}

func (lc *LogConfig) SaveConfig() map[string]any {
	logOutput := map[string]any{
		"mode":  lc.Mode.String(),
		"level": lc.Level.String(),
	}

	if lc != nil {
		logOutput["file_path"] = *lc.FilePath
	}

	return logOutput
}

// getParam : get param and convert
func getParam[T any](input map[string]any, paramName string) (T, error) {
	val, exists := input[paramName]
	if !exists {
		return *new(T), fmt.Errorf("cannot find param %s", paramName)
	}

	targetType := reflect.TypeOf(new(T)).Elem()
	valType := reflect.TypeOf(val)
	valReflect := reflect.ValueOf(val)
	// Slice 单独处理
	if targetType.Kind() == reflect.Slice && valType.Kind() == reflect.Slice {
		// 创建一个与目标类型相同的切片

		result := reflect.MakeSlice(targetType, valReflect.Len(), valReflect.Cap())

		// 遍历输入的切片
		for idx := 0; idx < valReflect.Len(); idx++ {
			// 获取当前元素
			currVal := reflect.ValueOf(val).Index(idx).Interface()
			currValType := reflect.TypeOf(currVal)
			if currValType.ConvertibleTo(targetType.Elem()) {
				convertedVal := reflect.ValueOf(currVal).Convert(targetType.Elem()).Interface()
				result.Index(idx).Set(reflect.ValueOf(convertedVal))
			}

		}
		return result.Interface().(T), nil
	}
	// Check if the value can be converted to the target type
	if valType.ConvertibleTo(targetType) {
		convertedVal := reflect.ValueOf(val).Convert(targetType).Interface()
		return convertedVal.(T), nil
	}

	return *new(T), fmt.Errorf("the param %s is not of type %T, real type: %T", paramName, *new(T), val)
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

// ServerConfig Common server config
// ServerName is an optional param, it is just used to note your services.
// ServerIP you have to configure it only when you to connect another server or connect to cluster.
// Port local service network port.
type ServerConfig struct {
	ServerId       int64      `json:"server_id"`
	ServerName     *string    `json:"server_name,omitempty"`
	ServerIP       *string    `json:"server_ip,omitempty"`
	Port           int32      `json:"port"`                       // RPC POrt
	RaftPort       *int32     `json:"raft_port, omitempty"`       // Raft Port
	MemberlistPort *int32     `json:"memberlist_port, omitempty"` // memberlist port
	ServerType     ServerType `json:"server_type"`
}

func (sc *ServerConfig) ReadConfig(input map[string]any) (Config, error) {
	var err error

	sc.ServerId, err = getParam[int64](input, "server_id")
	if err != nil {
		logs.Error("ServerConfig read server_id param err, err: %v", err)
		return nil, err
	}

	var serverName string
	serverName, _ = getParam[string](input, "server_name")
	sc.ServerName = &serverName

	var serverIP string
	serverIP, _ = getParam[string](input, "server_ip")
	sc.ServerIP = &serverIP

	sc.Port, err = getParam[int32](input, "port")
	if err != nil {
		logs.Error("ServerConfig read port param err, err: %v", err)
		return nil, err
	}

	var raftPort int32
	raftPort, _ = getParam[int32](input, "raft_port")
	sc.RaftPort = &raftPort

	var memberlistPort int32
	memberlistPort, _ = getParam[int32](input, "memberlist_port")
	sc.MemberlistPort = &memberlistPort

	sc.ServerType, err = convertEnumParam[ServerType](input, "server_type")
	if err != nil {
		logs.Error("ServerConfig read server_type param err, err: %v", err)
		return nil, err
	}

	return sc, nil
}

func (sc *ServerConfig) SaveConfig() map[string]any {
	config := map[string]any{
		"server_id":   sc.ServerId,
		"port":        sc.Port,
		"server_type": sc.ServerType.String(),
	}

	if sc.ServerName != nil {
		config["server_name"] = *sc.ServerName
	}
	if sc.ServerIP != nil {
		config["server_ip"] = *sc.ServerIP
	}

	return config
}
