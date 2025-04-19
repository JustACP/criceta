package config

import (
	"fmt"

	"github.com/JustACP/criceta/pkg/common/logs"
)

// TrackerNodeConfig Tracker server boot config.
// ClusterMode defines how tracker nodes work together.
// SlotRange defines the range of file IDs this tracker can assign.
type TrackerNodeConfig struct {
	*ServerConfig
	Log          *LogConfig     `json:"log"`
	ClusterMode  string         `json:"cluster_mode"` // "standalone", "cluster", "master-slave"
	NodeRole     string         `json:"node_role"`    // "master", "slave", "node"
	Slot         *SlotRange     `json:"slot_range,omitempty"`
	SlaveOf      *ServerConfig  `json:"slave_of,omitempty"`
	Slaves       []ServerConfig `json:"slaves,omitempty"`
	ClusterNodes []ServerConfig `json:"cluster_nodes,omitempty"` // 用于cluster模式下的节点列表
}

// SloatRange defines the range of file IDs this tracker can assign
type SlotRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// 有效的集群模式和节点角色
var (
	validClusterModes = map[string]bool{
		"standalone":   true,
		"cluster":      true,
		"master-slave": true,
	}

	validNodeRoles = map[string]bool{
		"master": true,
		"slave":  true,
		"node":   true,
	}
)

func (tc *TrackerNodeConfig) SaveConfig() map[string]any {
	// 验证集群模式和节点角色的有效性
	if !validClusterModes[tc.ClusterMode] || !validNodeRoles[tc.NodeRole] {
		return nil
	}

	output := tc.ServerConfig.SaveConfig()

	output["log"] = tc.Log.SaveConfig()
	output["cluster_mode"] = tc.ClusterMode
	output["node_role"] = tc.NodeRole

	if tc.Slot != nil {
		output["slot_range"] = map[string]any{
			"start": tc.Slot.Start,
			"end":   tc.Slot.End,
		}
	}

	if tc.SlaveOf != nil {
		output["slave_of"] = tc.SlaveOf.SaveConfig()
	}

	if len(tc.Slaves) > 0 {
		slaves := make([]map[string]any, len(tc.Slaves))
		for i, slave := range tc.Slaves {
			slaves[i] = slave.SaveConfig()
		}
		output["slaves"] = slaves
	}

	if len(tc.ClusterNodes) > 0 {
		nodes := make([]map[string]any, len(tc.ClusterNodes))
		for i, node := range tc.ClusterNodes {
			nodes[i] = node.SaveConfig()
		}
		output["cluster_nodes"] = nodes
	}

	return output
}

func (tc *TrackerNodeConfig) ReadConfig(input map[string]any) (Config, error) {
	var err error

	// Read base server config
	if tc.ServerConfig == nil {
		tc.ServerConfig = new(ServerConfig)
	}
	_, err = tc.ServerConfig.ReadConfig(input)
	if err != nil {
		err = fmt.Errorf("tracker config read server config err: %s", err.Error())
		logs.Error("%s", err.Error())
		return nil, err
	}

	// Read log config
	logInput, err := getParam[map[string]any](input, "log")
	if err != nil {
		err = fmt.Errorf("tracker config read log config err: %s", err.Error())
		logs.Error("%s", err.Error())
		return nil, err
	}

	if tc.Log == nil {
		tc.Log = new(LogConfig)
	}
	_, err = tc.Log.ReadConfig(logInput)
	if err != nil {
		err = fmt.Errorf("tracker config read log config err: %s", err.Error())
		logs.Error("%s", err.Error())
		return nil, err
	}

	// Read cluster mode
	tc.ClusterMode, err = getParam[string](input, "cluster_mode")
	if err != nil {
		err = fmt.Errorf("tracker config read cluster mode err: %s", err.Error())
		logs.Error("%s", err.Error())
		return nil, err
	}

	// Read node role
	tc.NodeRole, err = getParam[string](input, "node_role")
	if err != nil {
		err = fmt.Errorf("tracker config read node role err: %s", err.Error())
		logs.Error("%s", err.Error())
		return nil, err
	}

	// Read optional FileIDRange
	if slotRangeInput, err := getParam[map[string]any](input, "slot_range"); err == nil {
		tc.Slot = new(SlotRange)
		tc.Slot.Start, err = getParam[int64](slotRangeInput, "start")
		if err != nil {
			return nil, fmt.Errorf("invalid FileIDRange Start: %v", err)
		}
		tc.Slot.End, err = getParam[int64](slotRangeInput, "end")
		if err != nil {
			return nil, fmt.Errorf("invalid FileIDRange End: %v", err)
		}

		if tc.Slot.Start > tc.Slot.End {
			return nil, fmt.Errorf("invalid FileIDRange, start more than end, start: %v, end: %v", tc.Slot.Start, tc.Slot.End)
		}
	}

	// Read optional SlaveOf
	if slaveOfInput, err := getParam[map[string]any](input, "slave_of"); err == nil {
		tc.SlaveOf = new(ServerConfig)
		_, err = tc.SlaveOf.ReadConfig(slaveOfInput)
		if err != nil {
			return nil, fmt.Errorf("invalid SlaveOf config: %v", err)
		}
	}

	// Read optional Slaves
	if slavesInput, err := getParam[[]map[string]any](input, "slaves"); err == nil {
		tc.Slaves = make([]ServerConfig, len(slavesInput))
		for i, slaveInput := range slavesInput {
			_, err = tc.Slaves[i].ReadConfig(slaveInput)
			if err != nil {
				return nil, fmt.Errorf("invalid Slave config at index %d: %v", i, err)
			}
		}
	}

	// Read optional ClusterNodes
	if nodesInput, err := getParam[[]map[string]any](input, "cluster_nodes"); err == nil {
		tc.ClusterNodes = make([]ServerConfig, len(nodesInput))
		for i, nodeInput := range nodesInput {
			_, err = tc.ClusterNodes[i].ReadConfig(nodeInput)
			if err != nil {
				return nil, fmt.Errorf("invalid ClusterNode config at index %d: %v", i, err)
			}
		}
	}

	return tc, nil
}
