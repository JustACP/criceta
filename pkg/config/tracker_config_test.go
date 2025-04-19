package config

import (
	"reflect"
	"testing"

	"github.com/JustACP/criceta/pkg/common/logs"
	"github.com/JustACP/criceta/pkg/common/utils"
)

func TestTrackerNodeConfig_ReadConfig(t *testing.T) {
	tests := []struct {
		name    string
		input   map[string]any
		wantErr bool
	}{
		{
			name: "valid standalone config",
			input: map[string]any{
				"server_id":   int64(1),
				"server_name": "tracker1",
				"server_ip":   "127.0.0.1",
				"port":        int32(8080),
				"server_type": "TRACKER",
				"log": map[string]any{
					"mode":      "STDOUT",
					"level":     "INFO",
					"file_path": "/var/log/criceta.log",
				},
				"cluster_mode": "standalone",
				"node_role":    "master",
				"file_id_range": map[string]any{
					"start": int64(1),
					"end":   int64(1000000),
				},
			},
			wantErr: false,
		},
		{
			name: "valid cluster config",
			input: map[string]any{
				"server_id":   int64(1),
				"server_name": "tracker1",
				"server_ip":   "127.0.0.1",
				"port":        int32(8080),
				"server_type": "TRACKER",
				"log": map[string]any{
					"mode":      "STDOUT",
					"level":     "INFO",
					"file_path": "/var/log/criceta.log",
				},
				"cluster_mode": "cluster",
				"node_role":    "node",
				"cluster_nodes": []map[string]any{
					{
						"server_id":   int64(2),
						"server_name": "tracker2",
						"server_ip":   "127.0.0.1",
						"port":        int32(8081),
						"server_type": "TRACKER",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid master-slave config",
			input: map[string]any{
				"server_id":   int64(1),
				"server_name": "tracker1",
				"server_ip":   "127.0.0.1",
				"port":        int32(8080),
				"server_type": "TRACKER",
				"log": map[string]any{
					"mode":      "STDOUT",
					"level":     "INFO",
					"file_path": "/var/log/criceta.log",
				},
				"cluster_mode": "master-slave",
				"node_role":    "master",
				"slaves": []map[string]any{
					{
						"server_id":   int64(2),
						"server_name": "tracker2",
						"server_ip":   "127.0.0.1",
						"port":        int32(8081),
						"server_type": "TRACKER",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid slave config",
			input: map[string]any{
				"server_id":   int64(2),
				"server_name": "tracker2",
				"server_ip":   "127.0.0.1",
				"port":        int32(8081),
				"server_type": "TRACKER",
				"log": map[string]any{
					"mode":      "STDOUT",
					"level":     "INFO",
					"file_path": "/var/log/criceta.log",
				},
				"cluster_mode": "master-slave",
				"node_role":    "slave",
				"slave_of": map[string]any{
					"server_id":   int64(1),
					"server_name": "tracker1",
					"server_ip":   "127.0.0.1",
					"port":        int32(8080),
					"server_type": "TRACKER",
				},
			},
			wantErr: false,
		},
		{
			name: "missing required field",
			input: map[string]any{
				"server_id":   int64(1),
				"server_name": "tracker1",
				"server_ip":   "127.0.0.1",
				"port":        int32(8080),
				"server_type": "TRACKER",
				// Missing Log, ClusterMode and NodeRole
			},
			wantErr: true,
		},
		{
			name: "invalid_file_id_range",
			input: map[string]any{
				"server_id":   int64(1),
				"server_name": "tracker1",
				"server_ip":   "127.0.0.1",
				"port":        int32(8080),
				"server_type": "TRACKER",
				"log": map[string]any{
					"mode":      "STDOUT",
					"level":     "INFO",
					"file_path": "/var/log/criceta.log",
				},
				"cluster_mode": "standalone",
				"node_role":    "master",
				"file_id_range": map[string]any{
					"start": int64(1000),
					"end":   int64(100),
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &TrackerNodeConfig{}
			_, err := tc.ReadConfig(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("TrackerNodeConfig.ReadConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTrackerNodeConfig_SaveConfig(t *testing.T) {
	tests := []struct {
		name   string
		config *TrackerNodeConfig
		want   map[string]any
	}{
		{
			name: "standalone config",
			config: &TrackerNodeConfig{
				ServerConfig: &ServerConfig{
					ServerId:   1,
					ServerName: utils.Ptr("tracker1"),
					ServerIP:   utils.Ptr("127.0.0.1"),
					Port:       8080,
					ServerType: TRACKER,
				},
				Log: &LogConfig{
					Mode:     STDOUT,
					Level:    logs.INFO,
					FilePath: utils.Ptr("/var/log/criceta.log"),
				},
				ClusterMode: "standalone",
				NodeRole:    "master",
				Slot: &SlotRange{
					Start: 1,
					End:   1000000,
				},
			},
			want: map[string]any{
				"server_id":   int64(1),
				"server_name": "tracker1",
				"server_ip":   "127.0.0.1",
				"port":        int32(8080),
				"server_type": "TRACKER",
				"log": map[string]any{
					"mode":      "STDOUT",
					"level":     "INFO",
					"file_path": "/var/log/criceta.log",
				},
				"cluster_mode": "standalone",
				"node_role":    "master",
				"file_id_range": map[string]any{
					"start": int64(1),
					"end":   int64(1000000),
				},
			},
		},
		{
			name: "cluster config",
			config: &TrackerNodeConfig{
				ServerConfig: &ServerConfig{
					ServerId:   1,
					ServerName: utils.Ptr("tracker1"),
					ServerIP:   utils.Ptr("127.0.0.1"),
					Port:       8080,
					ServerType: TRACKER,
				},
				Log: &LogConfig{
					Mode:     STDOUT,
					Level:    logs.INFO,
					FilePath: utils.Ptr("/var/log/criceta.log"),
				},
				ClusterMode: "cluster",
				NodeRole:    "node",
				ClusterNodes: []ServerConfig{
					{
						ServerId:   2,
						ServerName: utils.Ptr("tracker2"),
						ServerIP:   utils.Ptr("127.0.0.1"),
						Port:       8081,
						ServerType: TRACKER,
					},
				},
			},
			want: map[string]any{
				"server_id":   int64(1),
				"server_name": "tracker1",
				"server_ip":   "127.0.0.1",
				"port":        int32(8080),
				"server_type": "TRACKER",
				"log": map[string]any{
					"mode":      "STDOUT",
					"level":     "INFO",
					"file_path": "/var/log/criceta.log",
				},
				"cluster_mode": "cluster",
				"node_role":    "node",
				"cluster_nodes": []map[string]any{
					{
						"server_id":   int64(2),
						"server_name": "tracker2",
						"server_ip":   "127.0.0.1",
						"port":        int32(8081),
						"server_type": "TRACKER",
					},
				},
			},
		},
		{
			name: "master-slave config with slaves",
			config: &TrackerNodeConfig{
				ServerConfig: &ServerConfig{
					ServerId:   1,
					ServerName: utils.Ptr("tracker1"),
					ServerIP:   utils.Ptr("127.0.0.1"),
					Port:       8080,
					ServerType: TRACKER,
				},
				Log: &LogConfig{
					Mode:     STDOUT,
					Level:    logs.INFO,
					FilePath: utils.Ptr("/var/log/criceta.log"),
				},
				ClusterMode: "master-slave",
				NodeRole:    "master",
				Slaves: []ServerConfig{
					{
						ServerId:   2,
						ServerName: utils.Ptr("tracker2"),
						ServerIP:   utils.Ptr("127.0.0.1"),
						Port:       8081,
						ServerType: TRACKER,
					},
				},
			},
			want: map[string]any{
				"server_id":   int64(1),
				"server_name": "tracker1",
				"server_ip":   "127.0.0.1",
				"port":        int32(8080),
				"server_type": "TRACKER",
				"log": map[string]any{
					"mode":      "STDOUT",
					"level":     "INFO",
					"file_path": "/var/log/criceta.log",
				},
				"cluster_mode": "master-slave",
				"node_role":    "master",
				"slaves": []map[string]any{
					{
						"server_id":   int64(2),
						"server_name": "tracker2",
						"server_ip":   "127.0.0.1",
						"port":        int32(8081),
						"server_type": "TRACKER",
					},
				},
			},
		},
		{
			name: "slave config with master",
			config: &TrackerNodeConfig{
				ServerConfig: &ServerConfig{
					ServerId:   2,
					ServerName: utils.Ptr("tracker2"),
					ServerIP:   utils.Ptr("127.0.0.1"),
					Port:       8081,
					ServerType: TRACKER,
				},
				Log: &LogConfig{
					Mode:     STDOUT,
					Level:    logs.INFO,
					FilePath: utils.Ptr("/var/log/criceta.log"),
				},
				ClusterMode: "master-slave",
				NodeRole:    "slave",
				SlaveOf: &ServerConfig{
					ServerId:   1,
					ServerName: utils.Ptr("tracker1"),
					ServerIP:   utils.Ptr("127.0.0.1"),
					Port:       8080,
					ServerType: TRACKER,
				},
			},
			want: map[string]any{
				"server_id":   int64(2),
				"server_name": "tracker2",
				"server_ip":   "127.0.0.1",
				"port":        int32(8081),
				"server_type": "TRACKER",
				"log": map[string]any{
					"mode":      "STDOUT",
					"level":     "INFO",
					"file_path": "/var/log/criceta.log",
				},
				"cluster_mode": "master-slave",
				"node_role":    "slave",
				"slave_of": map[string]any{
					"server_id":   int64(1),
					"server_name": "tracker1",
					"server_ip":   "127.0.0.1",
					"port":        int32(8080),
					"server_type": "TRACKER",
				},
			},
		},
		{
			name: "invalid cluster mode",
			config: &TrackerNodeConfig{
				ServerConfig: &ServerConfig{
					ServerId:   1,
					ServerName: utils.Ptr("tracker1"),
					ServerIP:   utils.Ptr("127.0.0.1"),
					Port:       8080,
					ServerType: TRACKER,
				},
				Log: &LogConfig{
					Mode:     STDOUT,
					Level:    logs.INFO,
					FilePath: utils.Ptr("/var/log/criceta.log"),
				},
				ClusterMode: "invalid-mode",
				NodeRole:    "master",
			},
			want: nil,
		},
		{
			name: "invalid node role",
			config: &TrackerNodeConfig{
				ServerConfig: &ServerConfig{
					ServerId:   1,
					ServerName: utils.Ptr("tracker1"),
					ServerIP:   utils.Ptr("127.0.0.1"),
					Port:       8080,
					ServerType: TRACKER,
				},
				Log: &LogConfig{
					Mode:     STDOUT,
					Level:    logs.INFO,
					FilePath: utils.Ptr("/var/log/criceta.log"),
				},
				ClusterMode: "standalone",
				NodeRole:    "invalid-role",
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.SaveConfig()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TrackerNodeConfig.SaveConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}
