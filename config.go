package cachalot

import (
	"log/slog"
	"time"
)

type Config struct {
	// Logger is the logger used for logging messages
	// throughout the application. If not provided, a default logger will be used.
	Logger *slog.Logger
	// DiscoveryBackend specifies which service discovery mechanism to use for node
	// discovery in the cluster. Currently supported: "serf"
	DiscoveryBackend string

	// ElectionConfig contains configuration parameters for lock election in the cluster.
	ElectionConfig *ElectionConfig
}

type ElectionConfig struct {
	// TimeToWaitForVotes is the time to wait for votes from other nodes
	// before proceeding with the election process. This is important for
	// ensuring that all nodes have a chance to participate in the election
	// and that the most up-to-date information is used to determine the
	// leader. If not provided, a default value will be used.
	// Warning: Higher values may lead to longer election times.
	// Increasing this value may be necessary in environments with high latency
	TimeToWaitForVotes time.Duration
}

var defaultConfig = &Config{
	Logger:           slog.Default(),
	DiscoveryBackend: "serf",
	ElectionConfig: &ElectionConfig{
		TimeToWaitForVotes: 2 * time.Second,
	},
}

func NewConfig(userConf *Config) *Config {
	if userConf == nil {
		return defaultConfig
	}

	if userConf.Logger == nil {
		userConf.Logger = defaultConfig.Logger
	}

	if userConf.DiscoveryBackend == "" {
		userConf.DiscoveryBackend = defaultConfig.DiscoveryBackend
	}

	if userConf.ElectionConfig == nil {
		userConf.ElectionConfig = defaultConfig.ElectionConfig
	} else {
		if userConf.ElectionConfig.TimeToWaitForVotes == 0 {
			userConf.ElectionConfig.TimeToWaitForVotes = defaultConfig.ElectionConfig.TimeToWaitForVotes
		}
	}

	return userConf
}
