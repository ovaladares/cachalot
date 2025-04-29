package cachalot

import (
	"log/slog"
	"time"
)

type Config struct {
	Logger              *slog.Logger
	DefaultLockDuration time.Duration
	DiscoveryBackend    string
	ElectionConfig      *ElectionConfig
}

type ElectionConfig struct {
	TimeToWaitForVotes time.Duration
}

var defaultConfig = &Config{
	Logger:              slog.Default(),
	DefaultLockDuration: 120 * time.Second,
	DiscoveryBackend:    "serf",
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

	if userConf.DefaultLockDuration == 0 {
		userConf.DefaultLockDuration = defaultConfig.DefaultLockDuration
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
