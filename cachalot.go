package cachalot

import (
	"github.com/otaviovaladares/cachalot/internal"
)

type Coordinator struct {
	internal internal.Coordinator
}

func NewCoordinator(bindAddr string, seedNodes []string, config *Config) *Coordinator {
	conf := NewConfig(config)

	conf.Logger.Debug("Creating new local coordinator")

	localCoordinator := internal.NewLocalCoordinator(
		conf.Logger,
		bindAddr,
		seedNodes,
		&internal.CoordinatorConfig{
			DefaultLockDuration: conf.DefaultLockDuration,
			DiscoveryProvider:   conf.DiscoveryBackend,
			ElectionConfig: &internal.ElectionConfig{
				TimeToWaitForVotes: conf.ElectionConfig.TimeToWaitForVotes,
			},
		},
	)

	return &Coordinator{
		internal: localCoordinator,
	}
}

func (c *Coordinator) Connect() error {
	err := c.internal.Connect()

	if err != nil {
		return err
	}

	return nil
}

func (c *Coordinator) Lock(key string) error {
	err := c.internal.Lock(key)

	if err != nil {
		return err
	}

	return nil
}

func (c *Coordinator) GetLocks() (map[string]string, error) {
	locks, err := c.internal.GetLocks()

	if err != nil {
		return nil, err
	}

	return locks, nil
}
