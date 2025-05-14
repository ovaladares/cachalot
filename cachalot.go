package cachalot

import (
	"time"

	cachalot "github.com/otaviovaladares/cachalot/pkg"
)

type Coordinator struct {
	internal cachalot.Coordinator
}

func NewCoordinator(bindAddr string, seedNodes []string, config *Config) *Coordinator {
	conf := NewConfig(config)

	conf.Logger.Debug("Creating new local coordinator")

	localCoordinator := cachalot.NewLocalCoordinator(
		conf.Logger,
		bindAddr,
		seedNodes,
		&cachalot.CoordinatorConfig{
			DefaultLockDuration: conf.DefaultLockDuration,
			DiscoveryProvider:   conf.DiscoveryBackend,
			ElectionConfig: &cachalot.ElectionConfig{
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

func (c *Coordinator) Lock(key string, duration time.Duration) error {
	err := c.internal.Lock(key, duration)

	if err != nil {
		return err
	}

	return nil
}

func (c *Coordinator) Renew(key string, duration time.Duration) error {
	err := c.internal.Renew(key, duration)

	if err != nil {
		return err
	}

	return nil
}

func (c *Coordinator) Release(key string) error {
	err := c.internal.Release(key)

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
