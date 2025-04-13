package cachalot

import (
	"log/slog"

	"github.com/otaviovaladares/cachalot/internal"
)

type Coordinator struct {
	internal internal.Coordinator
}

type CoordinatorConfig struct {
	logg *slog.Logger
}

func NewCoordinator(bindAddr string, seedNodes []string, config *CoordinatorConfig) *Coordinator {
	var logger *slog.Logger
	if config != nil && config.logg != nil {
		logger = config.logg
	} else {
		logger = slog.Default()
	}
	logger.Debug("Creating new local coordinator")

	return &Coordinator{
		internal: internal.NewLocalCoordinator(logger, bindAddr, seedNodes),
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
