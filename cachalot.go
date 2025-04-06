package cachalot

import (
	"github.com/otaviovaladares/cachalot/internal"
)

type Coordinator struct {
	internal internal.Coordinator
}

type CoordinatorConfig struct{}

func NewCoordinator(config *CoordinatorConfig) *Coordinator {
	return &Coordinator{
		internal: internal.NewLocalCoordinator(),
	}
}

func (c *Coordinator) RegisterNode(nodeID string) error {
	_, err := c.internal.RegisterNode(nodeID)

	if err != nil {
		return err
	}

	return nil
}
