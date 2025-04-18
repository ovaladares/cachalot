package discovery

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"

	"github.com/hashicorp/serf/serf"
)

type SerfDiscover struct {
	bindAddr   string
	seedNodes  []string
	eventsChan chan serf.Event
	handlers   []func(*ClusterEvent)
	mu         sync.RWMutex
	nodeID     string
	serf       *serf.Serf
	logg       *slog.Logger
	doneChan   chan struct{}
}

func NewSerfDiscover(bindAddr string, seedNodes []string, logg *slog.Logger) *SerfDiscover {
	return &SerfDiscover{
		bindAddr:   bindAddr,
		seedNodes:  seedNodes,
		eventsChan: make(chan serf.Event, 256),
		nodeID:     fmt.Sprintf("node-%d", rand.IntN(10000)), //TODO use a better way to generate node ID
		logg:       logg,
	}
}

func (s *SerfDiscover) Connect() error {
	addr, err := net.ResolveTCPAddr("tcp", s.bindAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve TCP address: %w", err)
	}

	serfConfig := serf.DefaultConfig()
	serfConfig.Init()

	serfConfig.MemberlistConfig.BindAddr = addr.IP.String()
	serfConfig.MemberlistConfig.BindPort = addr.Port
	serfConfig.LogOutput = io.Discard                  //TODO optional parameter
	serfConfig.MemberlistConfig.LogOutput = io.Discard //TODO optional parameter surpress serf logs

	serfConfig.EventCh = s.eventsChan
	serfConfig.EnableNameConflictResolution = false
	serfConfig.NodeName = s.nodeID

	serfInstance, err := serf.Create(serfConfig)
	if err != nil {
		return err
	}

	s.serf = serfInstance

	joined, err := serfInstance.Join(s.seedNodes, true) //Need to validate ignore old

	if err != nil {
		s.logg.Warn("Failed to join cluster, running as standalone", "node_id", s.nodeID, "error", err)
	}

	s.logg.Info("Joined cluster", "node_id", s.nodeID, "known_nodes", joined)

	s.doneChan = make(chan struct{})

	go s.processEvents()

	return nil
}

func (s *SerfDiscover) GetMembers() ([]*Member, error) {
	members := s.serf.Members()
	serfMembers := make([]*Member, len(members))

	for i, member := range members {
		if member.Status != serf.StatusAlive {
			continue
		}

		serfMembers[i] = &Member{
			NodeID: member.Name,
			Addr:   member.Addr.String(),
			Port:   member.Port,
			Tags:   member.Tags,
			Alive:  member.Status == serf.StatusAlive,
		}
	}

	return serfMembers, nil
}

func (s *SerfDiscover) Disconnect() error {
	if s.serf != nil {
		s.serf.Leave()
		s.serf.Shutdown()
	}

	if s.serf == nil {
		return fmt.Errorf("failed to disconnect from Serf cluster, Serf instance is initialized")
	}

	err := s.serf.Leave()

	if err != nil {
		err = s.serf.Shutdown()

		if err != nil {
			return fmt.Errorf("failed to shutdown Serf instance: %w", err)
		}
		s.logg.Info("Serf instance shutdown successfully")

		s.serf = nil

		return nil
	}

	close(s.doneChan)

	s.logg.Info("Disconnected from Serf cluster", "node_id", s.nodeID)

	s.serf = nil

	return nil
}

func (s *SerfDiscover) GetNodeID() string {
	return s.nodeID
}

func (s *SerfDiscover) GetMembersCount() (int, error) {
	members, err := s.GetMembers()

	if err != nil {
		return 0, fmt.Errorf("failed to get members: %w", err)
	}

	return len(members), nil
}

func (s *SerfDiscover) BroadcastEvent(eventType string, payload []byte) error {
	err := s.serf.UserEvent(eventType, payload, true)
	if err != nil {
		return fmt.Errorf("failed to broadcast event: %w", err)
	}

	return nil
}

func (s *SerfDiscover) processEvents() {
	for {
		select {
		case evt := <-s.eventsChan:
			event, err := s.convertSerfEvent(evt)

			if err != nil {
				s.logg.Error("Failed to convert Serf event", "error", err)
				continue
			}

			s.notifyHandlers(event)
		case <-s.doneChan:
			s.logg.Info("Stopping event processing")
			return
		}
	}
}

func (s *SerfDiscover) notifyHandlers(event *ClusterEvent) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.handlers) == 0 {
		s.logg.Debug("No handlers registered for event", "event_type", event.Type)
		return
	}

	s.logg.Debug("Notifying handlers for event", "event_type", event.Type)

	for _, handler := range s.handlers {
		go func(h func(*ClusterEvent)) {
			h(event)
		}(handler)
	}
}

func (s *SerfDiscover) RegisterEventHandler(handler func(*ClusterEvent)) error {
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.handlers = append(s.handlers, handler)

	return nil
}

func (s *SerfDiscover) convertSerfEvent(e serf.Event) (*ClusterEvent, error) {
	switch e.EventType() {
	case serf.EventMemberJoin:
		var nodes []string

		for _, member := range e.(serf.MemberEvent).Members {
			nodes = append(nodes, member.Name)
		}

		body := MemberJoinEvent{
			Nodes: nodes,
		}

		b, err := json.Marshal(body)

		if err != nil {
			return nil, fmt.Errorf("failed to marshal member join event: %w", err)
		}

		return &ClusterEvent{
			Type: MemberJoinEventType,
			Body: b,
		}, nil
	case serf.EventMemberLeave:
		var nodes []string

		for _, member := range e.(serf.MemberEvent).Members {
			nodes = append(nodes, member.Name)
		}

		body := MemberLeaveEvent{
			Nodes: nodes,
		}

		b, err := json.Marshal(body)

		if err != nil {
			return nil, fmt.Errorf("failed to marshal member leave event: %w", err)
		}

		return &ClusterEvent{
			Type: MemberLeaveEventType,
			Body: b,
		}, nil
	case serf.EventMemberFailed:
		var nodes []string

		for _, member := range e.(serf.MemberEvent).Members {
			nodes = append(nodes, member.Name)
		}

		body := MemberFailedEvent{
			Nodes: nodes,
		}

		b, err := json.Marshal(body)

		if err != nil {
			return nil, fmt.Errorf("failed to marshal member failed event: %w", err)
		}

		return &ClusterEvent{
			Type: MemberFailedEventType,
			Body: b,
		}, nil
	case serf.EventUser:
		userEvent := e.(serf.UserEvent)
		return &ClusterEvent{
			Type: userEvent.Name,
			Body: userEvent.Payload,
		}, nil
	}

	return nil, fmt.Errorf("unknown event type: %s", e.EventType())
}
