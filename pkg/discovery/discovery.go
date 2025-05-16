package discovery

type Member struct {
	NodeID string
	Addr   string
	Port   uint16
	Tags   map[string]string
	Alive  bool
}

const MemberJoinEventType = "member-join"
const MemberLeaveEventType = "member-leave"
const MemberFailedEventType = "member-failed"

type MemberJoinEvent struct {
	Nodes []string `json:"nodes"`
}

type MemberLeaveEvent struct {
	Nodes []string `json:"nodes"`
}

type MemberFailedEvent struct {
	Nodes []string `json:"nodes"`
}

type ClusterEvent struct {
	Type string `json:"type"`
	Body []byte `json:"body"`
}

// ClusterManager is an interface for service discovery
// It defines methods for connecting to a cluster, getting members
// and broadcasting events to other members
// It also provides methods for registering event handlers
type ClusterManager interface {
	GetMembers() ([]*Member, error)
	GetMembersCount() (int, error)
	Connect() error
	Disconnect() error
	GetNodeID() string

	// BroadcastEvent sends an event to all members of the cluster
	// The event is a JSON-encoded byte array
	// The event type is a string that identifies the type of event
	BroadcastEvent(string, []byte) error

	// RegisterEventHandler registers an event handler for cluster events
	// The handler will be called when a cluster event occurs
	RegisterEventHandler(handler func(*ClusterEvent)) error
}
