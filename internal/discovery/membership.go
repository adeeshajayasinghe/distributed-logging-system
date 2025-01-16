package discovery

import (
	"net"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// Provide discovery and cluster membership to our service
type Membership struct {
	Config
	handler Handler
	serf *serf.Serf
	events chan serf.Event
	logger *zap.Logger
}

func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config: config,
		handler: handler,
		logger: zap.L().Named("membership"),
	}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

type Config struct {
	NodeName string // Node's unique identifier across the serf cluster
	BindAddr string // Serf listning on this address for gossiping
	Tags map[string]string // User configured RPC address with a serf tag so the nodes know which addresses to send their RPCs
	StartJoinAddrs []string
}

// Creates and configures a serf instance and starts the event handler go routine to handle serf's events
func (m *Membership) setupSerf() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	config.EventCh = m.events // Event channel that receive serf's events when a node joins or leaves the cluster
	config.Tags = m.Tags
	config.NodeName = m.Config.NodeName
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}
	go m.eventHandler()
	// You have to specify at least one address of an existing node in the cluster 
	// Using that new node will learn about the rest of the nodes in the cluster
	if m.StartJoinAddrs != nil {
		// Join using the known node addresses of the cluster
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

func (m *Membership) eventHandler() {
	// Going loop reading events sent by Serf into the events channel
	// When a node joins or leaves the cluster, serf sends an event to all nodes, including the node that joined or left the cluster
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			// Need to iterate over members because serf will concatenate nodes that triggers the same event at a time
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(
		member.Name,
		member.Tags["rpc_addr"],
	); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(
		member.Name,
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Returns the snapshot of the members in the cluster
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Server doesn’t receive discovery events anymore
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

func (m *Membership) logError(err error, msg string, member serf.Member) {
	log := m.logger.Error
	// Will return this error if try to change the cluster on non-leader nodes
	// and we not log them as critical errors
	if err == raft.ErrNotLeader {
		log = m.logger.Debug
	}
	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}