package distribution

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// NodeID represents a unique identifier for a node in the cluster
type NodeID string

// NodeState represents the current state of a node
type NodeState int

const (
	NodeStateFollower NodeState = iota
	NodeStateCandidate
	NodeStateLeader
)

// String returns the string representation of the node state
func (s NodeState) String() string {
	switch s {
	case NodeStateFollower:
		return "FOLLOWER"
	case NodeStateCandidate:
		return "CANDIDATE"
	case NodeStateLeader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// Node represents a node in the distributed system
type Node struct {
	ID        NodeID
	State     NodeState
	mu        sync.RWMutex
	peers     map[NodeID]*Peer
	peersMu   sync.RWMutex
	term      uint64
	leaderID  NodeID
	stopCh    chan struct{}
}

// Peer represents a connection to another node
type Peer struct {
	ID      NodeID
	Address string
	// In a real implementation, this would be a gRPC client
	// client  *grpc.ClientConn
}

// NewNode creates a new node in the cluster
func NewNode(id NodeID) *Node {
	return &Node{
		ID:     id,
		State:  NodeStateFollower,
		peers:  make(map[NodeID]*Peer),
		term:   0,
		stopCh: make(chan struct{}),
	}
}

// AddPeer adds a new peer to the node
func (n *Node) AddPeer(peer *Peer) error {
	n.peersMu.Lock()
	defer n.peersMu.Unlock()

	if _, exists := n.peers[peer.ID]; exists {
		return fmt.Errorf("peer %s already exists", peer.ID)
	}

	n.peers[peer.ID] = peer
	return nil
}

// RemovePeer removes a peer from the node
func (n *Node) RemovePeer(peerID NodeID) error {
	n.peersMu.Lock()
	defer n.peersMu.Unlock()

	if _, exists := n.peers[peerID]; !exists {
		return fmt.Errorf("peer %s does not exist", peerID)
	}

	delete(n.peers, peerID)
	return nil
}

// Start begins the node's operation
func (n *Node) Start(ctx context.Context) error {
	go n.runElectionTimer(ctx)
	go n.runHeartbeatTimer(ctx)
	return nil
}

// Stop stops the node's operation
func (n *Node) Stop() {
	close(n.stopCh)
}

// runElectionTimer runs the election timer for follower nodes
func (n *Node) runElectionTimer(ctx context.Context) {
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.State == NodeStateFollower {
				// Start election
				n.State = NodeStateCandidate
				n.term++
				// In a real implementation, this would request votes from peers
			}
			n.mu.Unlock()
		}
	}
}

// runHeartbeatTimer runs the heartbeat timer for leader nodes
func (n *Node) runHeartbeatTimer(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.State == NodeStateLeader {
				// Send heartbeats to all peers
				// In a real implementation, this would send AppendEntries RPCs
			}
			n.mu.Unlock()
		}
	}
}

// GetState returns the current state of the node
func (n *Node) GetState() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.State
}

// GetTerm returns the current term of the node
func (n *Node) GetTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.term
}

// GetLeaderID returns the ID of the current leader
func (n *Node) GetLeaderID() NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID
} 