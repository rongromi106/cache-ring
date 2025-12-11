package cluster

import (
	"cache-ring/hashring"
)

// Cluster wraps a HashRing to manage nodes and key lookups.
type Cluster struct {
	ring  *hashring.HashRing
	nodes map[string]*CacheNode
}

type CacheNode struct {
	id   string
	data map[string]string
}

// New creates a new Cluster with the provided number of virtual node replicas.
func New(numReplicas int) *Cluster {
	return &Cluster{
		ring:  hashring.New(numReplicas),
		nodes: make(map[string]*CacheNode),
	}
}

// AddNode adds a node identifier to the cluster.
func (c *Cluster) AddNode(nodeID string) {
	node := &CacheNode{id: nodeID, data: make(map[string]string)}
	c.nodes[nodeID] = node
	c.ring.AddNode(nodeID)
}

// RemoveNode removes a node identifier from the cluster.
func (c *Cluster) RemoveNode(nodeID string) {
	delete(c.nodes, nodeID)
	c.ring.RemoveNode(nodeID)
}

// LookupKey returns the node responsible for key. ok is false if the cluster is empty.
func (c *Cluster) LookupKey(key string) (nodeID string, ok bool) { return c.ring.GetNode(key) }

// ListNodes returns all nodes in stable order.
func (c *Cluster) ListNodes() []string { return c.ring.Nodes() }

// Cache operations
func (c *Cluster) Set(key, value string) (nodeID string, ok bool) {
	nodeID, ok = c.LookupKey(key)
	if !ok {
		return "", false
	}
	node := c.nodes[nodeID]
	if node == nil {
		return "", false
	}
	node.data[key] = value
	return nodeID, true
}

func (c *Cluster) Get(key string) (value string, nodeID string, ok bool) {
	nodeID, ok = c.LookupKey(key)
	if !ok {
		return "", "", false
	}
	node := c.nodes[nodeID]
	if node == nil {
		return "", nodeID, false
	}
	val, exists := node.data[key]
	if !exists {
		return "", nodeID, false
	}
	return val, nodeID, true
}

// Introspection / stats
// func (c *Cluster) KeyCounts() map[string]int // nodeID -> #keys stored
// func (c *Cluster) SnapshotKeyOwners() map[string]string // key -> nodeID (for simulation)
