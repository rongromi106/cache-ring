package cluster

import (
	"cache-ring/hashring"
)

// Cluster wraps a HashRing to manage nodes and key lookups.
type Cluster struct {
	ring *hashring.HashRing
}

// New creates a new Cluster with the provided number of virtual node replicas.
func New(numReplicas int) *Cluster {
	return &Cluster{ring: hashring.New(numReplicas)}
}

// AddNode adds a node identifier to the cluster.
func (c *Cluster) AddNode(nodeID string) { c.ring.AddNode(nodeID) }

// RemoveNode removes a node identifier from the cluster.
func (c *Cluster) RemoveNode(nodeID string) { c.ring.RemoveNode(nodeID) }

// LookupKey returns the node responsible for key. ok is false if the cluster is empty.
func (c *Cluster) LookupKey(key string) (nodeID string, ok bool) { return c.ring.GetNode(key) }

// ListNodes returns all nodes in stable order.
func (c *Cluster) ListNodes() []string { return c.ring.Nodes() }
