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
	tokens := c.ring.TokensForNode(nodeID)
	// If ring is currently empty, just add and return (no keys to migrate).
	if len(c.ring.Nodes()) == 0 {
		c.ring.AddNode(nodeID)
		return
	}
	for _, token := range tokens {
		succ := c.ring.Successor(token)
		prev := c.ring.Predecessor(token)
		fromNode := c.ring.OwnerOfToken(succ)
		src := c.nodes[fromNode]
		if src == nil {
			continue
		}
		for key := range src.data {
			hash_value := hashring.HashBytes([]byte(key))
			// handles interval wraps
			if prev < token {
				if hash_value > prev && hash_value <= token {
					c.nodes[nodeID].data[key] = src.data[key]
					delete(src.data, key)
				}
			} else {
				if hash_value > prev || hash_value <= token {
					c.nodes[nodeID].data[key] = src.data[key]
					delete(src.data, key)
				}
			}

		}
	}
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
// nodeID -> #keys stored
func (c *Cluster) KeyCounts() map[string]int {
	counts := make(map[string]int)
	for nodeID, node := range c.nodes {
		counts[nodeID] = len(node.data)
	}
	return counts
}

// key -> nodeID
func (c *Cluster) SnapshotKeyOwners() map[string]string {
	owners := make(map[string]string)
	for nodeID, node := range c.nodes {
		for key := range node.data {
			owners[key] = nodeID
		}
	}
	return owners
}

func (c *Cluster) rebalance() {}
