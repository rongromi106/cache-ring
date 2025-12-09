package hashring

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

// HashRing implements a simple consistent hashing ring with virtual nodes.
// It maps arbitrary keys to added node identifiers.
// The ring is safe for concurrent use.
type HashRing struct {
	numReplicas int
	keyToNode   map[uint64]string
	sortedKeys  []uint64
	nodeSet     map[string]struct{}
	mu          sync.RWMutex
}

// New creates a HashRing with the given number of virtual node replicas per real node.
// If numReplicas <= 0, a reasonable default of 100 is used.
func New(numReplicas int) *HashRing {
	if numReplicas <= 0 {
		numReplicas = 100
	}
	return &HashRing{
		numReplicas: numReplicas,
		keyToNode:   make(map[uint64]string),
		nodeSet:     make(map[string]struct{}),
	}
}

func hashBytes(data []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(data)
	return h.Sum64()
}

// AddNode adds a node to the ring with the configured number of replicas.
// Adding an existing node is a no-op.
func (r *HashRing) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodeSet[nodeID]; exists {
		return
	}

	for replica := 0; replica < r.numReplicas; replica++ {
		key := hashBytes([]byte(fmt.Sprintf("%s#%d", nodeID, replica)))
		r.keyToNode[key] = nodeID
		r.sortedKeys = append(r.sortedKeys, key)
	}
	sort.Slice(r.sortedKeys, func(i, j int) bool { return r.sortedKeys[i] < r.sortedKeys[j] })
	r.nodeSet[nodeID] = struct{}{}
}

// RemoveNode removes a node and all its replicas from the ring.
// Removing a missing node is a no-op.
func (r *HashRing) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodeSet[nodeID]; !exists {
		return
	}

	// Remove all replicas for this node from the map
	for replica := 0; replica < r.numReplicas; replica++ {
		key := hashBytes([]byte(fmt.Sprintf("%s#%d", nodeID, replica)))
		delete(r.keyToNode, key)
	}

	// Rebuild sortedKeys to avoid O(n^2) deletions
	r.sortedKeys = r.sortedKeys[:0]
	for key := range r.keyToNode {
		r.sortedKeys = append(r.sortedKeys, key)
	}
	sort.Slice(r.sortedKeys, func(i, j int) bool { return r.sortedKeys[i] < r.sortedKeys[j] })
	delete(r.nodeSet, nodeID)
}

// GetNode returns the nodeID responsible for the given key.
// The second return value is false if the ring is empty.
func (r *HashRing) GetNode(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedKeys) == 0 {
		return "", false
	}

	h := hashBytes([]byte(key))
	idx := sort.Search(len(r.sortedKeys), func(i int) bool { return r.sortedKeys[i] >= h })
	if idx == len(r.sortedKeys) {
		idx = 0
	}
	nodeID := r.keyToNode[r.sortedKeys[idx]]
	return nodeID, true
}

// Nodes returns a stable-sorted list of node identifiers present in the ring.
func (r *HashRing) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]string, 0, len(r.nodeSet))
	for nodeID := range r.nodeSet {
		nodes = append(nodes, nodeID)
	}
	sort.Strings(nodes)
	return nodes
}
