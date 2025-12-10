package hashring

import (
	"testing"
)

func TestAddNode(t *testing.T) {
	const replicas = 3
	ring := New(replicas)

	t.Run("adds replicas and updates structures", func(t *testing.T) {
		ring.AddNode("nodeA")

		if _, exists := ring.nodeSet["nodeA"]; !exists {
			t.Fatalf("expected nodeA to be present in nodeSet")
		}
		if got, want := len(ring.nodeSet), 1; got != want {
			t.Fatalf("unexpected nodeSet size: got %d, want %d", got, want)
		}
		if got, want := len(ring.keyToNode), replicas; got != want {
			t.Fatalf("unexpected keyToNode size: got %d, want %d", got, want)
		}
		if got, want := len(ring.sortedKeys), replicas; got != want {
			t.Fatalf("unexpected sortedKeys size: got %d, want %d", got, want)
		}
		// all virtual nodes should map to nodeA
		for _, key := range ring.sortedKeys {
			if ring.keyToNode[key] != "nodeA" {
				t.Fatalf("key %d mapped to %q; want nodeA", key, ring.keyToNode[key])
			}
		}
		// sortedKeys must be non-decreasing
		for i := 1; i < len(ring.sortedKeys); i++ {
			if ring.sortedKeys[i-1] > ring.sortedKeys[i] {
				t.Fatalf("sortedKeys not sorted at %d: %d > %d", i, ring.sortedKeys[i-1], ring.sortedKeys[i])
			}
		}
	})

	t.Run("idempotent for existing node", func(t *testing.T) {
		beforeNodeSetSize := len(ring.nodeSet)
		beforeKeys := len(ring.sortedKeys)
		beforeMap := len(ring.keyToNode)

		ring.AddNode("nodeA")

		if len(ring.nodeSet) != beforeNodeSetSize {
			t.Fatalf("nodeSet size changed on duplicate add: got %d, want %d", len(ring.nodeSet), beforeNodeSetSize)
		}
		if len(ring.sortedKeys) != beforeKeys {
			t.Fatalf("sortedKeys length changed on duplicate add: got %d, want %d", len(ring.sortedKeys), beforeKeys)
		}
		if len(ring.keyToNode) != beforeMap {
			t.Fatalf("keyToNode size changed on duplicate add: got %d, want %d", len(ring.keyToNode), beforeMap)
		}
	})

	t.Run("adding second node increases replicas", func(t *testing.T) {
		beforeKeys := len(ring.sortedKeys)
		beforeMap := len(ring.keyToNode)
		beforeNodeSet := len(ring.nodeSet)

		ring.AddNode("nodeB")

		if _, exists := ring.nodeSet["nodeB"]; !exists {
			t.Fatalf("expected nodeB to be present in nodeSet")
		}
		if got, want := len(ring.nodeSet), beforeNodeSet+1; got != want {
			t.Fatalf("unexpected nodeSet size: got %d, want %d", got, want)
		}
		if got, want := len(ring.sortedKeys), beforeKeys+replicas; got != want {
			t.Fatalf("unexpected sortedKeys size after add: got %d, want %d", got, want)
		}
		if got, want := len(ring.keyToNode), beforeMap+replicas; got != want {
			t.Fatalf("unexpected keyToNode size after add: got %d, want %d", got, want)
		}
		// still sorted after second add
		for i := 1; i < len(ring.sortedKeys); i++ {
			if ring.sortedKeys[i-1] > ring.sortedKeys[i] {
				t.Fatalf("sortedKeys not sorted at %d after second add: %d > %d", i, ring.sortedKeys[i-1], ring.sortedKeys[i])
			}
		}
	})
}

func TestRemoveNode(t *testing.T) {
	const replicas = 3
	ring := New(replicas)
	ring.AddNode("nodeA")
	ring.AddNode("nodeB")

	t.Run("removes existing node and its replicas", func(t *testing.T) {
		beforeTotalKeys := len(ring.sortedKeys)
		beforeMap := len(ring.keyToNode)
		beforeNodeSet := len(ring.nodeSet)

		ring.RemoveNode("nodeA")

		if _, exists := ring.nodeSet["nodeA"]; exists {
			t.Fatalf("expected nodeA to be removed from nodeSet")
		}
		if got, want := len(ring.nodeSet), beforeNodeSet-1; got != want {
			t.Fatalf("unexpected nodeSet size after remove: got %d, want %d", got, want)
		}
		if got, want := len(ring.sortedKeys), beforeTotalKeys-replicas; got != want {
			t.Fatalf("unexpected sortedKeys size after remove: got %d, want %d", got, want)
		}
		if got, want := len(ring.keyToNode), beforeMap-replicas; got != want {
			t.Fatalf("unexpected keyToNode size after remove: got %d, want %d", got, want)
		}
		// all remaining keys should map to nodeB
		for _, key := range ring.sortedKeys {
			if ring.keyToNode[key] != "nodeB" {
				t.Fatalf("key %d mapped to %q; want nodeB", key, ring.keyToNode[key])
			}
		}
		// sortedKeys remains sorted
		for i := 1; i < len(ring.sortedKeys); i++ {
			if ring.sortedKeys[i-1] > ring.sortedKeys[i] {
				t.Fatalf("sortedKeys not sorted at %d after remove: %d > %d", i, ring.sortedKeys[i-1], ring.sortedKeys[i])
			}
		}
	})

	t.Run("removing non-existent node is no-op", func(t *testing.T) {
		beforeNodeSet := len(ring.nodeSet)
		beforeKeys := len(ring.sortedKeys)
		beforeMap := len(ring.keyToNode)

		ring.RemoveNode("does-not-exist")

		if len(ring.nodeSet) != beforeNodeSet {
			t.Fatalf("nodeSet size changed on removing non-existent node: got %d, want %d", len(ring.nodeSet), beforeNodeSet)
		}
		if len(ring.sortedKeys) != beforeKeys {
			t.Fatalf("sortedKeys size changed on removing non-existent node: got %d, want %d", len(ring.sortedKeys), beforeKeys)
		}
		if len(ring.keyToNode) != beforeMap {
			t.Fatalf("keyToNode size changed on removing non-existent node: got %d, want %d", len(ring.keyToNode), beforeMap)
		}
	})

	t.Run("removing last remaining node empties ring", func(t *testing.T) {
		// currently only nodeB should remain
		ring.RemoveNode("nodeB")

		if got := len(ring.nodeSet); got != 0 {
			t.Fatalf("expected empty nodeSet, got %d", got)
		}
		if got := len(ring.sortedKeys); got != 0 {
			t.Fatalf("expected empty sortedKeys, got %d", got)
		}
		if got := len(ring.keyToNode); got != 0 {
			t.Fatalf("expected empty keyToNode, got %d", got)
		}
		if _, ok := ring.GetNode("any-key"); ok {
			t.Fatalf("GetNode should report false when ring is empty")
		}
	})
}
