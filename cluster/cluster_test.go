package cluster

import (
	"cache-ring/hashring"
	"fmt"
	"testing"
)

func TestClusterAddAndRemoveNode(t *testing.T) {
	c := New(10)

	// Initially empty
	if got := len(c.ListNodes()); got != 0 {
		t.Fatalf("expected 0 nodes initially, got %d", got)
	}

	// Add a node
	c.AddNode("A")

	nodes := c.ListNodes()
	if len(nodes) != 1 || nodes[0] != "A" {
		t.Fatalf("expected nodes [A], got %v", nodes)
	}
	if _, exists := c.nodes["A"]; !exists {
		t.Fatalf("expected internal node map to contain A")
	}

	// With one node, any key should map to that node
	if nodeID, ok := c.LookupKey("foo"); !ok || nodeID != "A" {
		t.Fatalf("expected LookupKey(\"foo\") -> (A, true); got (%q, %v)", nodeID, ok)
	}

	// Optional: verify Set/Get route to the same node
	if nodeID, ok := c.Set("k", "v"); !ok || nodeID != "A" {
		t.Fatalf("expected Set to route to A, got (%q, %v)", nodeID, ok)
	}
	if val, nodeID, ok := c.Get("k"); !ok || nodeID != "A" || val != "v" {
		t.Fatalf("expected Get to return (v, A, true); got (%q, %q, %v)", val, nodeID, ok)
	}

	// Remove the node
	c.RemoveNode("A")

	if got := len(c.ListNodes()); got != 0 {
		t.Fatalf("expected 0 nodes after removal, got %d", got)
	}
	if _, exists := c.nodes["A"]; exists {
		t.Fatalf("expected internal node map to not contain A")
	}
	if nodeID, ok := c.LookupKey("foo"); ok {
		t.Fatalf("expected LookupKey to fail on empty cluster, got node %q", nodeID)
	}
}

func TestRemoveNodeMigratesKeysToSuccessors(t *testing.T) {
	c := New(10)
	c.AddNode("A")
	c.AddNode("B")
	c.AddNode("C")

	// Collect a deterministic set of keys owned by B and some by others.
	var bKeys []string
	var otherKeys []string
	expectedDest := make(map[string]string) // for keys that were on B
	originalOwner := make(map[string]string)

	// Helper to compute the destination node (pre-removal) for a key that belongs to removed node.
	computeExpectedDest := func(key string, removedNode string) string {
		hashValue := hashring.HashBytes([]byte(key))
		for _, token := range c.ring.TokensForNode(removedNode) {
			prev := c.ring.Predecessor(token)
			if prev < token {
				if hashValue > prev && hashValue <= token {
					succ := c.ring.Successor(token)
					return c.ring.OwnerOfToken(succ)
				}
			} else {
				if hashValue > prev || hashValue <= token {
					succ := c.ring.Successor(token)
					return c.ring.OwnerOfToken(succ)
				}
			}
		}
		return ""
	}

	for i := 0; len(bKeys) < 12 || len(otherKeys) < 8; i++ {
		key := fmt.Sprintf("key-%d", i)
		owner, ok := c.LookupKey(key)
		if !ok {
			t.Fatalf("LookupKey unexpectedly failed for %q", key)
		}
		if owner == "B" && len(bKeys) < 12 {
			if _, ok := c.Set(key, "val-"+key); !ok {
				t.Fatalf("Set failed for %q", key)
			}
			bKeys = append(bKeys, key)
			originalOwner[key] = owner
			expectedDest[key] = computeExpectedDest(key, "B")
		} else if owner != "B" && len(otherKeys) < 8 {
			if _, ok := c.Set(key, "val-"+key); !ok {
				t.Fatalf("Set failed for %q", key)
			}
			otherKeys = append(otherKeys, key)
			originalOwner[key] = owner
		}
		// Safety to avoid infinite loop in pathological cases
		if i > 50000 {
			t.Fatalf("failed to gather enough test keys; gathered B:%d, other:%d", len(bKeys), len(otherKeys))
		}
	}

	// Sanity: ensure we actually targeted B for some keys and computed destinations
	if len(bKeys) == 0 {
		t.Fatalf("expected to find keys mapping to B before removal")
	}
	for _, k := range bKeys {
		if expectedDest[k] == "" {
			t.Fatalf("expected non-empty destination for key %q", k)
		}
		if expectedDest[k] == "B" {
			t.Fatalf("destination for key %q should not be B", k)
		}
	}

	// Remove B and validate behavior.
	c.RemoveNode("B")

	// B should be gone from both ring and internal nodes map.
	nodes := c.ListNodes()
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes after removal, got %v", nodes)
	}
	if _, exists := c.nodes["B"]; exists {
		t.Fatalf("expected internal node map to not contain B after removal")
	}

	// Keys that were on B should now be on the expected successor owner, with values preserved.
	for _, k := range bKeys {
		wantOwner := expectedDest[k]
		gotOwner, ok := c.LookupKey(k)
		if !ok {
			t.Fatalf("LookupKey failed after removal for %q", k)
		}
		if gotOwner != wantOwner {
			t.Fatalf("key %q expected new owner %q, got %q", k, wantOwner, gotOwner)
		}
		val, nodeID, ok := c.Get(k)
		if !ok || nodeID != wantOwner || val != "val-"+k {
			t.Fatalf("Get(%q) expected (val-%s, %s, true); got (%q, %q, %v)", k, k, wantOwner, val, nodeID, ok)
		}
	}

	// Keys that were not on B should remain with their original owners.
	for _, k := range otherKeys {
		wantOwner := originalOwner[k]
		gotOwner, ok := c.LookupKey(k)
		if !ok {
			t.Fatalf("LookupKey failed after removal for %q", k)
		}
		if gotOwner != wantOwner {
			t.Fatalf("key %q expected owner to remain %q, got %q", k, wantOwner, gotOwner)
		}
		val, nodeID, ok := c.Get(k)
		if !ok || nodeID != wantOwner || val != "val-"+k {
			t.Fatalf("Get(%q) expected (val-%s, %s, true); got (%q, %q, %v)", k, k, wantOwner, val, nodeID, ok)
		}
	}
}
