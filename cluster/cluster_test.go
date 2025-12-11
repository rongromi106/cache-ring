package cluster

import "testing"

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
