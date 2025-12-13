package main

import (
	"flag"
	"fmt"
	"strings"

	"cache-ring/cluster"
)

func main() {
	var replicas int
	flag.IntVar(&replicas, "replicas", 100, "number of virtual node replicas per node")
	flag.Parse()

	c := cluster.New(replicas)

	nodes := []string{"node-a", "node-b", "node-c"}
	for _, n := range nodes {
		c.AddNode(n)
	}

	fmt.Println("Nodes:", strings.Join(c.ListNodes(), ", "))

	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		c.Set(key, fmt.Sprintf("value-%d", i))
	}
	keyCounts := c.KeyCounts()
	fmt.Printf("Initial Cluster: %d nodes, %d keys, %d replicas", len(c.ListNodes()), numKeys, replicas)
	fmt.Println("Key counts:")
	for nodeID, count := range keyCounts {
		fmt.Printf("%s: %d keys, %f%%\n", nodeID, count, float64(count)/float64(numKeys)*100)
	}
	before := c.SnapshotKeyOwners()
	// add a new node
	fmt.Printf("After adding a new node: %s", "node-d")
	fmt.Println()
	c.AddNode("node-d")
	keyCounts = c.KeyCounts()
	for nodeID, count := range keyCounts {
		fmt.Printf("%s: %d keys, %f%%\n", nodeID, count, float64(count)/float64(numKeys)*100)
	}
	after := c.SnapshotKeyOwners()
	remappedKeys := 0
	for key, nodeID := range after {
		if nodeID != before[key] {
			remappedKeys++
		}
	}
	fmt.Printf("Number of remapped keys: %d\n", remappedKeys)
	fmt.Printf("Remapped keys: %f%%\n", float64(remappedKeys)/float64(numKeys)*100)

	// keys := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"}
	// fmt.Println("\nInitial mapping:")
	// for _, k := range keys {
	// 	n, _ := c.LookupKey(k)
	// 	fmt.Printf("%10s -> %s\n", k, n)
	// }

	// fmt.Println("\nRemoving node-b and remapping:")
	// c.RemoveNode("node-b")
	// fmt.Println("Nodes:", strings.Join(c.ListNodes(), ", "))
	// for _, k := range keys {
	// 	n, _ := c.LookupKey(k)
	// 	fmt.Printf("%10s -> %s\n", k, n)
	// }
}
