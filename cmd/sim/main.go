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

	keys := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"}
	fmt.Println("\nInitial mapping:")
	for _, k := range keys {
		n, _ := c.LookupKey(k)
		fmt.Printf("%10s -> %s\n", k, n)
	}

	fmt.Println("\nRemoving node-b and remapping:")
	c.RemoveNode("node-b")
	fmt.Println("Nodes:", strings.Join(c.ListNodes(), ", "))
	for _, k := range keys {
		n, _ := c.LookupKey(k)
		fmt.Printf("%10s -> %s\n", k, n)
	}
}
