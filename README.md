cache-ring
==========

Minimal consistent hashing ring in Go with a small simulation CLI.

Project layout
--------------

```
cache-ring/
  go.mod
  hashring/
    hashring.go
  cluster/
    cluster.go
  cmd/
    sim/
      main.go
  README.md
```

Usage
-----

- Run the simulation:

```bash
go run ./cmd/sim -replicas 100
```

Flags:
- `-replicas`: number of virtual node replicas per real node (default 100)

Output:
```
➜  cache-ring git:(main) ✗ go run cmd/sim/main.go
Nodes: node-a, node-b, node-c
Initial Cluster: 3 nodes, 1000 keys, 100 replicasKey counts:
node-b: 373 keys, 37.300000%
node-c: 302 keys, 30.200000%
node-a: 325 keys, 32.500000%
After adding a new node: node-d
node-a: 243 keys, 24.300000%
node-b: 284 keys, 28.400000%
node-c: 251 keys, 25.100000%
node-d: 222 keys, 22.200000%
Number of remapped keys: 222
Remapped keys: 22.200000%
After removing an existing node: node-b
node-d: 306 keys, 30.600000%
node-a: 325 keys, 32.500000%
node-c: 369 keys, 36.900000%
Number of remapped keys: 506
Remapped keys: 50.600000%
```

What it does
------------
- Adds three nodes (`node-a`, `node-b`, `node-c`) to a consistent hash ring
- Maps example keys to nodes
- Removes `node-b` and shows the remapped results

Notes
-----
- Module path is `cache-ring`. Imports inside the repo use that path, e.g. `cache-ring/cluster`.


