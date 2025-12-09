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

What it does
------------
- Adds three nodes (`node-a`, `node-b`, `node-c`) to a consistent hash ring
- Maps example keys to nodes
- Removes `node-b` and shows the remapped results

Notes
-----
- Module path is `cache-ring`. Imports inside the repo use that path, e.g. `cache-ring/cluster`.
- No external dependencies.


