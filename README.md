# Undergrid
An easy to use locally hostable decentralized cloud capable of running on heterogeneous machines, with an automatic load balancer and parallelizer to automatically split tasks between nodes.

Every machine on the network runs the same binary. Nodes find each other over mDNS, elect a leader with Raft, and report their CPU, memory, and disk to that leader. Submit a job of Docker tasks to any node and the leader places each task on whichever machine fits it best, respecting the dependency graph between tasks.

## Requirements
```
rust
docker
protobuf-compiler
```

## Running a node
Clone the repository
```
git clone https://github.com/Rosnaky/undergrid.git
```

Start a node on a port. Currently only supports local networks.
```
cargo run -p agent -- --port <PORT>
```

Start more nodes the same way, on other ports or other machines. They discover each other automatically and manage joins and departures through Raft consensus, so there is nothing to configure.

## Submitting a job
A job is a TOML file listing Docker tasks and what each one needs. Tasks with `depends_on` wait for their dependencies to finish; everything else runs in parallel across the cluster.
```toml
# test-job.toml

[job]
id = "hello-world"

[tasks.greet]
image = "alpine:latest"
command = ["echo", "hello from undergrid!"]
cpu_cores = 0.5
memory_bytes = 67108864
disk_bytes = 1000000
depends_on = []
timeout_s = 30

[tasks.farewell]
image = "alpine:latest"
command = ["echo", "goodbye from undergrid!"]
cpu_cores = 0.5
memory_bytes = 67108864
disk_bytes = 1000000
depends_on = ["greet"]
timeout_s = 30
```

Tasks can also take environment variables, useful for containers that need credentials:
```toml
[tasks.start_pineventory.env]
DISCORD_TOKEN = ""
DB_URL = ""
```

Send it to any node on the network, it doesn't have to be the leader. Requests are forwarded for you.
```
cargo run -p cli -- --node http://127.0.0.1:<PORT> submit path/to/test-job.toml
```

Then check on it
```
cargo run -p cli -- --node http://127.0.0.1:<PORT> status <job_id>
```

## Architecture
Nodes talk to each other over gRPC, with the message and service definitions in `proto/undergrid.proto`.

A node advertises itself as `_undergrid._tcp.local.` and browses for the same, registering with any peer it finds. Raft then decides who leads: followers send heartbeats with their latest resource snapshot, and a leader that stops hearing from a peer for ten seconds drops it from the cluster.

The leader owns every job. It walks the task graph each tick, hands the ready tasks to the DRF scheduler along with what each node has free, and dispatches the resulting assignments. The scheduler orders tasks by their dominant resource share and places each one on the node it fits most tightly, so small tasks aren't starved by large ones and the cluster stays packed. Receiving nodes run the container through `docker run` and report stdout, stderr, and the exit code back to the leader, which advances the job until every task has finished or one has failed.

## Repository
```
.
|-- crates
|   |-- agent                // Node agent
|   |   |-- src
|   |   |   |-- client       // Server client
|   |   |   |-- config       // Node configuration
|   |   |   |-- node         // Node discovery and runtime
|   |   |   |-- orchestrator // Inter-node job orchestrator
|   |   |   |-- server       // Server
|   |   |   `-- system       // System diagnostics and resources
|   |   `-- tests
|   |-- cli                  // Command line interface
|   |   `-- src
|   |-- mesh                 // Mesh transport layer for communications
|   |   `-- src
|   |       `-- conversions  // Conversions between transport layer and application types
|   |-- raft                 // Raft Consensus algorithm
|   |   |-- src
|   |   `-- tests
|   |-- runtime              // Job execution
|   |   |-- src
|   |   |   |-- executor
|   |   |   |-- job
|   |   |   `-- task
|   |   `-- tests
|   `-- scheduler            // Intra-node task scheduler
|       |-- src
|       |   `-- drf          // Dominant resource fairness scheduler
|       `-- tests
|-- docs
`-- proto                    // Transport layer message definitions
```
