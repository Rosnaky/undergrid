# Undergrid
An easy to use locally hostable decentralized cloud capable of running on heterogeneous machines, with an automatic load balancer and parallelizer to automatically split tasks between nodes.

## Requirements
```
rust
docker
```

## Usage
Clone the repository
```
git clone https://github.com/Rosnaky/undergrid.git
```

Run a node locally on a port. Currently only supports local networks.
```
cargo run -- --port <PORT>
```

Now nodes will be able to join and leave the cluster, which is auto-managed with Raft consensus.

#### Submitting a Job
Create a TOML file defining the job with Docker images:
```
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

Submit the job using the CLI. You can send it to any node on the network, it doesn't have to be the leader.
```
cargo run -p cli  -- --node http://127.0.0.1:<PORT> submit path/to/test-job.toml 
```

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
