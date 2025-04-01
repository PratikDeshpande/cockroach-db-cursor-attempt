# Distributed Transactional Database Implementation

This project implements a distributed transactional database based on the CockroachDB paper. It includes the following core components:

## Core Components

1. **Storage Layer**
   - RocksDB-based storage engine
   - MVCC (Multi-Version Concurrency Control) implementation
   - Key-value store interface

2. **Transaction Layer**
   - Distributed transaction management
   - Two-phase commit protocol
   - Transaction isolation levels

3. **Distribution Layer**
   - Node membership and discovery
   - Raft consensus protocol
   - Data partitioning and replication

4. **SQL Layer**
   - SQL query parsing and planning
   - Query execution engine
   - Schema management

## Project Structure

```
.
├── cmd/                    # Command-line tools and main entry points
├── internal/              # Internal packages
│   ├── storage/          # Storage engine implementation
│   ├── transaction/      # Transaction management
│   ├── distribution/     # Distributed system components
│   └── sql/              # SQL layer implementation
├── pkg/                   # Public packages
└── test/                 # Test utilities and integration tests
```

## Building and Running

1. Ensure you have Go 1.21 or later installed
2. Clone the repository
3. Run `go mod download` to fetch dependencies
4. Run `go build ./...` to build all components

## Development Status

This is a work in progress. Currently implementing core components of the distributed database system. 