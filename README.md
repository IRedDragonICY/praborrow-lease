# praborrow-lease

Distributed lease consensus mechanism.

## Overview

Implements the consensus layer for managing sovereign leases in a distributed environment. This crate uses a Raft-based consensus algorithm to ensure linearizability of borrow checking across multiple nodes.

## Key Features

- **Raft Consensus**: Leader election and log replication for consistent state.
- **Lease Management**: Distributed locking and validity checking for `Sovereign<T>` resources.
- **Fault Tolerance**: Maintains system integrity despite node failures or network partitions.
