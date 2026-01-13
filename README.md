# praborrow-lease

English | [Indonesia](./README_ID.md)

Distributed lease consensus mechanism.

## Overview

Implements the consensus layer for managing sovereign leases in a distributed environment. This crate uses a Raft-based consensus algorithm to ensure linearizability of borrow checking across multiple nodes.

## Key Features

- **Raft Consensus**: Leader election and log replication for consistent state.
- **Lease Management**: Distributed locking and validity checking for `Sovereign<T>` resources.
- **Fault Tolerance**: Maintains system integrity despite node failures or network partitions.
- **Joint Consensus**: Safe dynamic membership changes using two-phase configuration transitions.
- **Security (mTLS)**: Mutual TLS support for secure inter-node communication.


