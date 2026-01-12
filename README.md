# praborrow-lease

Distributed lease consensus mechanism.

## Overview

Implements the consensus layer for managing sovereign leases in a distributed environment. This crate uses a Raft-based consensus algorithm to ensure linearizability of borrow checking across multiple nodes.

## Key Features

- **Raft Consensus**: Leader election and log replication for consistent state.
- **Lease Management**: Distributed locking and validity checking for `Sovereign<T>` resources.
- **Fault Tolerance**: Maintains system integrity despite node failures or network partitions.
- **Joint Consensus**: Safe dynamic membership changes using two-phase configuration transitions.
- **Security (mTLS)**: Mutual TLS support for secure inter-node communication.

---

# praborrow-lease (Bahasa Indonesia)

Mekanisme konsensus lease terdistribusi.

## Ikhtisar (Overview)

Mengimplementasikan lapisan konsensus untuk mengelola lease kedaulatan (sovereign leases) dalam lingkungan terdistribusi. Crate ini menggunakan algoritma konsensus berbasis Raft untuk memastikan linearizability dari borrow checking di beberapa node.

## Fitur Utama (Key Features)

- **Konsensus Raft**: Pemilihan pemimpin (leader election) dan replikasi log untuk status yang konsisten.
- **Manajemen Lease**: Penguncian terdistribusi dan pemeriksaan validitas untuk sumber daya `Sovereign<T>`.
- **Toleransi Kesalahan (Fault Tolerance)**: Menjaga integritas sistem meskipun terjadi kegagalan node atau partisi jaringan.
- **Konsensus Bersama (Joint Consensus)**: Perubahan keanggotaan dinamis yang aman menggunakan transisi konfigurasi dua fase.
- **Keamanan (mTLS)**: Dukungan Mutual TLS untuk komunikasi antar-node yang aman.

