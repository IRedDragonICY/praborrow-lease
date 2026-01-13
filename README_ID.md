# praborrow-lease (Bahasa Indonesia)

[English](./README.md) | Indonesia

Mekanisme konsensus lease terdistribusi.

## Ikhtisar (Overview)

Mengimplementasikan lapisan konsensus untuk mengelola lease kedaulatan (sovereign leases) dalam lingkungan terdistribusi. Crate ini menggunakan algoritma konsensus berbasis Raft untuk memastikan linearizability dari borrow checking di beberapa node.

## Fitur Utama (Key Features)

- **Konsensus Raft**: Pemilihan pemimpin (leader election) dan replikasi log untuk status yang konsisten.
- **Manajemen Lease**: Penguncian terdistribusi dan pemeriksaan validitas untuk sumber daya `Sovereign<T>`.
- **Toleransi Kesalahan (Fault Tolerance)**: Menjaga integritas sistem meskipun terjadi kegagalan node atau partisi jaringan.
- **Konsensus Bersama (Joint Consensus)**: Perubahan keanggotaan dinamis yang aman menggunakan transisi konfigurasi dua fase.
- **Keamanan (mTLS)**: Dukungan Mutual TLS untuk komunikasi antar-node yang aman.
