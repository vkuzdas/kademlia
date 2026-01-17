# 🔑 Kademlia DHT

A Java implementation of the [Kademlia](https://en.wikipedia.org/wiki/Kademlia) distributed hash table protocol with gRPC-based node communication.

[![Java CI](https://github.com/vkuzdas/kademlia/actions/workflows/java_ci.yml/badge.svg)](https://github.com/vkuzdas/kademlia/actions/workflows/java_ci.yml)

## Overview

Kademlia is a peer-to-peer DHT known for its XOR-based distance metric and parallel, asynchronous lookups. It powers major systems like BitTorrent, IPFS, and Ethereum. Each node maintains **k-buckets** organized by XOR distance and performs iterative lookups using $\alpha$ concurrent queries.

This implementation supports:
- **DHT Operations** — `put`, `get`, `delete` with k-closest replication
- **XOR Metric** — Distance = `nodeId ⊕ targetId`
- **K-Buckets** — 160-bit address space, LRU eviction
- **Iterative Lookup** — `FIND_NODE`, `FIND_VALUE` with α concurrency
- **Maintenance** — Key republishing, expiration, bucket refresh

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    KademliaNode                         │
├─────────────────────────────────────────────────────────┤
│  Routing Table (ID_LENGTH k-buckets)                    │
│  └── KBucket[i]: nodes with distance 2^i ≤ d < 2^(i+1) │
├─────────────────────────────────────────────────────────┤
│  Local Data Storage                                     │
│  ├── Republish tasks (original publisher)              │
│  └── Expire tasks (TTL enforcement)                    │
├─────────────────────────────────────────────────────────┤
│  gRPC Server (KademliaServiceGrpc)                      │
│  ├── PING / STORE                                      │
│  ├── FIND_NODE / FIND_VALUE                            │
│  └── DELETE                                            │
└─────────────────────────────────────────────────────────┘
```

## Project Structure

```
src/
├── main/java/kademlia/
│   ├── KademliaNode.java    # Core node logic & gRPC server
│   ├── RoutingTable.java    # K-bucket management
│   ├── KBucket.java         # Single bucket (size k, LRU)
│   ├── Shortlist.java       # Iterative lookup state
│   └── NodeReference.java   # Node identity
└── test/java/
    ├── unit/                # Fine-grained tests
    │   ├── join/            # Node joining scenarios
    │   ├── routingtable/    # Bucket operations
    │   └── schedules/       # Republish, expire, refresh
    └── integration/         # Large-scale network tests
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Java 17 |
| Build | Maven |
| RPC | gRPC + Protocol Buffers |
| Logging | SLF4J + Logback |
| Testing | JUnit 5 |

## Getting Started

```bash
# Build & test
mvn clean install

# Run a bootstrap node
java -cp target/classes kademlia.KademliaNode
```

## Configuration

```java
// ID length: 4-160 bits (default 160 for SHA-1)
KademliaNode.setIdLength(160);

// Replication factor & bucket size
KademliaNode.setKParameter(20);

// Concurrent lookups
KademliaNode.setAlphaParameter(3);

// Timing intervals
KademliaNode.setRepublishInterval(Duration.ofHours(1));
KademliaNode.setExpireInterval(Duration.ofHours(24));
KademliaNode.setRefreshInterval(Duration.ofHours(1));
```

## Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `k` | 20 | Bucket size & replication factor |
| `α` | 3 | Parallel lookup queries |
| `ID_LENGTH` | 160 | Bits in node/key IDs |
| `tRepublish` | 1h | Original publisher republish interval |
| `tExpire` | 24h | Key expiration TTL |
| `tRefresh` | 1h | Bucket refresh interval |

## References

- [Kademlia: A Peer-to-peer Information System Based on the XOR Metric](https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf) — Maymounkov & Mazières, IPTPS 2002
