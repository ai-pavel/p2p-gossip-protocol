# P2P Gossip Protocol

[![CI](https://github.com/pavel-genai/p2p-gossip-protocol/actions/workflows/ci.yml/badge.svg)](https://github.com/pavel-genai/p2p-gossip-protocol/actions/workflows/ci.yml)

A peer-to-peer gossip protocol implementation in Zig 0.13, based on the SWIM
(Scalable Weakly-consistent Infection-style Process Group Membership) protocol.

## Features

- **SWIM failure detection**: ping, ping-req, suspect, and confirm messages
- **Infection-style dissemination**: bounded fanout with TTL-based expiry
- **Membership management**: node join, graceful leave, and failure detection
- **UDP transport**: lightweight, connectionless communication between nodes

## Building

```bash
zig build
```

## Running

Start a seed node:

```bash
./zig-out/bin/gossip --port 7001
```

Join an existing cluster:

```bash
./zig-out/bin/gossip --port 7002 --join 127.0.0.1:7001
```

## Testing

```bash
zig build test
```

## Architecture

| File                 | Purpose                                      |
|----------------------|----------------------------------------------|
| `src/main.zig`       | CLI entry point, argument parsing             |
| `src/gossip.zig`     | Core gossip engine: SWIM protocol loop        |
| `src/membership.zig` | Peer list, state transitions, failure detect  |
| `src/transport.zig`  | UDP socket send/receive abstraction           |
| `src/message.zig`    | Message encoding/decoding, types              |

## Protocol Details

The protocol period runs in a configurable interval (default 500 ms). Each
period, the node:

1. Selects a random peer and sends a **ping**.
2. If no **ack** is received within a timeout, it sends **ping-req** to
   `k` random peers asking them to ping on its behalf.
3. If still no ack, the target is marked **suspect**.
4. After a suspect timeout, a **confirm** (faulty) message is disseminated
   and the node is removed from the membership list.
5. Dissemination updates (join, leave, suspect, confirm) are piggybacked
   on protocol messages with bounded fanout and TTL.
