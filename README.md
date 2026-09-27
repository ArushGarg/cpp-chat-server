# Multi-User Chat Server (C++17)

A production-style TCP chat server built around a single-threaded `epoll`
reactor plus a worker thread pool for off-loading disk I/O. No third-party
dependencies — just POSIX sockets and the STL.

## Why this architecture

- **One reactor thread, edge-triggered epoll.** All client state (`clients_`,
  `rooms_`) is touched only from this thread, so there's zero locking on the
  hot path and no data races to reason about.
- **Non-blocking sockets + per-client output queue.** If a client's socket
  buffer fills up, writes are queued and the reactor asks for `EPOLLOUT`;
  nothing ever blocks the whole server because one client is slow.
- **Thread pool for async logging.** Every chat event is handed to a 4-thread
  pool that appends to `chat.log` under a mutex — genuine off-thread work
  that would otherwise stall the reactor on `fsync`.

This is the same pattern used by real event-driven servers (nginx, Redis,
node.js) rather than the naive thread-per-client model, which doesn't scale
past a few thousand connections.

## Build

```bash
g++ -std=c++17 -O2 -Iinclude -pthread -Wall -Wextra \
    src/Server.cpp src/main_server.cpp -o chat_server

g++ -std=c++17 -O2 -pthread -Wall -Wextra src/client.cpp -o chat_client
```

(A `CMakeLists.txt` is included too, if you have cmake: `cmake -B build && cmake --build build`.)

## Run

```bash
./chat_server 5555          # start server on port 5555 (default 5555)
./chat_client 127.0.0.1 5555  # in another terminal, per user
```

## Protocol (plain text, newline-terminated)

| Command | Effect |
|---|---|
| `/nick <name>` | Set your username (required before anything else) |
| `/join <room>` | Join or create a room (leaves current room first) |
| `/leave` | Leave your current room |
| `/msg <user> <text>` | Private message to another connected user |
| `/who` | List users in your current room |
| `/rooms` | List all active rooms and their sizes |
| `/quit` | Disconnect |
| anything else | Broadcast to everyone in your current room |

All chat activity is logged to `chat.log` with timestamps.

## Design notes / things worth mentioning in an interview

- Edge-triggered epoll requires draining `read()`/`write()` in a loop until
  `EAGAIN` — handled in `handleReadable` / `flushOutgoing`.
- Partial reads/writes are buffered per-client (`inbuf`, `outq`) since TCP
  gives no message boundaries — the server reassembles newline-delimited
  lines itself.
- `SIGPIPE` is ignored so a client disconnecting mid-write can't kill the
  process; the resulting `EPIPE` is handled like any other socket error.
- Single-writer ownership of `clients_`/`rooms_` avoids a whole class of
  concurrency bugs you'd otherwise hit with thread-per-connection + shared
  state.

## Possible extensions

- TLS via OpenSSL BIO wrapping the socket
- Persistent chat history (SQLite) instead of a flat log file
- Multiple reactor threads with `SO_REUSEPORT` for multi-core scaling
- systemd unit file for daemonizing
