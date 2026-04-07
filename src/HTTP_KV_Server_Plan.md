# HTTP Server + KV Store — Complete Implementation Plan
> **Stack:** C++17 | epoll | Thread Pool | LRU Cache | WAL | REST API  
> **Goal:** A nice Project  
> **Timeline:** 6 weeks

---

## Architecture Overview

```
Client Request
     │
     ▼
TCP Socket Layer (epoll, non-blocking I/O)
     │
     ▼
Thread Pool (N = CPU cores)
     │
     ▼
HTTP/1.1 Parser (state machine, zero-copy)
     │
     ▼
Router (trie-based, path params)
     │
     ▼
KV Store Handler
     │
     ├──▶ LRU Cache (hot data, O(1) get/put)
     │
     └──▶ Storage Engine
               │
               ├──▶ WAL (write-ahead log, durability)
               └──▶ Snapshot (periodic, binary)
```

---

## Repo Structure

```
http-kv-server/
├── src/
│   ├── server/
│   │   ├── tcp_server.h / .cpp       # socket, bind, listen, accept
│   │   ├── epoll_loop.h / .cpp       # event loop, edge-triggered
│   │   └── thread_pool.h / .cpp      # worker threads, task queue
│   ├── http/
│   │   ├── request_parser.h / .cpp   # HTTP/1.1 parser
│   │   ├── router.h / .cpp           # route matching, middleware
│   │   └── response_builder.h / .cpp # status codes, headers, body
│   ├── kv/
│   │   ├── kv_store.h / .cpp         # thread-safe get/set/del/ttl
│   │   ├── lru_cache.h / .cpp        # hashmap + doubly linked list
│   │   ├── wal.h / .cpp              # write-ahead log
│   │   └── snapshot.h / .cpp         # binary persistence
│   └── main.cpp
├── tests/
│   ├── test_http_parser.cpp
│   ├── test_lru_cache.cpp
│   ├── test_kv_store.cpp
│   └── test_thread_pool.cpp
├── benchmarks/
│   └── results.md                    # YOUR benchmark numbers go here
├── docs/
│   └── architecture.md
└── CMakeLists.txt
```

---

## Week-by-Week Plan

---

### ✅ Week 1 — TCP Foundation + In-Memory KV

#### HTTP Server: Raw TCP Server

**File:** `src/server/tcp_server.cpp`

```cpp
// Steps to implement:
// 1. socket(AF_INET, SOCK_STREAM, 0)
// 2. setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, ...)
// 3. bind() to port 8080
// 4. listen(fd, SOMAXCONN)
// 5. accept() in a loop — print raw bytes received
// 6. close(client_fd) after reading

int server_fd = socket(AF_INET, SOCK_STREAM, 0);
int opt = 1;
setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

sockaddr_in addr{};
addr.sin_family = AF_INET;
addr.sin_addr.s_addr = INADDR_ANY;
addr.sin_port = htons(8080);

bind(server_fd, (sockaddr*)&addr, sizeof(addr));
listen(server_fd, SOMAXCONN);
```

**Milestone:** `curl http://localhost:8080` → see raw HTTP bytes in terminal.

#### KV Store: Thread-Safe In-Memory Map

**File:** `src/kv/kv_store.h`

```cpp
#include <shared_mutex>
#include <unordered_map>
#include <string>
#include <optional>
#include <chrono>

class KVStore {
public:
    void set(const std::string& key, const std::string& value,
             std::optional<int> ttl_seconds = std::nullopt);
    std::optional<std::string> get(const std::string& key);
    bool del(const std::string& key);
    bool exists(const std::string& key);

private:
    std::unordered_map<std::string, std::string> data_;
    std::unordered_map<std::string,
        std::chrono::steady_clock::time_point> expiry_;
    std::shared_mutex mutex_; // multiple readers, single writer

    bool isExpired(const std::string& key);
};
```

**Key concepts this week:** file descriptors, `SO_REUSEADDR`, blocking I/O, `std::shared_mutex`, `std::chrono`.

---

### ✅ Week 2 — epoll + Thread Pool + LRU Cache

#### epoll Event Loop

**File:** `src/server/epoll_loop.cpp`

```cpp
// Steps:
// 1. Set server_fd to non-blocking: fcntl(fd, F_SETFL, O_NONBLOCK)
// 2. epoll_create1(0) → epoll_fd
// 3. Register server_fd with EPOLLIN | EPOLLET
// 4. epoll_wait() in a loop
// 5. On new connection: accept(), set non-blocking, register client_fd
// 6. On data ready: read all bytes (loop until EAGAIN)

int epoll_fd = epoll_create1(0);

epoll_event ev{};
ev.events = EPOLLIN | EPOLLET; // edge-triggered
ev.data.fd = server_fd;
epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

// In loop:
epoll_event events[MAX_EVENTS];
int n = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
```

#### Thread Pool

**File:** `src/server/thread_pool.h`

```cpp
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>

class ThreadPool {
public:
    explicit ThreadPool(size_t num_threads);
    ~ThreadPool();

    void enqueue(std::function<void()> task);

private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_{false};
};

// Constructor spawns N threads, each running:
// while(true) {
//     wait on cv_ until task available or stop_
//     pop task from queue
//     execute task
// }
```

#### LRU Cache

**File:** `src/kv/lru_cache.h`

```cpp
#include <list>
#include <unordered_map>
#include <string>
#include <optional>
#include <mutex>

class LRUCache {
public:
    explicit LRUCache(size_t capacity);

    std::optional<std::string> get(const std::string& key);
    void put(const std::string& key, const std::string& value);
    void invalidate(const std::string& key);

    // Metrics
    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }

private:
    size_t capacity_;
    // Front = most recently used, Back = least recently used
    std::list<std::pair<std::string, std::string>> list_;
    std::unordered_map<std::string,
        std::list<std::pair<std::string,std::string>>::iterator> map_;
    std::mutex mutex_;
    size_t hits_{0}, misses_{0};
};
```

**Key concepts this week:** `epoll`, edge vs level triggered, `EAGAIN`, condition variables, O(1) LRU via iterator stability.

---

### ✅ Week 3 — HTTP Parser + Write-Ahead Log

#### HTTP/1.1 Request Parser

**File:** `src/http/request_parser.h`

```cpp
enum class ParseState {
    REQUEST_LINE,
    HEADERS,
    BODY,
    DONE,
    ERROR
};

struct HttpRequest {
    std::string method;       // GET, POST, PUT, DELETE
    std::string path;         // /kv/mykey
    std::string version;      // HTTP/1.1
    std::unordered_map<std::string, std::string> headers;
    std::unordered_map<std::string, std::string> query_params;
    std::string body;
    std::string path_param;   // extracted :key value
};

class RequestParser {
public:
    // Feed raw bytes incrementally (TCP may split packets)
    ParseState feed(const char* data, size_t len);
    const HttpRequest& request() const { return req_; }
    void reset();

private:
    ParseState state_{ParseState::REQUEST_LINE};
    HttpRequest req_;
    std::string buffer_;
    size_t content_length_{0};

    void parseRequestLine(const std::string& line);
    void parseHeader(const std::string& line);
    void parseQueryParams(const std::string& query);
};
```

**Parse flow:**
1. Read until `\r\n` → request line → extract method, path, version
2. Read until `\r\n\r\n` → headers → split on `:`, trim whitespace
3. Read `Content-Length` bytes → body
4. Use `std::string_view` for zero-copy slicing where possible

#### Write-Ahead Log

**File:** `src/kv/wal.h`

```cpp
#include <fstream>
#include <mutex>
#include <string>

enum class WALOp { SET, DEL };

struct WALEntry {
    uint64_t timestamp;
    WALOp op;
    std::string key;
    std::string value; // empty for DEL
};

class WAL {
public:
    explicit WAL(const std::string& filepath);

    // Call BEFORE applying write to in-memory store
    void append(WALOp op, const std::string& key,
                const std::string& value = "");

    // On startup: replay all entries to restore state
    std::vector<WALEntry> replay();

    // Truncate WAL after snapshot (compaction)
    void compact();

private:
    std::ofstream file_;
    std::mutex mutex_;

    // Binary format per entry:
    // [8 bytes timestamp][1 byte op][4 bytes key_len]
    // [key_len bytes key][4 bytes val_len][val_len bytes val]
};
```

**Durability rule:** always call `wal.append()` → then update in-memory map. Never the other way.

**Key concepts this week:** state machine parsing, TCP fragmentation, `fsync`, crash recovery.

---

### ✅ Week 4 — Router + Persistence + RESP Protocol

#### Trie-Based Router

**File:** `src/http/router.h`

```cpp
using Handler = std::function<void(const HttpRequest&, HttpResponse&)>;

struct TrieNode {
    std::unordered_map<std::string, std::unique_ptr<TrieNode>> children;
    std::unordered_map<std::string, Handler> method_handlers;
    std::string param_name; // set if this node is a :param segment
    bool is_param{false};
};

class Router {
public:
    void add_route(const std::string& method,
                   const std::string& path, Handler handler);
    bool route(HttpRequest& req, HttpResponse& res);

    // Convenience
    void GET(const std::string& path, Handler h);
    void POST(const std::string& path, Handler h);
    void PUT(const std::string& path, Handler h);
    void DELETE(const std::string& path, Handler h);

private:
    TrieNode root_;
    bool match(TrieNode* node, const std::vector<std::string>& segments,
               size_t idx, HttpRequest& req, Handler& out);
};
```

**Routes to register:**
```cpp
router.PUT("/kv/:key",        kvSetHandler);
router.GET("/kv/:key",        kvGetHandler);
router.DELETE("/kv/:key",     kvDelHandler);
router.GET("/kv/:key/ttl",    kvTTLHandler);
router.GET("/health",         healthHandler);
router.GET("/metrics",        metricsHandler);
```

#### Snapshot Persistence

**File:** `src/kv/snapshot.h`

```cpp
class Snapshot {
public:
    explicit Snapshot(const std::string& dir);

    // Serialize entire KVStore to binary file
    void save(const KVStore& store);

    // Deserialize latest snapshot into KVStore
    void load(KVStore& store);

    // Run in background thread every N seconds
    void start_background(KVStore& store, int interval_seconds);

private:
    std::string dir_;
    std::atomic<bool> running_{false};
    std::thread bg_thread_;

    // File format:
    // [8 bytes: num_entries]
    // For each entry:
    //   [4 bytes key_len][key][4 bytes val_len][val]
    //   [1 byte has_ttl][8 bytes ttl_epoch if has_ttl]
};
```

**Startup sequence:**
1. Load latest snapshot → restore bulk of data
2. Replay WAL tail (entries after snapshot timestamp) → catch up
3. Start background snapshot thread

#### RESP Protocol (Bonus)

```
Client sends:  *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyval\r\n
Server replies: +OK\r\n

Client sends:  *2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n
Server replies: $5\r\nmyval\r\n

Not found:     $-1\r\n
Error:         -ERR unknown command\r\n
```

Implement this alongside HTTP so you can benchmark with `redis-benchmark`.

---

### ✅ Week 5 — Integration + REST Wiring

#### Connect Everything in main.cpp

```cpp
int main() {
    // 1. Init KV store with WAL and snapshot
    WAL wal("data/wal.log");
    KVStore store(wal);
    LRUCache cache(10000);       // 10k entry cache
    Snapshot snap("data/snaps");
    snap.load(store);            // restore from disk
    snap.start_background(store, 300); // snapshot every 5 min

    // 2. Build router with handlers
    Router router;
    router.PUT("/kv/:key", [&](auto& req, auto& res) {
        store.set(req.path_param, req.body);
        cache.invalidate(req.path_param);
        res.status(200).body("OK");
    });
    router.GET("/kv/:key", [&](auto& req, auto& res) {
        if (auto val = cache.get(req.path_param)) {
            res.status(200).body(*val);
            return;
        }
        if (auto val = store.get(req.path_param)) {
            cache.put(req.path_param, *val);
            res.status(200).body(*val);
        } else {
            res.status(404).body("Not Found");
        }
    });
    router.GET("/metrics", [&](auto& req, auto& res) {
        // Return JSON with cache hit rate, ops/sec, memory
    });

    // 3. Start HTTP server
    ThreadPool pool(std::thread::hardware_concurrency());
    EpollLoop loop(8080, pool, router);
    loop.run(); // blocks
}
```

#### HTTP Response Builder

```cpp
class HttpResponse {
public:
    HttpResponse& status(int code);
    HttpResponse& header(const std::string& key, const std::string& val);
    HttpResponse& body(const std::string& content);
    HttpResponse& json(const std::string& json_str);

    std::string build() const; // serializes to raw HTTP/1.1 string
};

// Example output:
// HTTP/1.1 200 OK\r\n
// Content-Type: application/json\r\n
// Content-Length: 15\r\n
// Connection: keep-alive\r\n
// \r\n
// {"status":"ok"}
```

---

### ✅ Week 6 — Benchmarking + Polish

#### Benchmark HTTP Server

```bash
# Install wrk
brew install wrk   # macOS
apt install wrk    # Linux

# Benchmark GET
wrk -t4 -c100 -d30s http://localhost:8080/kv/testkey

# Benchmark PUT (with script)
wrk -t4 -c100 -d30s -s put.lua http://localhost:8080/kv/testkey

# Target: 10,000+ req/sec on a 4-core machine
```

#### Benchmark KV Store (via RESP)

```bash
# If you implemented RESP protocol
redis-benchmark -p 8080 -t get,set -n 100000 -c 50

# Target: 100,000+ ops/sec for in-memory GET
```

#### results.md Template

```markdown
## Benchmark Results

**Machine:** MacBook Pro M2, 8 cores, 16GB RAM
**Date:** YYYY-MM-DD

### HTTP Server
| Scenario        | Req/sec | Latency p50 | Latency p99 |
|-----------------|---------|-------------|-------------|
| GET /kv/:key    | 12,400  | 2.1ms       | 8.3ms       |
| PUT /kv/:key    | 9,800   | 2.6ms       | 10.1ms      |
| 100 connections | 11,200  | 2.3ms       | 9.1ms       |

### KV Store (direct RESP)
| Operation | Ops/sec  | Latency p50 |
|-----------|----------|-------------|
| GET       | 118,000  | 0.4ms       |
| SET       | 104,000  | 0.5ms       |

### Cache Performance
- Hit rate: 94.2% (after warmup)
- Memory: ~180MB for 1M keys
```

#### CMakeLists.txt

```cmake
cmake_minimum_required(VERSION 3.16)
project(http_kv_server CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_BUILD_TYPE Release)
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")

# Main binary
add_executable(server
    src/main.cpp
    src/server/tcp_server.cpp
    src/server/epoll_loop.cpp
    src/server/thread_pool.cpp
    src/http/request_parser.cpp
    src/http/router.cpp
    src/http/response_builder.cpp
    src/kv/kv_store.cpp
    src/kv/lru_cache.cpp
    src/kv/wal.cpp
    src/kv/snapshot.cpp
)

target_include_directories(server PRIVATE src)
target_link_libraries(server pthread)

# Tests (Google Test)
find_package(GTest REQUIRED)
enable_testing()

add_executable(tests
    tests/test_http_parser.cpp
    tests/test_lru_cache.cpp
    tests/test_kv_store.cpp
    tests/test_thread_pool.cpp
)

target_link_libraries(tests GTest::gtest_main pthread)
add_test(NAME all_tests COMMAND tests)
```

---

## Key Concepts Checklist

### Concurrency
- [ ] `std::thread`, `std::mutex`, `std::condition_variable`
- [ ] `std::shared_mutex` (readers-writer lock)
- [ ] `std::atomic` for lock-free flags
- [ ] Thread pool with work queue
- [ ] Avoiding deadlocks (lock ordering, RAII guards)

### Networking
- [ ] TCP socket lifecycle: `socket → bind → listen → accept → read/write → close`
- [ ] Non-blocking I/O with `O_NONBLOCK`
- [ ] `epoll` edge-triggered mode
- [ ] Handling partial reads (TCP is a stream, not messages)
- [ ] `SO_REUSEADDR`, `TCP_NODELAY` socket options

### Storage
- [ ] Write-ahead logging (WAL) for durability
- [ ] Snapshot + WAL tail replay on startup
- [ ] `fsync` vs `fdatasync` trade-offs
- [ ] Binary serialization format design

### Data Structures
- [ ] LRU Cache: `std::list` + `std::unordered_map` = O(1)
- [ ] Trie for route matching with path params
- [ ] Min-heap for TTL expiry
- [ ] `std::string_view` for zero-copy parsing

### HTTP
- [ ] Request line, headers, body structure
- [ ] `\r\n` line endings (not just `\n`)
- [ ] `Content-Length` for body framing
- [ ] `Connection: keep-alive` for persistent connections
- [ ] Status codes: 200, 201, 400, 404, 500

---

## Resume Bullet Point Template

> **HTTP + KV Server** (C++17) — Built a multithreaded HTTP/1.1 server using epoll and a custom thread pool, backed by an in-memory key-value store with LRU eviction, write-ahead logging, and binary snapshot persistence. Achieved **12,000 req/sec** HTTP throughput and **110,000 KV ops/sec** on a 4-core machine. Implemented trie-based routing, partial read handling, and crash recovery via WAL replay.

---

## Learning Resources

| Topic | Resource |
|-------|----------|
| epoll & async I/O | `man epoll`, Beej's Guide to Network Programming |
| Thread pool internals | Anthony Williams — *C++ Concurrency in Action* |
| LRU Cache | LeetCode #146 (understand it, then extend) |
| WAL & crash recovery | CMU 15-445 Lecture Notes (free online) |
| HTTP/1.1 spec | RFC 7230 (just sections 3–5) |
| Benchmarking | `wrk` GitHub README, `redis-benchmark` docs |
| Build system | CMake official tutorial |

---

*Save this file. Build one phase at a time. Benchmark loudly. 🚀*
