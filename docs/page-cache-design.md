# AFS Page Cache Design

## 1. Goal

Provide a **global shared page cache service** for AFS remote read-only filesystems:

1. **Cross-process sharing**: Multiple FUSE mount processes share a single cache, avoiding redundant remote data fetches for the same content.
2. **High-performance reads**: Zero-copy read path — FUSE processes `pread` cache disk files directly, bypassing the cache service process.
3. **Volatile cache**: Cache can be lost and rebuilt at any time. Index lives in memory; data blocks live on disk.
4. **Efficient space utilization**: Slab allocator with 2K–2MB page sizes to minimize disk fragmentation.

## 2. Non-Goals

- **Write support**: Remote filesystem is read-only; no write-back scenarios.
- **Consistency guarantees**: Remote file content is immutable; no invalidation or coherence protocol.
- **Distributed cache**: Single-machine cache service only (this phase).
- **Persistent recovery**: Index is lost on restart; cache must re-warm.

## 3. Background

### 3.1 Current Architecture

AFS mounts remote read-only container image layers via FUSE. Each layer is stored as an AFSLYR02 archive containing a JSON TOC (table of contents) followed by identity-encoded (plain) file payloads. The TOC includes a `sha256:<hex>` content digest per file entry.

Current usage patterns:
- **Short-lived mounts**: Each mount lasts ~10–30 minutes.
- **Concurrent mounts**: Multiple FUSE processes run simultaneously on the same node.
- **Overlapping reads**: Different FUSE processes often read identical files (shared base layers, common libraries, datasets).

### 3.2 Problem

Each FUSE process independently fetches data from remote layerstores via gRPC, causing:
1. Duplicate network I/O for the same file content across processes.
2. High latency (milliseconds) for remote reads.
3. Short-lived processes cannot accumulate effective local caches.

### 3.3 Integration with AFSLYR02 Format

The AFSLYR02 TOC `Entry` now includes:

```go
type Entry struct {
    Path             string       `json:"path"`
    Digest           string       `json:"digest"`          // "sha256:<hex>" content digest
    PayloadCodec     PayloadCodec `json:"payload_codec"`
    PayloadOffset    int64        `json:"payload_offset"`
    PayloadSize      int64        `json:"payload_size"`
    UncompressedSize int64        `json:"uncompressed_size"`
    // ... other fields
}
```

The `Digest` field enables **content-addressable caching**: files with identical content across different layers share the same cache entry. The cache key is `(digest, page_id)`.

## 4. Architecture

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ FUSE Proc 1 │  │ FUSE Proc 2 │  │ FUSE Proc N │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       │  gRPC over UDS │                │
       │   (control)    │                │
       ▼                ▼                ▼
┌──────────────────────────────────────────────┐
│              Cache Service                    │
│                                              │
│  ┌──────────┐  ┌───────────┐  ┌───────────┐ │
│  │ In-Memory│  │  Lease    │  │ Eviction  │ │
│  │  Index   │  │  Manager  │  │(W-TinyLFU)│ │
│  │(digest,  │  │ (pin/     │  │           │ │
│  │ page_id) │  │  release) │  │           │ │
│  │→ disk loc│  └───────────┘  └───────────┘ │
│  └──────────┘                                │
│  ┌──────────────────────────────────────┐    │
│  │         Slab Allocator               │    │
│  │  Manages 2MB chunk allocation        │    │
│  └──────────────────────────────────────┘    │
└──────────────────┬───────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────┐
│              Disk Cache Storage               │
│  ┌────────┐ ┌────────┐ ┌────────┐            │
│  │2MB     │ │2MB     │ │2MB     │  ...       │
│  │Chunk 0 │ │Chunk 1 │ │Chunk 2 │            │
│  └────────┘ └────────┘ └────────┘            │
│  Configurable total: 8–32 GB                 │
└──────────────────────────────────────────────┘
        ↑  pread (data plane, zero-copy)
        │
┌───────┴─────┐
│ FUSE Proc X │  ← GetPage returns disk location, then direct pread
└─────────────┘
```

## 5. Read/Write Paths

### 5.1 Write Path (Cache Miss → PutPage)

```
FUSE                     Cache Service                Disk
 │                            │                        │
 │── GetPage(digest,pid) ────>│                        │
 │<── CACHE_MISS ─────────────│                        │
 │                            │                        │
 │  (FUSE reads from remote)  │                        │
 │                            │                        │
 │── PutPage(digest,pid,buf)->│       (client streaming)
 │                            │── allocate slab ──────>│
 │                            │── write buffer ───────>│
 │                            │── update index         │
 │<── OK ─────────────────────│                        │
```

### 5.2 Read Path (Cache Hit → Lease/Pin)

```
FUSE                     Cache Service                Disk
 │                            │                        │
 │── GetPage(digest,pid) ────>│                        │
 │                            │── pin block            │
 │<── (path, offset, lease) ──│                        │
 │                            │                        │
 │──────────── pread(path, offset) ──────────────────>│
 │<─────────── data (zero-copy) ─────────────────────│
 │                            │                        │
 │── Release(lease) ─────────>│                        │
 │                            │── unpin block          │
```

Key points:
- Cache service **pins** the block on GetPage hit, preventing eviction during FUSE reads.
- FUSE must call `Release` after reading. Leases have a timeout to handle FUSE crashes.
- Data is read via `pread` directly from disk — **zero-copy, bypassing the cache service process**.

### 5.3 FUSE Integration Point

The current `FileNode.Read()` in `pkg/layerfuse/fs.go` reads via `FileSection.ReadAt()`. The page cache intercepts this path:

```
FileNode.Read(offset, size)
  → compute page_id from offset
  → GetPage(entry.Digest, page_id)
  → if HIT:  pread from cache disk file
  → if MISS: read from remote archive → PutPage → return data
  → Release(lease)
```

The `Entry.Digest` field (added to AFSLYR02 TOC) provides the content-addressable key. Identical files across layers (e.g., `/usr/lib/libc.so` in multiple images) share the same digest, so a single cache entry serves all of them.

## 6. gRPC Interface

### 6.1 Protobuf Service Definition

```protobuf
syntax = "proto3";
package afs.pagecache.v1;

service PageCacheService {
  rpc PutPage(stream PutPageChunk) returns (PutPageResponse);
  rpc GetPage(GetPageRequest) returns (GetPageResponse);
  rpc Release(ReleaseRequest) returns (ReleaseResponse);
  rpc BatchGetPage(BatchGetPageRequest) returns (BatchGetPageResponse);
  rpc BatchRelease(BatchReleaseRequest) returns (BatchReleaseResponse);
  rpc Register(RegisterRequest) returns (RegisterResponse);
  rpc GetStats(GetStatsRequest) returns (GetStatsResponse);
}
```

### 6.2 PutPage (Client Streaming)

```protobuf
message PutPageChunk {
  PutPageHeader header = 1;  // Only in first chunk
  bytes data = 2;            // ≤64KB per chunk
}

message PutPageHeader {
  string digest = 1;         // Content digest, e.g. "sha256:af3b..."
  uint64 page_id = 2;
  uint32 total_size = 3;     // Page total bytes for slab pre-allocation
}

message PutPageResponse {
  PutPageStatus status = 1;
}

enum PutPageStatus {
  PUT_PAGE_OK = 0;
  PUT_PAGE_ALREADY_EXISTS = 1;
  PUT_PAGE_NO_SPACE = 2;
  PUT_PAGE_INVALID = 3;
}
```

### 6.3 GetPage

```protobuf
message GetPageRequest {
  string digest = 1;
  uint64 page_id = 2;
}

message GetPageResponse {
  GetPageStatus status = 1;
  string cache_file_path = 2;  // Disk file path
  uint64 cache_offset = 3;     // Offset within file
  uint32 data_size = 4;
  uint64 lease_id = 5;
}

enum GetPageStatus {
  CACHE_HIT = 0;
  CACHE_MISS = 1;
}
```

### 6.4 Release, Batch, Register, Stats

```protobuf
message ReleaseRequest { uint64 lease_id = 1; }
message ReleaseResponse {}

message BatchGetPageRequest { repeated GetPageRequest pages = 1; }
message BatchGetPageResponse { repeated GetPageResponse pages = 1; }

message BatchReleaseRequest { repeated uint64 lease_ids = 1; }
message BatchReleaseResponse {}

message RegisterRequest { string client_id = 1; }
message RegisterResponse { string session_id = 1; }

message GetStatsRequest {}
message GetStatsResponse {
  uint64 total_pages = 1;
  uint64 total_bytes = 2;
  uint64 max_bytes = 3;
  uint64 hit_count = 4;
  uint64 miss_count = 5;
  uint32 active_leases = 6;
  uint32 connected_clients = 7;
}
```

## 7. Lease/Pin Protocol

### 7.1 Lifecycle

```
  ┌──────────┐   GetPage (hit)   ┌──────────┐   Release    ┌──────────┐
  │  FREE    │ ───────────────>  │  PINNED  │ ──────────>  │  FREE    │
  └──────────┘                   └──────────┘              └──────────┘
                                      │ timeout
                                      ▼
                                 ┌──────────┐
                                 │  FREE    │  (auto-release on timeout)
                                 └──────────┘
```

### 7.2 Pin Reference Counting

Multiple clients may pin the same block concurrently:

```
Client A: GetPage(digest, 42) → lease_1, pin_count = 1
Client B: GetPage(digest, 42) → lease_2, pin_count = 2
Client A: Release(lease_1)              → pin_count = 1
Client B: Release(lease_2)              → pin_count = 0  ← evictable
```

Only blocks with `pin_count == 0` can be evicted.

### 7.3 Crash Recovery

- gRPC connection close → release all leases for that session.
- Lease timeout (default 30s) as safety net.

## 8. Eviction: W-TinyLFU

### 8.1 Structure

```
                    ┌───────────────────────────────────────────┐
                    │           Count-Min Sketch                │
                    │    (global frequency estimator)           │
                    └───────────────────────────────────────────┘

┌──────────────────┐         ┌──────────────────────────────────┐
│  Window LRU      │         │  Main Cache (SLRU)               │
│  (~1% capacity)  │         │  (~99% capacity)                 │
│                  │ ──evict─>│  ┌────────────┐ ┌────────────┐  │
│  New writes      │ candidate│  │ Probation  │→│ Protected  │  │
│  enter here      │         │  │ (~20%)     │ │ (~80%)     │  │
└──────────────────┘         │  └────────────┘ └────────────┘  │
                             └──────────────────────────────────┘
```

### 8.2 Admission Decision

Window eviction candidate vs. Probation tail victim — compare frequencies from Count-Min Sketch. Higher frequency wins; ties favor the new candidate.

### 8.3 Pinned Block Handling

When selecting eviction candidates, skip blocks with `pin_count > 0`. If scan exceeds `max_eviction_scan` (default 64) without finding an evictable block, return `NO_SPACE`.

## 9. Slab Allocator

### 9.1 Slab Classes

| Class | Slab Size | Slabs/Chunk |
|-------|----------|-------------|
| 0     | 2 KB     | 1024        |
| 1     | 4 KB     | 512         |
| 2     | 8 KB     | 256         |
| 3     | 16 KB    | 128         |
| 4     | 32 KB    | 64          |
| 5     | 64 KB    | 32          |
| 6     | 128 KB   | 16          |
| 7     | 256 KB   | 8           |
| 8     | 512 KB   | 4           |
| 9     | 1 MB     | 2           |
| 10    | 2 MB     | 1           |

### 9.2 Disk Layout

```
$cache_dir/chunks/
├── chunk_00000.dat    # 2MB, pre-allocated via fallocate
├── chunk_00001.dat
└── ...
```

Each chunk stores slabs of a single class. Allocation is O(1) via per-class free-chunk lists and bitmaps.

## 10. Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `cache_dir` | Cache disk directory | `/var/cache/afs-page-cache` |
| `max_cache_size` | Maximum cache capacity | `8GB` |
| `uds_path` | gRPC UDS socket path | `/var/run/afs-page-cache.sock` |
| `lease_timeout` | Lease timeout duration | `30s` |
| `lease_check_interval` | Timeout scan interval | `5s` |

## 11. Capacity Planning

### 11.1 Disk

8 GB = 4,096 chunks; 32 GB = 16,384 chunks.

### 11.2 Memory Index

~40 bytes per `CacheEntry`:

| Page Size | Pages in 8GB | Index Memory |
|-----------|-------------|-------------|
| 4 KB      | 2,097,152   | ~80 MB      |
| 64 KB     | 131,072     | ~5 MB       |
| 2 MB      | 4,096       | ~160 KB     |

## 12. Error Handling

| Scenario | Strategy |
|----------|---------|
| Cache service unavailable | FUSE falls back to direct remote access |
| GetPage returns MISS | FUSE reads from remote, then PutPage |
| PutPage returns NO_SPACE | Ignore; data is still available from remote |
| PutPage returns ALREADY_EXISTS | Normal dedup; ignore |
| Release fails | Retry once, then rely on lease timeout |
| pread returns error | Lease may have expired; re-issue GetPage |

## 13. Rollout Phases

### Phase 1 (MVP)
- Single-machine cache service, gRPC over UDS
- PutPage / GetPage / Release
- Simple LRU eviction
- Fixed page size (e.g., 64K only)

### Phase 2
- Slab allocator with multi-size pages (2K–2MB)
- W-TinyLFU eviction
- Batch GetPage/Release
- Lease timeout and crash recovery

### Phase 3 (Optional)
- Cache warm-up from previous session
- Monitoring: hit rate, latency distribution, space utilization
- Prefetch hints for sequential reads
- gRPC over TCP for cross-machine cache sharing

## 14. Open Questions

1. **Concurrent PutPage dedup**: Multiple FUSE processes may miss the same page simultaneously. The cache service should deduplicate by returning `ALREADY_EXISTS` for the second PutPage.
2. **Sequential read prefetch**: Should the cache service support prefetch hints to pre-load adjacent pages?
3. **Disk selection**: Local NVMe SSD is recommended; HDD random read latency may negate caching benefits.
4. **O_DIRECT vs buffered I/O**: O_DIRECT avoids double-caching but requires buffer alignment and may hurt small I/O. Needs benchmarking.
5. **FUSE fd management**: FUSE processes need to `pread` chunk files. An fd cache pool with LRU eviction is recommended over open/close per read.
