# IPv4 Unique Counter

A high-performance IPv4 address counter for very large files.  
Optimized for **speed** and **low memory usage**.

---

## Optimizations Used

- **`mmap`** — memory-mapped file access for zero-copy reading.
- **Bitmap bitset (512 MB)** — one bit per IPv4 address (`uint32 → bit index`).
- **Sharding** — splits the bitset into independent segments to avoid lock contention.
- **Batch processing** — collects parsed IPs in buffers before updating the bitset.
- **`sync.Pool` reuse** — avoids allocations for batches.
- **Write-combining cache** — minimizes writes to the bitset.
- **`madvise` hints** (`SEQUENTIAL` / `RANDOM`) — optimizes OS page cache usage.

---

## Processing Modes

### **Full mmap mode** (`CountUnique`)

- Maps the entire file into memory at once.
- Fastest possible processing speed.
- Requires enough **virtual memory** to map the whole file.

### **Chunked mmap mode** (`CountUniqueChunked`)

- Maps the file in fixed-size windows (e.g., 1 GB).
- Slightly slower than full mode.
- Much lower peak **RSS** (resident set size), suitable for very large files.

---

## Trade-offs

| Mode          | Speed          | Memory usage | When to use                   |
| ------------- | -------------- | ------------ | ----------------------------- |
| **Full mmap** | Fastest        | Higher       | File fits in available memory |
| **Chunked**   | Slightly lower | Lower        | Files too large to map fully  |

---

## Usage

### Full mode (max performance)

```bash
go run ./cmd/main.go \
    -mode=full \
    -shards=256 \
    -batch=4096 \
    -chanbuf=64 \
    ./static/ip_addresses
```

### Chunked mode (low memory)

```bash
go run ./cmd/main.go \
    -mode=full \
    -shards=256 \
    -batch=4096 \
    -chanbuf=64 \
    ./static/ip_addresses
```

| Flag       | Description                                    |
| ---------- | ---------------------------------------------- |
| `-mode`    | `full` or `chunked`                            |
| `-shards`  | Number of bitmap shards for parallel writes    |
| `-batch`   | Per-parser batch size before flushing to shard |
| `-chanbuf` | Channel buffer size per shard                  |
| `-window`  | Chunk size (MB) for `chunked` mode             |

## Example Output

```bash
7,501,063,486 lines | 7,501,063,484 valid | 0 invalid | 51,581,321 lines/sec | 145s elapsed
mem: heap_alloc=640.9 MB heap_sys=689.5 MB gc=5 | rss=14.2 GB | bitset=512.0 MB | mmap=106.6 GB
```
