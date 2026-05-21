# Baseline JMH Results — rocksdbjni (2026-05-20)

Plugin: `plugins:rocksdb` / `OptimisticRocksDBColumnarKeyValueStorage`
RocksDB JNI version: 10.6.2
Corpus: 100K entries, 32-byte keys, 128-byte values, `TRIE_BRANCH_STORAGE` CF
Cache: 256 MB LRU block cache (all 100K values fit in cache ~12 MB)
JVM: OpenJDK 25.0.3, fork=3, warmup=3×2s, measure=5×2s

## Throughput (ops/ms)

| Benchmark | Threads | ops/ms | ± 99.9% | Alloc B/op |
|---|---|---|---|---|
| getHotSingleThread | 1 | 1,005 | ± 89 | 160 |
| getHotMultiThread | 8 | 3,964 | ± 252 | 160 |
| getColdSingleThread | 1 | 689 | ± 40 | 160 |
| getColdMultiThread | 8 | 2,897 | ± 121 | 160 |
| getNearestBefore | 1 | 382 | ± 19 | 296 |
| batchWrite (100 puts/op) | 1 | 2.98 | ± 0.30 | 19,278 |

## Latency (SampleTime)

| Benchmark | Threads | mean (ms/op) |
|---|---|---|
| getHotSingleThread | 1 | 0.001 |
| getColdSingleThread | 1 | 0.002 |
| getNearestBefore | 1 | 0.002 |
| batchWrite | 1 | 0.321 |

## Notes

- All point-reads allocate exactly **160 B/op** (128-byte value + ~32 bytes for Optional wrapper and byte[] header). This is the target for reduction via the scoped-reader path.
- `getNearestBefore` allocates 296 B/op (value + key copy for NearestKeyValue record).
- `batchWrite` allocates ~19 KB/op dominated by key/value byte[] generation inside the benchmark loop.
- Single-thread hot read ceiling is ~1 million ops/sec. Multi-thread scales to ~4× at 8 threads (sub-linear due to mutex contention on RocksDB read path and JNI thread state overhead).
