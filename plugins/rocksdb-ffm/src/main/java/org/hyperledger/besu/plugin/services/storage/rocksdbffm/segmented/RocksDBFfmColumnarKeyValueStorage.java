/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.plugin.services.storage.rocksdbffm.segmented;

import static java.util.stream.Collectors.toUnmodifiableSet;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.github.dfa1.rocksdbffm.BlockBasedTableOptions;
import io.github.dfa1.rocksdbffm.ColumnFamilyDescriptor;
import io.github.dfa1.rocksdbffm.ColumnFamilyHandle;
import io.github.dfa1.rocksdbffm.CompressionType;
import io.github.dfa1.rocksdbffm.FilterPolicy;
import io.github.dfa1.rocksdbffm.LRUCache;
import io.github.dfa1.rocksdbffm.MemorySize;
import io.github.dfa1.rocksdbffm.OptimisticTransactionDB;
import io.github.dfa1.rocksdbffm.Options;
import io.github.dfa1.rocksdbffm.ReadOptions;
import io.github.dfa1.rocksdbffm.RocksDB;
import io.github.dfa1.rocksdbffm.WriteOptions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageTransactionValidatorDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * rocksdbffm-backed implementation of {@link SegmentedKeyValueStorage}. Uses
 * {@link OptimisticTransactionDB} with one column family per {@link SegmentIdentifier}. The
 * {@link #getWithReader} override calls rocksdbffm's {@code withPinnedValue} to avoid materialising
 * the value as a heap {@code byte[]}.
 */
public class RocksDBFfmColumnarKeyValueStorage
    implements SegmentedKeyValueStorage, SnappableKeyValueStorage {

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBFfmColumnarKeyValueStorage.class);

  // Mirrors RocksDBColumnarKeyValueStorage constants
  private static final int ROCKSDB_FORMAT_VERSION = 5;
  private static final long ROCKSDB_BLOCK_SIZE = 32768;
  private static final long ROCKSDB_BLOCKCACHE_SIZE_HIGH_SPEC = 1_073_741_824L;
  private static final long WAL_MAX_TOTAL_SIZE = 1_073_741_824L;
  // Must be >= write_buffer_size so OptimisticTransactionDB conflict detection
  // always has history back to any live transaction's start sequence number.
  private static final long WRITE_BUFFER_SIZE_TO_MAINTAIN = 67_108_864L; // 64 MiB

  private final OptimisticTransactionDB db;
  private final Map<SegmentIdentifier, ColumnFamilyHandle> cfHandles;
  private final ReadOptions defaultReadOptions;
  private final WriteOptions defaultWriteOptions;
  private final List<LRUCache> blockCaches = new ArrayList<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Opens (or creates) the database at the given path, ensuring a column family exists for every
   * requested segment. RocksDB is configured with a per-segment bounded block cache and WAL cap
   * drawn from {@code rocksDBConfig} so that native memory usage stays within predictable limits.
   *
   * @param dbPath path to the RocksDB directory
   * @param segments segment identifiers to expose; must include the DEFAULT segment
   * @param rocksDBConfig RocksDB tuning parameters (cache capacity, max open files, etc.)
   * @throws StorageException if the database cannot be opened
   */
  public RocksDBFfmColumnarKeyValueStorage(
      final Path dbPath,
      final List<SegmentIdentifier> segments,
      final RocksDBFactoryConfiguration rocksDBConfig)
      throws StorageException {
    try (Options opts =
        Options.newOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setMaxOpenFiles(rocksDBConfig.getMaxOpenFiles())
            .setMaxTotalWalSize(WAL_MAX_TOTAL_SIZE)) {
      this.cfHandles = new HashMap<>();
      this.db = openWithSegments(opts, dbPath, segments, cfHandles, rocksDBConfig);
      this.defaultReadOptions = ReadOptions.newReadOptions();
      this.defaultWriteOptions = WriteOptions.newWriteOptions();
    } catch (final io.github.dfa1.rocksdbffm.RocksDBException e) {
      throw new StorageException(e);
    }
  }

  private BlockBasedTableOptions createTableOptions(
      final SegmentIdentifier segment, final RocksDBFactoryConfiguration config) {
    final long cacheSize =
        config.isHighSpec() && segment.isEligibleToHighSpecFlag()
            ? ROCKSDB_BLOCKCACHE_SIZE_HIGH_SPEC
            : config.getCacheCapacity();
    final LRUCache cache = LRUCache.newLRUCache(MemorySize.ofBytes(cacheSize));
    blockCaches.add(cache);
    // FilterPolicy ownership transfers to BlockBasedTableOptions on setFilterPolicy; do not close it.
    return BlockBasedTableOptions.newBlockBasedConfig()
        .setFormatVersion(ROCKSDB_FORMAT_VERSION)
        .setBlockCache(cache)
        .setFilterPolicy(FilterPolicy.newBloom(10))
        .setPartitionFilters(true)
        .setCacheIndexAndFilterBlocks(segment.isCacheIndexAndFilterBlocks())
        .setBlockSize(MemorySize.ofBytes(ROCKSDB_BLOCK_SIZE));
  }

  private Options createCfOptions(
      final SegmentIdentifier segment, final RocksDBFactoryConfiguration config) {
    final Options cfOpts = Options.newOptions().setCreateIfMissing(true);
    try (BlockBasedTableOptions tableOpts = createTableOptions(segment, config)) {
      cfOpts.setTableFormatConfig(tableOpts);
    }
    cfOpts.setCompression(CompressionType.LZ4);
    cfOpts.setLevelCompactionDynamicLevelBytes(true);
    cfOpts.setMaxWriteBufferSizeToMaintain(WRITE_BUFFER_SIZE_TO_MAINTAIN);
    return cfOpts;
  }

  /**
   * Opens the DB with all existing column families, then creates any that are missing from the
   * requested segments list. Per-segment {@link BlockBasedTableOptions} (with a bounded
   * {@link LRUCache}) are applied to each column family descriptor before open.
   */
  private OptimisticTransactionDB openWithSegments(
      final Options opts,
      final Path dbPath,
      final List<SegmentIdentifier> requestedSegments,
      final Map<SegmentIdentifier, ColumnFamilyHandle> cfMap,
      final RocksDBFactoryConfiguration rocksDBConfig) {

    // Build lookup: CF name → segment (for requested segments only)
    final Map<String, SegmentIdentifier> segsByName = new HashMap<>();
    for (final SegmentIdentifier seg : requestedSegments) {
      segsByName.put(new String(seg.getId(), StandardCharsets.UTF_8), seg);
    }

    // Find CFs already present in the DB (empty list for a fresh DB or non-existent path)
    List<byte[]> existingCfBytes;
    try {
      existingCfBytes = RocksDB.listColumnFamilies(opts, dbPath);
    } catch (final io.github.dfa1.rocksdbffm.RocksDBException ignored) {
      existingCfBytes = List.of();
    }

    // Build the descriptor list for the open call — must include every CF that exists in the DB.
    // For a fresh (empty) DB we seed with just "default".
    // For known segments, attach per-CF Options with a bounded block cache.
    final List<ColumnFamilyDescriptor> openDescriptors = new ArrayList<>();
    final List<Options> perCfOptions = new ArrayList<>(); // closed after open
    if (existingCfBytes.isEmpty()) {
      openDescriptors.add(ColumnFamilyDescriptor.of("default"));
      perCfOptions.add(null);
    } else {
      for (final byte[] cfName : existingCfBytes) {
        final String name = new String(cfName, StandardCharsets.UTF_8);
        final SegmentIdentifier seg = segsByName.get(name);
        if (seg != null) {
          final Options cfOpts = createCfOptions(seg, rocksDBConfig);
          openDescriptors.add(ColumnFamilyDescriptor.of(cfName, cfOpts));
          perCfOptions.add(cfOpts);
        } else {
          openDescriptors.add(ColumnFamilyDescriptor.of(cfName));
          perCfOptions.add(null);
        }
      }
    }

    // Open the DB
    final List<ColumnFamilyHandle> openHandles = new ArrayList<>();
    final OptimisticTransactionDB txDb =
        RocksDB.openOptimisticWithColumnFamilies(opts, dbPath, openDescriptors, openHandles);

    // Per-CF Options are no longer needed once the DB is open; RocksDB copied what it needed.
    for (final Options cfOpts : perCfOptions) {
      if (cfOpts != null) {
        cfOpts.close();
      }
    }

    // Map CF name → handle
    final Map<String, ColumnFamilyHandle> handlesByName = new HashMap<>();
    for (int i = 0; i < openDescriptors.size(); i++) {
      handlesByName.put(
          new String(openDescriptors.get(i).name(), StandardCharsets.UTF_8), openHandles.get(i));
    }

    // Wire segment → handle; create any CFs that don't exist yet (new segments added to a DB)
    for (final SegmentIdentifier seg : requestedSegments) {
      final String name = new String(seg.getId(), StandardCharsets.UTF_8);
      final ColumnFamilyHandle handle = handlesByName.get(name);
      if (handle != null) {
        cfMap.put(seg, handle);
      } else {
        final Options newCfOpts = createCfOptions(seg, rocksDBConfig);
        cfMap.put(seg, txDb.createColumnFamily(ColumnFamilyDescriptor.of(seg.getId(), newCfOpts)));
        newCfOpts.close();
      }
    }

    // Close handles for CFs that exist in the DB but weren't in the requested segments list
    for (final Map.Entry<String, ColumnFamilyHandle> entry : handlesByName.entrySet()) {
      if (!segsByName.containsKey(entry.getKey())) {
        try {
          entry.getValue().close();
        } catch (Exception ignored) {
          LOG.warn("Failed to close unused CF handle for {}", entry.getKey());
        }
      }
    }

    return txDb;
  }

  private ColumnFamilyHandle cfHandle(final SegmentIdentifier segment) {
    final ColumnFamilyHandle handle = cfHandles.get(segment);
    if (handle == null) {
      throw new IllegalArgumentException("Unknown segment: " + segment.getName());
    }
    return handle;
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    throwIfClosed();
    final byte[] result = db.get(cfHandle(segment), defaultReadOptions, key);
    return Optional.ofNullable(result);
  }

  @Override
  public <T> Optional<T> getWithReader(
      final SegmentIdentifier segment,
      final byte[] key,
      final Function<MemorySegment, T> reader)
      throws StorageException {
    throwIfClosed();
    return db.withPinnedValue(cfHandle(segment), defaultReadOptions, key, reader);
  }

  @Override
  public Optional<NearestKeyValue> getNearestBefore(
      final SegmentIdentifier segment, final Bytes key) throws StorageException {
    throwIfClosed();
    try (var iter = db.newIterator(cfHandle(segment), defaultReadOptions)) {
      iter.seekForPrev(key.toArrayUnsafe());
      if (!iter.isValid()) {
        return Optional.empty();
      }
      return Optional.of(new NearestKeyValue(Bytes.of(iter.key()), Optional.of(iter.value())));
    }
  }

  @Override
  public Optional<NearestKeyValue> getNearestAfter(
      final SegmentIdentifier segment, final Bytes key) throws StorageException {
    throwIfClosed();
    try (var iter = db.newIterator(cfHandle(segment), defaultReadOptions)) {
      iter.seek(key.toArrayUnsafe());
      if (!iter.isValid()) {
        return Optional.empty();
      }
      return Optional.of(new NearestKeyValue(Bytes.of(iter.key()), Optional.of(iter.value())));
    }
  }

  @Override
  public SegmentedKeyValueStorageTransaction startTransaction() throws StorageException {
    throwIfClosed();
    return new SegmentedKeyValueStorageTransactionValidatorDecorator(
        new RocksDBFfmTransaction(this::cfHandle, db.beginTransaction(defaultWriteOptions)),
        this.closed::get);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segment) {
    throwIfClosed();
    final var iter = db.newIterator(cfHandle(segment), defaultReadOptions);
    iter.seekToFirst();
    return RocksDBFfmIterator.create(iter).toStream();
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segment, final byte[] startKey) {
    throwIfClosed();
    final var iter = db.newIterator(cfHandle(segment), defaultReadOptions);
    iter.seek(startKey);
    return RocksDBFfmIterator.create(iter).toStream();
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segment, final byte[] startKey, final byte[] endKey) {
    throwIfClosed();
    final Bytes endKeyBytes = Bytes.wrap(endKey);
    final var iter = db.newIterator(cfHandle(segment), defaultReadOptions);
    iter.seek(startKey);
    return RocksDBFfmIterator.create(iter)
        .toStream()
        .takeWhile(e -> endKeyBytes.compareTo(Bytes.wrap(e.getKey())) >= 0);
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segment) {
    throwIfClosed();
    final var iter = db.newIterator(cfHandle(segment), defaultReadOptions);
    iter.seekToFirst();
    return RocksDBFfmIterator.create(iter).toStreamKeys();
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    throwIfClosed();
    try {
      db.delete(cfHandle(segment), key);
      return true;
    } catch (final io.github.dfa1.rocksdbffm.RocksDBException e) {
      return false;
    }
  }

  @Override
  public Set<byte[]> getAllKeysThat(
      final SegmentIdentifier segment, final Predicate<byte[]> returnCondition) {
    return streamKeys(segment).filter(returnCondition).collect(toUnmodifiableSet());
  }

  @Override
  public Set<byte[]> getAllValuesFromKeysThat(
      final SegmentIdentifier segment, final Predicate<byte[]> returnCondition) {
    return stream(segment)
        .filter(pair -> returnCondition.test(pair.getKey()))
        .map(Pair::getValue)
        .collect(toUnmodifiableSet());
  }

  @Override
  public void clear(final SegmentIdentifier segment) {
    throwIfClosed();
    try (var iter = db.newIterator(cfHandle(segment), defaultReadOptions)) {
      iter.seekToFirst();
      while (iter.isValid()) {
        db.delete(cfHandle(segment), iter.key());
        iter.next();
      }
    }
  }

  @Override
  public SnappedKeyValueStorage takeSnapshot() throws StorageException {
    throwIfClosed();
    return new RocksDBFfmSnapshot(db, this::cfHandle);
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      defaultReadOptions.close();
      defaultWriteOptions.close();
      for (final ColumnFamilyHandle handle : cfHandles.values()) {
        try {
          handle.close();
        } catch (Exception e) {
          LOG.warn("Error closing CF handle", e);
        }
      }
      db.close();
      blockCaches.forEach(LRUCache::close);
    }
  }

  private void throwIfClosed() {
    if (closed.get()) {
      LOG.error("Attempting to use a closed RocksDBFfmColumnarKeyValueStorage");
      throw new IllegalStateException("Storage has been closed");
    }
  }
}
