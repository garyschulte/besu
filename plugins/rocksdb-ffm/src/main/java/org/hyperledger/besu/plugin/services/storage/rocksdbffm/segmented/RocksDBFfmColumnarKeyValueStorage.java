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

import io.github.dfa1.rocksdbffm.ColumnFamilyDescriptor;
import io.github.dfa1.rocksdbffm.ColumnFamilyHandle;
import io.github.dfa1.rocksdbffm.OptimisticTransactionDB;
import io.github.dfa1.rocksdbffm.Options;
import io.github.dfa1.rocksdbffm.ReadOptions;
import io.github.dfa1.rocksdbffm.RocksDB;
import io.github.dfa1.rocksdbffm.WriteOptions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
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

  private final OptimisticTransactionDB db;
  private final Map<SegmentIdentifier, ColumnFamilyHandle> cfHandles;
  private final ReadOptions defaultReadOptions;
  private final WriteOptions defaultWriteOptions;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Opens (or creates) the database at the given path, ensuring a column family exists for every
   * requested segment.
   *
   * @param dbPath path to the RocksDB directory
   * @param segments segment identifiers to expose; must include the DEFAULT segment
   * @throws StorageException if the database cannot be opened
   */
  public RocksDBFfmColumnarKeyValueStorage(
      final Path dbPath, final List<SegmentIdentifier> segments) throws StorageException {
    try (Options opts = Options.newOptions().setCreateIfMissing(true)) {
      this.cfHandles = new HashMap<>();
      this.db = openWithSegments(opts, dbPath, segments, cfHandles);
      this.defaultReadOptions = ReadOptions.newReadOptions();
      this.defaultWriteOptions = WriteOptions.newWriteOptions();
    } catch (final io.github.dfa1.rocksdbffm.RocksDBException e) {
      throw new StorageException(e);
    }
  }

  /**
   * Opens the DB with all existing column families, then creates any that are missing from the
   * requested segments list.
   */
  private static OptimisticTransactionDB openWithSegments(
      final Options opts,
      final Path dbPath,
      final List<SegmentIdentifier> requestedSegments,
      final Map<SegmentIdentifier, ColumnFamilyHandle> cfMap) {

    // Find CFs already present in the DB (empty list for a fresh DB or non-existent path)
    List<byte[]> existingCfBytes;
    try {
      existingCfBytes = RocksDB.listColumnFamilies(opts, dbPath);
    } catch (final io.github.dfa1.rocksdbffm.RocksDBException ignored) {
      existingCfBytes = List.of();
    }

    // Build the descriptor list for the open call — must include every CF that exists in the DB.
    // For a fresh (empty) DB we seed with just "default".
    List<ColumnFamilyDescriptor> openDescriptors = new ArrayList<>();
    if (existingCfBytes.isEmpty()) {
      openDescriptors.add(ColumnFamilyDescriptor.of("default"));
    } else {
      for (byte[] cfName : existingCfBytes) {
        openDescriptors.add(ColumnFamilyDescriptor.of(cfName));
      }
    }

    // Open the DB
    List<ColumnFamilyHandle> openHandles = new ArrayList<>();
    OptimisticTransactionDB txDb =
        RocksDB.openOptimisticWithColumnFamilies(opts, dbPath, openDescriptors, openHandles);

    // Map requested segments to the handles returned by the open call
    Map<String, ColumnFamilyHandle> handlesByName = new HashMap<>();
    for (int i = 0; i < openDescriptors.size(); i++) {
      handlesByName.put(
          new String(openDescriptors.get(i).name(), StandardCharsets.UTF_8), openHandles.get(i));
    }

    // Build segment → handle map; create any CFs that don't exist yet
    Map<String, SegmentIdentifier> segsByName = new HashMap<>();
    for (SegmentIdentifier seg : requestedSegments) {
      segsByName.put(new String(seg.getId(), StandardCharsets.UTF_8), seg);
    }

    for (SegmentIdentifier seg : requestedSegments) {
      String name = new String(seg.getId(), StandardCharsets.UTF_8);
      ColumnFamilyHandle handle = handlesByName.get(name);
      if (handle != null) {
        cfMap.put(seg, handle);
      } else {
        cfMap.put(seg, txDb.createColumnFamily(ColumnFamilyDescriptor.of(seg.getId())));
      }
    }

    // Close handles for CFs that exist in the DB but weren't in the requested segments list
    for (Map.Entry<String, ColumnFamilyHandle> entry : handlesByName.entrySet()) {
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
      for (ColumnFamilyHandle handle : cfHandles.values()) {
        try {
          handle.close();
        } catch (Exception e) {
          LOG.warn("Error closing CF handle", e);
        }
      }
      db.close();
    }
  }

  private void throwIfClosed() {
    if (closed.get()) {
      LOG.error("Attempting to use a closed RocksDBFfmColumnarKeyValueStorage");
      throw new IllegalStateException("Storage has been closed");
    }
  }
}
