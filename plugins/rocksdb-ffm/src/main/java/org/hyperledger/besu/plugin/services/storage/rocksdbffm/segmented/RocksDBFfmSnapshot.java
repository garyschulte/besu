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
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.github.dfa1.rocksdbffm.ColumnFamilyHandle;
import io.github.dfa1.rocksdbffm.OptimisticTransactionDB;
import io.github.dfa1.rocksdbffm.ReadOptions;
import io.github.dfa1.rocksdbffm.Snapshot;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A point-in-time snapshot view of a {@link RocksDBFfmColumnarKeyValueStorage}. Reads are served
 * at the snapshot sequence number; writes are discarded (no-op transaction).
 */
public class RocksDBFfmSnapshot implements SegmentedKeyValueStorage, SnappedKeyValueStorage {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBFfmSnapshot.class);

  private final OptimisticTransactionDB db;
  private final Function<SegmentIdentifier, ColumnFamilyHandle> cfMapper;
  private final Snapshot snapshot;
  private final ReadOptions readOptions;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Creates a new snapshot view.
   *
   * @param db the underlying optimistic transaction DB
   * @param cfMapper maps a {@link SegmentIdentifier} to its {@link ColumnFamilyHandle}
   */
  public RocksDBFfmSnapshot(
      final OptimisticTransactionDB db,
      final Function<SegmentIdentifier, ColumnFamilyHandle> cfMapper) {
    this.db = db;
    this.cfMapper = cfMapper;
    this.snapshot = db.getSnapshot();
    this.readOptions = ReadOptions.newReadOptions().setSnapshot(snapshot);
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    throwIfClosed();
    final byte[] result = db.get(cfMapper.apply(segment), readOptions, key);
    return Optional.ofNullable(result);
  }

  @Override
  public <T> Optional<T> getWithReader(
      final SegmentIdentifier segment,
      final byte[] key,
      final Function<MemorySegment, T> reader)
      throws StorageException {
    throwIfClosed();
    return db.withPinnedValue(cfMapper.apply(segment), readOptions, key, reader);
  }

  @Override
  public Optional<NearestKeyValue> getNearestBefore(
      final SegmentIdentifier segment, final Bytes key) throws StorageException {
    throwIfClosed();
    try (var iter = db.newIterator(cfMapper.apply(segment), readOptions)) {
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
    try (var iter = db.newIterator(cfMapper.apply(segment), readOptions)) {
      iter.seek(key.toArrayUnsafe());
      if (!iter.isValid()) {
        return Optional.empty();
      }
      return Optional.of(new NearestKeyValue(Bytes.of(iter.key()), Optional.of(iter.value())));
    }
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segment) {
    throwIfClosed();
    final var iter = db.newIterator(cfMapper.apply(segment), readOptions);
    iter.seekToFirst();
    return RocksDBFfmIterator.create(iter).toStream();
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segment, final byte[] startKey) {
    throwIfClosed();
    final var iter = db.newIterator(cfMapper.apply(segment), readOptions);
    iter.seek(startKey);
    return RocksDBFfmIterator.create(iter).toStream();
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segment, final byte[] startKey, final byte[] endKey) {
    throwIfClosed();
    final Bytes endKeyBytes = Bytes.wrap(endKey);
    final var iter = db.newIterator(cfMapper.apply(segment), readOptions);
    iter.seek(startKey);
    return RocksDBFfmIterator.create(iter)
        .toStream()
        .takeWhile(e -> endKeyBytes.compareTo(Bytes.wrap(e.getKey())) >= 0);
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segment) {
    throwIfClosed();
    final var iter = db.newIterator(cfMapper.apply(segment), readOptions);
    iter.seekToFirst();
    return RocksDBFfmIterator.create(iter).toStreamKeys();
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    throw new StorageException("delete is unsupported in snapshots");
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
  public SegmentedKeyValueStorageTransaction startTransaction() throws StorageException {
    return NO_OP_TX;
  }

  @Override
  public SegmentedKeyValueStorageTransaction getSnapshotTransaction() {
    return NO_OP_TX;
  }

  @Override
  public void clear(final SegmentIdentifier segment) {
    throw new UnsupportedOperationException("RocksDBFfmSnapshot does not support clear");
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      readOptions.close();
      snapshot.close();
    }
  }

  private void throwIfClosed() {
    if (closed.get()) {
      LOG.error("Attempting to use a closed RocksDBFfmSnapshot");
      throw new IllegalStateException("Snapshot has been closed");
    }
  }

  private static final SegmentedKeyValueStorageTransaction NO_OP_TX =
      new SegmentedKeyValueStorageTransaction() {
        @Override
        public void put(
            final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {}

        @Override
        public void remove(final SegmentIdentifier segmentIdentifier, final byte[] key) {}

        @Override
        public void commit() throws StorageException {}

        @Override
        public void rollback() {}

        @Override
        public void close() {}
      };
}
