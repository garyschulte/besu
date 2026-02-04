/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.plugin.services.storage.postgresql.core;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;

import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;

/** PostgreSQL snapshot implementation providing point-in-time view of storage. */
public class PostgreSQLSnapshot implements SnappedKeyValueStorage {
  private final PostgreSQLColumnarKeyValueStorage parentStorage;
  private final long blockNumber;

  /**
   * Instantiates a new PostgreSQL snapshot.
   *
   * @param parentStorage the parent storage
   * @param blockNumber the block number for this snapshot
   */
  public PostgreSQLSnapshot(
      final PostgreSQLColumnarKeyValueStorage parentStorage, final long blockNumber) {
    this.parentStorage = parentStorage;
    this.blockNumber = blockNumber;
  }

  /**
   * Gets the block number for this snapshot.
   *
   * @return the block number
   */
  public long getBlockNumber() {
    return blockNumber;
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    return parentStorage.getAtBlock(segment, key, blockNumber);
  }

  @Override
  public Optional<NearestKeyValue> getNearestBefore(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {
    // For snapshots, we could implement historical nearest-key queries
    // For now, delegate to current implementation (could be enhanced)
    return parentStorage.getNearestBefore(segmentIdentifier, key);
  }

  @Override
  public Optional<NearestKeyValue> getNearestAfter(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {
    // For snapshots, we could implement historical nearest-key queries
    // For now, delegate to current implementation (could be enhanced)
    return parentStorage.getNearestAfter(segmentIdentifier, key);
  }

  @Override
  public SegmentedKeyValueStorageTransaction getSnapshotTransaction() {
    // Snapshots are read-only, so return a read-only transaction
    return new ReadOnlyTransaction();
  }

  @Override
  public SegmentedKeyValueStorageTransaction startTransaction() throws StorageException {
    // Snapshots are read-only
    return new ReadOnlyTransaction();
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segmentIdentifier) {
    return parentStorage.streamAtBlock(segmentIdentifier, blockNumber);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey) {
    // Filter the stream at block to only include keys >= startKey
    return parentStorage
        .streamAtBlock(segmentIdentifier, blockNumber)
        .filter(pair -> compareBytes(pair.getKey(), startKey) >= 0);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey, final byte[] endKey) {
    // Filter the stream at block to only include keys in range [startKey, endKey)
    return parentStorage
        .streamAtBlock(segmentIdentifier, blockNumber)
        .filter(
            pair ->
                compareBytes(pair.getKey(), startKey) >= 0
                    && compareBytes(pair.getKey(), endKey) < 0);
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segmentIdentifier) {
    return parentStorage.streamAtBlock(segmentIdentifier, blockNumber).map(Pair::getKey);
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segmentIdentifier, final byte[] key)
      throws StorageException {
    // Snapshots are read-only
    throw new UnsupportedOperationException("Snapshots are read-only");
  }

  @Override
  public Set<byte[]> getAllKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return parentStorage.getAllKeysThat(segmentIdentifier, returnCondition);
  }

  @Override
  public Set<byte[]> getAllValuesFromKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return parentStorage.getAllValuesFromKeysThat(segmentIdentifier, returnCondition);
  }

  @Override
  public void clear(final SegmentIdentifier segmentIdentifier) {
    // Snapshots are read-only
    throw new UnsupportedOperationException("Snapshots are read-only");
  }

  @Override
  public boolean isClosed() {
    return parentStorage.isClosed();
  }

  @Override
  public void close() {
    // Snapshots don't own the parent storage, so don't close it
  }

  /**
   * Compare two byte arrays lexicographically.
   *
   * @param a first array
   * @param b second array
   * @return comparison result
   */
  private int compareBytes(final byte[] a, final byte[] b) {
    final int minLength = Math.min(a.length, b.length);
    for (int i = 0; i < minLength; i++) {
      final int cmp = Byte.compareUnsigned(a[i], b[i]);
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(a.length, b.length);
  }

  /** Read-only transaction that throws on modification attempts. */
  private static class ReadOnlyTransaction implements SegmentedKeyValueStorageTransaction {
    @Override
    public void put(
        final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {
      throw new UnsupportedOperationException("Snapshot transactions are read-only");
    }

    @Override
    public void remove(final SegmentIdentifier segmentIdentifier, final byte[] key) {
      throw new UnsupportedOperationException("Snapshot transactions are read-only");
    }

    @Override
    public void commit() throws StorageException {
      // No-op for read-only transaction
    }

    @Override
    public void rollback() {
      // No-op for read-only transaction
    }

    @Override
    public void close() {
      // No-op for read-only transaction
    }
  }
}
