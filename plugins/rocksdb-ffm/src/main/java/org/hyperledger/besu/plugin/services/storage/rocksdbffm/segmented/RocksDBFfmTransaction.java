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
 */
package org.hyperledger.besu.plugin.services.storage.rocksdbffm.segmented;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetrics;

import java.util.function.Function;

import io.github.dfa1.rocksdbffm.ColumnFamilyHandle;
import io.github.dfa1.rocksdbffm.Transaction;
import io.github.dfa1.rocksdbffm.WriteOptions;

/**
 * Adapts a rocksdbffm {@link Transaction} to Besu's {@link SegmentedKeyValueStorageTransaction}.
 */
public class RocksDBFfmTransaction implements SegmentedKeyValueStorageTransaction {

  private final Function<SegmentIdentifier, ColumnFamilyHandle> cfMapper;
  private final Transaction txn;
  private final WriteOptions ownedWriteOptions;
  private final RocksDBMetrics metrics;

  /**
   * Creates a new transaction wrapper with shared write options (not closed on transaction end).
   *
   * @param cfMapper maps a {@link SegmentIdentifier} to its {@link ColumnFamilyHandle}
   * @param txn the underlying rocksdbffm transaction
   * @param metrics operation timers and counters for this transaction
   */
  public RocksDBFfmTransaction(
      final Function<SegmentIdentifier, ColumnFamilyHandle> cfMapper,
      final Transaction txn,
      final RocksDBMetrics metrics) {
    this(cfMapper, txn, null, metrics);
  }

  /**
   * Creates a new transaction wrapper that takes ownership of the supplied write options and closes
   * them when the transaction ends. Used for per-transaction options such as low-priority writes.
   *
   * @param cfMapper maps a {@link SegmentIdentifier} to its {@link ColumnFamilyHandle}
   * @param txn the underlying rocksdbffm transaction
   * @param ownedWriteOptions write options to close on transaction end, or {@code null} if shared
   * @param metrics operation timers and counters for this transaction
   */
  public RocksDBFfmTransaction(
      final Function<SegmentIdentifier, ColumnFamilyHandle> cfMapper,
      final Transaction txn,
      final WriteOptions ownedWriteOptions,
      final RocksDBMetrics metrics) {
    this.cfMapper = cfMapper;
    this.txn = txn;
    this.ownedWriteOptions = ownedWriteOptions;
    this.metrics = metrics;
  }

  @Override
  public void put(final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {
    try (var ignored = metrics.getWriteLatency().startTimer()) {
      txn.put(cfMapper.apply(segmentIdentifier), key, value);
    }
  }

  @Override
  public void remove(final SegmentIdentifier segmentIdentifier, final byte[] key) {
    try (var ignored = metrics.getRemoveLatency().startTimer()) {
      txn.delete(cfMapper.apply(segmentIdentifier), key);
    }
  }

  @Override
  public void commit() throws StorageException {
    try (var ignored = metrics.getCommitLatency().startTimer()) {
      try {
        txn.commit();
      } catch (final io.github.dfa1.rocksdbffm.RocksDBException e) {
        throw new StorageException(e);
      } finally {
        // rocksdbffm's NativeObject has no Cleaner; rocksdb_transaction_destroy must be
        // called explicitly. The decorator's close() guard throws after commit, so this
        // is the only reliable point to release the native transaction pointer.
        txn.close();
        if (ownedWriteOptions != null) {
          ownedWriteOptions.close();
        }
      }
    }
  }

  @Override
  public void rollback() {
    metrics.getRollbackCount().inc();
    txn.rollback();
  }

  @Override
  public void close() {
    txn.close();
    if (ownedWriteOptions != null) {
      ownedWriteOptions.close();
    }
  }
}
