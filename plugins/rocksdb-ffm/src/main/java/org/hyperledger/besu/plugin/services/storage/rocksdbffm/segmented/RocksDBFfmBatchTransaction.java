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

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import io.github.dfa1.rocksdbffm.ColumnFamilyHandle;
import io.github.dfa1.rocksdbffm.OptimisticTransactionDB;
import io.github.dfa1.rocksdbffm.WriteBatch;

/**
 * A {@link SegmentedKeyValueStorageTransaction} that accumulates mutations on the Java heap and
 * submits them as a single {@link WriteBatch} at commit time.
 *
 * <p>Unlike {@link RocksDBFfmTransaction}, which opens an optimistic transaction and issues one
 * downcall per mutation, this implementation performs zero native calls during {@link #put} and
 * {@link #remove}. All downcalls are deferred to {@link #commit}, where a {@link WriteBatch} is
 * constructed and submitted via {@code rocksdb_write} on the underlying {@code rocksdb_t*} handle.
 *
 * <p>This bypasses the optimistic-transaction conflict-detection scan without changing the DB type:
 * {@link OptimisticTransactionDB} is retained, so snapshot reads remain available.
 *
 * <p>Trade-off: mutations are not visible to subsequent {@code db.get()} calls within the same
 * logical operation until {@link #commit} completes. Besu reads exclusively via
 * {@link org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage#get}, which reads
 * committed state, so this is safe for all current Besu storage consumers.
 */
public class RocksDBFfmBatchTransaction implements SegmentedKeyValueStorageTransaction {

  private static final byte DELETE = 0;
  private static final byte PUT = 1;

  private final Function<SegmentIdentifier, ColumnFamilyHandle> cfMapper;
  private final OptimisticTransactionDB db;
  private final RocksDBMetrics metrics;

  // Each entry is (type, cf, key, value-or-null).
  // Using parallel arrays rather than a list of records avoids object allocation per mutation.
  private final List<Byte> types = new ArrayList<>();
  private final List<ColumnFamilyHandle> cfs = new ArrayList<>();
  private final List<byte[]> keys = new ArrayList<>();
  private final List<byte[]> values = new ArrayList<>();

  /**
   * Creates a new batch transaction.
   *
   * @param cfMapper maps a {@link SegmentIdentifier} to its {@link ColumnFamilyHandle}
   * @param db the underlying database; {@code write(WriteBatch)} is called at commit
   * @param metrics operation timers and counters for this transaction
   */
  public RocksDBFfmBatchTransaction(
      final Function<SegmentIdentifier, ColumnFamilyHandle> cfMapper,
      final OptimisticTransactionDB db,
      final RocksDBMetrics metrics) {
    this.cfMapper = cfMapper;
    this.db = db;
    this.metrics = metrics;
  }

  @Override
  public void put(final SegmentIdentifier segment, final byte[] key, final byte[] value) {
    types.add(PUT);
    cfs.add(cfMapper.apply(segment));
    keys.add(key);
    values.add(value);
  }

  @Override
  public void remove(final SegmentIdentifier segment, final byte[] key) {
    types.add(DELETE);
    cfs.add(cfMapper.apply(segment));
    keys.add(key);
    values.add(null);
  }

  @Override
  public void commit() throws StorageException {
    try (var ignored = metrics.getCommitLatency().startTimer();
        Arena arena = Arena.ofConfined();
        WriteBatch batch = WriteBatch.create()) {
      for (int i = 0; i < types.size(); i++) {
        final var cf = cfs.get(i);
        final var k = arena.allocateFrom(ValueLayout.JAVA_BYTE, keys.get(i));
        if (types.get(i) == PUT) {
          batch.put(cf, k, arena.allocateFrom(ValueLayout.JAVA_BYTE, values.get(i)));
        } else {
          batch.delete(cf, k);
        }
      }
      try {
        db.write(batch);
      } catch (final io.github.dfa1.rocksdbffm.RocksDBException e) {
        throw new StorageException(e);
      }
    }
  }

  @Override
  public void rollback() {
    metrics.getRollbackCount().inc();
    types.clear();
    cfs.clear();
    keys.clear();
    values.clear();
  }

  @Override
  public void close() {
    // Nothing to release — no native transaction was opened.
  }
}
