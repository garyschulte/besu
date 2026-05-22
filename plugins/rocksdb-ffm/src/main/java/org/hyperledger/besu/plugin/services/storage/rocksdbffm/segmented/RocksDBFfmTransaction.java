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

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.function.Function;

import io.github.dfa1.rocksdbffm.ColumnFamilyHandle;
import io.github.dfa1.rocksdbffm.Transaction;

/** Adapts a rocksdbffm {@link Transaction} to Besu's {@link SegmentedKeyValueStorageTransaction}. */
public class RocksDBFfmTransaction implements SegmentedKeyValueStorageTransaction {

  private final Function<SegmentIdentifier, ColumnFamilyHandle> cfMapper;
  private final Transaction txn;

  /**
   * Creates a new transaction wrapper.
   *
   * @param cfMapper maps a {@link SegmentIdentifier} to its {@link ColumnFamilyHandle}
   * @param txn the underlying rocksdbffm transaction
   */
  public RocksDBFfmTransaction(
      final Function<SegmentIdentifier, ColumnFamilyHandle> cfMapper, final Transaction txn) {
    this.cfMapper = cfMapper;
    this.txn = txn;
  }

  @Override
  public void put(
      final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {
    txn.put(cfMapper.apply(segmentIdentifier), key, value);
  }

  @Override
  public void remove(final SegmentIdentifier segmentIdentifier, final byte[] key) {
    txn.delete(cfMapper.apply(segmentIdentifier), key);
  }

  @Override
  public void commit() throws StorageException {
    try {
      txn.commit();
    } catch (final io.github.dfa1.rocksdbffm.RocksDBException e) {
      throw new StorageException(e);
    } finally {
      // rocksdbffm's NativeObject has no Cleaner; rocksdb_transaction_destroy must be
      // called explicitly. The decorator's close() guard throws after commit, so this
      // is the only reliable point to release the native transaction pointer.
      txn.close();
    }
  }

  @Override
  public void rollback() {
    txn.rollback();
  }

  @Override
  public void close() {
    txn.close();
  }
}
