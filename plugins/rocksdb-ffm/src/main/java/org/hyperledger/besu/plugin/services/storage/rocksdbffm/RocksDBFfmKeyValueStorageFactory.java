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
package org.hyperledger.besu.plugin.services.storage.rocksdbffm;

import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdbffm.segmented.RocksDBFfmColumnarKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for the rocksdbffm-backed storage plugin. Creates a single
 * {@link RocksDBFfmColumnarKeyValueStorage} per data directory.
 */
public class RocksDBFfmKeyValueStorageFactory implements KeyValueStorageFactory {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBFfmKeyValueStorageFactory.class);
  static final String NAME = "rocksdb-ffm";

  private final List<SegmentIdentifier> configuredSegments;
  private RocksDBFfmColumnarKeyValueStorage segmentedStorage;

  /**
   * Creates a new factory.
   *
   * @param configuredSegments all segment identifiers that this storage should expose
   */
  public RocksDBFfmKeyValueStorageFactory(final List<SegmentIdentifier> configuredSegments) {
    this.configuredSegments = configuredSegments;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public KeyValueStorage create(
      final SegmentIdentifier segment,
      final BesuConfiguration commonConfiguration,
      final MetricsSystem metricsSystem)
      throws StorageException {
    return new SegmentedKeyValueStorageAdapter(
        segment, create(List.of(segment), commonConfiguration, metricsSystem));
  }

  @Override
  public SegmentedKeyValueStorage create(
      final List<SegmentIdentifier> segments,
      final BesuConfiguration commonConfiguration,
      final MetricsSystem metricsSystem)
      throws StorageException {
    if (segmentedStorage == null) {
      segmentedStorage =
          new RocksDBFfmColumnarKeyValueStorage(
              commonConfiguration.getStoragePath(), configuredSegments);
      LOG.info("Opened rocksdb-ffm storage at {}", commonConfiguration.getStoragePath());
    }
    return segmentedStorage;
  }

  @Override
  public boolean isSegmentIsolationSupported() {
    return true;
  }

  @Override
  public boolean isSnapshotIsolationSupported() {
    return true;
  }

  @Override
  public void close() throws IOException {
    if (segmentedStorage != null) {
      segmentedStorage.close();
      segmentedStorage = null;
    }
  }
}
