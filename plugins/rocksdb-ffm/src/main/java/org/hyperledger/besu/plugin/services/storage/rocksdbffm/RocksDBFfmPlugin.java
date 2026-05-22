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

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.StorageService;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions;

import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Besu plugin that registers the rocksdbffm-backed storage factory under the name
 * {@value RocksDBFfmKeyValueStorageFactory#NAME}. Activate with
 * {@code --key-value-storage=rocksdb-ffm}.
 */
public class RocksDBFfmPlugin implements BesuPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBFfmPlugin.class);

  private ServiceManager context;
  private RocksDBFfmKeyValueStorageFactory factory;

  @Override
  public void register(final ServiceManager context) {
    LOG.debug("Registering rocksdb-ffm plugin");
    this.context = context;
    createAndRegisterFactory();
    LOG.debug("rocksdb-ffm plugin registered");
  }

  @Override
  public void start() {
    if (factory == null) {
      createAndRegisterFactory();
    }
  }

  @Override
  public void stop() {
    if (factory != null) {
      try {
        factory.close();
      } catch (final IOException e) {
        LOG.error("Failed to stop rocksdb-ffm plugin: {}", e.getMessage(), e);
      }
      factory = null;
    }
  }

  private void createAndRegisterFactory() {
    final Optional<StorageService> storageService = context.getService(StorageService.class);
    storageService.ifPresentOrElse(
        service -> {
          factory =
              new RocksDBFfmKeyValueStorageFactory(
                  service.getAllSegmentIdentifiers(),
                  () -> RocksDBCLIOptions.create().toDomainObject());
          service.registerKeyValueStorage(factory);
        },
        () -> LOG.error("Failed to register rocksdb-ffm factory: StorageService not found"));
  }
}
