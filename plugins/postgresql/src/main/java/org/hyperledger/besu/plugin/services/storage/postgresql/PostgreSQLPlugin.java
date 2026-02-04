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
package org.hyperledger.besu.plugin.services.storage.postgresql;

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.StorageService;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.postgresql.configuration.PostgreSQLCLIOptions;
import org.hyperledger.besu.plugin.services.storage.postgresql.configuration.PostgreSQLFactoryConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.google.auto.service.AutoService;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** PostgreSQL storage plugin for Besu. */
@AutoService(BesuPlugin.class)
public class PostgreSQLPlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLPlugin.class);
  private static final String NAME = "postgresql";

  private final PostgreSQLCLIOptions options;
  private ServiceManager context;
  private PostgreSQLKeyValueStorageFactory factory;

  /** Instantiates a new PostgreSQL plugin. */
  public PostgreSQLPlugin() {
    this.options = PostgreSQLCLIOptions.create();
  }

  @Override
  public void register(final ServiceManager context) {
    LOG.debug("Registering PostgreSQL plugin");
    this.context = context;

    final Optional<PicoCLIOptions> cmdlineOptions = context.getService(PicoCLIOptions.class);

    if (cmdlineOptions.isEmpty()) {
      throw new IllegalStateException(
          "Expecting a PicoCLI options to register CLI options with, but none found.");
    }

    cmdlineOptions.get().addPicoCLIOptions(NAME, options);
    createFactoriesAndRegisterWithStorageService();

    LOG.debug("PostgreSQL plugin registered.");
  }

  @Override
  public void start() {
    LOG.debug("Starting PostgreSQL plugin.");
    if (factory == null) {
      LOG.trace("Applied configuration: {}", options.toString());
      createFactoriesAndRegisterWithStorageService();
    }
  }

  @Override
  public void stop() {
    LOG.debug("Stopping PostgreSQL plugin.");

    try {
      if (factory != null) {
        factory.close();
        factory = null;
      }
    } catch (final IOException e) {
      LOG.error("Failed to stop PostgreSQL plugin: {}", e.getMessage(), e);
    }
  }

  /**
   * Gets the CLI options.
   *
   * @return the CLI options
   */
  public PostgreSQLCLIOptions getOptions() {
    return options;
  }

  private void createAndRegister(final StorageService service) {
    final List<SegmentIdentifier> segments = service.getAllSegmentIdentifiers();

    final Supplier<PostgreSQLFactoryConfiguration> configuration =
        Suppliers.memoize(options::toDomainObject);

    factory = new PostgreSQLKeyValueStorageFactory(configuration, segments);

    service.registerKeyValueStorage(factory);

    LOG.info("PostgreSQL storage factory registered with {} segments", segments.size());
  }

  private void createFactoriesAndRegisterWithStorageService() {
    context
        .getService(StorageService.class)
        .ifPresentOrElse(
            this::createAndRegister,
            () ->
                LOG.error(
                    "Failed to register PostgreSQL KeyValueFactory due to missing StorageService."));
  }
}
