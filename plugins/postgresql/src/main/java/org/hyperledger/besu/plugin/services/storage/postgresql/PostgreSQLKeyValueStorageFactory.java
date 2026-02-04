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

import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.postgresql.configuration.DatabaseMetadata;
import org.hyperledger.besu.plugin.services.storage.postgresql.configuration.PostgreSQLConfiguration;
import org.hyperledger.besu.plugin.services.storage.postgresql.configuration.PostgreSQLConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.postgresql.configuration.PostgreSQLFactoryConfiguration;
import org.hyperledger.besu.plugin.services.storage.postgresql.connection.PostgreSQLConnectionManager;
import org.hyperledger.besu.plugin.services.storage.postgresql.core.PostgreSQLColumnarKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.postgresql.core.PostgreSQLPartitionManager;
import org.hyperledger.besu.plugin.services.storage.postgresql.util.PostgreSQLSchemaManager;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** PostgreSQL key-value storage factory. */
public class PostgreSQLKeyValueStorageFactory implements KeyValueStorageFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(PostgreSQLKeyValueStorageFactory.class);
  private static final String NAME = "postgresql";

  private final Supplier<PostgreSQLFactoryConfiguration> configuration;
  private final List<SegmentIdentifier> configuredSegments;

  private DatabaseMetadata databaseMetadata;
  private PostgreSQLConfiguration postgresqlConfiguration;
  private PostgreSQLConnectionManager connectionManager;
  private PostgreSQLSchemaManager schemaManager;
  private PostgreSQLPartitionManager partitionManager;
  private PostgreSQLColumnarKeyValueStorage segmentedStorage;

  /**
   * Instantiates a new PostgreSQL key value storage factory.
   *
   * @param configuration the configuration
   * @param configuredSegments the configured segments
   */
  public PostgreSQLKeyValueStorageFactory(
      final Supplier<PostgreSQLFactoryConfiguration> configuration,
      final List<SegmentIdentifier> configuredSegments) {
    this.configuration = configuration;
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

    if (requiresInit()) {
      init(commonConfiguration);
    }

    // Safety check to see that segments all exist within configured segments
    if (!configuredSegments.containsAll(segments)) {
      throw new StorageException(
          "Attempted to create storage for segments that are not configured: "
              + segments.stream()
                  .filter(segment -> !configuredSegments.contains(segment))
                  .map(SegmentIdentifier::toString)
                  .collect(Collectors.joining(", ")));
    }

    if (segmentedStorage == null) {
      final List<SegmentIdentifier> segmentsForFormat =
          configuredSegments.stream()
              .filter(
                  segmentId ->
                      segmentId.includeInDatabaseFormat(
                          databaseMetadata.getVersionedStorageFormat().getFormat()))
              .toList();

      segmentedStorage =
          new PostgreSQLColumnarKeyValueStorage(
              connectionManager,
              schemaManager,
              partitionManager,
              segmentsForFormat,
              metricsSystem);

      LOG.info("PostgreSQL storage created with {} segments", segmentsForFormat.size());
    }

    return segmentedStorage;
  }

  private boolean requiresInit() {
    return segmentedStorage == null;
  }

  private void init(final BesuConfiguration commonConfiguration) {
    try {
      databaseMetadata = readDatabaseMetadata(commonConfiguration);
    } catch (final IOException e) {
      final String message =
          "Failed to retrieve the PostgreSQL database metadata: "
              + e.getMessage()
              + " could not be found. You may not have the appropriate permission to access the item.";
      throw new StorageException(message, e);
    }

    final PostgreSQLFactoryConfiguration factoryConfig = configuration.get();
    postgresqlConfiguration =
        PostgreSQLConfigurationBuilder.from(factoryConfig)
            .databaseDir(commonConfiguration.getStoragePath())
            .build();

    // Initialize connection manager
    connectionManager = new PostgreSQLConnectionManager(postgresqlConfiguration);

    // Initialize schema manager
    schemaManager =
        new PostgreSQLSchemaManager(
            connectionManager, postgresqlConfiguration.getSchema());

    // Initialize partition manager
    partitionManager = new PostgreSQLPartitionManager(postgresqlConfiguration.getSchema());

    // Create schema and metadata table
    schemaManager.initializeSchema();

    LOG.info("PostgreSQL storage initialized: {}", factoryConfig.getHost());
  }

  private DatabaseMetadata readDatabaseMetadata(final BesuConfiguration commonConfiguration)
      throws IOException {
    final Path dataDir = commonConfiguration.getDataPath();
    final boolean dataDirExists = dataDir.toFile().exists();
    final boolean metadataExists = DatabaseMetadata.isPresent(dataDir);

    DatabaseMetadata metadata;

    if (metadataExists) {
      metadata = DatabaseMetadata.lookUpFrom(dataDir);
      LOG.info("Existing database metadata at {}. Metadata: {}", dataDir, metadata);
    } else {
      metadata = DatabaseMetadata.defaultForNewDb(commonConfiguration);
      LOG.info(
          "No existing database metadata at {}. Using default metadata for new db: {}",
          dataDir,
          metadata);

      if (!dataDirExists) {
        Files.createDirectories(dataDir);
      }
      metadata.writeToDirectory(dataDir);
    }

    return metadata;
  }

  @Override
  public void close() throws IOException {
    if (segmentedStorage != null) {
      segmentedStorage.close();
      segmentedStorage = null;
    }
    if (connectionManager != null) {
      connectionManager.close();
      connectionManager = null;
    }
    LOG.info("PostgreSQL storage factory closed");
  }

  @Override
  public boolean isSegmentIsolationSupported() {
    return true;
  }

  @Override
  public boolean isSnapshotIsolationSupported() {
    return true;
  }
}
