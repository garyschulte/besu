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
package org.hyperledger.besu.plugin.services.storage.postgresql.connection;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.postgresql.configuration.PostgreSQLConfiguration;

import java.sql.Connection;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages PostgreSQL connections using HikariCP connection pool. */
public class PostgreSQLConnectionManager implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLConnectionManager.class);

  private final HikariDataSource dataSource;
  private final PostgreSQLConfiguration configuration;

  /**
   * Instantiates a new PostgreSQL connection manager.
   *
   * @param configuration the PostgreSQL configuration
   */
  public PostgreSQLConnectionManager(final PostgreSQLConfiguration configuration) {
    this.configuration = configuration;
    this.dataSource = createDataSource(configuration);
    LOG.info(
        "PostgreSQL connection pool initialized with {} connections", configuration.getPoolSize());
  }

  private HikariDataSource createDataSource(final PostgreSQLConfiguration config) {
    final HikariConfig hikariConfig = new HikariConfig();

    // JDBC URL
    final String jdbcUrl =
        String.format(
            "jdbc:postgresql://%s:%d/%s?sslmode=%s",
            config.getHost(), config.getPort(), config.getDatabase(), config.getSslMode());
    hikariConfig.setJdbcUrl(jdbcUrl);

    // Credentials
    config.getUsername().ifPresent(hikariConfig::setUsername);
    config.getPassword().ifPresent(hikariConfig::setPassword);

    // Pool configuration
    hikariConfig.setMaximumPoolSize(config.getPoolSize());
    hikariConfig.setMinimumIdle(Math.max(2, config.getPoolSize() / 5));
    hikariConfig.setConnectionTimeout(config.getConnectionTimeout());
    hikariConfig.setIdleTimeout(600000); // 10 minutes
    hikariConfig.setMaxLifetime(1800000); // 30 minutes

    // Performance tuning
    hikariConfig.setAutoCommit(false); // We manage transactions explicitly

    // Prepared statement caching via connection properties
    hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
    hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
    hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    hikariConfig.addDataSourceProperty("useServerPrepStmts", "true");

    // Pool name
    hikariConfig.setPoolName("BesuPostgreSQLPool");

    // Leak detection (helpful for debugging)
    hikariConfig.setLeakDetectionThreshold(60000); // 60 seconds

    // Connection test query
    hikariConfig.setConnectionTestQuery("SELECT 1");

    // Schema setting
    hikariConfig.setSchema(config.getSchema());

    LOG.debug("HikariCP configuration: {}", jdbcUrl);

    try {
      return new HikariDataSource(hikariConfig);
    } catch (Exception e) {
      throw new StorageException("Failed to create PostgreSQL connection pool", e);
    }
  }

  /**
   * Gets a connection from the pool.
   *
   * @return the connection
   * @throws StorageException if connection cannot be obtained
   */
  public Connection getConnection() throws StorageException {
    try {
      final Connection connection = dataSource.getConnection();
      // Ensure we're in the correct schema
      connection.setSchema(configuration.getSchema());
      return connection;
    } catch (SQLException e) {
      throw new StorageException("Failed to get database connection", e);
    }
  }

  /**
   * Gets the configuration.
   *
   * @return the configuration
   */
  public PostgreSQLConfiguration getConfiguration() {
    return configuration;
  }

  /**
   * Check if the connection pool is closed.
   *
   * @return true if closed
   */
  public boolean isClosed() {
    return dataSource.isClosed();
  }

  @Override
  public void close() {
    if (!dataSource.isClosed()) {
      LOG.info("Closing PostgreSQL connection pool");
      dataSource.close();
    }
  }

  /**
   * Get pool statistics for metrics.
   *
   * @return pool stats
   */
  public PoolStats getPoolStats() {
    return new PoolStats(
        dataSource.getHikariPoolMXBean().getActiveConnections(),
        dataSource.getHikariPoolMXBean().getIdleConnections(),
        dataSource.getHikariPoolMXBean().getTotalConnections(),
        dataSource.getHikariPoolMXBean().getThreadsAwaitingConnection());
  }

  /** Record for pool statistics. */
  public record PoolStats(int active, int idle, int total, int waiting) {}
}
