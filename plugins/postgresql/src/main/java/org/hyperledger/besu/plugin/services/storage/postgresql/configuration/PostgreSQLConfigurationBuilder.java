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
package org.hyperledger.besu.plugin.services.storage.postgresql.configuration;

import java.nio.file.Path;
import java.util.Optional;

/** Builder for PostgreSQL configuration. */
public class PostgreSQLConfigurationBuilder {
  private String host = PostgreSQLCLIOptions.DEFAULT_HOST;
  private int port = PostgreSQLCLIOptions.DEFAULT_PORT;
  private String database = PostgreSQLCLIOptions.DEFAULT_DATABASE;
  private Optional<String> username = Optional.empty();
  private Optional<String> password = Optional.empty();
  private String schema = PostgreSQLCLIOptions.DEFAULT_SCHEMA;
  private int poolSize = PostgreSQLCLIOptions.DEFAULT_POOL_SIZE;
  private String sslMode = PostgreSQLCLIOptions.DEFAULT_SSL_MODE;
  private int connectionTimeout = PostgreSQLCLIOptions.DEFAULT_CONNECTION_TIMEOUT;
  private Path databaseDir;

  /**
   * Create a builder from a factory configuration.
   *
   * @param config the factory configuration
   * @return the builder
   */
  public static PostgreSQLConfigurationBuilder from(final PostgreSQLFactoryConfiguration config) {
    return new PostgreSQLConfigurationBuilder()
        .host(config.getHost())
        .port(config.getPort())
        .database(config.getDatabase())
        .username(config.getUsername())
        .password(config.getPassword())
        .schema(config.getSchema())
        .poolSize(config.getPoolSize())
        .sslMode(config.getSslMode())
        .connectionTimeout(config.getConnectionTimeout());
  }

  /**
   * Sets host.
   *
   * @param host the host
   * @return the builder
   */
  public PostgreSQLConfigurationBuilder host(final String host) {
    this.host = host;
    return this;
  }

  /**
   * Sets port.
   *
   * @param port the port
   * @return the builder
   */
  public PostgreSQLConfigurationBuilder port(final int port) {
    this.port = port;
    return this;
  }

  /**
   * Sets database.
   *
   * @param database the database
   * @return the builder
   */
  public PostgreSQLConfigurationBuilder database(final String database) {
    this.database = database;
    return this;
  }

  /**
   * Sets username.
   *
   * @param username the username
   * @return the builder
   */
  public PostgreSQLConfigurationBuilder username(final String username) {
    this.username = Optional.of(username);
    return this;
  }

  /**
   * Sets password.
   *
   * @param password the password
   * @return the builder
   */
  public PostgreSQLConfigurationBuilder password(final String password) {
    this.password = Optional.of(password);
    return this;
  }

  /**
   * Sets schema.
   *
   * @param schema the schema
   * @return the builder
   */
  public PostgreSQLConfigurationBuilder schema(final String schema) {
    this.schema = schema;
    return this;
  }

  /**
   * Sets pool size.
   *
   * @param poolSize the pool size
   * @return the builder
   */
  public PostgreSQLConfigurationBuilder poolSize(final int poolSize) {
    this.poolSize = poolSize;
    return this;
  }

  /**
   * Sets SSL mode.
   *
   * @param sslMode the SSL mode
   * @return the builder
   */
  public PostgreSQLConfigurationBuilder sslMode(final String sslMode) {
    this.sslMode = sslMode;
    return this;
  }

  /**
   * Sets connection timeout.
   *
   * @param connectionTimeout the connection timeout
   * @return the builder
   */
  public PostgreSQLConfigurationBuilder connectionTimeout(final int connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
    return this;
  }

  /**
   * Sets database directory.
   *
   * @param databaseDir the database directory
   * @return the builder
   */
  public PostgreSQLConfigurationBuilder databaseDir(final Path databaseDir) {
    this.databaseDir = databaseDir;
    return this;
  }

  /**
   * Build PostgreSQL configuration.
   *
   * @return the PostgreSQL configuration
   */
  public PostgreSQLConfiguration build() {
    return new PostgreSQLConfiguration(
        host,
        port,
        database,
        username,
        password,
        schema,
        poolSize,
        sslMode,
        connectionTimeout,
        databaseDir);
  }
}
