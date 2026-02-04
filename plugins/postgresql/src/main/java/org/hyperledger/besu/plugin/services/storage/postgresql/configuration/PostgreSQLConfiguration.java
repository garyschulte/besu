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

/** PostgreSQL runtime configuration. */
public class PostgreSQLConfiguration {
  private final String host;
  private final int port;
  private final String database;
  private final Optional<String> username;
  private final Optional<String> password;
  private final String schema;
  private final int poolSize;
  private final String sslMode;
  private final int connectionTimeout;
  private final Path databaseDir;

  /**
   * Instantiates a new PostgreSQL configuration.
   *
   * @param host the PostgreSQL host
   * @param port the PostgreSQL port
   * @param database the database name
   * @param username the username
   * @param password the password
   * @param schema the schema name
   * @param poolSize the connection pool size
   * @param sslMode the SSL mode
   * @param connectionTimeout the connection timeout in ms
   * @param databaseDir the database directory
   */
  public PostgreSQLConfiguration(
      final String host,
      final int port,
      final String database,
      final Optional<String> username,
      final Optional<String> password,
      final String schema,
      final int poolSize,
      final String sslMode,
      final int connectionTimeout,
      final Path databaseDir) {
    this.host = host;
    this.port = port;
    this.database = database;
    this.username = username;
    this.password = password;
    this.schema = schema;
    this.poolSize = poolSize;
    this.sslMode = sslMode;
    this.connectionTimeout = connectionTimeout;
    this.databaseDir = databaseDir;
  }

  /**
   * Gets host.
   *
   * @return the host
   */
  public String getHost() {
    return host;
  }

  /**
   * Gets port.
   *
   * @return the port
   */
  public int getPort() {
    return port;
  }

  /**
   * Gets database.
   *
   * @return the database
   */
  public String getDatabase() {
    return database;
  }

  /**
   * Gets username.
   *
   * @return the username
   */
  public Optional<String> getUsername() {
    return username;
  }

  /**
   * Gets password.
   *
   * @return the password
   */
  public Optional<String> getPassword() {
    return password;
  }

  /**
   * Gets schema.
   *
   * @return the schema
   */
  public String getSchema() {
    return schema;
  }

  /**
   * Gets pool size.
   *
   * @return the pool size
   */
  public int getPoolSize() {
    return poolSize;
  }

  /**
   * Gets SSL mode.
   *
   * @return the SSL mode
   */
  public String getSslMode() {
    return sslMode;
  }

  /**
   * Gets connection timeout.
   *
   * @return the connection timeout
   */
  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  /**
   * Gets database dir.
   *
   * @return the database dir
   */
  public Path getDatabaseDir() {
    return databaseDir;
  }
}
