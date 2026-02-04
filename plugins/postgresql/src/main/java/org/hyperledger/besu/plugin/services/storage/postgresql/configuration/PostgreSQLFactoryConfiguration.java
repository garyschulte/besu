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

/** PostgreSQL factory configuration containing all database connection parameters. */
public class PostgreSQLFactoryConfiguration {
  private final String host;
  private final int port;
  private final String database;
  private final String username;
  private final String password;
  private final String schema;
  private final int poolSize;
  private final String sslMode;
  private final int connectionTimeout;

  /**
   * Instantiates a new PostgreSQL factory configuration.
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
   */
  public PostgreSQLFactoryConfiguration(
      final String host,
      final int port,
      final String database,
      final String username,
      final String password,
      final String schema,
      final int poolSize,
      final String sslMode,
      final int connectionTimeout) {
    this.host = host;
    this.port = port;
    this.database = database;
    this.username = username;
    this.password = password;
    this.schema = schema;
    this.poolSize = poolSize;
    this.sslMode = sslMode;
    this.connectionTimeout = connectionTimeout;
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
  public String getUsername() {
    return username;
  }

  /**
   * Gets password.
   *
   * @return the password
   */
  public String getPassword() {
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
}
