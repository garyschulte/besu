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

import java.util.Optional;

import com.google.common.base.MoreObjects;
import picocli.CommandLine;

/** PostgreSQL CLI options for configuring the PostgreSQL storage plugin. */
public class PostgreSQLCLIOptions {

  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 5432;
  public static final String DEFAULT_DATABASE = "besu";
  public static final String DEFAULT_SCHEMA = "besu_storage";
  public static final int DEFAULT_POOL_SIZE = 10;
  public static final String DEFAULT_SSL_MODE = "disable";
  public static final int DEFAULT_CONNECTION_TIMEOUT = 30000;

  public static final String HOST_FLAG = "--Xplugin-postgresql-host";
  public static final String PORT_FLAG = "--Xplugin-postgresql-port";
  public static final String DATABASE_FLAG = "--Xplugin-postgresql-database";
  public static final String USERNAME_FLAG = "--Xplugin-postgresql-username";
  public static final String PASSWORD_FLAG = "--Xplugin-postgresql-password";
  public static final String SCHEMA_FLAG = "--Xplugin-postgresql-schema";
  public static final String POOL_SIZE_FLAG = "--Xplugin-postgresql-pool-size";
  public static final String SSL_MODE_FLAG = "--Xplugin-postgresql-ssl-mode";
  public static final String CONNECTION_TIMEOUT_FLAG = "--Xplugin-postgresql-connection-timeout";

  @CommandLine.Option(
      names = {HOST_FLAG},
      hidden = true,
      defaultValue = DEFAULT_HOST,
      paramLabel = "<STRING>",
      description = "PostgreSQL host (default: ${DEFAULT-VALUE})")
  String host;

  @CommandLine.Option(
      names = {PORT_FLAG},
      hidden = true,
      defaultValue = "5432",
      paramLabel = "<INTEGER>",
      description = "PostgreSQL port (default: ${DEFAULT-VALUE})")
  int port;

  @CommandLine.Option(
      names = {DATABASE_FLAG},
      hidden = true,
      defaultValue = DEFAULT_DATABASE,
      paramLabel = "<STRING>",
      description = "PostgreSQL database name (default: ${DEFAULT-VALUE})")
  String database;

  @CommandLine.Option(
      names = {USERNAME_FLAG},
      hidden = true,
      paramLabel = "<STRING>",
      description = "PostgreSQL username (or use PGUSER env var)")
  Optional<String> username;

  @CommandLine.Option(
      names = {PASSWORD_FLAG},
      hidden = true,
      paramLabel = "<STRING>",
      description = "PostgreSQL password (or use PGPASSWORD env var)")
  Optional<String> password;

  @CommandLine.Option(
      names = {SCHEMA_FLAG},
      hidden = true,
      defaultValue = DEFAULT_SCHEMA,
      paramLabel = "<STRING>",
      description = "PostgreSQL schema name (default: ${DEFAULT-VALUE})")
  String schema;

  @CommandLine.Option(
      names = {POOL_SIZE_FLAG},
      hidden = true,
      defaultValue = "10",
      paramLabel = "<INTEGER>",
      description = "HikariCP connection pool size (default: ${DEFAULT-VALUE})")
  int poolSize;

  @CommandLine.Option(
      names = {SSL_MODE_FLAG},
      hidden = true,
      defaultValue = DEFAULT_SSL_MODE,
      paramLabel = "<STRING>",
      description =
          "PostgreSQL SSL mode: disable, require, verify-ca, verify-full (default: ${DEFAULT-VALUE})")
  String sslMode;

  @CommandLine.Option(
      names = {CONNECTION_TIMEOUT_FLAG},
      hidden = true,
      defaultValue = "30000",
      paramLabel = "<INTEGER>",
      description = "Connection timeout in milliseconds (default: ${DEFAULT-VALUE})")
  int connectionTimeout;

  private PostgreSQLCLIOptions() {}

  /**
   * Create PostgreSQL CLI options.
   *
   * @return the PostgreSQL CLI options
   */
  public static PostgreSQLCLIOptions create() {
    return new PostgreSQLCLIOptions();
  }

  /**
   * PostgreSQL CLI options from config.
   *
   * @param config the config
   * @return the PostgreSQL CLI options
   */
  public static PostgreSQLCLIOptions fromConfig(final PostgreSQLConfiguration config) {
    final PostgreSQLCLIOptions options = create();
    options.host = config.getHost();
    options.port = config.getPort();
    options.database = config.getDatabase();
    options.username = config.getUsername();
    options.password = config.getPassword();
    options.schema = config.getSchema();
    options.poolSize = config.getPoolSize();
    options.sslMode = config.getSslMode();
    options.connectionTimeout = config.getConnectionTimeout();
    return options;
  }

  /**
   * To domain object PostgreSQL factory configuration.
   *
   * @return the PostgreSQL factory configuration
   */
  public PostgreSQLFactoryConfiguration toDomainObject() {
    // Check environment variables for username/password if not provided
    final String effectiveUsername =
        username.or(() -> Optional.ofNullable(System.getenv("PGUSER"))).orElse("postgres");

    final String effectivePassword =
        password.or(() -> Optional.ofNullable(System.getenv("PGPASSWORD"))).orElse("");

    return new PostgreSQLFactoryConfiguration(
        host,
        port,
        database,
        effectiveUsername,
        effectivePassword,
        schema,
        poolSize,
        sslMode,
        connectionTimeout);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("host", host)
        .add("port", port)
        .add("database", database)
        .add("username", username.orElse("(from env or default)"))
        .add("password", password.isPresent() ? "***" : "(from env or default)")
        .add("schema", schema)
        .add("poolSize", poolSize)
        .add("sslMode", sslMode)
        .add("connectionTimeout", connectionTimeout)
        .toString();
  }
}
