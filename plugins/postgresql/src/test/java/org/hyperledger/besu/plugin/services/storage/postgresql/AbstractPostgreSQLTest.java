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

import org.hyperledger.besu.plugin.services.storage.postgresql.configuration.PostgreSQLConfiguration;
import org.hyperledger.besu.plugin.services.storage.postgresql.configuration.PostgreSQLConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.postgresql.connection.PostgreSQLConnectionManager;

import java.nio.file.Path;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Abstract base class for PostgreSQL integration tests using Testcontainers. */
@Testcontainers
public abstract class AbstractPostgreSQLTest {

  protected static final String TEST_SCHEMA = "test_besu_storage";

  @Container
  protected static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
          .withDatabaseName("test_besu")
          .withUsername("test_user")
          .withPassword("test_password")
          .withCommand("postgres", "-c", "fsync=off", "-c", "max_connections=200")
          .withReuse(true); // Reuse container across test classes

  protected static PostgreSQLConfiguration testConfig;

  @BeforeAll
  static void setupContainer() {
    testConfig = createTestConfiguration();
  }

  protected static PostgreSQLConfiguration createTestConfiguration() {
    return new PostgreSQLConfigurationBuilder()
        .host(POSTGRES.getHost())
        .port(POSTGRES.getFirstMappedPort())
        .database(POSTGRES.getDatabaseName())
        .username(POSTGRES.getUsername())
        .password(POSTGRES.getPassword())
        .schema(TEST_SCHEMA)
        .poolSize(5)
        .sslMode("disable")
        .connectionTimeout(10000)
        .databaseDir(Path.of("/tmp/besu-test"))
        .build();
  }

  protected PostgreSQLConnectionManager createConnectionManager() {
    return new PostgreSQLConnectionManager(testConfig);
  }

  protected byte[] bytes(final String hex) {
    return Bytes.fromHexString(hex).toArrayUnsafe();
  }
}
