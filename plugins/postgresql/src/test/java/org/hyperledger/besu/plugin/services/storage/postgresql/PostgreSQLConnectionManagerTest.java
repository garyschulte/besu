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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.services.storage.postgresql.connection.PostgreSQLConnectionManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

/** Integration tests for PostgreSQL connection manager. */
class PostgreSQLConnectionManagerTest extends AbstractPostgreSQLTest {

  private PostgreSQLConnectionManager connectionManager;

  @org.junit.jupiter.api.BeforeEach
  void setup() {
    connectionManager = createConnectionManager();
  }

  @org.junit.jupiter.api.AfterEach
  void teardown() {
    if (connectionManager != null && !connectionManager.isClosed()) {
      connectionManager.close();
    }
  }

  @Test
  void testGetConnection() throws Exception {
    try (Connection conn = connectionManager.getConnection()) {
      assertThat(conn).isNotNull();
      assertThat(conn.isClosed()).isFalse();
      assertThat(conn.getAutoCommit()).isFalse();
    }
  }

  @Test
  void testConnectionPooling() throws Exception {
    // Get multiple connections
    try (Connection conn1 = connectionManager.getConnection();
        Connection conn2 = connectionManager.getConnection();
        Connection conn3 = connectionManager.getConnection()) {

      assertThat(conn1).isNotNull();
      assertThat(conn2).isNotNull();
      assertThat(conn3).isNotNull();

      // All should be different connections
      assertThat(conn1).isNotSameAs(conn2);
      assertThat(conn2).isNotSameAs(conn3);
    }

    // Verify pool stats
    final var stats = connectionManager.getPoolStats();
    assertThat(stats.total()).isGreaterThan(0);
  }

  @Test
  void testExecuteQuery() throws Exception {
    try (Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 as test_value")) {

      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt("test_value")).isEqualTo(1);
    }
  }

  @Test
  void testTransactionSupport() throws Exception {
    try (Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement()) {

      // Create temp table
      stmt.execute("CREATE TEMP TABLE test_tx (id INT, value TEXT)");
      stmt.execute("INSERT INTO test_tx VALUES (1, 'test')");

      // Commit
      conn.commit();

      // Verify
      try (ResultSet rs = stmt.executeQuery("SELECT value FROM test_tx WHERE id = 1")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("value")).isEqualTo("test");
      }

      // Test rollback
      stmt.execute("INSERT INTO test_tx VALUES (2, 'rollback')");
      conn.rollback();

      // Verify rollback worked
      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_tx")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isEqualTo(1);
      }
    }
  }

  @Test
  void testConnectionClose() {
    assertThat(connectionManager.isClosed()).isFalse();

    final PostgreSQLConnectionManager tempManager = createConnectionManager();
    assertThat(tempManager.isClosed()).isFalse();

    tempManager.close();
    assertThat(tempManager.isClosed()).isTrue();
  }

  @Test
  void testPoolStats() {
    final var stats = connectionManager.getPoolStats();

    assertThat(stats).isNotNull();
    assertThat(stats.total()).isGreaterThanOrEqualTo(0);
    assertThat(stats.active()).isGreaterThanOrEqualTo(0);
    assertThat(stats.idle()).isGreaterThanOrEqualTo(0);
    assertThat(stats.waiting()).isGreaterThanOrEqualTo(0);
  }
}
