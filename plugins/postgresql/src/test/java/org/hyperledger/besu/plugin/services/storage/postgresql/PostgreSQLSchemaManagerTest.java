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

import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.postgresql.connection.PostgreSQLConnectionManager;
import org.hyperledger.besu.plugin.services.storage.postgresql.util.PostgreSQLSchemaManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration tests for PostgreSQL schema manager. */
class PostgreSQLSchemaManagerTest extends AbstractPostgreSQLTest {

  private PostgreSQLConnectionManager connectionManager;
  private PostgreSQLSchemaManager schemaManager;

  @BeforeEach
  void setup() {
    connectionManager = createConnectionManager();
    schemaManager = new PostgreSQLSchemaManager(connectionManager, TEST_SCHEMA);
  }

  @org.junit.jupiter.api.AfterEach
  void teardown() {
    if (connectionManager != null && !connectionManager.isClosed()) {
      connectionManager.close();
    }
  }

  @Test
  void testInitializeSchema() throws Exception {
    schemaManager.initializeSchema();

    try (Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '"
                    + TEST_SCHEMA
                    + "'")) {

      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("schema_name")).isEqualTo(TEST_SCHEMA);
    }

    // Verify metadata table exists
    try (Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                "SELECT table_name FROM information_schema.tables "
                    + "WHERE table_schema = '"
                    + TEST_SCHEMA
                    + "' AND table_name = 'metadata'")) {

      assertThat(rs.next()).isTrue();
    }
  }

  @Test
  void testCreateSegmentTable() throws Exception {
    schemaManager.initializeSchema();

    final TestSegment segment = new TestSegment("test_segment");
    schemaManager.createSegmentTable(segment);

    // Verify table exists
    try (Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                "SELECT table_name FROM information_schema.tables "
                    + "WHERE table_schema = '"
                    + TEST_SCHEMA
                    + "' AND table_name = 'segment_test_segment'")) {

      assertThat(rs.next()).isTrue();
    }

    // Verify table structure
    try (Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                "SELECT column_name FROM information_schema.columns "
                    + "WHERE table_schema = '"
                    + TEST_SCHEMA
                    + "' AND table_name = 'segment_test_segment'")) {

      final java.util.Set<String> columns = new java.util.HashSet<>();
      while (rs.next()) {
        columns.add(rs.getString("column_name"));
      }

      assertThat(columns).contains("key", "value", "block_start", "block_end", "created_at");
    }

    // Verify indexes exist
    try (Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                "SELECT indexname FROM pg_indexes "
                    + "WHERE schemaname = '"
                    + TEST_SCHEMA
                    + "' AND tablename = 'segment_test_segment'")) {

      final java.util.Set<String> indexes = new java.util.HashSet<>();
      while (rs.next()) {
        indexes.add(rs.getString("indexname"));
      }

      assertThat(indexes)
          .contains(
              "idx_test_segment_key_current",
              "idx_test_segment_key_range",
              "idx_test_segment_block_end");
    }
  }

  @Test
  void testListTables() {
    schemaManager.initializeSchema();

    final TestSegment segment1 = new TestSegment("segment_one");
    final TestSegment segment2 = new TestSegment("segment_two");

    schemaManager.createSegmentTable(segment1);
    schemaManager.createSegmentTable(segment2);

    final var tables = schemaManager.listTables();

    assertThat(tables).contains("metadata", "segment_segment_one", "segment_segment_two");
  }

  @Test
  void testClearSegmentTable() throws Exception {
    schemaManager.initializeSchema();

    final TestSegment segment = new TestSegment("test_clear");
    schemaManager.createSegmentTable(segment);

    // Insert some data
    try (Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute(
          String.format(
              "INSERT INTO %s.segment_test_clear (key, value, block_start) VALUES "
                  + "('\\x0001'::bytea, '\\x0010'::bytea, 1), "
                  + "('\\x0002'::bytea, '\\x0020'::bytea, 1)",
              TEST_SCHEMA));
      conn.commit();
    }

    // Verify data exists
    try (Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                String.format("SELECT COUNT(*) FROM %s.segment_test_clear", TEST_SCHEMA))) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt(1)).isEqualTo(2);
    }

    // Clear table
    schemaManager.clearSegmentTable(segment);

    // Verify data is gone
    try (Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                String.format("SELECT COUNT(*) FROM %s.segment_test_clear", TEST_SCHEMA))) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt(1)).isEqualTo(0);
    }
  }

  @Test
  void testDropSegmentTable() throws Exception {
    schemaManager.initializeSchema();

    final TestSegment segment = new TestSegment("test_drop");
    schemaManager.createSegmentTable(segment);

    // Verify table exists
    assertThat(schemaManager.listTables()).contains("segment_test_drop");

    // Drop table
    schemaManager.dropSegmentTable(segment);

    // Verify table is gone
    assertThat(schemaManager.listTables()).doesNotContain("segment_test_drop");
  }

  @Test
  void testGetTableName() {
    final TestSegment segment = new TestSegment("my_segment");
    final String tableName = schemaManager.getTableName(segment);

    assertThat(tableName).isEqualTo(TEST_SCHEMA + ".segment_my_segment");
  }

  /** Test segment identifier. */
  private record TestSegment(String name) implements SegmentIdentifier {
    @Override
    public String getName() {
      return name;
    }

    @Override
    public byte[] getId() {
      return name.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public boolean containsStaticData() {
      return false;
    }

    @Override
    public boolean isEligibleToHighSpecFlag() {
      return false;
    }
  }
}
