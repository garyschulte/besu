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
import org.hyperledger.besu.plugin.services.storage.postgresql.core.PostgreSQLPartitionManager;
import org.hyperledger.besu.plugin.services.storage.postgresql.util.PostgreSQLSchemaManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration tests for PostgreSQL partition manager. */
class PostgreSQLPartitionManagerTest extends AbstractPostgreSQLTest {

  private PostgreSQLConnectionManager connectionManager;
  private PostgreSQLSchemaManager schemaManager;
  private PostgreSQLPartitionManager partitionManager;
  private TestSegment testSegment;

  @BeforeEach
  void setup() {
    connectionManager = createConnectionManager();
    schemaManager = new PostgreSQLSchemaManager(connectionManager, TEST_SCHEMA);
    partitionManager = new PostgreSQLPartitionManager(TEST_SCHEMA);
    testSegment = new TestSegment("partition_test");

    schemaManager.initializeSchema();
    schemaManager.createSegmentTable(testSegment);
  }

  @AfterEach
  void teardown() {
    if (schemaManager != null && testSegment != null) {
      try {
        schemaManager.dropSegmentTable(testSegment);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
    }
    if (partitionManager != null) {
      partitionManager.clearCache();
    }
    if (connectionManager != null && !connectionManager.isClosed()) {
      connectionManager.close();
    }
  }

  @Test
  void testEnsurePartitionExists() throws Exception {
    try (Connection conn = connectionManager.getConnection()) {
      // Create partition for block range 0-100000
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 50000);

      // Verify partition was created
      final var partitions = partitionManager.listPartitions(conn, testSegment.getName());
      assertThat(partitions).anyMatch(p -> p.contains("part_0_100000"));
    }
  }

  @Test
  void testEnsureMultiplePartitions() throws Exception {
    try (Connection conn = connectionManager.getConnection()) {
      // Create partitions for different block ranges
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 50000); // 0-100K
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 150000); // 100K-200K
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 250000); // 200K-300K

      // Verify all partitions exist
      final var partitions = partitionManager.listPartitions(conn, testSegment.getName());
      assertThat(partitions).hasSize(3);
      assertThat(partitions).anyMatch(p -> p.contains("part_0_100000"));
      assertThat(partitions).anyMatch(p -> p.contains("part_100000_200000"));
      assertThat(partitions).anyMatch(p -> p.contains("part_200000_300000"));
    }
  }

  @Test
  void testEnsurePartitionIdempotent() throws Exception {
    try (Connection conn = connectionManager.getConnection()) {
      // Create partition twice
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 50000);
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 50000);

      // Should only have one partition
      final var partitions = partitionManager.listPartitions(conn, testSegment.getName());
      assertThat(partitions).hasSize(1);
    }
  }

  @Test
  void testListPartitions() throws Exception {
    try (Connection conn = connectionManager.getConnection()) {

      // Add more partitions
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 50000);
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 150000);

      var partitions = partitionManager.listPartitions(conn, testSegment.getName());
      assertThat(partitions).hasSizeGreaterThanOrEqualTo(2);
    }
  }

  @Test
  void testPartitionContainsData() throws Exception {
    try (Connection conn = connectionManager.getConnection()) {
      conn.setAutoCommit(false);

      // Ensure partition exists
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 50000);

      // Insert data into the partition
      final String tableName = schemaManager.getTableName(testSegment);
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
            String.format(
                "INSERT INTO %s (key, value, block_start) VALUES "
                    + "('\\x0001'::bytea, '\\x1111'::bytea, 50000)",
                tableName));
        conn.commit();
      }

      // Verify data is in the partition
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName))) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isEqualTo(1);
      }
    }
  }

  @Test
  void testCleanupColdPartition() throws Exception {
    try (Connection conn = connectionManager.getConnection()) {
      conn.setAutoCommit(false);

      // Create partition and add data
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 50000);

      final String tableName = schemaManager.getTableName(testSegment);
      try (Statement stmt = conn.createStatement()) {
        // Insert multiple versions of same key
        stmt.execute(
            String.format(
                "INSERT INTO %s (key, value, block_start, block_end) VALUES "
                    + "('\\x0001'::bytea, '\\x1111'::bytea, 10000, 20000), "
                    + "('\\x0001'::bytea, '\\x2222'::bytea, 20000, 30000), "
                    + "('\\x0001'::bytea, '\\x3333'::bytea, 30000, NULL)",
                tableName));
        conn.commit();
      }

      // Verify 3 rows exist
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName))) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isEqualTo(3);
      }

      // Cleanup old versions (before block 30000)
      final String partitionName = "segment_partition_test_part_0_100000";
      final long deleted = partitionManager.cleanupColdPartition(conn, partitionName, 30000);

      assertThat(deleted).isEqualTo(2); // Should delete 2 superseded rows

      // Verify only current version remains
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName))) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isEqualTo(1);
      }
    }
  }

  @Test
  void testTruncateColdPartition() throws Exception {
    try (Connection conn = connectionManager.getConnection()) {
      conn.setAutoCommit(false);

      // Create partition and add data
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 50000);

      final String tableName = schemaManager.getTableName(testSegment);
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
            String.format(
                "INSERT INTO %s (key, value, block_start) VALUES "
                    + "('\\x0001'::bytea, '\\x1111'::bytea, 50000), "
                    + "('\\x0002'::bytea, '\\x2222'::bytea, 50000)",
                tableName));
        conn.commit();
      }

      // Verify data exists
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName))) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isEqualTo(2);
      }

      // Truncate partition
      final String partitionName = "segment_partition_test_part_0_100000";
      partitionManager.truncateColdPartition(conn, partitionName);
      conn.commit();

      // Verify data is gone
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName))) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isEqualTo(0);
      }
    }
  }

  @Test
  void testDropPartition() throws Exception {
    try (Connection conn = connectionManager.getConnection()) {
      conn.setAutoCommit(false);

      // Create partition
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 50000);
      conn.commit();

      // Verify it exists
      var partitions = partitionManager.listPartitions(conn, testSegment.getName());
      final int initialCount = partitions.size();
      assertThat(partitions).anyMatch(p -> p.contains("part_0_100000"));

      // Drop partition
      final String partitionName = "segment_partition_test_part_0_100000";
      partitionManager.dropPartition(conn, partitionName);
      conn.commit();

      // Verify it's gone
      partitions = partitionManager.listPartitions(conn, testSegment.getName());
      assertThat(partitions).hasSize(initialCount - 1);
      assertThat(partitions).noneMatch(p -> p.contains("part_0_100000"));
    }
  }

  @Test
  void testClearCache() throws Exception {
    try (Connection conn = connectionManager.getConnection()) {
      // Create partitions
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 50000);
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 150000);

      // Clear cache
      partitionManager.clearCache();

      // Should still be able to create partitions (will check DB again)
      partitionManager.ensurePartitionExists(conn, testSegment.getName(), 250000);

      final var partitions = partitionManager.listPartitions(conn, testSegment.getName());
      assertThat(partitions).hasSizeGreaterThanOrEqualTo(3);
    }
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
