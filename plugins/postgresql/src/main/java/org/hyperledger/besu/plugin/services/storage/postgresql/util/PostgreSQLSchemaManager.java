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
package org.hyperledger.besu.plugin.services.storage.postgresql.util;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.postgresql.connection.PostgreSQLConnectionManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.google.common.base.Splitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages PostgreSQL schema and table creation. */
public class PostgreSQLSchemaManager {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLSchemaManager.class);

  private final PostgreSQLConnectionManager connectionManager;
  private final String schemaName;

  /**
   * Instantiates a new PostgreSQL schema manager.
   *
   * @param connectionManager the connection manager
   * @param schemaName the schema name
   */
  public PostgreSQLSchemaManager(
      final PostgreSQLConnectionManager connectionManager, final String schemaName) {
    this.connectionManager = connectionManager;
    this.schemaName = schemaName;
  }

  /** Initialize schema and metadata table. */
  public void initializeSchema() {
    try (Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement()) {

      // Create schema if not exists
      final String createSchema = String.format("CREATE SCHEMA IF NOT EXISTS %s", schemaName);
      stmt.execute(createSchema);
      LOG.info("Schema {} created or already exists", schemaName);

      // Create metadata table
      final String createMetadataTable =
          String.format(
              """
              CREATE TABLE IF NOT EXISTS %s.metadata (
                  key VARCHAR(255) PRIMARY KEY,
                  value JSONB NOT NULL,
                  updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
              )
              """,
              schemaName);
      stmt.execute(createMetadataTable);
      LOG.info("Metadata table created or already exists");

      conn.commit();
    } catch (SQLException e) {
      throw new StorageException("Failed to initialize schema", e);
    }
  }

  /**
   * Create a segment table with temporal versioning support.
   *
   * @param segment the segment identifier
   */
  public void createSegmentTable(final SegmentIdentifier segment) {
    final String tableName = getTableName(segment);
    final String baseTableName = getBaseTableName(segment);

    try (Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement()) {

      // Check if table already exists
      if (tableExists(conn, tableName)) {
        LOG.debug("Table {} already exists", tableName);
        return;
      }

      // Create partitioned table with temporal versioning
      final String createTable =
          String.format(
              """
              CREATE TABLE %s (
                  key BYTEA NOT NULL,
                  value BYTEA NOT NULL,
                  block_start NUMERIC NOT NULL,
                  block_end NUMERIC,
                  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                  PRIMARY KEY (key, block_start)
              ) PARTITION BY RANGE (block_start)
              """,
              tableName);
      stmt.execute(createTable);
      LOG.info("Created partitioned table {}", tableName);

      // Create default partition (catches all data initially)
      final String defaultPartition = baseTableName + "_default";
      final String createDefaultPartition =
          String.format(
              """
              CREATE TABLE IF NOT EXISTS %s.%s
              PARTITION OF %s DEFAULT
              """,
              schemaName, defaultPartition, tableName);
      stmt.execute(createDefaultPartition);
      LOG.info("Created default partition {}", defaultPartition);

      // Create partial index for current values (block_end IS NULL)
      final String createCurrentIndex =
          String.format(
              """
              CREATE INDEX IF NOT EXISTS idx_%s_key_current
              ON %s (key)
              WHERE block_end IS NULL
              """,
              segment.getName().toLowerCase(Locale.ROOT), tableName);
      stmt.execute(createCurrentIndex);

      // Create index for range queries
      final String createRangeIndex =
          String.format(
              """
              CREATE INDEX IF NOT EXISTS idx_%s_key_range
              ON %s (key, block_start, block_end)
              """,
              segment.getName().toLowerCase(Locale.ROOT), tableName);
      stmt.execute(createRangeIndex);

      // Create index for cleanup queries
      final String createEndIndex =
          String.format(
              """
              CREATE INDEX IF NOT EXISTS idx_%s_block_end
              ON %s (block_end)
              WHERE block_end IS NOT NULL
              """,
              segment.getName().toLowerCase(Locale.ROOT), tableName);
      stmt.execute(createEndIndex);

      conn.commit();
      LOG.info("Created table and indexes for segment {}", segment.getName());
    } catch (SQLException e) {
      throw new StorageException("Failed to create segment table: " + tableName, e);
    }
  }

  /**
   * Check if a table exists.
   *
   * @param conn the connection
   * @param tableName the fully qualified table name
   * @return true if exists
   */
  private boolean tableExists(final Connection conn, final String tableName) throws SQLException {
    final List<String> parts = Splitter.on('.').splitToList(tableName);
    final String schema = parts.get(0);
    final String table = parts.get(1);

    final String query =
        "SELECT EXISTS ("
            + "SELECT FROM information_schema.tables "
            + "WHERE table_schema = ? AND table_name = ?)";

    try (var ps = conn.prepareStatement(query)) {
      ps.setString(1, schema);
      ps.setString(2, table);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getBoolean(1);
      }
    }
  }

  /**
   * Get fully qualified table name for a segment.
   *
   * @param segment the segment
   * @return the table name
   */
  public String getTableName(final SegmentIdentifier segment) {
    return String.format("%s.segment_%s", schemaName, segment.getName().toLowerCase(Locale.ROOT));
  }

  /**
   * Get base table name (without schema) for a segment.
   *
   * @param segment the segment
   * @return the base table name
   */
  public String getBaseTableName(final SegmentIdentifier segment) {
    return String.format("segment_%s", segment.getName().toLowerCase(Locale.ROOT));
  }

  /**
   * List all tables in the schema.
   *
   * @return set of table names
   */
  public Set<String> listTables() {
    final Set<String> tables = new HashSet<>();
    final String query =
        "SELECT table_name FROM information_schema.tables WHERE table_schema = ?";

    try (Connection conn = connectionManager.getConnection();
        var ps = conn.prepareStatement(query)) {
      ps.setString(1, schemaName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          tables.add(rs.getString(1));
        }
      }
      return tables;
    } catch (SQLException e) {
      throw new StorageException("Failed to list tables", e);
    }
  }

  /**
   * Drop a segment table (for testing/cleanup).
   *
   * @param segment the segment
   */
  public void dropSegmentTable(final SegmentIdentifier segment) {
    final String tableName = getTableName(segment);

    try (Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement()) {
      final String dropTable = String.format("DROP TABLE IF EXISTS %s CASCADE", tableName);
      stmt.execute(dropTable);
      conn.commit();
      LOG.info("Dropped table {}", tableName);
    } catch (SQLException e) {
      throw new StorageException("Failed to drop table: " + tableName, e);
    }
  }

  /**
   * Clear all data from a segment table.
   *
   * @param segment the segment
   */
  public void clearSegmentTable(final SegmentIdentifier segment) {
    final String tableName = getTableName(segment);

    try (Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement()) {
      final String truncate = String.format("TRUNCATE TABLE %s", tableName);
      stmt.execute(truncate);
      conn.commit();
      LOG.info("Cleared table {}", tableName);
    } catch (SQLException e) {
      throw new StorageException("Failed to clear table: " + tableName, e);
    }
  }
}
