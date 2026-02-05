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
package org.hyperledger.besu.plugin.services.storage.postgresql.core;

import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages PostgreSQL table partitions for temporal data. */
public class PostgreSQLPartitionManager {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLPartitionManager.class);

  private static final long COLD_PARTITION_SIZE = 100_000;

  private final String schemaName;
  private final Map<String, Set<String>> segmentPartitions = new ConcurrentHashMap<>();

  /**
   * Instantiates a new PostgreSQL partition manager.
   *
   * @param schemaName the schema name
   */
  public PostgreSQLPartitionManager(final String schemaName) {
    this.schemaName = schemaName;
  }

  /**
   * Ensure partition exists for the given block number.
   *
   * @param conn the database connection
   * @param segmentName the segment name
   * @param blockNumber the block number
   */
  public void ensurePartitionExists(
      final Connection conn, final String segmentName, final long blockNumber) {

    // Calculate partition range
    final long partitionStart = (blockNumber / COLD_PARTITION_SIZE) * COLD_PARTITION_SIZE;
    final long partitionEnd = partitionStart + COLD_PARTITION_SIZE;

    final String partitionName =
        String.format(
            "segment_%s_part_%d_%d",
            segmentName.toLowerCase(Locale.ROOT), partitionStart, partitionEnd);

    // Check cache first
    final Set<String> partitions =
        segmentPartitions.computeIfAbsent(segmentName, k -> ConcurrentHashMap.newKeySet());

    if (partitions.contains(partitionName)) {
      return;
    }

    // Create partition if it doesn't exist
    try (Statement stmt = conn.createStatement()) {
      final String sql =
          String.format(
              """
              CREATE TABLE IF NOT EXISTS %s.%s
              PARTITION OF %s.segment_%s
              FOR VALUES FROM (%d) TO (%d)
              """,
              schemaName,
              partitionName,
              schemaName,
              segmentName.toLowerCase(Locale.ROOT),
              partitionStart,
              partitionEnd);

      stmt.execute(sql);
      partitions.add(partitionName);
      LOG.debug("Ensured partition {} exists for segment {}", partitionName, segmentName);
    } catch (SQLException e) {
      // Ignore if partition already exists (race condition)
      if ("42P07".equals(e.getSQLState())) { // duplicate table error
        partitions.add(partitionName);
        LOG.debug("Partition {} already exists (race condition)", partitionName);
      } else {
        throw new StorageException("Failed to create partition: " + partitionName, e);
      }
    }
  }

  /**
   * List all partitions for a segment.
   *
   * @param conn the connection
   * @param segmentName the segment name
   * @return list of partition names
   */
  public List<String> listPartitions(final Connection conn, final String segmentName) {
    final String sql =
        """
        SELECT tablename FROM pg_tables
        WHERE schemaname = ? AND tablename LIKE ?
        """;

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, schemaName);
      ps.setString(2, "segment_" + segmentName.toLowerCase(Locale.ROOT) + "_part_%");

      final List<String> partitions = new ArrayList<>();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          partitions.add(rs.getString(1));
        }
      }
      return partitions;
    } catch (SQLException e) {
      throw new StorageException("Failed to list partitions for segment: " + segmentName, e);
    }
  }

  /**
   * Cleanup superseded rows from a cold partition.
   *
   * @param conn the connection
   * @param partitionName the partition name
   * @param beforeBlock delete rows with block_end less than this
   * @return number of rows deleted
   */
  public long cleanupColdPartition(
      final Connection conn, final String partitionName, final long beforeBlock) {
    final String sql =
        String.format(
            """
            DELETE FROM %s.%s
            WHERE block_end IS NOT NULL AND block_end <= ?
            """,
            schemaName, partitionName);

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, beforeBlock);
      final long deleted = ps.executeUpdate();
      LOG.info("Deleted {} superseded rows from {}", deleted, partitionName);
      return deleted;
    } catch (SQLException e) {
      throw new StorageException("Failed to cleanup partition: " + partitionName, e);
    }
  }

  /**
   * Truncate entire cold partition.
   *
   * @param conn the connection
   * @param partitionName the partition name
   */
  public void truncateColdPartition(final Connection conn, final String partitionName) {
    final String sql = String.format("TRUNCATE TABLE %s.%s", schemaName, partitionName);

    try (Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
      LOG.info("Truncated partition {}", partitionName);
    } catch (SQLException e) {
      throw new StorageException("Failed to truncate partition: " + partitionName, e);
    }
  }

  /**
   * Drop a partition.
   *
   * @param conn the connection
   * @param partitionName the partition name
   */
  public void dropPartition(final Connection conn, final String partitionName) {
    final String sql = String.format("DROP TABLE IF EXISTS %s.%s", schemaName, partitionName);

    try (Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
      LOG.info("Dropped partition {}", partitionName);

      // Remove from cache
      segmentPartitions.values().forEach(set -> set.remove(partitionName));
    } catch (SQLException e) {
      throw new StorageException("Failed to drop partition: " + partitionName, e);
    }
  }

  /** Clear the partition cache. */
  public void clearCache() {
    segmentPartitions.clear();
  }
}
