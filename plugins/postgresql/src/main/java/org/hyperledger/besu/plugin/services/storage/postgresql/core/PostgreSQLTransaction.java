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
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.postgresql.connection.PostgreSQLConnectionManager;
import org.hyperledger.besu.plugin.services.storage.postgresql.util.PostgreSQLExceptionMapper;
import org.hyperledger.besu.plugin.services.storage.postgresql.util.PostgreSQLSchemaManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** PostgreSQL transaction implementation with batching and temporal versioning. */
public class PostgreSQLTransaction implements SegmentedKeyValueStorageTransaction {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLTransaction.class);

  private final PostgreSQLConnectionManager connectionManager;
  private final PostgreSQLSchemaManager schemaManager;
  private final PostgreSQLPartitionManager partitionManager;
  private final long blockNumber;
  private final Map<SegmentIdentifier, List<Operation>> operations = new HashMap<>();

  /**
   * Instantiates a new PostgreSQL transaction.
   *
   * @param connectionManager the connection manager
   * @param schemaManager the schema manager
   * @param partitionManager the partition manager
   * @param blockNumber the current block number
   */
  public PostgreSQLTransaction(
      final PostgreSQLConnectionManager connectionManager,
      final PostgreSQLSchemaManager schemaManager,
      final PostgreSQLPartitionManager partitionManager,
      final long blockNumber) {
    this.connectionManager = connectionManager;
    this.schemaManager = schemaManager;
    this.partitionManager = partitionManager;
    this.blockNumber = blockNumber;
  }

  @Override
  public void put(final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {
    operations
        .computeIfAbsent(segmentIdentifier, k -> new ArrayList<>())
        .add(new PutOperation(key, value));
  }

  @Override
  public void remove(final SegmentIdentifier segmentIdentifier, final byte[] key) {
    operations
        .computeIfAbsent(segmentIdentifier, k -> new ArrayList<>())
        .add(new RemoveOperation(key));
  }

  @Override
  public void commit() throws StorageException {
    if (operations.isEmpty()) {
      return;
    }

    Connection conn = null;
    try {
      conn = connectionManager.getConnection();
      // Set isolation level BEFORE starting transaction (before setAutoCommit(false))
      if (conn.getAutoCommit()) {
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      }
      conn.setAutoCommit(false);

      // Process each segment's operations
      for (Map.Entry<SegmentIdentifier, List<Operation>> entry : operations.entrySet()) {
        final SegmentIdentifier segment = entry.getKey();
        final List<Operation> segmentOps = entry.getValue();

        // Ensure partition exists for this block
        partitionManager.ensurePartitionExists(conn, segment.getName(), blockNumber);

        // Execute batched operations
        executeBatchWithVersioning(conn, segment, segmentOps);
      }

      conn.commit();
      LOG.debug(
          "Transaction committed {} operations at block {}", getTotalOperationCount(), blockNumber);
    } catch (SQLException e) {
      if (conn != null) {
        try {
          conn.rollback();
          LOG.warn("Transaction rolled back due to error", e);
        } catch (SQLException rollbackException) {
          LOG.error("Failed to rollback transaction", rollbackException);
        }
      }
      throw PostgreSQLExceptionMapper.map("Failed to commit transaction", e);
    } finally {
      closeQuietly(conn);
      operations.clear();
    }
  }

  private void executeBatchWithVersioning(
      final Connection conn, final SegmentIdentifier segment, final List<Operation> segmentOps)
      throws SQLException {

    final String tableName = schemaManager.getTableName(segment);

    // Step 1: Close out old versions (UPDATE block_end)
    final String updateSql =
        String.format("UPDATE %s SET block_end = ? WHERE key = ? AND block_end IS NULL", tableName);

    // Step 2: Insert new versions
    final String insertSql =
        String.format(
            "INSERT INTO %s (key, value, block_start, block_end) VALUES (?, ?, ?, NULL)",
            tableName);

    try (PreparedStatement updatePs = conn.prepareStatement(updateSql);
        PreparedStatement insertPs = conn.prepareStatement(insertSql)) {

      // First: batch all UPDATEs to close old versions
      for (Operation op : segmentOps) {
        updatePs.setLong(1, blockNumber);
        updatePs.setBytes(2, op.key);
        updatePs.addBatch();
      }
      updatePs.executeBatch();

      // Second: batch all INSERTs for PUT operations
      for (Operation op : segmentOps) {
        if (op instanceof PutOperation put) {
          insertPs.setBytes(1, put.key);
          insertPs.setBytes(2, put.value);
          insertPs.setLong(3, blockNumber);
          insertPs.addBatch();
        }
        // RemoveOperation just closed the old version above
      }
      insertPs.executeBatch();

      LOG.debug(
          "Executed batch for segment {} with {} operations", segment.getName(), segmentOps.size());
    }
  }

  @Override
  public void rollback() {
    operations.clear();
    LOG.debug("Transaction rolled back");
  }

  @Override
  public void close() {
    operations.clear();
  }

  private int getTotalOperationCount() {
    return operations.values().stream().mapToInt(List::size).sum();
  }

  private void closeQuietly(final AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Exception e) {
        LOG.warn("Error closing resource", e);
      }
    }
  }

  /** Base class for operations. */
  private abstract static class Operation {
    final byte[] key;

    Operation(final byte[] key) {
      this.key = key;
    }
  }

  /** Put operation. */
  private static class PutOperation extends Operation {
    final byte[] value;

    PutOperation(final byte[] key, final byte[] value) {
      super(key);
      this.value = value;
    }
  }

  /** Remove operation. */
  private static class RemoveOperation extends Operation {
    RemoveOperation(final byte[] key) {
      super(key);
    }
  }
}
