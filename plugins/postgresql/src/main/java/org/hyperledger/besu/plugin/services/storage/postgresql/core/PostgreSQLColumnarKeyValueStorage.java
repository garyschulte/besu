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

import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.postgresql.connection.PostgreSQLConnectionManager;
import org.hyperledger.besu.plugin.services.storage.postgresql.util.PostgreSQLExceptionMapper;
import org.hyperledger.besu.plugin.services.storage.postgresql.util.PostgreSQLSchemaManager;

import static java.util.stream.Collectors.toUnmodifiableSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** PostgreSQL columnar key-value storage implementation with temporal versioning. */
public class PostgreSQLColumnarKeyValueStorage implements SnappableKeyValueStorage {
  private static final Logger LOG =
      LoggerFactory.getLogger(PostgreSQLColumnarKeyValueStorage.class);

  private final PostgreSQLConnectionManager connectionManager;
  private final PostgreSQLSchemaManager schemaManager;
  private final PostgreSQLPartitionManager partitionManager;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicLong currentBlock = new AtomicLong(0);

  /**
   * Instantiates a new PostgreSQL columnar key value storage.
   *
   * @param connectionManager the connection manager
   * @param schemaManager the schema manager
   * @param partitionManager the partition manager
   * @param segments the segments
   * @param metricsSystem the metrics system
   */
  public PostgreSQLColumnarKeyValueStorage(
      final PostgreSQLConnectionManager connectionManager,
      final PostgreSQLSchemaManager schemaManager,
      final PostgreSQLPartitionManager partitionManager,
      final List<SegmentIdentifier> segments,
      final MetricsSystem metricsSystem) {
    this.connectionManager = connectionManager;
    this.schemaManager = schemaManager;
    this.partitionManager = partitionManager;

    // Create tables for all segments
    for (SegmentIdentifier segment : segments) {
      schemaManager.createSegmentTable(segment);
    }

    LOG.info("PostgreSQL columnar storage initialized with {} segments", segments.size());
  }

  /**
   * Sets current block number.
   *
   * @param blockNumber the block number
   */
  public void setCurrentBlock(final long blockNumber) {
    this.currentBlock.set(blockNumber);
    LOG.debug("Current block set to {}", blockNumber);
  }

  /**
   * Gets current block number.
   *
   * @return the current block number
   */
  public long getCurrentBlock() {
    return currentBlock.get();
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    throwIfClosed();

    final String tableName = schemaManager.getTableName(segment);
    final String sql =
        String.format(
            "SELECT value FROM %s WHERE key = ? AND block_end IS NULL", tableName);

    try (Connection conn = connectionManager.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {

      ps.setBytes(1, key);

      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return Optional.of(rs.getBytes("value"));
        }
        return Optional.empty();
      }
    } catch (SQLException e) {
      throw PostgreSQLExceptionMapper.map("Failed to get value", e);
    }
  }

  /**
   * Get value at specific block number (for snapshots).
   *
   * @param segment the segment
   * @param key the key
   * @param blockNumber the block number
   * @return the value at that block
   */
  public Optional<byte[]> getAtBlock(
      final SegmentIdentifier segment, final byte[] key, final long blockNumber) {
    throwIfClosed();

    final String tableName = schemaManager.getTableName(segment);
    final String sql =
        String.format(
            """
            SELECT value FROM %s
            WHERE key = ?
              AND block_start <= ?
              AND (block_end IS NULL OR block_end > ?)
            ORDER BY block_start DESC
            LIMIT 1
            """,
            tableName);

    try (Connection conn = connectionManager.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {

      ps.setBytes(1, key);
      ps.setLong(2, blockNumber);
      ps.setLong(3, blockNumber);

      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return Optional.of(rs.getBytes("value"));
        }
        return Optional.empty();
      }
    } catch (SQLException e) {
      throw PostgreSQLExceptionMapper.map("Failed to get value at block", e);
    }
  }

  @Override
  public Optional<NearestKeyValue> getNearestBefore(
      final SegmentIdentifier segment, final Bytes key) throws StorageException {
    throwIfClosed();

    final String tableName = schemaManager.getTableName(segment);
    final String sql =
        String.format(
            """
            SELECT key, value FROM %s
            WHERE key <= ? AND block_end IS NULL
            ORDER BY key DESC
            LIMIT 1
            """,
            tableName);

    try (Connection conn = connectionManager.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {

      ps.setBytes(1, key.toArrayUnsafe());

      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return Optional.of(
              new NearestKeyValue(
                  Bytes.wrap(rs.getBytes("key")), Optional.of(rs.getBytes("value"))));
        }
        return Optional.empty();
      }
    } catch (SQLException e) {
      throw PostgreSQLExceptionMapper.map("Failed to get nearest before", e);
    }
  }

  @Override
  public Optional<NearestKeyValue> getNearestAfter(
      final SegmentIdentifier segment, final Bytes key) throws StorageException {
    throwIfClosed();

    final String tableName = schemaManager.getTableName(segment);
    final String sql =
        String.format(
            """
            SELECT key, value FROM %s
            WHERE key >= ? AND block_end IS NULL
            ORDER BY key ASC
            LIMIT 1
            """,
            tableName);

    try (Connection conn = connectionManager.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {

      ps.setBytes(1, key.toArrayUnsafe());

      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return Optional.of(
              new NearestKeyValue(
                  Bytes.wrap(rs.getBytes("key")), Optional.of(rs.getBytes("value"))));
        }
        return Optional.empty();
      }
    } catch (SQLException e) {
      throw PostgreSQLExceptionMapper.map("Failed to get nearest after", e);
    }
  }

  @Override
  public SegmentedKeyValueStorageTransaction startTransaction() throws StorageException {
    throwIfClosed();
    return new PostgreSQLTransaction(
        connectionManager, schemaManager, partitionManager, currentBlock.get());
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segment) {
    throwIfClosed();

    final String tableName = schemaManager.getTableName(segment);
    final String sql =
        String.format(
            "SELECT key, value FROM %s WHERE block_end IS NULL ORDER BY key", tableName);

    return createStream(sql);
  }

  /**
   * Stream at specific block number (for snapshots).
   *
   * @param segment the segment
   * @param blockNumber the block number
   * @return stream of key-value pairs
   */
  public Stream<Pair<byte[], byte[]>> streamAtBlock(
      final SegmentIdentifier segment, final long blockNumber) {
    throwIfClosed();

    final String tableName = schemaManager.getTableName(segment);
    // Use DISTINCT ON to get latest version per key at the block
    final String sql =
        String.format(
            """
            SELECT DISTINCT ON (key) key, value FROM %s
            WHERE block_start <= ? AND (block_end IS NULL OR block_end > ?)
            ORDER BY key, block_start DESC
            """,
            tableName);

    try {
      final Connection conn = connectionManager.getConnection();
      final PreparedStatement ps = conn.prepareStatement(sql);
      ps.setFetchSize(1000); // Cursor-based streaming
      ps.setLong(1, blockNumber);
      ps.setLong(2, blockNumber);

      final ResultSet rs = ps.executeQuery();

      return StreamSupport.stream(new ResultSetSpliterator(rs), false)
          .onClose(
              () -> {
                closeQuietly(rs);
                closeQuietly(ps);
                closeQuietly(conn);
              });
    } catch (SQLException e) {
      throw PostgreSQLExceptionMapper.map("Failed to create stream at block", e);
    }
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segment, final byte[] startKey) {
    throwIfClosed();

    final String tableName = schemaManager.getTableName(segment);
    final String sql =
        String.format(
            "SELECT key, value FROM %s WHERE key >= ? AND block_end IS NULL ORDER BY key",
            tableName);

    return createStreamWithKey(sql, startKey);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segment, final byte[] startKey, final byte[] endKey) {
    throwIfClosed();

    final String tableName = schemaManager.getTableName(segment);
    final String sql =
        String.format(
            "SELECT key, value FROM %s WHERE key >= ? AND key < ? AND block_end IS NULL ORDER BY key",
            tableName);

    try {
      final Connection conn = connectionManager.getConnection();
      final PreparedStatement ps = conn.prepareStatement(sql);
      ps.setFetchSize(1000);
      ps.setBytes(1, startKey);
      ps.setBytes(2, endKey);

      final ResultSet rs = ps.executeQuery();

      return StreamSupport.stream(new ResultSetSpliterator(rs), false)
          .onClose(
              () -> {
                closeQuietly(rs);
                closeQuietly(ps);
                closeQuietly(conn);
              });
    } catch (SQLException e) {
      throw PostgreSQLExceptionMapper.map("Failed to create stream with range", e);
    }
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segment) {
    throwIfClosed();

    final String tableName = schemaManager.getTableName(segment);
    final String sql =
        String.format("SELECT key FROM %s WHERE block_end IS NULL ORDER BY key", tableName);

    try {
      final Connection conn = connectionManager.getConnection();
      final PreparedStatement ps = conn.prepareStatement(sql);
      ps.setFetchSize(1000);
      final ResultSet rs = ps.executeQuery();

      return StreamSupport.stream(
              new java.util.Spliterators.AbstractSpliterator<byte[]>(
                  Long.MAX_VALUE, java.util.Spliterator.ORDERED) {
                @Override
                public boolean tryAdvance(final java.util.function.Consumer<? super byte[]> action) {
                  try {
                    if (rs.next()) {
                      action.accept(rs.getBytes("key"));
                      return true;
                    }
                    return false;
                  } catch (SQLException e) {
                    throw new RuntimeException(e);
                  }
                }
              },
              false)
          .onClose(
              () -> {
                closeQuietly(rs);
                closeQuietly(ps);
                closeQuietly(conn);
              });
    } catch (SQLException e) {
      throw PostgreSQLExceptionMapper.map("Failed to stream keys", e);
    }
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    throwIfClosed();

    final String tableName = schemaManager.getTableName(segment);
    final String sql =
        String.format("UPDATE %s SET block_end = ? WHERE key = ? AND block_end IS NULL", tableName);

    try (Connection conn = connectionManager.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {

      conn.setAutoCommit(true);
      ps.setLong(1, currentBlock.get());
      ps.setBytes(2, key);

      int updated = ps.executeUpdate();
      return updated > 0;
    } catch (SQLException e) {
      // Log but don't throw, as tryDelete should not fail
      LOG.warn("tryDelete failed for key in segment {}", segment.getName(), e);
      return false;
    }
  }

  @Override
  public Set<byte[]> getAllKeysThat(
      final SegmentIdentifier segment, final Predicate<byte[]> returnCondition) {
    throwIfClosed();
    return streamKeys(segment).filter(returnCondition).collect(toUnmodifiableSet());
  }

  @Override
  public Set<byte[]> getAllValuesFromKeysThat(
      final SegmentIdentifier segment, final Predicate<byte[]> returnCondition) {
    throwIfClosed();
    return stream(segment)
        .filter(pair -> returnCondition.test(pair.getKey()))
        .map(Pair::getValue)
        .collect(toUnmodifiableSet());
  }

  @Override
  public void clear(final SegmentIdentifier segment) {
    throwIfClosed();
    schemaManager.clearSegmentTable(segment);
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public SnappedKeyValueStorage takeSnapshot() {
    throwIfClosed();
    final long snapshotBlock = currentBlock.get();
    LOG.debug("Taking snapshot at block {}", snapshotBlock);
    return new PostgreSQLSnapshot(this, snapshotBlock);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      LOG.info("Closing PostgreSQL columnar storage");
      connectionManager.close();
    }
  }

  private void throwIfClosed() {
    if (closed.get()) {
      throw new StorageException("Storage is closed");
    }
  }

  private Stream<Pair<byte[], byte[]>> createStream(final String sql) {
    try {
      final Connection conn = connectionManager.getConnection();
      final PreparedStatement ps = conn.prepareStatement(sql);
      ps.setFetchSize(1000); // Cursor-based streaming
      final ResultSet rs = ps.executeQuery();

      return StreamSupport.stream(new ResultSetSpliterator(rs), false)
          .onClose(
              () -> {
                closeQuietly(rs);
                closeQuietly(ps);
                closeQuietly(conn);
              });
    } catch (SQLException e) {
      throw PostgreSQLExceptionMapper.map("Failed to create stream", e);
    }
  }

  private Stream<Pair<byte[], byte[]>> createStreamWithKey(final String sql, final byte[] key) {
    try {
      final Connection conn = connectionManager.getConnection();
      final PreparedStatement ps = conn.prepareStatement(sql);
      ps.setFetchSize(1000);
      ps.setBytes(1, key);
      final ResultSet rs = ps.executeQuery();

      return StreamSupport.stream(new ResultSetSpliterator(rs), false)
          .onClose(
              () -> {
                closeQuietly(rs);
                closeQuietly(ps);
                closeQuietly(conn);
              });
    } catch (SQLException e) {
      throw PostgreSQLExceptionMapper.map("Failed to create stream with key", e);
    }
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

  /** Spliterator for ResultSet to Stream conversion. */
  private static class ResultSetSpliterator
      extends java.util.Spliterators.AbstractSpliterator<Pair<byte[], byte[]>> {
    private final ResultSet resultSet;

    ResultSetSpliterator(final ResultSet resultSet) {
      super(Long.MAX_VALUE, java.util.Spliterator.ORDERED);
      this.resultSet = resultSet;
    }

    @Override
    public boolean tryAdvance(
        final java.util.function.Consumer<? super Pair<byte[], byte[]>> action) {
      try {
        if (resultSet.next()) {
          final byte[] key = resultSet.getBytes("key");
          final byte[] value = resultSet.getBytes("value");
          action.accept(Pair.of(key, value));
          return true;
        }
        return false;
      } catch (SQLException e) {
        throw new RuntimeException("Error reading from ResultSet", e);
      }
    }
  }
}
