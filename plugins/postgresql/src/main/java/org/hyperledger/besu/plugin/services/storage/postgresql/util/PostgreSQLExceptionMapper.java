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

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Maps SQL exceptions to storage exceptions with appropriate handling. */
public class PostgreSQLExceptionMapper {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLExceptionMapper.class);

  /**
   * Map SQLException to StorageException.
   *
   * @param e the SQL exception
   * @return the storage exception
   */
  public static StorageException map(final SQLException e) {
    final String sqlState = e.getSQLState();
    final String message = e.getMessage();

    LOG.debug("Mapping SQLException: SQLState={}, Message={}", sqlState, message);

    // Connection errors (08xxx)
    if (sqlState != null && sqlState.startsWith("08")) {
      return new StorageException("Database connection error: " + message, e);
    }

    // Serialization failure (40001)
    if ("40001".equals(sqlState)) {
      return new StorageException("Transaction serialization failure, retry may help: " + message, e);
    }

    // Deadlock (40P01)
    if ("40P01".equals(sqlState)) {
      return new StorageException("Database deadlock detected: " + message, e);
    }

    // Disk full (53100)
    if ("53100".equals(sqlState)) {
      LOG.error("PostgreSQL disk full error detected. Exiting.");
      System.exit(0); // Like RocksDB behavior
    }

    // Insufficient resources (53xxx)
    if (sqlState != null && sqlState.startsWith("53")) {
      return new StorageException("Database insufficient resources: " + message, e);
    }

    // Constraint violation (23xxx)
    if (sqlState != null && sqlState.startsWith("23")) {
      return new StorageException("Database constraint violation: " + message, e);
    }

    // Generic error
    return new StorageException("Database error: " + message, e);
  }

  /**
   * Map SQLException to StorageException with custom message.
   *
   * @param customMessage the custom message
   * @param e the SQL exception
   * @return the storage exception
   */
  public static StorageException map(final String customMessage, final SQLException e) {
    final StorageException mapped = map(e);
    return new StorageException(customMessage + ": " + mapped.getMessage(), mapped.getCause());
  }

  /**
   * Check if exception is retryable.
   *
   * @param e the exception
   * @return true if retryable
   */
  public static boolean isRetryable(final Throwable e) {
    if (e instanceof SQLException sqlException) {
      final String sqlState = sqlException.getSQLState();
      // Serialization failures and deadlocks are retryable
      return "40001".equals(sqlState) || "40P01".equals(sqlState);
    }
    return false;
  }
}
