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

import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.postgresql.connection.PostgreSQLConnectionManager;
import org.hyperledger.besu.plugin.services.storage.postgresql.core.PostgreSQLColumnarKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.postgresql.core.PostgreSQLPartitionManager;
import org.hyperledger.besu.plugin.services.storage.postgresql.util.PostgreSQLSchemaManager;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Integration tests for PostgreSQL columnar storage. */
class PostgreSQLColumnarStorageTest extends AbstractPostgreSQLTest {

  private PostgreSQLConnectionManager connectionManager;
  private PostgreSQLSchemaManager schemaManager;
  private PostgreSQLPartitionManager partitionManager;
  private PostgreSQLColumnarKeyValueStorage storage;
  private TestSegment testSegment;

  @BeforeEach
  void setup() {
    connectionManager = createConnectionManager();
    schemaManager = new PostgreSQLSchemaManager(connectionManager, TEST_SCHEMA);
    partitionManager = new PostgreSQLPartitionManager(TEST_SCHEMA);
    testSegment = new TestSegment("test_data");

    schemaManager.initializeSchema();

    final MetricsSystem metricsSystem = Mockito.mock(MetricsSystem.class);
    storage =
        new PostgreSQLColumnarKeyValueStorage(
            connectionManager,
            schemaManager,
            partitionManager,
            List.of(testSegment),
            metricsSystem);

    storage.setCurrentBlock(1);
  }

  @AfterEach
  void cleanup() {
    if (storage != null && !storage.isClosed()) {
      // Clear test data
      try {
        storage.clear(testSegment);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
      storage.close();
    }
    if (connectionManager != null && !connectionManager.isClosed()) {
      connectionManager.close();
    }
  }

  @Test
  void testPutAndGet() {
    final byte[] key = bytes("0001");
    final byte[] value = bytes("1234567890abcdef");

    // Put value
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key, value);
    tx.commit();

    // Get value
    final var result = storage.get(testSegment, key);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(value);
  }

  @Test
  void testGetNonExistent() {
    final byte[] key = bytes("9999");

    final var result = storage.get(testSegment, key);

    assertThat(result).isEmpty();
  }

  @Test
  void testUpdate() {
    final byte[] key = bytes("0001");
    final byte[] value1 = bytes("1111");
    final byte[] value2 = bytes("2222");

    // Initial put
    storage.setCurrentBlock(1);
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key, value1);
    tx.commit();

    // Verify initial value
    assertThat(storage.get(testSegment, key)).contains(value1);

    // Update value
    storage.setCurrentBlock(2);
    tx = storage.startTransaction();
    tx.put(testSegment, key, value2);
    tx.commit();

    // Verify updated value
    assertThat(storage.get(testSegment, key)).contains(value2);
  }

  @Test
  void testRemove() {
    final byte[] key = bytes("0001");
    final byte[] value = bytes("1111");

    // Put value
    storage.setCurrentBlock(1);
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key, value);
    tx.commit();

    // Verify exists
    assertThat(storage.get(testSegment, key)).isPresent();

    // Remove
    storage.setCurrentBlock(2);
    tx = storage.startTransaction();
    tx.remove(testSegment, key);
    tx.commit();

    // Verify removed
    assertThat(storage.get(testSegment, key)).isEmpty();
  }

  @Test
  void testMultipleKeysInTransaction() {
    final byte[] key1 = bytes("0001");
    final byte[] key2 = bytes("0002");
    final byte[] key3 = bytes("0003");
    final byte[] value1 = bytes("1111");
    final byte[] value2 = bytes("2222");
    final byte[] value3 = bytes("3333");

    // Put multiple values in one transaction
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key1, value1);
    tx.put(testSegment, key2, value2);
    tx.put(testSegment, key3, value3);
    tx.commit();

    // Verify all values
    assertThat(storage.get(testSegment, key1)).contains(value1);
    assertThat(storage.get(testSegment, key2)).contains(value2);
    assertThat(storage.get(testSegment, key3)).contains(value3);
  }

  @Test
  void testTransactionRollback() {
    final byte[] key = bytes("0001");
    final byte[] value = bytes("1111");

    // Put value but rollback
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key, value);
    tx.rollback();

    // Verify value was not committed
    assertThat(storage.get(testSegment, key)).isEmpty();
  }

  @Test
  void testStream() {
    // Insert test data
    final byte[] key1 = bytes("0001");
    final byte[] key2 = bytes("0002");
    final byte[] key3 = bytes("0003");
    final byte[] value1 = bytes("1111");
    final byte[] value2 = bytes("2222");
    final byte[] value3 = bytes("3333");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key1, value1);
    tx.put(testSegment, key2, value2);
    tx.put(testSegment, key3, value3);
    tx.commit();

    // Stream all values
    final List<Pair<byte[], byte[]>> results;
    try (final var stream = storage.stream(testSegment)) {
      results = stream.toList();
    }

    assertThat(results).hasSize(3);
    assertThat(results.stream().map(Pair::getKey)).containsExactly(key1, key2, key3);
  }

  @Test
  void testStreamKeys() {
    // Insert test data
    final byte[] key1 = bytes("0001");
    final byte[] key2 = bytes("0002");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key1, bytes("1111"));
    tx.put(testSegment, key2, bytes("2222"));
    tx.commit();

    // Stream keys
    final List<byte[]> keys;
    try (final var stream = storage.streamKeys(testSegment)) {
      keys = stream.toList();
    }

    assertThat(keys).hasSize(2);
    assertThat(keys).containsExactly(key1, key2);
  }

  @Test
  void testStreamFromKey() {
    // Insert test data in sorted order
    final byte[] key1 = bytes("0001");
    final byte[] key2 = bytes("0002");
    final byte[] key3 = bytes("0003");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key1, bytes("1111"));
    tx.put(testSegment, key2, bytes("2222"));
    tx.put(testSegment, key3, bytes("3333"));
    tx.commit();

    // Stream from key2
    final List<Pair<byte[], byte[]>> results;
    try (final var stream = storage.streamFromKey(testSegment, key2)) {
      results = stream.toList();
    }

    assertThat(results).hasSize(2);
    assertThat(results.stream().map(Pair::getKey)).containsExactly(key2, key3);
  }

  @Test
  void testGetNearestBefore() {
    // Insert test data
    final byte[] key1 = bytes("0001");
    final byte[] key2 = bytes("0003");
    final byte[] key3 = bytes("0005");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key1, bytes("1111"));
    tx.put(testSegment, key2, bytes("3333"));
    tx.put(testSegment, key3, bytes("5555"));
    tx.commit();

    // Find nearest before key 0004 (should be key2)
    final var result = storage.getNearestBefore(testSegment, Bytes.wrap(bytes("0004")));

    assertThat(result).isPresent();
    assertThat(result.get().key().toArrayUnsafe()).isEqualTo(key2);
    assertThat(result.get().value()).contains(bytes("3333"));
  }

  @Test
  void testGetNearestAfter() {
    // Insert test data
    final byte[] key1 = bytes("0001");
    final byte[] key2 = bytes("0003");
    final byte[] key3 = bytes("0005");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key1, bytes("1111"));
    tx.put(testSegment, key2, bytes("3333"));
    tx.put(testSegment, key3, bytes("5555"));
    tx.commit();

    // Find nearest after key 0002 (should be key2)
    final var result = storage.getNearestAfter(testSegment, Bytes.wrap(bytes("0002")));

    assertThat(result).isPresent();
    assertThat(result.get().key().toArrayUnsafe()).isEqualTo(key2);
    assertThat(result.get().value()).contains(bytes("3333"));
  }

  @Test
  void testTryDelete() {
    final byte[] key = bytes("0001");
    final byte[] value = bytes("1111");

    // Put value
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key, value);
    tx.commit();

    // Try delete
    storage.setCurrentBlock(2);
    final boolean deleted = storage.tryDelete(testSegment, key);

    assertThat(deleted).isTrue();
    assertThat(storage.get(testSegment, key)).isEmpty();
  }

  @Test
  void testClear() {
    // Insert test data
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, bytes("0001"), bytes("1111"));
    tx.put(testSegment, bytes("0002"), bytes("2222"));
    tx.commit();

    // Verify data exists
    try (final var stream = storage.stream(testSegment)) {
      assertThat(stream.count()).isEqualTo(2);
    }

    // Clear
    storage.clear(testSegment);

    // Verify empty
    try (final var stream = storage.stream(testSegment)) {
      assertThat(stream.count()).isZero();
    }
  }

  @Test
  void testGetAllKeysThat() {
    // Insert test data
    final byte[] key1 = bytes("0001");
    final byte[] key2 = bytes("0101");
    final byte[] key3 = bytes("0201");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key1, bytes("1111"));
    tx.put(testSegment, key2, bytes("2222"));
    tx.put(testSegment, key3, bytes("3333"));
    tx.commit();

    // Get all keys that start with 0x01
    final var keys = storage.getAllKeysThat(testSegment, key -> key[0] == (byte) 0x01);

    assertThat(keys).hasSize(1);
    assertThat(keys).anyMatch(key -> Arrays.equals(key, key2));
  }

  @Test
  void testIsClosed() {
    assertThat(storage.isClosed()).isFalse();

    storage.close();

    assertThat(storage.isClosed()).isTrue();
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
