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
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.postgresql.connection.PostgreSQLConnectionManager;
import org.hyperledger.besu.plugin.services.storage.postgresql.core.PostgreSQLColumnarKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.postgresql.core.PostgreSQLPartitionManager;
import org.hyperledger.besu.plugin.services.storage.postgresql.core.PostgreSQLSnapshot;
import org.hyperledger.besu.plugin.services.storage.postgresql.util.PostgreSQLSchemaManager;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Integration tests for PostgreSQL snapshot functionality. */
class PostgreSQLSnapshotTest extends AbstractPostgreSQLTest {

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
    testSegment = new TestSegment("snapshot_test");

    schemaManager.initializeSchema();

    final MetricsSystem metricsSystem = Mockito.mock(MetricsSystem.class);
    storage =
        new PostgreSQLColumnarKeyValueStorage(
            connectionManager,
            schemaManager,
            partitionManager,
            List.of(testSegment),
            metricsSystem);
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
  void testTakeSnapshot() {
    storage.setCurrentBlock(100);

    final SnappedKeyValueStorage snapshot = storage.takeSnapshot();

    assertThat(snapshot).isNotNull();
    assertThat(snapshot).isInstanceOf(PostgreSQLSnapshot.class);

    final PostgreSQLSnapshot pgSnapshot = (PostgreSQLSnapshot) snapshot;
    assertThat(pgSnapshot.getBlockNumber()).isEqualTo(100);
  }

  @Test
  void testSnapshotReadAtPointInTime() {
    final byte[] key = bytes("0001");
    final byte[] value1 = bytes("1111");
    final byte[] value2 = bytes("2222");
    final byte[] value3 = bytes("3333");

    // Block 1: Insert initial value
    storage.setCurrentBlock(1);
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key, value1);
    tx.commit();

    // Take snapshot at block 1
    final SnappedKeyValueStorage snapshot1 = storage.takeSnapshot();

    // Block 2: Update value
    storage.setCurrentBlock(2);
    tx = storage.startTransaction();
    tx.put(testSegment, key, value2);
    tx.commit();

    // Take snapshot at block 2
    final SnappedKeyValueStorage snapshot2 = storage.takeSnapshot();

    // Block 3: Update value again
    storage.setCurrentBlock(3);
    tx = storage.startTransaction();
    tx.put(testSegment, key, value3);
    tx.commit();

    // Verify current storage sees latest value
    assertThat(storage.get(testSegment, key)).contains(value3);

    // Verify snapshot1 sees value from block 1
    assertThat(snapshot1.get(testSegment, key)).contains(value1);

    // Verify snapshot2 sees value from block 2
    assertThat(snapshot2.get(testSegment, key)).contains(value2);
  }

  @Test
  void testSnapshotIsolation() {
    final byte[] key = bytes("0001");
    final byte[] value1 = bytes("1111");
    final byte[] value2 = bytes("2222");

    // Block 1: Insert value
    storage.setCurrentBlock(1);
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key, value1);
    tx.commit();

    // Take snapshot
    final SnappedKeyValueStorage snapshot = storage.takeSnapshot();

    // Update value in current storage
    storage.setCurrentBlock(2);
    tx = storage.startTransaction();
    tx.put(testSegment, key, value2);
    tx.commit();

    // Current storage sees new value
    assertThat(storage.get(testSegment, key)).contains(value2);

    // Snapshot still sees old value
    assertThat(snapshot.get(testSegment, key)).contains(value1);
  }

  @Test
  void testSnapshotOfDeletedKey() {
    final byte[] key = bytes("0001");
    final byte[] value = bytes("1111");

    // Block 1: Insert value
    storage.setCurrentBlock(1);
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key, value);
    tx.commit();

    // Take snapshot with value
    final SnappedKeyValueStorage snapshot1 = storage.takeSnapshot();
    assertThat(snapshot1.get(testSegment, key)).contains(value);

    // Block 2: Delete value
    storage.setCurrentBlock(2);
    tx = storage.startTransaction();
    tx.remove(testSegment, key);
    tx.commit();

    // Take snapshot after deletion
    final SnappedKeyValueStorage snapshot2 = storage.takeSnapshot();

    // Current storage doesn't see value
    assertThat(storage.get(testSegment, key)).isEmpty();

    // Snapshot2 doesn't see value
    assertThat(snapshot2.get(testSegment, key)).isEmpty();

    // Snapshot1 still sees value (before deletion)
    assertThat(snapshot1.get(testSegment, key)).contains(value);
  }

  @Test
  void testMultipleConcurrentSnapshots() {
    final byte[] key = bytes("0001");

    // Create multiple snapshots at different blocks with different values
    storage.setCurrentBlock(1);
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, key, bytes("0001"));
    tx.commit();
    final SnappedKeyValueStorage snapshot1 = storage.takeSnapshot();

    storage.setCurrentBlock(2);
    tx = storage.startTransaction();
    tx.put(testSegment, key, bytes("0002"));
    tx.commit();
    final SnappedKeyValueStorage snapshot2 = storage.takeSnapshot();

    storage.setCurrentBlock(3);
    tx = storage.startTransaction();
    tx.put(testSegment, key, bytes("0003"));
    tx.commit();
    final SnappedKeyValueStorage snapshot3 = storage.takeSnapshot();

    storage.setCurrentBlock(4);
    tx = storage.startTransaction();
    tx.put(testSegment, key, bytes("0004"));
    tx.commit();

    // Verify all snapshots see their respective values
    assertThat(snapshot1.get(testSegment, key)).contains(bytes("0001"));
    assertThat(snapshot2.get(testSegment, key)).contains(bytes("0002"));
    assertThat(snapshot3.get(testSegment, key)).contains(bytes("0003"));
    assertThat(storage.get(testSegment, key)).contains(bytes("0004"));
  }

  @Test
  void testSnapshotStream() {
    // Insert multiple keys at block 1
    storage.setCurrentBlock(1);
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, bytes("0001"), bytes("1111"));
    tx.put(testSegment, bytes("0002"), bytes("2222"));
    tx.put(testSegment, bytes("0003"), bytes("3333"));
    tx.commit();

    // Take snapshot
    final SnappedKeyValueStorage snapshot = storage.takeSnapshot();

    // Update and add more keys at block 2
    storage.setCurrentBlock(2);
    tx = storage.startTransaction();
    tx.put(testSegment, bytes("0001"), bytes("deadbeef"));
    tx.put(testSegment, bytes("0004"), bytes("4444"));
    tx.commit();

    // Current storage stream sees 4 keys
    try (final var stream = storage.stream(testSegment)) {
      assertThat(stream.count()).isEqualTo(4);
    }

    // Snapshot stream sees 3 keys with original values
    final List<Pair<byte[], byte[]>> snapshotData;
    try (final var stream = snapshot.stream(testSegment)) {
      snapshotData = stream.toList();
    }
    assertThat(snapshotData).hasSize(3);
    assertThat(snapshotData)
        .anyMatch(pair -> java.util.Arrays.equals(pair.getValue(), bytes("1111")));
  }

  @Test
  void testSnapshotStreamKeys() {
    // Insert keys at block 1
    storage.setCurrentBlock(1);
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(testSegment, bytes("0001"), bytes("1111"));
    tx.put(testSegment, bytes("0002"), bytes("2222"));
    tx.commit();

    // Take snapshot
    final SnappedKeyValueStorage snapshot = storage.takeSnapshot();

    // Add more keys at block 2
    storage.setCurrentBlock(2);
    tx = storage.startTransaction();
    tx.put(testSegment, bytes("0003"), bytes("3333"));
    tx.commit();

    // Current storage sees 3 keys
    try (final var stream = storage.streamKeys(testSegment)) {
      assertThat(stream.count()).isEqualTo(3);
    }

    // Snapshot sees 2 keys
    try (final var stream = snapshot.streamKeys(testSegment)) {
      assertThat(stream.count()).isEqualTo(2);
    }
  }

  @Test
  void testSnapshotOfEmptyStorage() {
    storage.setCurrentBlock(1);
    final SnappedKeyValueStorage snapshot = storage.takeSnapshot();

    assertThat(snapshot.get(testSegment, bytes("0001"))).isEmpty();
    try (final var stream = snapshot.stream(testSegment)) {
      assertThat(stream.count()).isZero();
    }
  }

  @Test
  void testSnapshotReadOnlyTransaction() {
    storage.setCurrentBlock(1);
    final SnappedKeyValueStorage snapshot = storage.takeSnapshot();

    final var snapshotTx = snapshot.getSnapshotTransaction();
    assertThat(snapshotTx).isNotNull();

    // Snapshot transactions should not throw on commit/rollback
    snapshotTx.commit();
    snapshotTx.rollback();
    snapshotTx.close();
  }

  @Test
  void testManySnapshots() {
    // Test that we can create many snapshots (Bonsai needs 512+)
    final int snapshotCount = 600;
    final SnappedKeyValueStorage[] snapshots = new SnappedKeyValueStorage[snapshotCount];

    for (int i = 0; i < snapshotCount; i++) {
      storage.setCurrentBlock(i);
      snapshots[i] = storage.takeSnapshot();
    }

    // Verify all snapshots are independent
    for (int i = 0; i < snapshotCount; i++) {
      final PostgreSQLSnapshot snapshot = (PostgreSQLSnapshot) snapshots[i];
      assertThat(snapshot.getBlockNumber()).isEqualTo(i);
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
