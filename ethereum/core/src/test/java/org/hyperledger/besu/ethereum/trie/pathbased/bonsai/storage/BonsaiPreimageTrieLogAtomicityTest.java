/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class BonsaiPreimageTrieLogAtomicityTest {

  private static final Address ADDRESS = Address.fromHexString("0xdeadbeef");
  private static final Bytes32 ADDRESS_HASH = Bytes32.wrap(ADDRESS.addressHash().getBytes());
  private static final UInt256 SLOT = UInt256.valueOf(42);
  private static final Bytes32 SLOT_HASH = Bytes32.wrap(Hash.hash(SLOT).getBytes());
  private static final byte[] TRIE_LOG_KEY = Hash.hash(Bytes32.ZERO).getBytes().toArrayUnsafe();

  private static BonsaiWorldStateKeyValueStorage storage(final boolean preimagesEnabled) {
    return new BonsaiWorldStateKeyValueStorage(
        new InMemoryKeyValueStorageProvider(),
        new NoOpMetricsSystem(),
        ImmutableDataStorageConfiguration.builder()
            .from(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG)
            .hashPreImageStorageEnabled(preimagesEnabled)
            .build());
  }

  private static void stage(final BonsaiWorldStateKeyValueStorage.Updater updater) {
    updater.getTrieLogStorageTransaction().put(TRIE_LOG_KEY, new byte[] {1});
    updater
        .getPreimageUpdater()
        .orElseThrow()
        .putAccountTrieKeyPreimage(ADDRESS_HASH, ADDRESS)
        .putStorageTrieKeyPreimage(SLOT_HASH, SLOT);
  }

  @Test
  void preimagesCommitWithTrieLog() {
    final var storage = storage(true);
    final var updater = storage.updater();
    stage(updater);

    // nothing visible until the trie log transaction commits
    assertThat(storage.getPreimageStorage().orElseThrow().getAccountTrieKeyPreimage(ADDRESS_HASH))
        .isEmpty();

    updater.commitTrieLogOnly();

    assertThat(storage.getTrieLogStorage().get(TRIE_LOG_KEY)).isPresent();
    final var preimages = storage.getPreimageStorage().orElseThrow();
    assertThat(preimages.getAccountTrieKeyPreimage(ADDRESS_HASH)).contains(ADDRESS);
    assertThat(preimages.getStorageTrieKeyPreimage(SLOT_HASH)).contains(SLOT);
  }

  @Test
  void rollbackDiscardsTrieLogAndPreimagesTogether() {
    final var storage = storage(true);
    final var updater = storage.updater();
    stage(updater);

    updater.rollback();

    assertThat(storage.getTrieLogStorage().get(TRIE_LOG_KEY)).isEmpty();
    final var preimages = storage.getPreimageStorage().orElseThrow();
    assertThat(preimages.getAccountTrieKeyPreimage(ADDRESS_HASH)).isEmpty();
    assertThat(preimages.getStorageTrieKeyPreimage(SLOT_HASH)).isEmpty();
  }

  @Test
  void snapshotUpdaterWritesPreimagesToSharedStore() {
    final var storage = storage(true);
    final var updater = new BonsaiSnapshotWorldStateKeyValueStorage(storage).updater();
    stage(updater);

    updater.commitTrieLogOnly();

    assertThat(storage.getTrieLogStorage().get(TRIE_LOG_KEY)).isPresent();
    assertThat(storage.getPreimageStorage().orElseThrow().getAccountTrieKeyPreimage(ADDRESS_HASH))
        .contains(ADDRESS);
  }

  @Test
  void noPreimageUpdaterWhenDisabled() {
    final var storage = storage(false);

    assertThat(storage.getPreimageStorage()).isEmpty();
    assertThat(storage.updater().getPreimageUpdater()).isEmpty();
  }
}
