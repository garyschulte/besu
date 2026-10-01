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
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/** Asserts on the raw preimage segment, bypassing the known-slot resolution of the read path. */
class BonsaiPreimageWriteFilterTest {

  private static final Address A = Address.fromHexString("0xa0");
  private static final Address B = Address.fromHexString("0xb0");
  private static final UInt256 UNKNOWN_SLOT = UInt256.valueOf(5000);
  private static final UInt256 OTHER_UNKNOWN_SLOT = UInt256.valueOf(7000);

  private BonsaiWorldState worldState() {
    final var archive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(
            InMemoryKeyValueStorageProvider.createInMemoryBlockchain(
                new BlockDataGenerator().genesisBlock()),
            EvmConfiguration.DEFAULT,
            null,
            ImmutableDataStorageConfiguration.builder()
                .from(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG)
                .hashPreImageStorageEnabled(true)
                .build());
    final BonsaiWorldState state = (BonsaiWorldState) archive.getWorldState();
    state.disableTrie();
    return state;
  }

  @Test
  void newKeysAreWrittenKnownSlotsAndExistingKeysAreNot() {
    final BonsaiWorldState state = worldState();
    final KeyValueStorage raw =
        new SegmentedKeyValueStorageAdapter(
            KeyValueSegmentIdentifier.HASH_PREIMAGE_STORE,
            state.getWorldStateStorage().trieLogAndPreimageStorage);

    // block 1: two new accounts, a known slot, and an unknown slot shared by both accounts
    WorldUpdater updater = state.updater();
    MutableAccount a = updater.createAccount(A, 1, Wei.of(1));
    a.setStorageValue(UInt256.ZERO, UInt256.ONE);
    a.setStorageValue(UNKNOWN_SLOT, UInt256.ONE);
    MutableAccount b = updater.createAccount(B, 1, Wei.of(1));
    b.setStorageValue(UNKNOWN_SLOT, UInt256.ONE);
    updater.commit();
    state.persist(new BlockHeaderTestFixture().number(1L).buildHeader());

    assertThat(raw.get(A.addressHash().getBytes().toArrayUnsafe())).isPresent();
    assertThat(raw.get(B.addressHash().getBytes().toArrayUnsafe())).isPresent();
    assertThat(raw.get(Hash.hash(UNKNOWN_SLOT).getBytes().toArrayUnsafe())).isPresent();
    assertThat(raw.get(Hash.hash(UInt256.ZERO).getBytes().toArrayUnsafe())).isEmpty();
    assertThat(raw.stream().count()).isEqualTo(3);

    // drop A's preimage so that a rewrite would be observable
    final var tx = raw.startTransaction();
    tx.remove(A.addressHash().getBytes().toArrayUnsafe());
    tx.commit();

    // block 2: update existing account and slot, and add one new slot
    updater = state.updater();
    a = updater.getAccount(A);
    a.setBalance(Wei.of(2));
    a.setStorageValue(UNKNOWN_SLOT, UInt256.valueOf(2));
    a.setStorageValue(OTHER_UNKNOWN_SLOT, UInt256.ONE);
    updater.commit();
    state.persist(new BlockHeaderTestFixture().number(2L).buildHeader());

    assertThat(raw.get(A.addressHash().getBytes().toArrayUnsafe()))
        .as("existing account is not rewritten")
        .isEmpty();
    assertThat(raw.get(Hash.hash(OTHER_UNKNOWN_SLOT).getBytes().toArrayUnsafe()))
        .as("new slot key is written")
        .isPresent();
  }
}
