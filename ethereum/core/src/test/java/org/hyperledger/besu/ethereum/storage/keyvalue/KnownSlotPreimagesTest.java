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
package org.hyperledger.besu.ethereum.storage.keyvalue;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class KnownSlotPreimagesTest {

  @Test
  void sequentialSlotsAreKnownUpToTheLimit() {
    assertThat(KnownSlotPreimages.isKnown(UInt256.ZERO)).isTrue();
    assertThat(KnownSlotPreimages.isKnown(UInt256.valueOf(1023))).isTrue();
    assertThat(KnownSlotPreimages.isKnown(UInt256.valueOf(1024))).isFalse();
    assertThat(KnownSlotPreimages.isKnown(UInt256.MAX_VALUE)).isFalse();
  }

  @Test
  void eip1967SlotsAreKnown() {
    final UInt256 implementation =
        UInt256.fromHexString("0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc");
    final UInt256 admin =
        UInt256.fromHexString("0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103");

    assertThat(KnownSlotPreimages.isKnown(implementation)).isTrue();
    assertThat(KnownSlotPreimages.isKnown(admin)).isTrue();
  }

  @Test
  void everyKnownSlotResolvesFromItsHash() {
    assertThat(KnownSlotPreimages.all().count()).isGreaterThan(1024);
    KnownSlotPreimages.all()
        .forEach(
            slot -> {
              assertThat(KnownSlotPreimages.isKnown(slot)).isTrue();
              assertThat(KnownSlotPreimages.lookup(Bytes32.wrap(Hash.hash(slot).getBytes())))
                  .contains(slot);
            });
  }

  @Test
  void unknownHashDoesNotResolve() {
    assertThat(
            KnownSlotPreimages.lookup(Bytes32.wrap(Hash.hash(UInt256.valueOf(99_999)).getBytes())))
        .isEmpty();
  }
}
