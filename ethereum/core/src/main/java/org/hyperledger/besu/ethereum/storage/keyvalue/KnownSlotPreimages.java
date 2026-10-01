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

import org.hyperledger.besu.datatypes.Hash;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Storage slot keys that are used by a very large number of contracts (low sequential slots and
 * well known proxy slots). Their preimages are never written to the preimage store: they are
 * resolved from this table instead, so the same few entries are not rewritten for every contract
 * that touches them.
 *
 * <p>Changing this set only ever adds entries that are resolved from memory; entries that were
 * already persisted before being added here remain valid, so the set may safely grow over time.
 */
public final class KnownSlotPreimages {

  /** Slots {@code 0 .. SEQUENTIAL_SLOT_COUNT - 1} are known. */
  static final int SEQUENTIAL_SLOT_COUNT = 1024;

  private static final Set<UInt256> WELL_KNOWN_SLOTS = createWellKnownSlots();
  private static final Map<Bytes32, UInt256> BY_HASH = createHashLookup();

  private KnownSlotPreimages() {}

  /**
   * Whether the slot key's preimage is resolved from this table rather than from storage.
   *
   * @param slotKey the storage slot key
   * @return true if the slot key is known
   */
  public static boolean isKnown(final UInt256 slotKey) {
    if (slotKey.fitsInt()) {
      final int value = slotKey.intValue();
      return value >= 0 && value < SEQUENTIAL_SLOT_COUNT;
    }
    return WELL_KNOWN_SLOTS.contains(slotKey);
  }

  /**
   * Resolve a known slot key from its hash.
   *
   * @param slotHash the hashed slot key
   * @return the slot key, if known
   */
  public static Optional<UInt256> lookup(final Bytes32 slotHash) {
    return Optional.ofNullable(BY_HASH.get(slotHash));
  }

  /**
   * All known slot keys.
   *
   * @return a stream of every known slot key
   */
  public static Stream<UInt256> all() {
    return BY_HASH.values().stream();
  }

  private static Set<UInt256> createWellKnownSlots() {
    return Set.of(
        // EIP-1967: keccak256(label) - 1
        eip1967Slot("eip1967.proxy.implementation"),
        eip1967Slot("eip1967.proxy.admin"),
        eip1967Slot("eip1967.proxy.beacon"),
        eip1967Slot("eip1967.proxy.rollback"),
        // EIP-1822: keccak256("PROXIABLE")
        UInt256.fromBytes(hashOf("PROXIABLE")));
  }

  private static Map<Bytes32, UInt256> createHashLookup() {
    final Map<Bytes32, UInt256> byHash = new HashMap<>();
    for (int i = 0; i < SEQUENTIAL_SLOT_COUNT; i++) {
      register(byHash, UInt256.valueOf(i));
    }
    WELL_KNOWN_SLOTS.forEach(slot -> register(byHash, slot));
    return Map.copyOf(byHash);
  }

  private static void register(final Map<Bytes32, UInt256> byHash, final UInt256 slot) {
    byHash.put(Bytes32.wrap(Hash.hash(slot).getBytes()), slot);
  }

  private static UInt256 eip1967Slot(final String label) {
    return UInt256.fromBytes(hashOf(label)).subtract(1);
  }

  private static Bytes32 hashOf(final String label) {
    return Bytes32.wrap(Hash.hash(Bytes.wrap(label.getBytes(StandardCharsets.UTF_8))).getBytes());
  }
}
