/*
 * Copyright Hyperledger Besu Contributors.
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
package org.hyperledger.besu.plugin.services.sync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.data.BlockBody;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.BesuService;

/**
 * A service for managing the synchronization of the blockchain.
 */
public interface SynchronizationService extends BesuService {

  /**
   * Fires a new unverified forkchoice event.
   *
   * @param head hed
   * @param safeBlock safe
   * @param finalizedBlock  fin
   */
  void fireNewUnverifiedForkchoiceEvent(Hash head, Hash safeBlock, Hash finalizedBlock);

  /**
   * Sets the head of the blockchain to the given block header and body.
   *
   * @param blockHeader head
   * @param blockBody bod
   * @return bool
   */
  boolean setHead(final BlockHeader blockHeader, final BlockBody blockBody);

  /**
   * Sets the head of the blockchain to the given block header and body.
   * @param blockHeader hed
   * @param blockBody bod
   * @return bool
   */
  boolean setHeadUnsafe(BlockHeader blockHeader, BlockBody blockBody);

  /**
   * Returns whether the trie is disabled.
   *
   * @return true if the trie is disabled, false otherwise.
   */
  boolean isInitialSyncPhaseDone();

  /**
   * Disables the world state trie.
   */
  void disableWorldStateTrie();
}
