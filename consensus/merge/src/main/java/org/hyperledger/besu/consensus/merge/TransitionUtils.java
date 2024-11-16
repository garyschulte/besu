/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.consensus.merge;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

  public class TransitionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TransitionUtils.class);

  /**
   * Is terminal proof of work block boolean.
   *
   * @param header the header
   * @param context the context
   * @return the boolean
   */
  public static boolean isTerminalProofOfWorkBlock(
      final ProcessableBlockHeader header, final ProtocolContext context) {

    Difficulty headerDifficulty =
        Optional.ofNullable(header.getDifficulty()).orElse(Difficulty.ZERO);

    Difficulty currentChainTotalDifficulty =
        context
            .getBlockchain()
            .getTotalDifficultyByHash(header.getParentHash())
            // if we cannot find difficulty or are merge-at-genesis
            .orElse(Difficulty.ZERO);

    final MergeContext consensusContext = context.getConsensusContext(MergeContext.class);

    // Genesis is configured for post-merge we will never have a terminal pow block
    if (consensusContext.isPostMergeAtGenesis()) {
      return false;
    }

    if (currentChainTotalDifficulty.isZero()) {
      LOG.atWarn()
          .setMessage("unable to get total difficulty for {}, parent hash {} difficulty not found")
          .addArgument(header::toLogString)
          .addArgument(header::getParentHash)
          .log();
    }
    Difficulty configuredTotalTerminalDifficulty = consensusContext.getTerminalTotalDifficulty();

    if (currentChainTotalDifficulty
            .add(headerDifficulty)
            .greaterOrEqualThan(
                configuredTotalTerminalDifficulty) // adding would equal or go over limit
        && currentChainTotalDifficulty.lessThan(
            configuredTotalTerminalDifficulty) // parent was under
    ) {
      return true;
    }

    // return true for genesis block when merge-at-genesis, otherwise false
    return header.getNumber() == 0L
        && header.getDifficulty().greaterOrEqualThan(configuredTotalTerminalDifficulty);
  }
}
