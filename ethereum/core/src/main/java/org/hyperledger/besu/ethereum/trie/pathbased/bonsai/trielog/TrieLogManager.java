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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.storage.keyvalue.KnownSlotPreimages;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.PreimageWriteMetrics;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.PreimageWriteMetrics.Outcome;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.TrieLogService;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogEvent;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogFactory;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogProvider;
import org.hyperledger.besu.util.Subscribers;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrieLogManager {
  private static final Logger LOG = LoggerFactory.getLogger(TrieLogManager.class);
  public static final long LOG_RANGE_LIMIT = 1000; // restrict trielog range queries to 1k logs
  protected final Blockchain blockchain;
  protected final BonsaiWorldStateKeyValueStorage rootWorldStateStorage;

  protected final long maxLayersToLoad;
  protected final Subscribers<TrieLogEvent.TrieLogObserver> trieLogObservers = Subscribers.create();

  protected final TrieLogFactory trieLogFactory;

  public TrieLogManager(
      final Blockchain blockchain,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final long maxLayersToLoad,
      final ServiceManager pluginContext) {
    this.blockchain = blockchain;
    this.rootWorldStateStorage = worldStateKeyValueStorage;
    this.maxLayersToLoad = maxLayersToLoad;
    this.trieLogFactory = setupTrieLogFactory(pluginContext);
  }

  public synchronized void saveTrieLog(
      final PathBasedWorldStateUpdateAccumulator<?> localUpdater,
      final Hash forWorldStateRootHash,
      final BlockHeader forBlockHeader,
      final PathBasedWorldState forWorldState) {
    // do not overwrite a trielog layer that already exists in the database.
    // if it's only in memory we need to save it
    // for example, in case of reorg we don't replace a trielog layer
    if (rootWorldStateStorage.getTrieLog(forBlockHeader.getBlockHash()).isEmpty()) {
      final BonsaiWorldStateKeyValueStorage.Updater stateUpdater =
          forWorldState.getWorldStateStorage().updater();
      boolean success = false;
      try {
        final TrieLog trieLog = prepareTrieLog(forBlockHeader, localUpdater);
        persistTrieLog(forBlockHeader, forWorldStateRootHash, trieLog, stateUpdater);
        // preimages ride in the trie log transaction so both commit (or neither does) together.
        // Slot keys are only known to the accumulator that executed the block, so this has to
        // happen here, when the trie log is first written, and not on the later head roll-forward.
        stagePreimages(localUpdater, stateUpdater);

        // notify trie log added observers, synchronously
        trieLogObservers.forEach(o -> o.onTrieLogAdded(new TrieLogAddedEvent(trieLog)));

        success = true;
      } finally {
        if (success) {
          stateUpdater.commitTrieLogOnly();
        } else {
          stateUpdater.rollback();
        }
      }
    }
  }

  public TrieLogFactory getTrieLogFactory() {
    return trieLogFactory;
  }

  private TrieLog prepareTrieLog(
      final BlockHeader blockHeader, final PathBasedWorldStateUpdateAccumulator<?> localUpdater) {
    LOG.atDebug()
        .setMessage("Adding layered world state for {}")
        .addArgument(blockHeader::toLogString)
        .log();
    final TrieLog trieLog = trieLogFactory.create(localUpdater, blockHeader);
    trieLog.freeze();
    return trieLog;
  }

  private void persistTrieLog(
      final BlockHeader blockHeader,
      final Hash worldStateRootHash,
      final TrieLog trieLog,
      final BonsaiWorldStateKeyValueStorage.Updater stateUpdater) {
    LOG.atDebug()
        .setMessage("Persisting trie log for block hash {} and world state root {}")
        .addArgument(blockHeader::toLogString)
        .addArgument(() -> worldStateRootHash.getBytes().toHexString())
        .log();

    stateUpdater
        .getTrieLogStorageTransaction()
        .put(
            blockHeader.getBlockHash().getBytes().toArrayUnsafe(),
            trieLogFactory.serialize(trieLog));
  }

  /**
   * Stage preimages for keys that enter state in this block. A preimage only has to be written the
   * first time its key is created: updates to existing keys, and read-only touches, were already
   * covered when the key was created, so they are skipped without any storage lookup. Slot keys
   * that are common to many contracts are never written, see {@link KnownSlotPreimages}.
   *
   * <p>This assumes the preimage store has been populated since the key was first created.
   */
  private void stagePreimages(
      final PathBasedWorldStateUpdateAccumulator<?> accumulator,
      final BonsaiWorldStateKeyValueStorage.Updater stateUpdater) {
    stateUpdater
        .getPreimageUpdater()
        .ifPresent(
            preimages -> {
              final PreimageWriteMetrics metrics = rootWorldStateStorage.getPreimageWriteMetrics();

              long accountsWritten = 0;
              long accountsExisting = 0;
              for (final var entry : accumulator.getAccountsToUpdate().entrySet()) {
                final var change = entry.getValue();
                if (change.getPrior() != null) {
                  accountsExisting++;
                } else if (change.getUpdated() != null) {
                  final Address address = entry.getKey();
                  preimages.putAccountTrieKeyPreimage(
                      Bytes32.wrap(address.addressHash().getBytes()), address);
                  accountsWritten++;
                }
              }

              long slotsWritten = 0;
              long slotsExisting = 0;
              long slotsKnown = 0;
              // the preimage store is keyed by slot hash only, so a slot key shared by many
              // accounts in this block needs staging once
              final Set<UInt256> stagedSlots = new HashSet<>();
              for (final var accountSlots : accumulator.getStorageToUpdate().values()) {
                for (final var entry : accountSlots.entrySet()) {
                  final StorageSlotKey slot = entry.getKey();
                  if (slot.getSlotKey().isEmpty()) {
                    continue;
                  }
                  final UInt256 prior = entry.getValue().getPrior();
                  final UInt256 updated = entry.getValue().getUpdated();
                  if (prior != null && !prior.isZero()) {
                    slotsExisting++;
                  } else if (updated != null && !updated.isZero()) {
                    // the key enters state in this block
                    final UInt256 slotKey = slot.getSlotKey().get();
                    if (KnownSlotPreimages.isKnown(slotKey)) {
                      slotsKnown++;
                    } else if (stagedSlots.add(slotKey)) {
                      preimages.putStorageTrieKeyPreimage(
                          Bytes32.wrap(slot.getSlotHash().getBytes()), slotKey);
                      slotsWritten++;
                    }
                  }
                }
              }

              metrics.record("account", Outcome.WRITTEN, accountsWritten);
              metrics.record("account", Outcome.SKIPPED_EXISTING, accountsExisting);
              metrics.record("slot", Outcome.WRITTEN, slotsWritten);
              metrics.record("slot", Outcome.SKIPPED_EXISTING, slotsExisting);
              metrics.record("slot", Outcome.SKIPPED_KNOWN, slotsKnown);
            });
  }

  public long getMaxLayersToLoad() {
    return maxLayersToLoad;
  }

  public Optional<TrieLog> getTrieLogLayer(final Hash blockHash) {
    return rootWorldStateStorage.getTrieLog(blockHash).map(trieLogFactory::deserialize);
  }

  public synchronized long subscribe(final TrieLogEvent.TrieLogObserver sub) {
    return trieLogObservers.subscribe(sub);
  }

  public synchronized void unsubscribe(final long id) {
    trieLogObservers.unsubscribe(id);
  }

  private TrieLogFactory setupTrieLogFactory(final ServiceManager pluginContext) {
    // if we have a TrieLogService from pluginContext, use it.
    var trieLogServicez =
        Optional.ofNullable(pluginContext)
            .flatMap(context -> context.getService(TrieLogService.class));

    if (trieLogServicez.isPresent()) {
      var trieLogService = trieLogServicez.get();
      // push the TrieLogProvider into the TrieLogService
      trieLogService.configureTrieLogProvider(getTrieLogProvider());

      // configure plugin observers:
      trieLogService.getObservers().forEach(trieLogObservers::subscribe);

      // return the TrieLogFactory implementation from the TrieLogService
      if (trieLogService.getTrieLogFactory().isPresent()) {
        return trieLogService.getTrieLogFactory().get();
      }
    }
    // Otherwise default to BonsaiTrieLogFactory
    return new BonsaiTrieLogFactory();
  }

  private TrieLogProvider getTrieLogProvider() {
    return new TrieLogProvider() {
      @Override
      public Optional<Bytes> getRawTrieLogLayer(final Hash blockHash) {
        return rootWorldStateStorage.getTrieLog(blockHash).map(Bytes::wrap);
      }

      @Override
      public Optional<Bytes> getRawTrieLogLayer(final long blockNumber) {
        return TrieLogManager.this
            .blockchain
            .getBlockHeader(blockNumber)
            .map(BlockHeader::getBlockHash)
            .flatMap(this::getRawTrieLogLayer);
      }

      @Override
      public void saveRawTrieLogLayer(
          final Hash blockHash, final long blockNumber, final Bytes trieLog) {
        final BonsaiWorldStateKeyValueStorage.Updater updater = rootWorldStateStorage.updater();
        updater
            .getTrieLogStorageTransaction()
            .put(blockHash.getBytes().toArrayUnsafe(), trieLog.toArrayUnsafe());
        updater.commitTrieLogOnly();
        // TODO maybe find a way to have a clean and complete trielog for observers
        trieLogObservers.forEach(
            o ->
                o.onTrieLogAdded(
                    new TrieLogAddedEvent(
                        new TrieLogLayer().setBlockHash(blockHash).setBlockNumber(blockNumber))));
      }

      @Override
      public Optional<TrieLog> getTrieLogLayer(final Hash blockHash) {
        return TrieLogManager.this.getTrieLogLayer(blockHash);
      }

      @Override
      public Optional<TrieLog> getTrieLogLayer(final long blockNumber) {
        return TrieLogManager.this
            .blockchain
            .getBlockHeader(blockNumber)
            .map(BlockHeader::getBlockHash)
            .flatMap(TrieLogManager.this::getTrieLogLayer);
      }

      @Override
      public List<TrieLogRangeTuple> getTrieLogsByRange(
          final long fromBlockNumber, final long toBlockNumber) {
        return rangeAsStream(fromBlockNumber, toBlockNumber)
            .map(blockchain::getBlockHeader)
            .map(
                headerOpt ->
                    headerOpt.flatMap(
                        header ->
                            TrieLogManager.this
                                .getTrieLogLayer(header.getBlockHash())
                                .map(
                                    layer ->
                                        new TrieLogRangeTuple(
                                            header.getBlockHash(), header.getNumber(), layer))))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .toList();
      }

      Stream<Long> rangeAsStream(final long fromBlockNumber, final long toBlockNumber) {
        if (Math.abs(toBlockNumber - fromBlockNumber) > LOG_RANGE_LIMIT) {
          throw new IllegalArgumentException("Requested Range too large");
        }
        long left = Math.min(fromBlockNumber, toBlockNumber);
        long right = Math.max(fromBlockNumber, toBlockNumber);
        return LongStream.range(left, right).boxed();
      }
    };
  }
}
