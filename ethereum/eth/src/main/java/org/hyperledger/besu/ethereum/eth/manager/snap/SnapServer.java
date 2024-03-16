/*
 * Copyright contributors to Hyperledger Besu
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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.ByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetAccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetStorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetTrieNodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV1;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.TrieNodesMessage;
import org.hyperledger.besu.ethereum.eth.sync.DefaultSynchronizer;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import kotlin.Pair;
import kotlin.collections.ArrayDeque;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** See https://github.com/ethereum/devp2p/blob/master/caps/snap.md */
@SuppressWarnings("unused")
class SnapServer implements BesuEvents.InitialSyncCompletionListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SnapServer.class);
  private static final int PRIME_STATE_ROOT_CACHE_LIMIT = 128;
  private static final int MAX_ENTRIES_PER_REQUEST = 100000;
  private static final int MAX_RESPONSE_SIZE = 2 * 1024 * 1024;
  private static final AccountRangeMessage EMPTY_ACCOUNT_RANGE =
      AccountRangeMessage.create(new HashMap<>(), new ArrayDeque<>());
  private static final StorageRangeMessage EMPTY_STORAGE_RANGE =
      StorageRangeMessage.create(new ArrayDeque<>(), Collections.emptyList());
  private static final TrieNodesMessage EMPTY_TRIE_NODES_MESSAGE =
      TrieNodesMessage.create(new ArrayList<>());
  private static final ByteCodesMessage EMPTY_BYTE_CODES_MESSAGE =
      ByteCodesMessage.create(new ArrayDeque<>());

  static final Hash HASH_LAST = Hash.wrap(Bytes32.leftPad(Bytes.fromHexString("FF"), (byte) 0xFF));

  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final AtomicLong listenerId = new AtomicLong();
  private final EthMessages snapMessages;

  private final WorldStateStorageCoordinator worldStateStorageCoordinator;
  private final Optional<ProtocolContext> protocolContext;

  // provide worldstate storage by root hash
  private Function<Optional<Hash>, Optional<BonsaiWorldStateKeyValueStorage>>
      worldStateStorageProvider = __ -> Optional.empty();

  SnapServer(
      final EthMessages snapMessages,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final ProtocolContext protocolContext) {

    this.snapMessages = snapMessages;
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.protocolContext = Optional.of(protocolContext);
    registerResponseConstructors();
  }

  /**
   * Create a snap server without registering a listener for worldstate initial sync events or
   * priming worldstates by root hash.
   */
  @VisibleForTesting
  SnapServer(
      final EthMessages snapMessages,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final Function<Optional<Hash>, Optional<BonsaiWorldStateKeyValueStorage>>
          worldStateStorageProvider) {
    this.snapMessages = snapMessages;
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.worldStateStorageProvider = worldStateStorageProvider;
    this.protocolContext = Optional.empty();
  }

  @Override
  public void onInitialSyncCompleted() {
    start();
  }

  @Override
  public void onInitialSyncRestart() {
    stop();
  }

  public synchronized SnapServer start() {

    // if we are bonsai and full flat, we can provide a worldstate storage:
    var worldStateKeyValueStorage = worldStateStorageCoordinator.worldStateKeyValueStorage();
    if (worldStateKeyValueStorage.getDataStorageFormat().equals(DataStorageFormat.BONSAI)
        && worldStateStorageCoordinator.isMatchingFlatMode(FlatDbMode.FULL)) {
      LOGGER.debug("Starting snap server with Bonsai full flat db");
      var bonsaiArchive =
          protocolContext
              .map(ProtocolContext::getWorldStateArchive)
              .map(BonsaiWorldStateProvider.class::cast);
      var cachedStorageManagerOpt =
          bonsaiArchive.map(archive -> archive.getCachedWorldStorageManager());

      if (cachedStorageManagerOpt.isPresent()) {
        var cachedStorageManager = cachedStorageManagerOpt.get();
        this.worldStateStorageProvider = cachedStorageManager::getStorageByRootHash;

        // when we start we need to build the cache of latest 128 worldstates trielogs-to-root-hash:
        var blockchain = protocolContext.map(ProtocolContext::getBlockchain).orElse(null);

        // at startup, prime the latest worldstates by roothash:
        cachedStorageManager.primeRootToBlockHashCache(blockchain, PRIME_STATE_ROOT_CACHE_LIMIT);

        // subscribe to initial sync completed events to start/stop snap server:
        protocolContext
            .flatMap(ProtocolContext::getSynchronizer)
            .filter(z -> z instanceof DefaultSynchronizer)
            .map(DefaultSynchronizer.class::cast)
            .ifPresentOrElse(
                z -> this.listenerId.set(z.subscribeInitialSync(this)),
                () -> LOGGER.warn("SnapServer created without reference to sync status"));

        var flatDbStrategy =
            ((BonsaiWorldStateKeyValueStorage)
                    worldStateStorageCoordinator.worldStateKeyValueStorage())
                .getFlatDbStrategy();
        if (!flatDbStrategy.isCodeByCodeHash()) {
          LOGGER.warn("SnapServer requires code stored by codehash, but it is not enabled");
        }
      } else {
        LOGGER.warn(
            "SnapServer started without cached storage manager, this should only happen in tests");
      }
      isStarted.set(true);
    }
    return this;
  }

  public synchronized SnapServer stop() {
    isStarted.set(false);
    return this;
  }

  private void registerResponseConstructors() {
    snapMessages.registerResponseConstructor(
        SnapV1.GET_ACCOUNT_RANGE, messageData -> constructGetAccountRangeResponse(messageData));
    snapMessages.registerResponseConstructor(
        SnapV1.GET_STORAGE_RANGE, messageData -> constructGetStorageRangeResponse(messageData));
    snapMessages.registerResponseConstructor(
        SnapV1.GET_BYTECODES, messageData -> constructGetBytecodesResponse(messageData));
    snapMessages.registerResponseConstructor(
        SnapV1.GET_TRIE_NODES, messageData -> constructGetTrieNodesResponse(messageData));
  }

  MessageData constructGetAccountRangeResponse(final MessageData message) {
    if (!isStarted.get()) {
      return EMPTY_ACCOUNT_RANGE;
    }
    StopWatch stopWatch = StopWatch.createStarted();

    final GetAccountRangeMessage getAccountRangeMessage = GetAccountRangeMessage.readFrom(message);
    final GetAccountRangeMessage.Range range = getAccountRangeMessage.range(true);
    final int maxResponseBytes = Math.min(range.responseBytes().intValue(), MAX_RESPONSE_SIZE);

    LOGGER
        .atTrace()
        .setMessage("Receive getAccountRangeMessage for {} from {} to {}")
        .addArgument(() -> asLogHash(range.worldStateRootHash()))
        .addArgument(() -> asLogHash(range.startKeyHash()))
        .addArgument(() -> asLogHash(range.endKeyHash()))
        .log();
    try {
      return worldStateStorageProvider
          .apply(Optional.of(range.worldStateRootHash()))
          .map(
              storage -> {
                LOGGER.trace("obtained worldstate in {}", stopWatch);
                NavigableMap<Bytes32, Bytes> accounts =
                    storage.streamFlatAccounts(
                        range.startKeyHash(),
                        range.endKeyHash(),
                        new StatefulPredicate(
                            "account",
                            maxResponseBytes,
                            (pair) -> {
                              var rlpOutput = new BytesValueRLPOutput();
                              rlpOutput.startList();
                              rlpOutput.writeBytes(pair.getFirst());
                              rlpOutput.writeRLPBytes(pair.getSecond());
                              rlpOutput.endList();
                              return rlpOutput.encodedSize();
                            }));

                if (accounts.isEmpty()) {
                  // fetch next account after range, if it exists
                  LOGGER.debug(
                      "found no accounts in range, taking first value starting from {}",
                      asLogHash(range.endKeyHash()));
                  accounts = storage.streamFlatAccounts(range.endKeyHash(), UInt256.MAX_VALUE, 1L);
                }

                final var worldStateProof =
                    new WorldStateProofProvider(worldStateStorageCoordinator);
                final List<Bytes> proof =
                    worldStateProof.getAccountProofRelatedNodes(
                        range.worldStateRootHash(), Hash.wrap(range.startKeyHash()));

                if (!accounts.isEmpty()) {
                  proof.addAll(
                      worldStateProof.getAccountProofRelatedNodes(
                          range.worldStateRootHash(), Hash.wrap(accounts.lastKey())));
                }
                var resp = AccountRangeMessage.create(accounts, proof);
                if (accounts.isEmpty()) {
                  LOGGER.debug(
                      "returned empty account range message for {} to  {}, proof count {}",
                      asLogHash(range.startKeyHash()),
                      asLogHash(range.endKeyHash()),
                      proof.size());
                }
                LOGGER.debug(
                    "returned in {} account range {} to {} with {} accounts and {} proofs, resp size {} of max {}",
                    stopWatch,
                    asLogHash(range.startKeyHash()),
                    asLogHash(range.endKeyHash()),
                    accounts.size(),
                    proof.size(),
                    resp.getSize(),
                    maxResponseBytes);
                return resp;
              })
          .orElseGet(
              () -> {
                LOGGER.debug("returned empty account range due to worldstate not present");
                return EMPTY_ACCOUNT_RANGE;
              });
    } catch (Exception ex) {
      LOGGER.error("Unexpected exception serving account range request", ex);
    }
    return EMPTY_ACCOUNT_RANGE;
  }

  MessageData constructGetStorageRangeResponse(final MessageData message) {
    if (!isStarted.get()) {
      return EMPTY_STORAGE_RANGE;
    }
    StopWatch stopWatch = StopWatch.createStarted();

    final GetStorageRangeMessage getStorageRangeMessage = GetStorageRangeMessage.readFrom(message);
    final GetStorageRangeMessage.StorageRange range = getStorageRangeMessage.range(true);
    final int maxResponseBytes = Math.min(range.responseBytes().intValue(), MAX_RESPONSE_SIZE);

    LOGGER
        .atTrace()
        .setMessage("Receive get storage range message size {} from {} to {} for {}")
        .addArgument(message::getSize)
        .addArgument(() -> asLogHash(range.startKeyHash()))
        .addArgument(
            () -> Optional.ofNullable(range.endKeyHash()).map(SnapServer::asLogHash).orElse("''"))
        .addArgument(
            () ->
                range.hashes().stream()
                    .map(SnapServer::asLogHash)
                    .collect(Collectors.joining(",", "[", "]")))
        .log();
    try {
      return worldStateStorageProvider
          .apply(Optional.of(range.worldStateRootHash()))
          .map(
              storage -> {
                LOGGER.trace("obtained worldstate in {}", stopWatch);
                // reusable predicate to limit by rec count and bytes:
                var statefulPredicate =
                    new StatefulPredicate(
                        "storage",
                        maxResponseBytes,
                        (pair) -> {
                          var slotRlpOutput = new BytesValueRLPOutput();
                          slotRlpOutput.startList();
                          slotRlpOutput.writeBytes(pair.getFirst());
                          slotRlpOutput.writeBytes(pair.getSecond());
                          slotRlpOutput.endList();
                          return slotRlpOutput.encodedSize();
                        });

                // only honor start and end hash if request is for a single account's storage:
                Bytes32 startKeyBytes, endKeyBytes;
                boolean isPartialRange = false;
                if (range.hashes().size() > 1) {
                  startKeyBytes = Bytes32.ZERO;
                  endKeyBytes = HASH_LAST;
                } else {
                  startKeyBytes = range.startKeyHash();
                  endKeyBytes = range.endKeyHash();
                  isPartialRange =
                      !(startKeyBytes.equals(Hash.ZERO) && endKeyBytes.equals(HASH_LAST));
                }

                ArrayDeque<NavigableMap<Bytes32, Bytes>> collectedStorages = new ArrayDeque<>();
                List<Bytes> proofNodes = new ArrayList<>();
                final var worldStateProof =
                    new WorldStateProofProvider(worldStateStorageCoordinator);

                for (var forAccountHash : range.hashes()) {
                  var accountStorages =
                      storage.streamFlatStorages(
                          Hash.wrap(forAccountHash), startKeyBytes, endKeyBytes, statefulPredicate);

                  //// address partial range queries that return empty
                  if (accountStorages.isEmpty() && isPartialRange) {
                    // fetch next slot after range, if it exists
                    LOGGER.debug(
                        "found no slots in range, taking first value starting from {}",
                        asLogHash(range.endKeyHash()));
                    accountStorages =
                        storage.streamFlatStorages(
                            Hash.wrap(forAccountHash), range.endKeyHash(), UInt256.MAX_VALUE, 1L);
                  }

                  // don't send empty storage ranges
                  if (!accountStorages.isEmpty()) {
                    collectedStorages.add(accountStorages);
                  }

                  // if a partial storage range was requested, or we interrupted storage due to
                  // request limits, send proofs:
                  if (isPartialRange || !statefulPredicate.shouldGetMore()) {
                    // send a proof for the left side range origin
                    proofNodes.addAll(
                        worldStateProof.getStorageProofRelatedNodes(
                            getAccountStorageRoot(forAccountHash, storage),
                            forAccountHash,
                            Hash.wrap(startKeyBytes)));
                    if (!accountStorages.isEmpty()) {
                      // send a proof for the last key on the right
                      proofNodes.addAll(
                          worldStateProof.getStorageProofRelatedNodes(
                              getAccountStorageRoot(forAccountHash, storage),
                              forAccountHash,
                              Hash.wrap(accountStorages.lastKey())));
                    }
                  }

                  if (!statefulPredicate.shouldGetMore()) {
                    break;
                  }
                }

                var resp = StorageRangeMessage.create(collectedStorages, proofNodes);
                LOGGER.debug(
                    "returned in {} storage {} to {} range {} to {} with {} storages and {} proofs, resp size {} of max {}",
                    stopWatch,
                    asLogHash(range.hashes().first()),
                    asLogHash(range.hashes().last()),
                    asLogHash(range.startKeyHash()),
                    asLogHash(range.endKeyHash()),
                    collectedStorages.size(),
                    proofNodes.size(),
                    resp.getSize(),
                    maxResponseBytes);
                return resp;
              })
          .orElseGet(
              () -> {
                LOGGER.debug("returned empty storage range due to missing worldstate");
                return EMPTY_STORAGE_RANGE;
              });
    } catch (Exception ex) {
      LOGGER.error("Unexpected exception serving storage range request", ex);
      return EMPTY_STORAGE_RANGE;
    }
  }

  MessageData constructGetBytecodesResponse(final MessageData message) {
    if (!isStarted.get()) {
      return EMPTY_BYTE_CODES_MESSAGE;
    }
    StopWatch stopWatch = StopWatch.createStarted();

    final GetByteCodesMessage getByteCodesMessage = GetByteCodesMessage.readFrom(message);
    final GetByteCodesMessage.CodeHashes codeHashes = getByteCodesMessage.codeHashes(true);
    final int maxResponseBytes = Math.min(codeHashes.responseBytes().intValue(), MAX_RESPONSE_SIZE);
    LOGGER
        .atTrace()
        .setMessage("Receive get bytecodes message for {} hashes")
        .addArgument(codeHashes.hashes()::size)
        .log();

    try {
      List<Bytes> codeBytes = new ArrayDeque<>();
      for (Bytes32 codeHash : codeHashes.hashes()) {
        Optional<Bytes> optCode = worldStateStorageCoordinator.getCode(Hash.wrap(codeHash), null);
        if (optCode.isPresent()) {
          if (sumListBytes(codeBytes) + optCode.get().size() > maxResponseBytes) {
            break;
          }
          codeBytes.add(optCode.get());
        }
      }
      var resp = ByteCodesMessage.create(codeBytes);
      LOGGER.debug(
          "returned in {} code bytes message with {} entries, resp size {} of max {}",
          stopWatch,
          codeBytes.size(),
          resp.getSize(),
          maxResponseBytes);
      return resp;
    } catch (Exception ex) {
      LOGGER.error("Unexpected exception serving bytecodes request", ex);
      return EMPTY_BYTE_CODES_MESSAGE;
    }
  }

  MessageData constructGetTrieNodesResponse(final MessageData message) {
    if (!isStarted.get()) {
      return EMPTY_TRIE_NODES_MESSAGE;
    }
    StopWatch stopWatch = StopWatch.createStarted();

    final GetTrieNodesMessage getTrieNodesMessage = GetTrieNodesMessage.readFrom(message);
    final GetTrieNodesMessage.TrieNodesPaths triePaths = getTrieNodesMessage.paths(true);
    final int maxResponseBytes = Math.min(triePaths.responseBytes().intValue(), MAX_RESPONSE_SIZE);
    LOGGER
        .atTrace()
        .setMessage("Receive get trie nodes message of size {}")
        .addArgument(() -> triePaths.paths().size())
        .log();

    try {
      return worldStateStorageProvider
          .apply(Optional.of(triePaths.worldStateRootHash()))
          .map(
              storage -> {
                LOGGER.trace("obtained worldstate in {}", stopWatch);
                ArrayList<Bytes> trieNodes = new ArrayList<>();
                for (var triePath : triePaths.paths()) {
                  // first element in paths is account
                  if (triePath.size() == 1) {
                    // if there is only one path, presume it should be compact encoded account path
                    var optStorage =
                        storage.getTrieNodeUnsafe(CompactEncoding.decode(triePath.get(0)));
                    if (optStorage.isPresent()) {
                      if (sumListBytes(trieNodes) + optStorage.get().size() > maxResponseBytes) {
                        break;
                      }
                      trieNodes.add(optStorage.get());
                    }

                  } else {
                    // otherwise the first element should be account hash, and subsequent paths
                    // are compact encoded account storage paths

                    final Bytes accountPrefix = triePath.get(0);

                    List<Bytes> storagePaths = triePath.subList(1, triePath.size());
                    for (var path : storagePaths) {
                      var optStorage =
                          storage.getTrieNodeUnsafe(
                              Bytes.concatenate(accountPrefix, CompactEncoding.decode(path)));
                      if (optStorage.isPresent()) {
                        if (sumListBytes(trieNodes) + optStorage.get().size() > maxResponseBytes) {
                          break;
                        }
                        trieNodes.add(optStorage.get());
                      }
                    }
                  }
                }
                var resp = TrieNodesMessage.create(trieNodes);
                LOGGER.debug(
                    "returned in {} trie nodes message with {} entries, resp size {} of max {}",
                    stopWatch,
                    trieNodes.size(),
                    resp.getCode(),
                    maxResponseBytes);
                return resp;
              })
          .orElseGet(
              () -> {
                LOGGER.debug("returned empty trie nodes message due to missing worldstate");
                return EMPTY_TRIE_NODES_MESSAGE;
              });
    } catch (Exception ex) {
      LOGGER.error("Unexpected exception serving trienodes request", ex);
      return EMPTY_TRIE_NODES_MESSAGE;
    }
  }

  static class StatefulPredicate implements Predicate<Pair<Bytes32, Bytes>> {

    final AtomicInteger byteLimit = new AtomicInteger(0);
    final AtomicInteger recordLimit = new AtomicInteger(0);
    final AtomicBoolean shouldContinue = new AtomicBoolean(true);
    final Function<Pair<Bytes32, Bytes>, Integer> encodingSizeAccumulator;
    final int maxResponseBytes;
    // TODO: remove this hack,  10% is a fudge factor to account for the proof node size
    final int maxResponseBytesFudgeFactor;
    final String forWhat;

    StatefulPredicate(
        final String forWhat,
        final int maxResponseBytes,
        final Function<Pair<Bytes32, Bytes>, Integer> encodingSizeAccumulator) {
      this.maxResponseBytes = maxResponseBytes;
      this.maxResponseBytesFudgeFactor = maxResponseBytes * 9 / 10;
      this.forWhat = forWhat;
      this.encodingSizeAccumulator = encodingSizeAccumulator;
    }

    public boolean shouldGetMore() {
      return shouldContinue.get();
    }

    @Override
    public boolean test(final Pair<Bytes32, Bytes> pair) {
      LOGGER
          .atTrace()
          .setMessage("{} pre-accumulate limits, bytes: {} , stream count: {}")
          .addArgument(() -> forWhat)
          .addArgument(byteLimit::get)
          .addArgument(recordLimit::get)
          .log();

      var underRecordLimit = recordLimit.addAndGet(1) <= MAX_ENTRIES_PER_REQUEST;
      var underByteLimit =
          byteLimit.accumulateAndGet(0, (cur, __) -> cur + encodingSizeAccumulator.apply(pair))
              < maxResponseBytesFudgeFactor;
      if (underRecordLimit && underByteLimit) {
        return true;
      } else {
        shouldContinue.set(false);
        LOGGER
            .atDebug()
            .setMessage("{} post-accumulate limits, bytes: {} , stream count: {}")
            .addArgument(() -> forWhat)
            .addArgument(byteLimit::get)
            .addArgument(recordLimit::get)
            .log();
        return false;
      }
    }
  }

  Hash getAccountStorageRoot(
      final Bytes32 accountHash, final BonsaiWorldStateKeyValueStorage storage) {
    return storage
        .getTrieNodeUnsafe(Bytes.concatenate(accountHash, Bytes.EMPTY))
        .map(Hash::hash)
        .orElse(Hash.EMPTY_TRIE_HASH);
  }

  private static int sumListBytes(final List<Bytes> listOfBytes) {
    // TODO: remove hack, 10% is a fudge factor to account for the overhead of rlp encoding
    return listOfBytes.stream().map(Bytes::size).reduce((a, b) -> a + b).orElse(0) * 11 / 10;
  }

  private static String asLogHash(final Bytes32 hash) {
    var str = hash.toHexString();
    return str.substring(0, 4) + ".." + str.substring(59, 63);
  }
}
