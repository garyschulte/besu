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
package org.hyperledger.besu.ethereum.mainnet;

import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;
import static org.hyperledger.besu.evm.operation.BlockHashOperation.BlockHashLookup;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.requests.RequestProcessorCoordinator;
import org.hyperledger.besu.ethereum.privacy.storage.PrivateMetadataUpdater;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.diffbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.diffbased.bonsai.cache.LazyBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.diffbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.diffbased.bonsai.worldview.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.diffbased.common.worldview.DiffBasedWorldStateConfig;
import org.hyperledger.besu.ethereum.trie.diffbased.common.worldview.accumulator.DiffBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.vm.CachingBlockHashLookup;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldState;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("SuspiciousMethodCalls")
public abstract class AbstractBlockProcessor implements BlockProcessor {

  @FunctionalInterface
  public interface TransactionReceiptFactory {

    TransactionReceipt create(
        TransactionType transactionType,
        TransactionProcessingResult result,
        WorldState worldState,
        long gasUsed);
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBlockProcessor.class);

  static final int MAX_GENERATION = 6;

  protected final MainnetTransactionProcessor transactionProcessor;

  protected final AbstractBlockProcessor.TransactionReceiptFactory transactionReceiptFactory;

  final Wei blockReward;

  protected final boolean skipZeroBlockRewards;
  private final ProtocolSchedule protocolSchedule;

  protected final MiningBeneficiaryCalculator miningBeneficiaryCalculator;

  private static final Executor executor = Executors.newFixedThreadPool(4);

  protected AbstractBlockProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final TransactionReceiptFactory transactionReceiptFactory,
      final Wei blockReward,
      final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
      final boolean skipZeroBlockRewards,
      final ProtocolSchedule protocolSchedule) {
    this.transactionProcessor = transactionProcessor;
    this.transactionReceiptFactory = transactionReceiptFactory;
    this.blockReward = blockReward;
    this.miningBeneficiaryCalculator = miningBeneficiaryCalculator;
    this.skipZeroBlockRewards = skipZeroBlockRewards;
    this.protocolSchedule = protocolSchedule;
  }

  @SuppressWarnings({"unchecked", "ReassignedVariable"})
  @Override
  public BlockProcessingResult processBlock(
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final BlockHeader blockHeader,
      final List<Transaction> transactions,
      final List<BlockHeader> ommers,
      final Optional<List<Withdrawal>> maybeWithdrawals,
      final PrivateMetadataUpdater privateMetadataUpdater) {
    final List<TransactionReceipt> receipts = new ArrayList<>();
    long currentGasUsed = 0;
    long currentBlobGasUsed = 0;

    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(blockHeader);

    protocolSpec.getBlockHashProcessor().processBlockHashes(blockchain, worldState, blockHeader);

    final BlockHashLookup blockHashLookup = new CachingBlockHashLookup(blockHeader, blockchain);
    final Address miningBeneficiary = miningBeneficiaryCalculator.calculateBeneficiary(blockHeader);

    TransactionConflictChecker transactionConflictChecker = new TransactionConflictChecker();

    Optional<BlockHeader> maybeParentHeader =
        blockchain.getBlockHeader(blockHeader.getParentHash());

    Wei blobGasPrice =
        maybeParentHeader
            .map(
                parentHeader ->
                    protocolSpec
                        .getFeeMarket()
                        .blobGasPricePerGas(
                            calculateExcessBlobGasForParent(protocolSpec, parentHeader)))
            .orElse(Wei.ZERO);

    final BonsaiWorldStateUpdateAccumulator blockUpdater =
        (BonsaiWorldStateUpdateAccumulator) worldState.updater();

    // load withdrawal addresses in the cache in background
    maybeWithdrawals.ifPresent(
        withdrawals ->
            CompletableFuture.runAsync(
                () ->
                    withdrawals.forEach(withdrawal -> blockUpdater.get(withdrawal.getAddress()))));

    final long time = System.currentTimeMillis();

    for (int i = 0; i < transactions.size(); i++) {
      final TransactionConflictChecker.TransactionWithLocation transactionWithLocation =
          new TransactionConflictChecker.TransactionWithLocation(i, transactions.get(i));
      CompletableFuture.runAsync(
          () -> {
            BonsaiWorldState roundWorldState =
                new BonsaiWorldState(
                    (BonsaiWorldState) worldState, new LazyBonsaiCachedMerkleTrieLoader());
            WorldUpdater roundWorldStateUpdater = roundWorldState.updater();

            final TransactionProcessingResult result =
                transactionProcessor.processTransaction(
                    roundWorldStateUpdater,
                    blockHeader,
                    transactionWithLocation.transaction(),
                    miningBeneficiary,
                    OperationTracer.NO_TRACING,
                    blockHashLookup,
                    true,
                    TransactionValidationParams.processingBlock(),
                    privateMetadataUpdater,
                    blobGasPrice);
            transactionConflictChecker.saveParallelizedTransactionProcessingResult(
                transactionWithLocation, roundWorldState.getAccumulator(), result);
          },
          executor);
    }

    System.out.println("preload " + (System.currentTimeMillis() - time));

    int confirmedParallelizedTransaction = 0;
    int conflictingButCachedTransaction = 0;
    try {
      for (int i = 0; i < transactions.size(); i++) {
        Transaction transaction = transactions.get(i);
        final TransactionProcessingResult transactionProcessingResult;
        final DiffBasedWorldStateUpdateAccumulator<BonsaiAccount> transactionAccumulator =
            (DiffBasedWorldStateUpdateAccumulator<BonsaiAccount>)
                transactionConflictChecker.getAccumulatorByTransaction().get((long) i);
        if (transactionAccumulator != null
            && !transactionConflictChecker.checkConflicts(
                miningBeneficiary,
                new TransactionConflictChecker.TransactionWithLocation(i, transaction),
                transactionAccumulator,
                blockUpdater)) {
          transactionAccumulator.commit();
          blockUpdater.cloneFromUpdaterWithPreloader(transactionAccumulator);
          transactionProcessingResult =
              transactionConflictChecker.getResultByTransaction().get((long) i);

          confirmedParallelizedTransaction++;
        } else {
          if (transactionAccumulator != null) {
            blockUpdater.clonePriorFromUpdater(transactionAccumulator);
            conflictingButCachedTransaction++;
          }
          transactionProcessingResult =
              transactionProcessor.processTransaction(
                  blockUpdater,
                  blockHeader,
                  transaction,
                  miningBeneficiary,
                  OperationTracer.NO_TRACING,
                  blockHashLookup,
                  true,
                  TransactionValidationParams.processingBlock(),
                  privateMetadataUpdater,
                  blobGasPrice);
        }
        if (transactionProcessingResult.isInvalid()) {
          String errorMessage =
              MessageFormat.format(
                  "Block processing error: transaction invalid {0}. Block {1} Transaction {2}",
                  transactionProcessingResult.getValidationResult().getErrorMessage(),
                  blockHeader.getHash().toHexString(),
                  transaction.getHash().toHexString());
          LOG.info(errorMessage);
          if (worldState instanceof BonsaiWorldState) {
            blockUpdater.reset();
          }
          return new BlockProcessingResult(Optional.empty(), errorMessage);
        }

        final var coinbase = blockUpdater.getOrCreate(miningBeneficiary);
        if (transactionProcessingResult.getMiningBenef() != null) {
          coinbase.incrementBalance(transactionProcessingResult.getMiningBenef());
        }

        blockUpdater.commit();

        currentGasUsed += transaction.getGasLimit() - transactionProcessingResult.getGasRemaining();
        if (transaction.getVersionedHashes().isPresent()) {
          currentBlobGasUsed +=
              (transaction.getVersionedHashes().get().size()
                  * CancunGasCalculator.BLOB_GAS_PER_BLOB);
        }
        final TransactionReceipt transactionReceipt =
            transactionReceiptFactory.create(
                transaction.getType(), transactionProcessingResult, worldState, currentGasUsed);
        receipts.add(transactionReceipt);
      }

      System.out.println("conflictingButCachedTransaction " + conflictingButCachedTransaction);
      System.out.println("confirmedParallelizedTransaction " + confirmedParallelizedTransaction);
    } catch (Throwable e) {
      e.printStackTrace();
      System.out.println("error " + e);
    }

    if (blockHeader.getBlobGasUsed().isPresent()
        && currentBlobGasUsed != blockHeader.getBlobGasUsed().get()) {
      String errorMessage =
          String.format(
              "block did not consume expected blob gas: header %d, transactions %d",
              blockHeader.getBlobGasUsed().get(), currentBlobGasUsed);
      LOG.error(errorMessage);
      return new BlockProcessingResult(Optional.empty(), errorMessage);
    }
    final Optional<WithdrawalsProcessor> maybeWithdrawalsProcessor =
        protocolSpec.getWithdrawalsProcessor();
    if (maybeWithdrawalsProcessor.isPresent() && maybeWithdrawals.isPresent()) {
      try {
        maybeWithdrawalsProcessor
            .get()
            .processWithdrawals(maybeWithdrawals.get(), worldState.updater());
      } catch (final Exception e) {
        LOG.error("failed processing withdrawals", e);
        return new BlockProcessingResult(Optional.empty(), e);
      }
    }

    if (!rewardCoinbase(worldState, blockHeader, ommers, skipZeroBlockRewards)) {
      // no need to log, rewardCoinbase logs the error.
      if (worldState instanceof BonsaiWorldState) {
        ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
      }
      return new BlockProcessingResult(Optional.empty(), "ommer too old");
    }
    try {
      worldState.persist(blockHeader);
    } catch (MerkleTrieException e) {
      LOG.trace("Merkle trie exception during Transaction processing ", e);
      if (worldState instanceof BonsaiWorldState) {
        ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
      }
      throw e;
    } catch (Exception e) {
      LOG.error("failed persisting block", e);
      return new BlockProcessingResult(Optional.empty(), e);
    }
    return new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(worldState, receipts)));
  }

  protected boolean hasAvailableBlockBudget(
      final BlockHeader blockHeader, final Transaction transaction, final long currentGasUsed) {
    final long remainingGasBudget = blockHeader.getGasLimit() - currentGasUsed;
    if (Long.compareUnsigned(transaction.getGasLimit(), remainingGasBudget) > 0) {
      LOG.info(
          "Block processing error: transaction gas limit {} exceeds available block budget"
              + " remaining {}. Block {} Transaction {}",
          transaction.getGasLimit(),
          remainingGasBudget,
          blockHeader.getHash().toHexString(),
          transaction.getHash().toHexString());
      return false;
    }

    return true;
  }

  protected MiningBeneficiaryCalculator getMiningBeneficiaryCalculator() {
    return miningBeneficiaryCalculator;
  }

  abstract boolean rewardCoinbase(
      final MutableWorldState worldState,
      final BlockHeader header,
      final List<BlockHeader> ommers,
      final boolean skipZeroBlockRewards);
}
