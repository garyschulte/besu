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
package org.hyperledger.besu.ethereum.blockcreation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreator.BlockCreationResult;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.SealableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionBroadcaster;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.AbstractPendingTransactionsSorter;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.GasPricePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidator;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidatorFactory;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiExecutionWitnessBuilder;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.ssz.SSZ;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TinyGenesisBlockCreationTest {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  // well-known test key pair from tiny-genesis.json
  private static final SECPPrivateKey SENDER_PRIVATE_KEY =
      SIGNATURE_ALGORITHM.createPrivateKey(
          Bytes32.fromHexString(
              "c87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3"));
  private static final KeyPair SENDER_KEY_PAIR =
      new KeyPair(SENDER_PRIVATE_KEY, SIGNATURE_ALGORITHM.createPublicKey(SENDER_PRIVATE_KEY));

  private static final Address SENDER =
      Address.fromHexString("0x627306090abaB3A6e1400e9345bC60c78a8BEf57");
  private static final Address RECIPIENT =
      Address.fromHexString("0x0000000000000000000000000000000000000001");
  private static final BigInteger CHAIN_ID = BigInteger.valueOf(1982);

  private final GenesisConfig genesisConfig = GenesisConfig.fromResource("/tiny-genesis.json");
  private final DeterministicEthScheduler ethScheduler = new DeterministicEthScheduler();

  @Test
  void shouldAppendBlockWithOneEthTransfer() {
    final ExecutionContextTestFixture ctx = buildExecutionContext();
    final MutableBlockchain blockchain = ctx.getBlockchain();
    final BlockHeader parentHeader = blockchain.getChainHeadHeader();

    final Transaction tx =
        new TransactionTestFixture()
            .type(TransactionType.EIP1559)
            .sender(SENDER)
            .to(Optional.of(RECIPIENT))
            .value(Wei.fromEth(1))
            .gasLimit(21_000L)
            .nonce(0L)
            .maxFeePerGas(Optional.of(Wei.of(10)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .chainId(Optional.of(CHAIN_ID))
            .createTransaction(SENDER_KEY_PAIR);

    final TestBlockCreator blockCreator = buildBlockCreator(ctx);

    final BlockCreationResult result =
        blockCreator.createBlock(
            Optional.of(List.of(tx)),
            Optional.empty(),
            parentHeader.getTimestamp() + 1L,
            parentHeader);

    assertThat(result.getBlock()).isNotNull();
    assertThat(result.getBlock().getHeader().getNumber()).isEqualTo(1L);
    assertThat(result.getBlock().getBody().getTransactions()).hasSize(1);
    assertThat(result.getBlock().getBody().getTransactions().get(0).getSender()).isEqualTo(SENDER);
    assertThat(result.getBlock().getBody().getTransactions().get(0).getTo())
        .contains(RECIPIENT);
    assertThat(result.getBlock().getBody().getTransactions().get(0).getValue())
        .isEqualTo(Wei.fromEth(1));

    // append block to blockchain
    blockchain.appendBlock(
        result.getBlock(),
        result.getTransactionSelectionResults().getReceipts());
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(1L);
  }

  @Test
  void shouldGenerateStatelessInput() throws Exception {
    final ExecutionContextTestFixture ctx = buildExecutionContext();
    final MutableBlockchain blockchain = ctx.getBlockchain();
    final BlockHeader genesisHeader = blockchain.getChainHeadHeader();

    final Transaction tx =
        new TransactionTestFixture()
            .type(TransactionType.EIP1559)
            .sender(SENDER)
            .to(Optional.of(RECIPIENT))
            .value(Wei.fromEth(1))
            .gasLimit(21_000L)
            .nonce(0L)
            .maxFeePerGas(Optional.of(Wei.of(10)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .chainId(Optional.of(CHAIN_ID))
            .createTransaction(SENDER_KEY_PAIR);

    final Block block =
        buildBlockCreator(ctx)
            .createBlock(
                Optional.of(List.of(tx)),
                Optional.empty(),
                Optional.<List<Withdrawal>>of(List.of()),
                genesisHeader.getTimestamp() + 1L,
                genesisHeader)
            .getBlock();

    assertThat(block).isNotNull();

    // Process and persist — this generates the trie log required by BonsaiExecutionWitnessBuilder.
    final BlockProcessingResult persistResult =
        ctx.getProtocolSchedule()
            .getByBlockHeader(block.getHeader())
            .getBlockValidator()
            .validateAndProcessBlock(
                ctx.getProtocolContext(),
                block,
                HeaderValidationMode.NONE,
                HeaderValidationMode.NONE,
                Optional.empty(),
                true,
                false);

    assertThat(persistResult.isSuccessful()).isTrue();
    final var persistOutputs = persistResult.getYield().orElseThrow();

    // Append to blockchain (with BAL) so headers can be resolved during witness construction.
    blockchain.appendBlock(block, persistOutputs.getReceipts(), persistOutputs.getBlockAccessList());

    // Re-execute without persisting to capture accessedAncestors and BAL for the witness builder.
    final BlockProcessingResult reexecResult =
        ctx.getProtocolSchedule()
            .getByBlockHeader(block.getHeader())
            .getBlockValidator()
            .validateAndProcessBlock(
                ctx.getProtocolContext(),
                block,
                HeaderValidationMode.NONE,
                HeaderValidationMode.NONE,
                blockchain.getBlockAccessList(block.getHash()),
                false,
                false);

    assertThat(reexecResult.isSuccessful()).isTrue();

    final BonsaiExecutionWitnessBuilder.Witness witness =
        new BonsaiExecutionWitnessBuilder()
            .buildWitness(
                block.getHeader(),
                genesisHeader,
                ctx.getStateArchive(),
                blockchain,
                reexecResult.getYield());

    assertThat(witness.state()).isNotEmpty();
    assertThat(witness.headers()).isNotEmpty();

    // Serialize to zesu-compatible JSON files.
    final ObjectMapper mapper = new ObjectMapper();
    final Path outputDir = Paths.get(System.getProperty("user.dir"));

    final ObjectNode blockJson = mapper.createObjectNode();
    blockJson.put("block", block.toRlp().toHexString());
    mapper
        .writerWithDefaultPrettyPrinter()
        .writeValue(outputDir.resolve("block.json").toFile(), blockJson);

    final ObjectNode witnessJson = mapper.createObjectNode();
    final ArrayNode stateArray = witnessJson.putArray("state");
    witness.state().forEach(stateArray::add);
    final ArrayNode codesArray = witnessJson.putArray("codes");
    witness.codes().forEach(codesArray::add);
    final ArrayNode headersArray = witnessJson.putArray("headers");
    witness.headers().forEach(headersArray::add);
    mapper
        .writerWithDefaultPrettyPrinter()
        .writeValue(outputDir.resolve("witness.json").toFile(), witnessJson);
  }

  @Test
  void shouldGenerateStatelessInputSsz() throws Exception {
    final ExecutionContextTestFixture ctx = buildExecutionContext();
    final MutableBlockchain blockchain = ctx.getBlockchain();
    final BlockHeader genesisHeader = blockchain.getChainHeadHeader();

    final Transaction tx =
        new TransactionTestFixture()
            .type(TransactionType.EIP1559)
            .sender(SENDER)
            .to(Optional.of(RECIPIENT))
            .value(Wei.fromEth(1))
            .gasLimit(21_000L)
            .nonce(0L)
            .maxFeePerGas(Optional.of(Wei.of(10)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .chainId(Optional.of(CHAIN_ID))
            .createTransaction(SENDER_KEY_PAIR);

    final Block block =
        buildBlockCreator(ctx)
            .createBlock(
                Optional.of(List.of(tx)),
                Optional.empty(),
                Optional.<List<Withdrawal>>of(List.of()),
                genesisHeader.getTimestamp() + 1L,
                genesisHeader)
            .getBlock();

    final BlockProcessingResult persistResult =
        ctx.getProtocolSchedule()
            .getByBlockHeader(block.getHeader())
            .getBlockValidator()
            .validateAndProcessBlock(
                ctx.getProtocolContext(),
                block,
                HeaderValidationMode.NONE,
                HeaderValidationMode.NONE,
                Optional.empty(),
                true,
                false);
    assertThat(persistResult.isSuccessful()).isTrue();
    final var persistOutputs = persistResult.getYield().orElseThrow();

    blockchain.appendBlock(block, persistOutputs.getReceipts(), persistOutputs.getBlockAccessList());

    final BlockProcessingResult reexecResult =
        ctx.getProtocolSchedule()
            .getByBlockHeader(block.getHeader())
            .getBlockValidator()
            .validateAndProcessBlock(
                ctx.getProtocolContext(),
                block,
                HeaderValidationMode.NONE,
                HeaderValidationMode.NONE,
                blockchain.getBlockAccessList(block.getHash()),
                false,
                false);
    assertThat(reexecResult.isSuccessful()).isTrue();

    final BonsaiExecutionWitnessBuilder.Witness witness =
        new BonsaiExecutionWitnessBuilder()
            .buildWitness(
                block.getHeader(),
                genesisHeader,
                ctx.getStateArchive(),
                blockchain,
                reexecResult.getYield());
    assertThat(witness.state()).isNotEmpty();
    assertThat(witness.headers()).isNotEmpty();

    final byte[] txRaw =
        TransactionEncoder.encodeOpaqueBytes(tx, EncodingContext.BLOCK_BODY).toArrayUnsafe();
    final Optional<BlockAccessList> maybeBal = persistOutputs.getBlockAccessList();
    final Bytes balBytes =
        maybeBal.isPresent() ? RLP.encode(maybeBal.get()::writeTo) : Bytes.EMPTY;
    final byte[] ssz = encodeSszStatelessInput(block, txRaw, witness, CHAIN_ID.longValue(), balBytes);

    final Path outputDir = Paths.get(System.getProperty("user.dir"));
    Files.write(outputDir.resolve("stateless_input.ssz"), ssz);

    // 8-byte zisk length prefix, then schema_id at bytes 8-9
    assertThat(ssz[8]).isEqualTo((byte) 0x00);
    assertThat(ssz[9]).isEqualTo((byte) 0x01);
    assertThat(ssz.length % 8).isEqualTo(0);
  }

  // ── SSZ v0.4.1 encoding helpers ──────────────────────────────────────────────

  /**
   * Standard SSZ variable-size container: N×4-byte LE offset table followed by concatenated field
   * data. This is also the correct encoding for {@code List[ByteList]} (list of byte arrays).
   *
   * <p>NOTE: Tuweni's {@code SSZ.encodeBytesList} produces a non-standard length-prefixed format
   * and must NOT be used here.
   */
  private static Bytes sszVariableContainer(final List<Bytes> fields) {
    if (fields.isEmpty()) return Bytes.EMPTY;
    return SSZ.encode(
        writer -> {
          int offset = fields.size() * 4;
          for (final Bytes field : fields) {
            writer.writeUInt32(offset);
            offset += field.size();
          }
          for (final Bytes field : fields) {
            writer.writeSSZ(field);
          }
        });
  }

  /** SszExecutionRequests with all-empty fields — 3-element variable container, all empty. */
  private static Bytes emptyExecutionRequests() {
    return sszVariableContainer(List.of(Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY));
  }

  /**
   * SszExecutionPayload (V4 Amsterdam, 540-byte fixed region + variable data).
   *
   * <p>off_extra_data == 540 is the V4 signal the zesu decoder checks.
   */
  private static Bytes encodeExecutionPayload(
      final Block block, final byte[] txRaw, final Bytes balBytes) {
    final BlockHeader h = block.getHeader();
    final Bytes txsEncoded = sszVariableContainer(List.of(Bytes.wrap(txRaw)));

    final int offExtraData = 540; // == EP_FIXED_SIZE; signals V4 to the zesu decoder
    final int offTxs = offExtraData + h.getExtraData().size();
    final int offWithdrawals = offTxs + txsEncoded.size();
    final int offBlockAccessList = offWithdrawals; // withdrawals are empty

    // base_fee_per_gas as uint256 LE (32 bytes; zesu reads only the low 8)
    final long baseFee = h.getBaseFee().map(w -> w.getAsBigInteger().longValue()).orElse(0L);
    final Bytes bfpg = Bytes.concatenate(SSZ.encodeUInt64(baseFee), Bytes.wrap(new byte[24]));

    return SSZ.encode(
        writer -> {
          writer.writeSSZ(h.getParentHash().getBytes());   // [0..32]
          writer.writeSSZ(h.getCoinbase().getBytes());     // [32..52]
          writer.writeSSZ(h.getStateRoot().getBytes());    // [52..84]
          writer.writeSSZ(h.getReceiptsRoot().getBytes()); // [84..116]
          writer.writeSSZ(h.getLogsBloom().getBytes());    // [116..372]
          writer.writeSSZ(h.getMixHash().getBytes());      // [372..404]
          writer.writeUInt64(h.getNumber());               // [404..412]
          writer.writeUInt64(h.getGasLimit());             // [412..420]
          writer.writeUInt64(h.getGasUsed());              // [420..428]
          writer.writeUInt64(h.getTimestamp());            // [428..436]
          writer.writeUInt32(offExtraData);                // [436..440]
          writer.writeSSZ(bfpg);                          // [440..472]
          writer.writeSSZ(h.getHash().getBytes());         // [472..504]
          writer.writeUInt32(offTxs);                     // [504..508]
          writer.writeUInt32(offWithdrawals);             // [508..512]
          writer.writeUInt64(0L);                         // [512..520] blob_gas_used
          writer.writeUInt64(
              h.getExcessBlobGas()
                  .map(q -> q.getAsBigInteger().longValue())
                  .orElse(0L));                           // [520..528]
          writer.writeUInt32(offBlockAccessList);         // [528..532]
          writer.writeUInt64(h.getSlotNumber());          // [532..540]
          writer.writeSSZ(h.getExtraData());              // variable: extraData
          writer.writeSSZ(txsEncoded);                    // variable: txs
          // withdrawals: empty (0 bytes between offWithdrawals and offBlockAccessList)
          writer.writeSSZ(balBytes);                      // variable: blockAccessList (RLP)
        });
  }

  /**
   * SszNewPayloadRequest: 44-byte fixed region + execution_payload + versioned_hashes (empty) +
   * execution_requests (all-empty).
   */
  private static Bytes encodeNewPayloadRequest(
      final Block block, final byte[] txRaw, final Bytes balBytes) {
    final Bytes epBytes = encodeExecutionPayload(block, txRaw, balBytes);
    final Bytes erBytes = emptyExecutionRequests();

    final int offEp = 44;
    final int offVh = offEp + epBytes.size(); // versioned_hashes (empty → off_vh == off_er)
    final int offEr = offVh;

    return SSZ.encode(
        writer -> {
          writer.writeUInt32(offEp);                      // [0..4]
          writer.writeUInt32(offVh);                      // [4..8]
          writer.writeSSZ(Bytes.wrap(new byte[32]));      // [8..40] parent_beacon_block_root
          writer.writeUInt32(offEr);                      // [40..44]
          writer.writeSSZ(epBytes);
          // versioned_hashes: empty (0 bytes between off_vh and off_er)
          writer.writeSSZ(erBytes);
        });
  }

  /** SszExecutionWitness: 3-field all-variable container; each field is a List[ByteList]. */
  private static Bytes encodeWitnessBytes(final BonsaiExecutionWitnessBuilder.Witness witness) {
    final Bytes stateEnc =
        sszVariableContainer(
            witness.state().stream().map(Bytes::fromHexString).collect(Collectors.toList()));
    final Bytes codesEnc =
        sszVariableContainer(
            witness.codes().stream().map(Bytes::fromHexString).collect(Collectors.toList()));
    final Bytes headersEnc =
        sszVariableContainer(
            witness.headers().stream().map(Bytes::fromHexString).collect(Collectors.toList()));
    return sszVariableContainer(List.of(stateEnc, codesEnc, headersEnc));
  }

  /**
   * SszChainConfig: chain_id(u64 LE) + off_active_fork(u32 LE) + fork_idx(u64 LE).
   *
   * <p>Amsterdam fork index is 24 (from zesu's {@code forkNameFromIndex}).
   */
  private static Bytes encodeChainConfig(final long chainId) {
    return SSZ.encode(
        writer -> {
          writer.writeUInt64(chainId);
          writer.writeUInt32(12L); // off_active_fork (right after chain_id + this u32)
          writer.writeUInt64(24L); // Amsterdam = index 24
        });
  }

  /**
   * Full SszStatelessInput (v0.4.1): zisk 8-byte length header + schema_id(0x0001 BE) + 16-byte
   * all-variable container body.
   *
   * <p>The zisk input memory layout at ZISK_INPUT_BASE+8 is: [input_len: u64 LE][payload...]. The
   * -i file is mapped starting at ZISK_INPUT_BASE+8, so file bytes 0-7 become input_len.
   *
   * <p>Includes the sender's uncompressed 65-byte secp256k1 public key so zesu can skip ecrecover.
   */
  private static byte[] encodeSszStatelessInput(
      final Block block,
      final byte[] txRaw,
      final BonsaiExecutionWitnessBuilder.Witness witness,
      final long chainId,
      final Bytes balBytes) {
    final Bytes nprBytes = encodeNewPayloadRequest(block, txRaw, balBytes);
    final Bytes witnessBytes = encodeWitnessBytes(witness);
    final Bytes chainConfigBytes = encodeChainConfig(chainId);
    final Bytes pubkeyBytes =
        Bytes.concatenate(Bytes.of(0x04), SENDER_KEY_PAIR.getPublicKey().getEncodedBytes());

    // All 4 fields are variable → 16-byte offset table + content
    final Bytes sszBody = sszVariableContainer(List.of(nprBytes, witnessBytes, chainConfigBytes, pubkeyBytes));

    // schema_id prefix (2 bytes: 0x00, 0x01)
    final Bytes sszPayload = Bytes.concatenate(Bytes.of(0x00, 0x01), sszBody);

    // input_len = actual SSZ size (decoder must NOT see trailing padding bytes —
    // pubkeys_data = body[off_pubkeys..buf_size], padding would break the 65-byte alignment check).
    // File must be 8-byte aligned for the emulator; pad after the actual content.
    final int actualLen = sszPayload.size();
    final int paddedLen = (actualLen + 7) & ~7;
    final byte[] file = new byte[8 + paddedLen];
    System.arraycopy(SSZ.encodeUInt64((long) actualLen).toArrayUnsafe(), 0, file, 0, 8);
    System.arraycopy(sszPayload.toArrayUnsafe(), 0, file, 8, actualLen);
    // bytes from 8+actualLen to end are zero-initialized padding (ignored by decoder)
    return file;
  }

  private ExecutionContextTestFixture buildExecutionContext() {
    final TransactionValidatorFactory alwaysValid = mock(TransactionValidatorFactory.class);
    when(alwaysValid.get()).thenReturn(new AlwaysValidTransactionValidator());

    final ProtocolSpecAdapters adapters =
        ProtocolSpecAdapters.create(
            0,
            spec -> {
              spec.isReplayProtectionSupported(true);
              spec.transactionValidatorFactoryBuilder(
                  (evm, gasLimitCalculator, feeMarket) -> alwaysValid);
              return spec;
            });

    final ProtocolSchedule schedule =
        new ProtocolScheduleBuilder(
                genesisConfig.getConfigOptions(),
                Optional.of(CHAIN_ID),
                adapters,
                false,
                EvmConfiguration.DEFAULT,
                MiningConfiguration.MINING_DISABLED,
                new BadBlockManager(),
                false,
                ImmutableBalConfiguration.builder().build(),
                new NoOpMetricsSystem())
            .createProtocolSchedule();

    return ExecutionContextTestFixture.builder(genesisConfig)
        .protocolSchedule(schedule)
        .dataStorageFormat(DataStorageFormat.BONSAI)
        .build();
  }

  private TestBlockCreator buildBlockCreator(final ExecutionContextTestFixture ctx) {
    final BlockHeader parentHeader = ctx.getBlockchain().getChainHeadHeader();
    final TransactionPoolConfiguration poolConf =
        ImmutableTransactionPoolConfiguration.builder().txPoolMaxSize(100).build();
    final AbstractPendingTransactionsSorter sorter =
        new GasPricePendingTransactionsSorter(
            poolConf,
            Clock.systemUTC(),
            new NoOpMetricsSystem(),
            Suppliers.ofInstance(parentHeader));

    final EthContext ethContext = mock(EthContext.class, RETURNS_DEEP_STUBS);
    when(ethContext.getEthPeers().subscribeConnect(any())).thenReturn(1L);

    final TransactionPool txPool =
        new TransactionPool(
            () -> sorter,
            ctx.getProtocolSchedule(),
            ctx.getProtocolContext(),
            mock(TransactionBroadcaster.class),
            ethContext,
            new TransactionPoolMetrics(new NoOpMetricsSystem()),
            poolConf,
            new BlobCache());
    txPool.setEnabled();

    final MiningConfiguration miningConfig =
        ImmutableMiningConfiguration.builder()
            .mutableInitValues(
                MutableInitValues.builder()
                    .extraData(Bytes.fromHexString("deadbeef"))
                    .minTransactionGasPrice(Wei.ONE)
                    .coinbase(Address.ZERO)
                    .build())
            .build();

    return new TestBlockCreator(
        miningConfig,
        (__, ___) -> Address.ZERO,
        __ -> Bytes.fromHexString("deadbeef"),
        txPool,
        ctx.getProtocolContext(),
        ctx.getProtocolSchedule(),
        ethScheduler);
  }

  static class TestBlockCreator extends AbstractBlockCreator {
    protected TestBlockCreator(
        final MiningConfiguration miningConfiguration,
        final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
        final ExtraDataCalculator extraDataCalculator,
        final TransactionPool transactionPool,
        final org.hyperledger.besu.ethereum.ProtocolContext protocolContext,
        final ProtocolSchedule protocolSchedule,
        final org.hyperledger.besu.ethereum.eth.manager.EthScheduler ethScheduler) {
      super(
          miningConfiguration,
          miningBeneficiaryCalculator,
          extraDataCalculator,
          transactionPool,
          protocolContext,
          protocolSchedule,
          ethScheduler);
    }

    @Override
    protected BlockHeader createFinalBlockHeader(final SealableBlockHeader sealableBlockHeader) {
      return BlockHeaderBuilder.create()
          .difficulty(Difficulty.ZERO)
          .populateFrom(sealableBlockHeader)
          .mixHash(org.hyperledger.besu.datatypes.Hash.EMPTY)
          .nonce(0L)
          .blockHeaderFunctions(blockHeaderFunctions)
          .buildBlockHeader();
    }
  }

  static class AlwaysValidTransactionValidator implements TransactionValidator {
    @Override
    public ValidationResult<TransactionInvalidReason> validate(
        final Transaction transaction,
        final Optional<Wei> baseFee,
        final Optional<Wei> blobBaseFee,
        final TransactionValidationParams transactionValidationParams) {
      return ValidationResult.valid();
    }

    @Override
    public ValidationResult<TransactionInvalidReason> validateForSender(
        final Transaction transaction,
        final Account sender,
        final TransactionValidationParams validationParams) {
      return ValidationResult.valid();
    }
  }
}
