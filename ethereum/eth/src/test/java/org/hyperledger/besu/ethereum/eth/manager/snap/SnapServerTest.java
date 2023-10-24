/*
 * Copyright Hyperledger Besu Contributors
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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.hyperledger.besu.ethereum.eth.manager.snap.SnapServer.HASH_LAST;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.ByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetAccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetStorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetTrieNodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.TrieNodesMessage;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.StateTrieAccountValue;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration.DEFAULT_CONFIG;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class SnapServerTest {
  static Random rand = new Random();

  record SnapTestAccount(
      Hash addressHash,
      StateTrieAccountValue accountValue,
      MerkleTrie<Bytes32, Bytes> storage,
      Bytes code) {
    Bytes accountRLP() {
      return RLP.encode(accountValue::writeTo);
    }
  }

  static final ObservableMetricsSystem noopMetrics = new NoOpMetricsSystem();

  final KeyValueStorageProvider storageProvider = new InMemoryKeyValueStorageProvider();
  final BonsaiWorldStateKeyValueStorage inMemoryStorage =
      new BonsaiWorldStateKeyValueStorage(storageProvider, noopMetrics, DEFAULT_CONFIG);

  final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
      new StoredMerklePatriciaTrie<>(
          inMemoryStorage::getAccountStateTrieNode, Function.identity(), Function.identity());
  final WorldStateProofProvider proofProvider = new WorldStateProofProvider(inMemoryStorage);

  final SnapServer snapServer =
      new SnapServer(new EthMessages(), __ -> Optional.of(inMemoryStorage));

  final SnapTestAccount acct1 = createTestAccount("10");
  final SnapTestAccount acct2 = createTestAccount("20");
  final SnapTestAccount acct3 = createTestContractAccount("30", inMemoryStorage);
  final SnapTestAccount acct4 = createTestContractAccount("40", inMemoryStorage);

  // TODO: add requestID assertions (and implementations in their corresponding message parsing)

  @Test
  public void assertEmptyRangeLeftProofOfExclusionAndNextAccount() {
    // for a range request that returns empty, we should return just a proof of exclusion on the
    // left and the next account after the limit hash
    insertTestAccounts(acct1, acct4);

    var rangeData =
        getAndVerifyAccountRangeData(requestAccountRange(acct2.addressHash, acct3.addressHash), 1);

    // expect to find only one value acct4, outside the requested range
    var outOfRangeVal = rangeData.accounts().entrySet().stream().findFirst();
    assertThat(outOfRangeVal).isPresent();
    assertThat(outOfRangeVal.get().getKey()).isEqualTo(acct4.addressHash());

    // assert proofs are valid for the requested range
    assertThat(assertIsValidAccountRangeProof(acct2.addressHash, rangeData)).isTrue();
  }

  @Test
  public void assertAccountLimitRangeResponse() {
    // assert we limit the range response according to size
    final int acctCount = 2000;
    final long acctRLPSize = 105;

    List<Integer> randomLoad = IntStream.range(1, 4096).boxed().collect(Collectors.toList());
    Collections.shuffle(randomLoad);
    randomLoad.stream()
        .forEach(
            i ->
                insertTestAccounts(
                    createTestAccount(
                        Bytes.concatenate(
                                Bytes.fromHexString("0x40"),
                                Bytes.fromHexStringLenient(Integer.toHexString(i * 256)))
                            .toHexString())));

    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBytes(storageTrie.getRootHash());
    tmp.writeBytes(Hash.ZERO);
    tmp.writeBytes(HASH_LAST);
    tmp.writeBigIntegerScalar(BigInteger.valueOf(acctRLPSize * acctCount));
    tmp.endList();
    var tinyRangeLimit = new GetAccountRangeMessage(tmp.encoded()).wrapMessageData(BigInteger.ONE);

    var rangeData =
        getAndVerifyAccountRangeData(
            (AccountRangeMessage) snapServer.constructGetAccountRangeResponse(tinyRangeLimit),
            acctCount);

    // assert proofs are valid for the requested range
    assertThat(assertIsValidAccountRangeProof(Hash.ZERO, rangeData)).isTrue();
  }

  @Test
  public void assertLastEmptyRange() {
    // When our final range request is empty, no next account is possible,
    //      and we should return just a proof of exclusion of the right
    insertTestAccounts(acct1, acct2);
    var rangeData =
        getAndVerifyAccountRangeData(requestAccountRange(acct3.addressHash, acct4.addressHash), 0);

    // assert proofs are valid for the requested range
    assertThat(assertIsValidAccountRangeProof(acct3.addressHash, rangeData)).isTrue();
  }

  @Test
  public void assertAccountFoundAtStartHashProof() {
    // account found at startHash
    insertTestAccounts(acct4, acct3, acct1, acct2);
    var rangeData =
        getAndVerifyAccountRangeData(requestAccountRange(acct1.addressHash, acct4.addressHash), 4);

    // assert proofs are valid for requested range
    assertThat(assertIsValidAccountRangeProof(acct1.addressHash, rangeData)).isTrue();
  }

  @Test
  public void assertCompleteStorageForSingleAccount() {
    insertTestAccounts(acct1, acct2, acct3, acct4);
    var rangeData = requestStorageRange(List.of(acct3.addressHash), Hash.ZERO, HASH_LAST);
    assertThat(rangeData).isNotNull();
    var slotsData = rangeData.slotsData(false);
    assertThat(slotsData).isNotNull();
    assertThat(slotsData.slots()).isNotNull();
    assertThat(slotsData.slots().size()).isEqualTo(1);
    var firstAccountStorages = slotsData.slots().first();
    assertThat(firstAccountStorages.size()).isEqualTo(10);
    // no proofs for complete storage range:
    assertThat(slotsData.proofs().size()).isEqualTo(0);

    // TODO: fixme, prob a fixture issue with contract storage.
    // assertThat(
    //        assertIsValidStorageProof(acct3, Hash.ZERO, firstAccountStorages, slotsData.proofs()))
    //    .isTrue();
  }

  @Test
  public void assertStorageLimitRangeResponse() {
    // assert we limit the range response according to bytessize
    final int storageSlotSize = 70;
    final int storageSlotCount = 16;
    insertTestAccounts(acct1, acct2, acct3, acct4);

    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBigIntegerScalar(BigInteger.ONE);
    tmp.writeBytes(storageTrie.getRootHash());
    tmp.writeList(
        List.of(acct3.addressHash, acct4.addressHash),
        (hash, rlpOutput) -> rlpOutput.writeBytes(hash));
    tmp.writeBytes(Hash.ZERO);
    tmp.writeBytes(HASH_LAST);
    tmp.writeBigIntegerScalar(BigInteger.valueOf(storageSlotCount * storageSlotSize));
    tmp.endList();
    var tinyRangeLimit = new GetStorageRangeMessage(tmp.encoded());

    var rangeData =
        (StorageRangeMessage) snapServer.constructGetStorageRangeResponse(tinyRangeLimit);

    // assert proofs are valid for the requested range
    assertThat(rangeData).isNotNull();
    var slotsData = rangeData.slotsData(false);
    assertThat(slotsData).isNotNull();
    assertThat(slotsData.slots()).isNotNull();
    assertThat(slotsData.slots().size()).isEqualTo(2);
    var firstAccountStorages = slotsData.slots().first();
    // expecting to see complete 10 slot storage for acct3
    assertThat(firstAccountStorages.size()).isEqualTo(10);
    var secondAccountStorages = slotsData.slots().last();
    // expecting to see only 6 since request was limited to 16 slots
    assertThat(secondAccountStorages.size()).isEqualTo(6);
    // proofs required for interrupted storage range:
    assertThat(slotsData.proofs().size()).isNotEqualTo(0);

    // TODO: fixme, prob a fixture issue with contract storage.
    // assertThat(
    //        assertIsValidStorageProof(acct4, Hash.ZERO, secondAccountStorages,
    // slotsData.proofs()))
    //    .isTrue();

  }

  @Test
  public void assertAccountTriePathRequest() {
    insertTestAccounts(acct1, acct2, acct3, acct4);
    var partialPathToAcct2 = CompactEncoding.bytesToPath(acct2.addressHash).slice(0, 1);
    var partialPathToAcct1 = Bytes.fromHexString("0x01"); // first nibble is 1
    var trieNodeRequest =
        requestTrieNodes(
            storageTrie.getRootHash(),
            List.of(List.of(partialPathToAcct2), List.of(partialPathToAcct1)));
    assertThat(trieNodeRequest).isNotNull();
    List<Bytes> trieNodes = trieNodeRequest.nodes(false);
    assertThat(trieNodes).isNotNull();
    assertThat(trieNodes.size()).isEqualTo(2);
  }

  @Test
  public void assertStorageTriePathRequest() {
    insertTestAccounts(acct1, acct2, acct3, acct4);
    var pathToSlot11 = CompactEncoding.encode(Bytes.fromHexStringLenient("0x0101"));
    var pathToSlot12 = CompactEncoding.encode(Bytes.fromHexStringLenient("0x0102"));
    var pathToSlot1a = CompactEncoding.encode(Bytes.fromHexStringLenient("0x010A")); // not present
    var trieNodeRequest =
        requestTrieNodes(
            storageTrie.getRootHash(),
            List.of(
                List.of(acct3.addressHash, pathToSlot11, pathToSlot12, pathToSlot1a),
                List.of(acct4.addressHash, pathToSlot11, pathToSlot12, pathToSlot1a)));
    assertThat(trieNodeRequest).isNotNull();
    List<Bytes> trieNodes = trieNodeRequest.nodes(false);
    assertThat(trieNodes).isNotNull();
    assertThat(trieNodes.size()).isEqualTo(4);
  }

  @Test
  public void assertCodePresent() {
    insertTestAccounts(acct1, acct2, acct3, acct4);
    var codeRequest =
        requestByteCodes(
            List.of(acct3.accountValue.getCodeHash(), acct4.accountValue.getCodeHash()));
    assertThat(codeRequest).isNotNull();
    ByteCodesMessage.ByteCodes codes = codeRequest.bytecodes(false);
    assertThat(codes).isNotNull();
    assertThat(codes.codes().size()).isEqualTo(2);
  }

  static SnapTestAccount createTestAccount(final String hexAddr) {
    return new SnapTestAccount(
        Hash.wrap(Bytes32.rightPad(Bytes.fromHexString(hexAddr))),
        new StateTrieAccountValue(
            rand.nextInt(0, 1), Wei.of(rand.nextLong(0L, 1L)), Hash.EMPTY_TRIE_HASH, Hash.EMPTY),
        new SimpleMerklePatriciaTrie<>(a -> a),
        Bytes.EMPTY);
  }

  static SnapTestAccount createTestContractAccount(
      final String hexAddr, final BonsaiWorldStateKeyValueStorage storage) {
    Hash acctHash = Hash.wrap(Bytes32.rightPad(Bytes.fromHexString(hexAddr)));
    MerkleTrie<Bytes32, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (loc, hash) -> storage.getAccountStorageTrieNode(acctHash, loc, hash),
            Hash.EMPTY_TRIE_HASH,
            a -> a,
            a -> a);
    Bytes32 mockCode = Bytes32.random();

    // mock some storage data
    var flatdb = storage.getFlatDbStrategy();
    var updater = storage.updater();
    updater.putCode(Hash.hash(mockCode), mockCode);
    IntStream.range(10, 20)
        .boxed()
        .forEach(
            i -> {
              Bytes32 mockBytes32 = Bytes32.rightPad(Bytes.fromHexString(i.toString()));
              trie.put(mockBytes32, mockBytes32);
              flatdb.putFlatAccountStorageValueByStorageSlotHash(
                  updater.getWorldStateTransaction(),
                  acctHash,
                  Hash.wrap(mockBytes32),
                  mockBytes32);
            });
    trie.commit(
        (location, key, value) ->
            updater.putAccountStorageTrieNode(acctHash, location, key, value));
    updater.commit();
    return new SnapTestAccount(
        acctHash,
        new StateTrieAccountValue(
            rand.nextInt(0, 1), Wei.of(rand.nextLong(0L, 1L)),
            Hash.wrap(trie.getRootHash()), Hash.hash(mockCode)),
        trie,
        mockCode);
  }

  void insertTestAccounts(final SnapTestAccount... accounts) {
    final var updater = inMemoryStorage.updater();
    for (SnapTestAccount account : accounts) {
      updater.putAccountInfoState(account.addressHash(), account.accountRLP());
      storageTrie.put(account.addressHash(), account.accountRLP());
    }
    storageTrie.commit(updater::putAccountStateTrieNode);
    updater.commit();
  }

  boolean assertIsValidAccountRangeProof(
      final Hash startHash, final AccountRangeMessage.AccountRangeData accountRange) {
    Bytes32 lastKey =
        Optional.of(accountRange.accounts())
            .filter(z -> z.size() > 0)
            .map(NavigableMap::lastKey)
            .orElse(startHash);

    return proofProvider.isValidRangeProof(
        startHash,
        lastKey,
        storageTrie.getRootHash(),
        accountRange.proofs(),
        accountRange.accounts());
  }

  boolean assertIsValidStorageProof(
      final SnapTestAccount account,
      final Hash startHash,
      final NavigableMap<Bytes32, Bytes> slotRangeData,
      final List<Bytes> proofs) {

    Bytes32 lastKey =
        Optional.of(slotRangeData)
            .filter(z -> z.size() > 0)
            .map(NavigableMap::lastKey)
            .orElse(startHash);

    // this is only working for single account ranges for now
    return proofProvider.isValidRangeProof(
        startHash, lastKey, account.accountValue.getStorageRoot(), proofs, slotRangeData);
  }

  AccountRangeMessage requestAccountRange(final Hash startHash, final Hash limitHash) {
    return (AccountRangeMessage)
        snapServer.constructGetAccountRangeResponse(
            GetAccountRangeMessage.create(
                    Hash.wrap(storageTrie.getRootHash()), startHash, limitHash)
                .wrapMessageData(BigInteger.ONE));
  }

  StorageRangeMessage requestStorageRange(
      final List<Bytes32> accountHashes, final Hash startHash, final Hash limitHash) {
    return (StorageRangeMessage)
        snapServer.constructGetStorageRangeResponse(
            GetStorageRangeMessage.create(
                    Hash.wrap(storageTrie.getRootHash()), accountHashes, startHash, limitHash)
                .wrapMessageData(BigInteger.ONE));
  }

  TrieNodesMessage requestTrieNodes(final Bytes32 rootHash, final List<List<Bytes>> trieNodesList) {
    return (TrieNodesMessage)
        snapServer.constructGetTrieNodesResponse(
            GetTrieNodesMessage.create(Hash.wrap(rootHash), trieNodesList)
                .wrapMessageData(BigInteger.ONE));
  }

  ByteCodesMessage requestByteCodes(final List<Bytes32> codeHashes) {
    return (ByteCodesMessage)
        snapServer.constructGetBytecodesResponse(
            GetByteCodesMessage.create(codeHashes).wrapMessageData(BigInteger.ONE));
  }

  AccountRangeMessage.AccountRangeData getAndVerifyAccountRangeData(
      final AccountRangeMessage range, final int expectedSize) {
    assertThat(range).isNotNull();
    var accountData = range.accountData(false);
    assertThat(accountData).isNotNull();
    assertThat(accountData.accounts().size()).isEqualTo(expectedSize);
    return accountData;
  }
}
