package org.hyperledger.besu.evm.gascalculator.stateless;

import static org.hyperledger.besu.ethereum.trie.verkle.util.Parameters.BALANCE_LEAF_KEY;
import static org.hyperledger.besu.ethereum.trie.verkle.util.Parameters.CODE_KECCAK_LEAF_KEY;
import static org.hyperledger.besu.ethereum.trie.verkle.util.Parameters.CODE_OFFSET;
import static org.hyperledger.besu.ethereum.trie.verkle.util.Parameters.CODE_SIZE_LEAF_KEY;
import static org.hyperledger.besu.ethereum.trie.verkle.util.Parameters.NONCE_LEAF_KEY;
import static org.hyperledger.besu.ethereum.trie.verkle.util.Parameters.VERKLE_NODE_WIDTH;
import static org.hyperledger.besu.ethereum.trie.verkle.util.Parameters.VERSION_LEAF_KEY;
import static org.hyperledger.besu.evm.internal.Words.clampedAdd;

import org.hyperledger.besu.datatypes.AccessWitness;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.trie.verkle.adapter.TrieKeyAdapter;
import org.hyperledger.besu.ethereum.trie.verkle.hasher.PedersenHasher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Eip4762AccessWitness implements org.hyperledger.besu.datatypes.AccessWitness {

  private static final TrieKeyAdapter TRIE_KEY_ADAPTER = new TrieKeyAdapter(new PedersenHasher());
  private static final Logger LOG = LoggerFactory.getLogger(AccessWitness.class);
  private static final long WITNESS_BRANCH_READ_COST = 1900;
  private static final long WITNESS_CHUNK_READ_COST = 200;
  private static final long WITNESS_BRANCH_WRITE_COST = 3000;
  private static final long WITNESS_CHUNK_WRITE_COST = 500;
  private static final long WITNESS_CHUNK_FILL_COST = 6200;

  private static final UInt256 zeroTreeIndex = UInt256.ZERO;
  private static final byte AccessWitnessReadFlag = 1;
  private static final byte AccessWitnessWriteFlag = 2;
  private final Map<BranchAccessKey, Byte> branches;
  private final Map<ChunkAccessKey, Byte> chunks;
  private long totalWitnessGas = 0L;

  public Eip4762AccessWitness() {
    this(new HashMap<>(), new HashMap<>());
  }

  public Eip4762AccessWitness(
      final Map<BranchAccessKey, Byte> branches, final Map<ChunkAccessKey, Byte> chunks) {
    this.branches = branches;
    this.chunks = chunks;
  }

  @Override
  public List<Address> keys() {
    return this.chunks.keySet().stream()
        .map(chunkAccessKey -> chunkAccessKey.branchAccessKey().address())
        .toList();
  }

  @Override
  public long touchAndChargeProofOfAbsence(final Address address) {
    long gas = 0;
    gas =
        clampedAdd(
            gas, internalTouchAddressOnReadAndComputeGas(address, zeroTreeIndex, VERSION_LEAF_KEY));
    gas =
        clampedAdd(
            gas, internalTouchAddressOnReadAndComputeGas(address, zeroTreeIndex, BALANCE_LEAF_KEY));
    gas =
        clampedAdd(
            gas, internalTouchAddressOnReadAndComputeGas(address, zeroTreeIndex, NONCE_LEAF_KEY));
    gas =
        clampedAdd(
            gas,
            internalTouchAddressOnReadAndComputeGas(address, zeroTreeIndex, CODE_KECCAK_LEAF_KEY));
    gas =
        clampedAdd(
            gas,
            internalTouchAddressOnReadAndComputeGas(address, zeroTreeIndex, CODE_SIZE_LEAF_KEY));
    totalWitnessGas += gas;
    return gas;
  }

  @Override
  public long touchAndChargeMessageCall(final Address address) {
    long gas = 0;
    gas =
        clampedAdd(
            gas, internalTouchAddressOnReadAndComputeGas(address, zeroTreeIndex, VERSION_LEAF_KEY));
    gas =
        clampedAdd(
            gas,
            internalTouchAddressOnReadAndComputeGas(address, zeroTreeIndex, CODE_SIZE_LEAF_KEY));
    totalWitnessGas += gas;
    return gas;
  }

  @Override
  public long touchAndChargeValueTransfer(final Address caller, final Address target) {
    long gas = 0;
    gas =
        clampedAdd(
            gas, internalTouchAddressOnWriteAndComputeGas(caller, zeroTreeIndex, BALANCE_LEAF_KEY));
    gas =
        clampedAdd(
            gas, internalTouchAddressOnWriteAndComputeGas(target, zeroTreeIndex, BALANCE_LEAF_KEY));
    totalWitnessGas += gas;
    return gas;
  }

  @Override
  public long touchAndChargeContractCreateInit(
      final Address address, final boolean createSendsValue) {
    long gas = 0;

    gas =
        clampedAdd(
            gas,
            internalTouchAddressOnWriteAndComputeGas(address, zeroTreeIndex, VERSION_LEAF_KEY));
    gas =
        clampedAdd(
            gas, internalTouchAddressOnWriteAndComputeGas(address, zeroTreeIndex, NONCE_LEAF_KEY));

    if (createSendsValue) {
      gas =
          clampedAdd(
              gas,
              internalTouchAddressOnWriteAndComputeGas(address, zeroTreeIndex, BALANCE_LEAF_KEY));
    }
    totalWitnessGas += gas;
    return gas;
  }

  @Override
  public long touchAndChargeContractCreateCompleted(final Address address) {
    long gas = 0;

    gas =
        clampedAdd(
            gas,
            internalTouchAddressOnWriteAndComputeGas(address, zeroTreeIndex, VERSION_LEAF_KEY));
    gas =
        clampedAdd(
            gas,
            internalTouchAddressOnWriteAndComputeGas(address, zeroTreeIndex, BALANCE_LEAF_KEY));
    gas =
        clampedAdd(
            gas, internalTouchAddressOnWriteAndComputeGas(address, zeroTreeIndex, NONCE_LEAF_KEY));
    gas =
        clampedAdd(
            gas,
            internalTouchAddressOnWriteAndComputeGas(address, zeroTreeIndex, CODE_KECCAK_LEAF_KEY));
    gas =
        clampedAdd(
            gas,
            internalTouchAddressOnWriteAndComputeGas(address, zeroTreeIndex, CODE_SIZE_LEAF_KEY));
    totalWitnessGas += gas;
    return gas;
  }

  @Override
  public long touchTxOriginAndComputeGas(final Address origin) {
    long gas = 0;

    gas =
        clampedAdd(
            gas, internalTouchAddressOnReadAndComputeGas(origin, zeroTreeIndex, VERSION_LEAF_KEY));
    gas =
        clampedAdd(
            gas, internalTouchAddressOnWriteAndComputeGas(origin, zeroTreeIndex, BALANCE_LEAF_KEY));
    gas =
        clampedAdd(
            gas, internalTouchAddressOnWriteAndComputeGas(origin, zeroTreeIndex, NONCE_LEAF_KEY));
    gas =
        clampedAdd(
            gas,
            internalTouchAddressOnReadAndComputeGas(origin, zeroTreeIndex, CODE_KECCAK_LEAF_KEY));
    gas =
        clampedAdd(
            gas,
            internalTouchAddressOnReadAndComputeGas(origin, zeroTreeIndex, CODE_SIZE_LEAF_KEY));

    // modifying this after update on EIP-4762 to not charge simple transfers
    LOG.trace("touchTxOriginAndComputeGas {} not charged gas: {}", origin, gas);

    return 0;
  }

  @SuppressWarnings("unused")
  @Override
  public long touchTxExistingAndComputeGas(final Address target, final boolean sendsValue) {
    long gas = 0;

    gas =
        clampedAdd(
            gas, internalTouchAddressOnReadAndComputeGas(target, zeroTreeIndex, VERSION_LEAF_KEY));
    gas =
        clampedAdd(
            gas, internalTouchAddressOnReadAndComputeGas(target, zeroTreeIndex, NONCE_LEAF_KEY));
    gas =
        clampedAdd(
            gas,
            internalTouchAddressOnReadAndComputeGas(target, zeroTreeIndex, CODE_SIZE_LEAF_KEY));
    gas =
        clampedAdd(
            gas,
            internalTouchAddressOnReadAndComputeGas(target, zeroTreeIndex, CODE_KECCAK_LEAF_KEY));

    if (sendsValue) {
      gas =
          clampedAdd(
              gas,
              internalTouchAddressOnWriteAndComputeGas(target, zeroTreeIndex, BALANCE_LEAF_KEY));
    } else {
      gas =
          clampedAdd(
              gas,
              internalTouchAddressOnReadAndComputeGas(target, zeroTreeIndex, BALANCE_LEAF_KEY));
    }
    // modifying this after update on EIP-4762 to not charge simple transfers
    LOG.trace(
        "touchTxExistingAndComputeGas target: {} sendsValue {} not charged gas {}",
        target,
        sendsValue,
        gas);

    return 0;
  }

  @Override
  public long touchCodeChunksUponContractCreation(final Address address, final long codeLength) {

    long gas = 0;
    for (long i = 0; i < (codeLength + 30) / 31; i++) {
      gas =
          clampedAdd(
              gas,
              internalTouchAddressOnWriteAndComputeGas(
                  address,
                  CODE_OFFSET.add(i).divide(VERKLE_NODE_WIDTH),
                  CODE_OFFSET.add(i).mod(VERKLE_NODE_WIDTH)));
    }
    System.out.println(
        "touchCodeChunksUponContractCreation: "
            + address
            + " codeLength: "
            + codeLength
            + " gas: "
            + gas);
    totalWitnessGas += gas;

    return gas;
  }

  @Override
  public long touchCodeChunks(
      final Address address, final long startPc, final long readSize, final long codeLength) {
    long gas = 0;
    if ((readSize == 0 && codeLength == 0) || startPc > codeLength) {
      return 0;
    }
    long endPc = startPc + readSize;
    if (endPc > codeLength) {
      endPc = codeLength;
    }
    if (endPc > 0) {
      endPc -= 1;
    }
    for (long i = startPc / 31; i <= endPc / 31; i++) {
      gas =
          clampedAdd(
              gas,
              touchAddressOnReadAndComputeGas(
                  address,
                  CODE_OFFSET.add(i).divide(VERKLE_NODE_WIDTH),
                  CODE_OFFSET.add(i).mod(VERKLE_NODE_WIDTH)));
    }
    System.out.println(
        "touchCodeChunks: " + address + " codeLength: " + codeLength + " gas: " + gas);
    totalWitnessGas += gas;
    return gas;
  }

  @Override
  public long touchAddressOnWriteAndComputeGas(
      final Address address, final UInt256 treeIndex, final UInt256 subIndex) {
    long gas = internalTouchAddressOnWriteAndComputeGas(address, treeIndex, subIndex);
    totalWitnessGas += gas;
    return gas;
  }

  protected long internalTouchAddressOnWriteAndComputeGas(
      final Address address, final UInt256 treeIndex, final UInt256 subIndex) {
    return touchAddressAndChargeGas(address, treeIndex, subIndex, true);
  }

  @Override
  public long touchAddressOnReadAndComputeGas(
      final Address address, final UInt256 treeIndex, final UInt256 subIndex) {
    long gas = internalTouchAddressOnReadAndComputeGas(address, treeIndex, subIndex);
    totalWitnessGas += gas;
    return gas;
  }

  protected long internalTouchAddressOnReadAndComputeGas(
      final Address address, final UInt256 treeIndex, final UInt256 subIndex) {
    return touchAddressAndChargeGas(address, treeIndex, subIndex, false);
  }

  protected long touchAddressAndChargeGas(
      final Address address,
      final UInt256 treeIndex,
      final UInt256 subIndex,
      final boolean isWrite) {

    AccessEvents accessEvent = touchAddress(address, treeIndex, subIndex, isWrite);
    long gas = 0;
    if (accessEvent.isBranchRead()) {
      gas = clampedAdd(gas, WITNESS_BRANCH_READ_COST);
    }
    if (accessEvent.isChunkRead()) {
      gas = clampedAdd(gas, WITNESS_CHUNK_READ_COST);
    }
    if (accessEvent.isBranchWrite()) {
      gas = clampedAdd(gas, WITNESS_BRANCH_WRITE_COST);
    }
    if (accessEvent.isChunkWrite()) {
      gas = clampedAdd(gas, WITNESS_CHUNK_WRITE_COST);
    }
    if (accessEvent.isChunkFill()) {
      gas = clampedAdd(gas, WITNESS_CHUNK_FILL_COST);
    }

    return gas;
  }

  public AccessEvents touchAddress(
      final Address addr, final UInt256 treeIndex, final UInt256 subIndex, final boolean isWrite) {
    AccessEvents accessEvents = new AccessEvents();
    BranchAccessKey branchKey = new BranchAccessKey(addr, treeIndex);

    ChunkAccessKey chunkKey = new ChunkAccessKey(addr, treeIndex, subIndex);

    // Read access.
    if (!this.branches.containsKey(branchKey)) {
      accessEvents.setBranchRead(true);
      this.branches.put(branchKey, AccessWitnessReadFlag);
    }
    if (!this.chunks.containsKey(chunkKey)) {
      accessEvents.setChunkRead(true);
      this.chunks.put(chunkKey, AccessWitnessReadFlag);
    }

    // TODO VERKLE: for now testnet doesn't charge
    //  chunk filling costs if the leaf was previously empty in the state
    //    boolean chunkFill = false;

    if (isWrite) {

      if ((this.branches.get(branchKey) & AccessWitnessWriteFlag) == 0) {
        accessEvents.setBranchWrite(true);
        this.branches.put(
            branchKey, (byte) (this.branches.get(branchKey) | AccessWitnessWriteFlag));
      }

      byte chunkValue = this.chunks.get(chunkKey);

      if ((chunkValue & AccessWitnessWriteFlag) == 0) {
        accessEvents.setChunkWrite(true);
        this.chunks.put(chunkKey, (byte) (this.chunks.get(chunkKey) | AccessWitnessWriteFlag));
      }
    }

    return accessEvents;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Eip4762AccessWitness that = (Eip4762AccessWitness) o;
    return Objects.equals(branches, that.branches) && Objects.equals(chunks, that.chunks);
  }

  @Override
  public int hashCode() {
    return Objects.hash(branches, chunks);
  }

  @Override
  public String toString() {
    return "AccessWitness{" + "branches=" + branches + ", chunks=" + chunks + '}';
  }

  public Map<BranchAccessKey, Byte> getBranches() {
    return branches;
  }

  public Map<ChunkAccessKey, Byte> getChunks() {
    return chunks;
  }

  public record BranchAccessKey(Address address, UInt256 treeIndex) {}
  ;

  public record ChunkAccessKey(BranchAccessKey branchAccessKey, UInt256 chunkIndex) {
    public ChunkAccessKey(
        final Address address, final UInt256 treeIndex, final UInt256 chunkIndex) {
      this(new BranchAccessKey(address, treeIndex), chunkIndex);
    }
  }

  @Override
  public List<UInt256> getStorageSlotTreeIndexes(final UInt256 storageKey) {
    return List.of(
        TRIE_KEY_ADAPTER.locateStorageKeyOffset(storageKey),
        TRIE_KEY_ADAPTER.locateStorageKeySuffix(storageKey));
  }

  @Override
  public long getTotalWitnessGas() {
    return totalWitnessGas;
  }
}
