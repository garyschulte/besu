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
package org.hyperledger.besu.evm.frame;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.internal.MemoryEntry;
import org.hyperledger.besu.evm.internal.ReturnStack;
import org.hyperledger.besu.evm.internal.StorageEntry;
import org.hyperledger.besu.evm.log.Log;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Table;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * A zero-copy MessageFrame implementation backed by native memory via Panama FFM.
 *
 * <p>This implementation reads and writes directly to shared memory managed by C++ EVM,
 * eliminating the need to copy data back and forth between Java and native code.
 *
 * <p>This is primarily intended for use in OperationTracer callbacks where the frame
 * state needs to be observable during native execution.
 *
 * <p><b>Thread Safety:</b> This class is NOT thread-safe and assumes single-threaded access
 * during native EVM execution.
 *
 * <p><b>Portability Notes:</b>
 * <ul>
 *   <li>Endianness: Assumes little-endian (x86-64, aarch64 Linux/Darwin)</li>
 *   <li>Signedness: C++ uses uint32_t for sizes, Java reads as signed int32. Values must be &lt; 2^31.</li>
 *   <li>GC Safety: Memory is Arena-backed and off-heap during native execution</li>
 * </ul>
 */
public class NativeMessageFrame implements IMessageFrame {

  private final MemorySegment frameMemory;
  private final Code code;
  private final BlockValues blockValues;
  private Operation currentOperation;

  /**
   * Validates that a size value read from native memory is non-negative.
   * C++ uses uint32_t but Java reads as signed int32. Negative values indicate
   * the C++ value exceeded 2^31-1, which violates our contract.
   *
   * @param size the size value to validate
   * @param fieldName the field name for error messages
   * @throws IllegalStateException if size is negative
   */
  private static void validateSize(final int size, final String fieldName) {
    if (size < 0) {
      throw new IllegalStateException(
          String.format(
              "Size field '%s' is negative (%d), indicating C++ value exceeded 2^31-1. "
                  + "This violates the Java/C++ interop contract.",
              fieldName, size));
    }
  }

  /**
   * Creates a new NativeMessageFrame backed by the given memory segment.
   *
   * @param frameMemory the memory segment containing the MessageFrameMemory structure
   * @param code the code being executed
   * @param blockValues the block context values
   */
  public NativeMessageFrame(
      final MemorySegment frameMemory, final Code code, final BlockValues blockValues) {
    this.frameMemory = frameMemory;
    this.code = code;
    this.blockValues = blockValues;
  }

  // ========== Program Counter ==========

  @Override
  public int getPC() {
    return (int) MessageFrameLayout.PC.get(frameMemory, 0L);
  }

  @Override
  public void setPC(final int pc) {
    MessageFrameLayout.PC.set(frameMemory, 0L, pc);
  }

  @Override
  public int getSection() {
    return (int) MessageFrameLayout.SECTION.get(frameMemory, 0L);
  }

  @Override
  public void setSection(final int section) {
    MessageFrameLayout.SECTION.set(frameMemory, 0L, section);
  }

  // ========== Gas Management ==========

  @Override
  public long getRemainingGas() {
    return (long) MessageFrameLayout.GAS_REMAINING.get(frameMemory, 0L);
  }

  @Override
  public void setGasRemaining(final long amount) {
    MessageFrameLayout.GAS_REMAINING.set(frameMemory, 0L, amount);
  }

  @Override
  public long decrementRemainingGas(final long amount) {
    long current = getRemainingGas();
    long newValue = current - amount;
    setGasRemaining(newValue);
    return newValue;
  }

  @Override
  public void incrementRemainingGas(final long amount) {
    long current = getRemainingGas();
    setGasRemaining(current + amount);
  }

  @Override
  public void clearGasRemaining() {
    setGasRemaining(0);
  }

  @Override
  public long getGasRefund() {
    return (long) MessageFrameLayout.GAS_REFUND.get(frameMemory, 0L);
  }

  @Override
  public void incrementGasRefund(final long amount) {
    long current = getGasRefund();
    MessageFrameLayout.GAS_REFUND.set(frameMemory, 0L, current + amount);
  }

  @Override
  public void clearGasRefund() {
    MessageFrameLayout.GAS_REFUND.set(frameMemory, 0L, 0L);
  }

  // ========== Stack Operations ==========

  @Override
  public Bytes getStackItem(final int offset) {
    int stackSize = stackSize();
    if (offset >= stackSize) {
      throw new IndexOutOfBoundsException(
          "Stack offset " + offset + " >= stack size " + stackSize);
    }

    long stackPtr = (long) MessageFrameLayout.STACK_PTR.get(frameMemory, 0L);
    if (stackPtr == 0) {
      throw new IllegalStateException("Stack pointer is null");
    }

    // Stack grows from bottom (index 0) to top (index stackSize-1)
    // getStackItem(0) returns the top of stack, so we need to reverse the index
    int actualIndex = stackSize - 1 - offset;

    // Read directly from shared memory - minimal copy
    byte[] item = new byte[MessageFrameLayout.STACK_ITEM_SIZE];
    long itemOffset = actualIndex * (long) MessageFrameLayout.STACK_ITEM_SIZE;

    MemorySegment stackSegment =
        frameMemory.asSlice(
            stackPtr, stackSize * (long) MessageFrameLayout.STACK_ITEM_SIZE);
    MemorySegment.copy(
        stackSegment, ValueLayout.JAVA_BYTE, itemOffset, item, 0, MessageFrameLayout.STACK_ITEM_SIZE);

    return Bytes.wrap(item);
  }

  @Override
  public Bytes popStackItem() {
    throw new UnsupportedOperationException(
        "Native frame does not support stack modification during callbacks");
  }

  @Override
  public void popStackItems(final int n) {
    throw new UnsupportedOperationException(
        "Native frame does not support stack modification during callbacks");
  }

  @Override
  public void pushStackItem(final Bytes value) {
    throw new UnsupportedOperationException(
        "Native frame does not support stack modification during callbacks");
  }

  @Override
  public void setStackItem(final int offset, final Bytes value) {
    throw new UnsupportedOperationException(
        "Native frame does not support stack modification during callbacks");
  }

  @Override
  public int stackSize() {
    return (int) MessageFrameLayout.STACK_SIZE.get(frameMemory, 0L);
  }

  // ========== Memory Operations ==========

  @Override
  public long calculateMemoryExpansion(final long offset, final long length) {
    if (length == 0) {
      return 0;
    }
    long end = offset + length;
    long currentSize = memoryByteSize();
    if (end <= currentSize) {
      return 0;
    }
    // Simplified gas calculation - should match EVM spec
    long wordSize = (end + 31) / 32;
    long currentWordSize = (currentSize + 31) / 32;
    long newWords = wordSize - currentWordSize;
    return newWords * 3 + (newWords * newWords) / 512;
  }

  @Override
  public void expandMemory(final long offset, final long length) {
    throw new UnsupportedOperationException(
        "Native frame does not support memory expansion during callbacks");
  }

  @Override
  public long memoryByteSize() {
    int size = (int) MessageFrameLayout.MEMORY_SIZE.get(frameMemory, 0L);
    validateSize(size, "memory_size");
    return size;
  }

  @Override
  public int memoryWordSize() {
    long byteSize = memoryByteSize();
    return (int) ((byteSize + 31) / 32);
  }

  @Override
  public Bytes readMemory(final long offset, final long length) {
    if (length == 0) {
      return Bytes.EMPTY;
    }

    long memorySize = memoryByteSize();  // Already validated in memoryByteSize()
    if (offset + length > memorySize) {
      throw new IndexOutOfBoundsException(
          "Memory read out of bounds: offset="
              + offset
              + ", length="
              + length
              + ", memorySize="
              + memorySize);
    }

    long memoryPtr = (long) MessageFrameLayout.MEMORY_PTR.get(frameMemory, 0L);
    if (memoryPtr == 0) {
      return Bytes.EMPTY;
    }

    // Read directly from shared memory
    byte[] data = new byte[(int) length];
    MemorySegment memorySegment = frameMemory.asSlice(memoryPtr, memorySize);
    MemorySegment.copy(memorySegment, ValueLayout.JAVA_BYTE, offset, data, 0, (int) length);

    return Bytes.wrap(data);
  }

  @Override
  public MutableBytes readMutableMemory(final long offset, final long length) {
    return readMemory(offset, length).mutableCopy();
  }

  @Override
  public MutableBytes readMutableMemory(
      final long offset, final long length, final boolean explicitMemoryRead) {
    return readMutableMemory(offset, length);
  }

  @Override
  public Bytes shadowReadMemory(final long offset, final long length) {
    return readMemory(offset, length);
  }

  @Override
  public void writeMemory(final long offset, final byte value, final boolean explicitMemoryUpdate) {
    throw new UnsupportedOperationException(
        "Native frame does not support memory writes during callbacks");
  }

  @Override
  public void writeMemory(final long offset, final long length, final Bytes value) {
    throw new UnsupportedOperationException(
        "Native frame does not support memory writes during callbacks");
  }

  @Override
  public void writeMemory(
      final long offset, final long length, final Bytes value, final boolean explicitMemoryUpdate) {
    throw new UnsupportedOperationException(
        "Native frame does not support memory writes during callbacks");
  }

  @Override
  public void writeMemory(
      final long offset, final long sourceOffset, final long length, final Bytes value) {
    throw new UnsupportedOperationException(
        "Native frame does not support memory writes during callbacks");
  }

  @Override
  public void writeMemory(
      final long offset,
      final long sourceOffset,
      final long length,
      final Bytes value,
      final boolean explicitMemoryUpdate) {
    throw new UnsupportedOperationException(
        "Native frame does not support memory writes during callbacks");
  }

  @Override
  public void writeMemoryRightAligned(
      final long offset, final long length, final Bytes value, final boolean explicitMemoryUpdate) {
    throw new UnsupportedOperationException(
        "Native frame does not support memory writes during callbacks");
  }

  @Override
  public void copyMemory(
      final long destination, final long source, final long length, final boolean explicitMemoryUpdate) {
    throw new UnsupportedOperationException(
        "Native frame does not support memory writes during callbacks");
  }

  // ========== State and Context ==========

  @Override
  public State getState() {
    int stateOrdinal = (int) MessageFrameLayout.STATE.get(frameMemory, 0L);
    return State.values()[stateOrdinal];
  }

  @Override
  public void setState(final State state) {
    MessageFrameLayout.STATE.set(frameMemory, 0L, state.ordinal());
  }

  @Override
  public Type getType() {
    int typeOrdinal = (int) MessageFrameLayout.TYPE.get(frameMemory, 0L);
    return Type.values()[typeOrdinal];
  }

  @Override
  public boolean isStatic() {
    return (int) MessageFrameLayout.IS_STATIC.get(frameMemory, 0L) != 0;
  }

  @Override
  public Code getCode() {
    return code;
  }

  @Override
  public Bytes getInputData() {
    int inputSize = (int) MessageFrameLayout.INPUT_SIZE.get(frameMemory, 0L);
    validateSize(inputSize, "input_size");

    if (inputSize == 0) {
      return Bytes.EMPTY;
    }

    long inputPtr = (long) MessageFrameLayout.INPUT_PTR.get(frameMemory, 0L);
    if (inputPtr == 0) {
      return Bytes.EMPTY;
    }

    byte[] data = new byte[inputSize];
    MemorySegment inputSegment = frameMemory.asSlice(inputPtr, inputSize);
    MemorySegment.copy(inputSegment, ValueLayout.JAVA_BYTE, 0, data, 0, inputSize);

    return Bytes.wrap(data);
  }

  /**
   * Helper method to read an address from memory at the specified offset.
   *
   * @param offset the offset in the frame memory
   * @return the address
   */
  private Address readAddress(final long offset) {
    byte[] addressBytes = new byte[20];
    MemorySegment.copy(frameMemory, ValueLayout.JAVA_BYTE, offset, addressBytes, 0, 20);
    return Address.wrap(Bytes.wrap(addressBytes));
  }

  /**
   * Helper method to read a 256-bit value from memory at the specified offset.
   *
   * @param offset the offset in the frame memory
   * @return the value as Wei
   */
  private Wei readWei(final long offset) {
    byte[] valueBytes = new byte[32];
    MemorySegment.copy(frameMemory, ValueLayout.JAVA_BYTE, offset, valueBytes, 0, 32);
    return Wei.wrap(Bytes32.wrap(valueBytes));
  }

  @Override
  public Address getRecipientAddress() {
    return readAddress(MessageFrameLayout.OFFSET_RECIPIENT);
  }

  @Override
  public Address getContractAddress() {
    return readAddress(MessageFrameLayout.OFFSET_CONTRACT);
  }

  @Override
  public Address getSenderAddress() {
    return readAddress(MessageFrameLayout.OFFSET_SENDER);
  }

  @Override
  public Address getOriginatorAddress() {
    return readAddress(MessageFrameLayout.OFFSET_ORIGINATOR);
  }

  @Override
  public Wei getValue() {
    return readWei(MessageFrameLayout.OFFSET_VALUE);
  }

  @Override
  public Wei getApparentValue() {
    return readWei(MessageFrameLayout.OFFSET_APPARENT_VALUE);
  }

  @Override
  public Wei getGasPrice() {
    return readWei(MessageFrameLayout.OFFSET_GAS_PRICE);
  }

  @Override
  public Wei getBlobGasPrice() {
    // Not stored in native memory - should come from TxValues, not currently implemented
    return null;
  }

  @Override
  public BlockValues getBlockValues() {
    return blockValues;
  }

  @Override
  public Address getMiningBeneficiary() {
    return readAddress(MessageFrameLayout.OFFSET_MINING_BENEFICIARY);
  }

  @Override
  public BlockHashLookup getBlockHashLookup() {
    // Not applicable for native frame during callbacks
    throw new UnsupportedOperationException(
        "Block hash lookup not available in native frame callbacks");
  }

  @Override
  public int getDepth() {
    return (int) MessageFrameLayout.DEPTH.get(frameMemory, 0L);
  }

  @Override
  public int getMessageStackSize() {
    // During native execution, we don't track the full message stack
    return getDepth() + 1;
  }

  @Override
  public Deque<MessageFrame> getMessageFrameStack() {
    throw new UnsupportedOperationException(
        "Message frame stack not available in native frame callbacks");
  }

  @Override
  public int getMaxStackSize() {
    return MessageFrameLayout.MAX_STACK_SIZE;
  }

  // ========== Output and Return Data ==========

  @Override
  public Bytes getOutputData() {
    int outputSize = (int) MessageFrameLayout.OUTPUT_SIZE.get(frameMemory, 0L);
    validateSize(outputSize, "output_size");

    if (outputSize == 0) {
      return Bytes.EMPTY;
    }

    long outputPtr = (long) MessageFrameLayout.OUTPUT_PTR.get(frameMemory, 0L);
    if (outputPtr == 0) {
      return Bytes.EMPTY;
    }

    byte[] data = new byte[outputSize];
    MemorySegment outputSegment = frameMemory.asSlice(outputPtr, outputSize);
    MemorySegment.copy(outputSegment, ValueLayout.JAVA_BYTE, 0, data, 0, outputSize);

    return Bytes.wrap(data);
  }

  @Override
  public void setOutputData(final Bytes output) {
    throw new UnsupportedOperationException(
        "Native frame does not support output modification during callbacks");
  }

  @Override
  public void clearOutputData() {
    throw new UnsupportedOperationException(
        "Native frame does not support output modification during callbacks");
  }

  @Override
  public Bytes getReturnData() {
    int returnSize = (int) MessageFrameLayout.RETURN_DATA_SIZE.get(frameMemory, 0L);
    validateSize(returnSize, "return_data_size");

    if (returnSize == 0) {
      return Bytes.EMPTY;
    }

    long returnPtr = (long) MessageFrameLayout.RETURN_DATA_PTR.get(frameMemory, 0L);
    if (returnPtr == 0) {
      return Bytes.EMPTY;
    }

    byte[] data = new byte[returnSize];
    MemorySegment returnSegment = frameMemory.asSlice(returnPtr, returnSize);
    MemorySegment.copy(returnSegment, ValueLayout.JAVA_BYTE, 0, data, 0, returnSize);

    return Bytes.wrap(data);
  }

  @Override
  public void setReturnData(final Bytes returnData) {
    throw new UnsupportedOperationException(
        "Native frame does not support return data modification during callbacks");
  }

  @Override
  public void clearReturnData() {
    throw new UnsupportedOperationException(
        "Native frame does not support return data modification during callbacks");
  }

  @Override
  public Optional<Bytes> getRevertReason() {
    // Revert reasons are not currently stored in native memory
    return Optional.empty();
  }

  @Override
  public void setRevertReason(final Bytes revertReason) {
    throw new UnsupportedOperationException(
        "Native frame does not support revert reason modification during callbacks");
  }

  // ========== Operations Not Supported in Native Frame ==========
  // These methods are part of the IMessageFrame interface but don't make sense
  // for a read-only view of native execution state

  @Override
  public Code getCreatedCode() {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public void setCreatedCode(final Code createdCode) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public WorldUpdater getWorldUpdater() {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public void addLog(final Log log) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public void addLogs(final List<Log> logs) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public void clearLogs() {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public List<Log> getLogs() {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public void addSelfDestruct(final Address address) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public void addSelfDestructs(final Set<Address> addresses) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public Set<Address> getSelfDestructs() {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public void addCreate(final Address address) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public void addCreates(final Set<Address> addresses) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public Set<Address> getCreates() {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public boolean wasCreatedInTransaction(final Address address) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public void addRefund(final Address beneficiary, final Wei amount) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public Map<Address, Wei> getRefunds() {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public boolean warmUpAddress(final Address address) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public boolean isAddressWarm(final Address address) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public boolean warmUpStorage(final Address address, final Bytes32 slot) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public Table<Address, Bytes32, Boolean> getWarmedUpStorage() {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public Bytes32 getTransientStorageValue(final Address accountAddress, final Bytes32 slot) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public void setTransientStorageValue(
      final Address accountAddress, final Bytes32 slot, final Bytes32 value) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public ReturnStack getReturnStack() {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public int returnStackSize() {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public ReturnStack.ReturnStackItem peekReturnStack() {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public void pushReturnStackItem(final ReturnStack.ReturnStackItem returnStackItem) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public void setExceptionalHaltReason(final Optional<ExceptionalHaltReason> haltReason) {
    if (haltReason.isPresent() && haltReason.get() instanceof Enum) {
      MessageFrameLayout.HALT_REASON.set(frameMemory, 0L, ((Enum<?>) haltReason.get()).ordinal());
    } else {
      MessageFrameLayout.HALT_REASON.set(frameMemory, 0L, 0);
    }
  }

  @Override
  public Optional<ExceptionalHaltReason> getExceptionalHaltReason() {
    int haltReasonOrdinal = (int) MessageFrameLayout.HALT_REASON.get(frameMemory, 0L);
    if (haltReasonOrdinal <= 0 || haltReasonOrdinal >= ExceptionalHaltReason.DefaultExceptionalHaltReason.values().length) {
      return Optional.empty();
    }
    return Optional.of(ExceptionalHaltReason.DefaultExceptionalHaltReason.values()[haltReasonOrdinal]);
  }

  // ========== Tracing Support ==========

  @Override
  public Operation getCurrentOperation() {
    return currentOperation;
  }

  @Override
  public void setCurrentOperation(final Operation currentOperation) {
    this.currentOperation = currentOperation;
  }

  @Override
  public void storageWasUpdated(final UInt256 storageAddress, final Bytes value) {
    // Tracing hook - no-op for native frame
  }

  @Override
  public Optional<MemoryEntry> getMaybeUpdatedMemory() {
    return Optional.empty();
  }

  @Override
  public Optional<StorageEntry> getMaybeUpdatedStorage() {
    return Optional.empty();
  }

  @Override
  public void rollback() {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public <T> T getContextVariable(final String name) {
    throw new UnsupportedOperationException("Not supported in native frame callbacks");
  }

  @Override
  public <T> T getContextVariable(final String name, final T defaultValue) {
    return defaultValue;
  }

  @Override
  public boolean hasContextVariable(final String name) {
    return false;
  }

  @Override
  public Optional<List<VersionedHash>> getVersionedHashes() {
    // Not stored in native memory - versioned hashes (EIP-4844) not currently tracked
    return Optional.empty();
  }

  @Override
  public void notifyCompletion() {
    // No-op for native frame
  }
}
