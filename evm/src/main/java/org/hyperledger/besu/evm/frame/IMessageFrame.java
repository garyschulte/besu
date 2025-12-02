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
 * Interface for MessageFrame to support both Java and native (C++ JNI) implementations.
 *
 * <p>This interface defines the contract for EVM execution context, allowing drop-in replacement
 * of the standard Java MessageFrame with a native implementation for performance.
 */
public interface IMessageFrame {

  /** The message type the frame corresponds to. */
  enum Type {
    /** A Contract creation message. */
    CONTRACT_CREATION,

    /** A message call message. */
    MESSAGE_CALL,
  }

  /** Message Frame State. */
  enum State {
    /** Message execution has not started. */
    NOT_STARTED,

    /** Code execution within the message is in progress. */
    CODE_EXECUTING,

    /** Code execution within the message has finished successfully. */
    CODE_SUCCESS,

    /** Code execution within the message has been suspended. */
    CODE_SUSPENDED,

    /** An exceptional halting condition has occurred. */
    EXCEPTIONAL_HALT,

    /** State changes were reverted during execution. */
    REVERT,

    /** The message execution has failed to complete successfully. */
    COMPLETED_FAILED,

    /** The message execution has completed successfully. */
    COMPLETED_SUCCESS,
  }

  // ========== Program Counter ==========

  /**
   * Gets program counter.
   *
   * @return the program counter
   */
  int getPC();

  /**
   * Sets program counter.
   *
   * @param pc the program counter
   */
  void setPC(int pc);

  /**
   * Gets section (for EOF support).
   *
   * @return the section
   */
  int getSection();

  /**
   * Sets section (for EOF support).
   *
   * @param section the section
   */
  void setSection(int section);

  // ========== Gas Management ==========

  /**
   * Gets remaining gas.
   *
   * @return the remaining gas
   */
  long getRemainingGas();

  /**
   * Sets gas remaining.
   *
   * @param amount the amount
   */
  void setGasRemaining(long amount);

  /**
   * Decrement remaining gas.
   *
   * @param amount the amount
   * @return the new gas remaining
   */
  long decrementRemainingGas(long amount);

  /**
   * Increment remaining gas.
   *
   * @param amount the amount
   */
  void incrementRemainingGas(long amount);

  /** Clear gas remaining. */
  void clearGasRemaining();

  /**
   * Gets gas refund.
   *
   * @return the gas refund
   */
  long getGasRefund();

  /**
   * Increment gas refund.
   *
   * @param amount the amount
   */
  void incrementGasRefund(long amount);

  /** Clear gas refund. */
  void clearGasRefund();

  // ========== Stack Operations ==========

  /**
   * Gets stack item.
   *
   * @param offset the offset from top (0 = top)
   * @return the stack item
   */
  Bytes getStackItem(int offset);

  /**
   * Pop stack item.
   *
   * @return the popped bytes
   */
  Bytes popStackItem();

  /**
   * Pop stack items.
   *
   * @param n the number of items to pop
   */
  void popStackItems(int n);

  /**
   * Push stack item.
   *
   * @param value the value
   */
  void pushStackItem(Bytes value);

  /**
   * Sets stack item.
   *
   * @param offset the offset from top (0 = top)
   * @param value the value
   */
  void setStackItem(int offset, Bytes value);

  /**
   * Stack size.
   *
   * @return the stack size
   */
  int stackSize();

  // ========== Memory Operations ==========

  /**
   * Calculate memory expansion cost.
   *
   * @param offset the offset
   * @param length the length
   * @return the gas cost
   */
  long calculateMemoryExpansion(long offset, long length);

  /**
   * Expand memory if needed.
   *
   * @param offset the offset
   * @param length the length
   */
  void expandMemory(long offset, long length);

  /**
   * Memory byte size.
   *
   * @return the memory byte size
   */
  long memoryByteSize();

  /**
   * Memory word size.
   *
   * @return the memory word size
   */
  int memoryWordSize();

  /**
   * Read memory.
   *
   * @param offset the offset
   * @param length the length
   * @return the bytes
   */
  Bytes readMemory(long offset, long length);

  /**
   * Read mutable memory.
   *
   * @param offset the offset
   * @param length the length
   * @return the mutable bytes
   */
  MutableBytes readMutableMemory(long offset, long length);

  /**
   * Read mutable memory with explicit flag.
   *
   * @param offset the offset
   * @param length the length
   * @param explicitMemoryRead explicit memory read flag for tracing
   * @return the mutable bytes
   */
  MutableBytes readMutableMemory(long offset, long length, boolean explicitMemoryRead);

  /**
   * Shadow read memory (for read-only access).
   *
   * @param offset the offset
   * @param length the length
   * @return the bytes
   */
  Bytes shadowReadMemory(long offset, long length);

  /**
   * Write memory.
   *
   * @param offset the offset
   * @param value the value
   * @param explicitMemoryUpdate explicit memory update flag for tracing
   */
  void writeMemory(long offset, byte value, boolean explicitMemoryUpdate);

  /**
   * Write memory.
   *
   * @param offset the offset
   * @param length the length
   * @param value the value
   */
  void writeMemory(long offset, long length, Bytes value);

  /**
   * Write memory.
   *
   * @param offset the offset
   * @param length the length
   * @param value the value
   * @param explicitMemoryUpdate explicit memory update flag for tracing
   */
  void writeMemory(long offset, long length, Bytes value, boolean explicitMemoryUpdate);

  /**
   * Write bytes to memory from a specific source offset.
   *
   * @param offset The offset in memory to start writing
   * @param sourceOffset The offset in the source value to start writing
   * @param length The length of the bytes to write
   * @param value The value to write
   */
  void writeMemory(long offset, long sourceOffset, long length, Bytes value);

  /**
   * Write bytes to memory from a specific source offset.
   *
   * @param offset The offset in memory to start writing
   * @param sourceOffset The offset in the source value to start writing
   * @param length The length of the bytes to write
   * @param value The value to write
   * @param explicitMemoryUpdate true if triggered by a memory opcode, false otherwise
   */
  void writeMemory(
      long offset, long sourceOffset, long length, Bytes value, boolean explicitMemoryUpdate);

  /**
   * Write memory right aligned.
   *
   * @param offset the offset
   * @param length the length
   * @param value the value
   * @param explicitMemoryUpdate explicit memory update flag for tracing
   */
  void writeMemoryRightAligned(long offset, long length, Bytes value, boolean explicitMemoryUpdate);

  /**
   * Copy memory.
   *
   * @param destination the destination offset
   * @param source the source offset
   * @param length the length
   * @param explicitMemoryUpdate explicit memory update flag for tracing
   */
  void copyMemory(long destination, long source, long length, boolean explicitMemoryUpdate);

  // ========== State and Context ==========

  /**
   * Gets state.
   *
   * @return the state
   */
  State getState();

  /**
   * Sets state.
   *
   * @param state the state
   */
  void setState(State state);

  /**
   * Gets type.
   *
   * @return the type
   */
  Type getType();

  /**
   * Is static call.
   *
   * @return true if static
   */
  boolean isStatic();

  /**
   * Gets code.
   *
   * @return the code
   */
  Code getCode();

  /**
   * Gets input data.
   *
   * @return the input data
   */
  Bytes getInputData();

  /**
   * Gets recipient address.
   *
   * @return the recipient address
   */
  Address getRecipientAddress();

  /**
   * Gets contract address.
   *
   * @return the contract address
   */
  Address getContractAddress();

  /**
   * Gets sender address.
   *
   * @return the sender address
   */
  Address getSenderAddress();

  /**
   * Gets originator address (transaction sender).
   *
   * @return the originator address
   */
  Address getOriginatorAddress();

  /**
   * Gets value.
   *
   * @return the value
   */
  Wei getValue();

  /**
   * Gets apparent value (for CALLCODE/DELEGATECALL).
   *
   * @return the apparent value
   */
  Wei getApparentValue();

  /**
   * Gets gas price.
   *
   * @return the gas price
   */
  Wei getGasPrice();

  /**
   * Gets blob gas price (EIP-4844).
   *
   * @return the blob gas price
   */
  Wei getBlobGasPrice();

  /**
   * Gets block values.
   *
   * @return the block values
   */
  BlockValues getBlockValues();

  /**
   * Gets mining beneficiary (coinbase).
   *
   * @return the mining beneficiary
   */
  Address getMiningBeneficiary();

  /**
   * Gets block hash lookup.
   *
   * @return the block hash lookup
   */
  BlockHashLookup getBlockHashLookup();

  /**
   * Gets call depth.
   *
   * @return the depth
   */
  int getDepth();

  /**
   * Gets message stack size.
   *
   * @return the message stack size
   */
  int getMessageStackSize();

  /**
   * Gets message frame stack.
   *
   * @return the message frame stack
   */
  Deque<MessageFrame> getMessageFrameStack();

  /**
   * Gets max stack size.
   *
   * @return the max stack size
   */
  int getMaxStackSize();

  // ========== Output and Return Data ==========

  /**
   * Gets output data.
   *
   * @return the output data
   */
  Bytes getOutputData();

  /**
   * Sets output data.
   *
   * @param output the output
   */
  void setOutputData(Bytes output);

  /** Clear output data. */
  void clearOutputData();

  /**
   * Gets return data.
   *
   * @return the return data
   */
  Bytes getReturnData();

  /**
   * Sets return data.
   *
   * @param returnData the return data
   */
  void setReturnData(Bytes returnData);

  /** Clear return data. */
  void clearReturnData();

  /**
   * Gets revert reason.
   *
   * @return the revert reason
   */
  Optional<Bytes> getRevertReason();

  /**
   * Sets revert reason.
   *
   * @param revertReason the revert reason
   */
  void setRevertReason(Bytes revertReason);

  // ========== Created Code (for CREATE operations) ==========

  /**
   * Gets created code.
   *
   * @return the created code
   */
  Code getCreatedCode();

  /**
   * Sets created code.
   *
   * @param createdCode the created code
   */
  void setCreatedCode(Code createdCode);

  // ========== World State ==========

  /**
   * Gets world updater.
   *
   * @return the world updater
   */
  WorldUpdater getWorldUpdater();

  // ========== Logs ==========

  /**
   * Add log.
   *
   * @param log the log
   */
  void addLog(Log log);

  /**
   * Add logs.
   *
   * @param logs the logs
   */
  void addLogs(List<Log> logs);

  /** Clear logs. */
  void clearLogs();

  /**
   * Gets logs.
   *
   * @return the logs
   */
  List<Log> getLogs();

  // ========== Self Destructs ==========

  /**
   * Add self destruct.
   *
   * @param address the address
   */
  void addSelfDestruct(Address address);

  /**
   * Add self destructs.
   *
   * @param addresses the addresses
   */
  void addSelfDestructs(Set<Address> addresses);

  /**
   * Gets self destructs.
   *
   * @return the self destructs
   */
  Set<Address> getSelfDestructs();

  // ========== Creates ==========

  /**
   * Add create.
   *
   * @param address the address
   */
  void addCreate(Address address);

  /**
   * Add creates.
   *
   * @param addresses the addresses
   */
  void addCreates(Set<Address> addresses);

  /**
   * Gets creates.
   *
   * @return the creates
   */
  Set<Address> getCreates();

  /**
   * Was created in transaction.
   *
   * @param address the address
   * @return true if created
   */
  boolean wasCreatedInTransaction(Address address);

  // ========== Refunds ==========

  /**
   * Add refund.
   *
   * @param beneficiary the beneficiary
   * @param amount the amount
   */
  void addRefund(Address beneficiary, Wei amount);

  /**
   * Gets refunds.
   *
   * @return the refunds
   */
  Map<Address, Wei> getRefunds();

  // ========== EIP-2929 Access Lists (Warm/Cold Storage) ==========

  /**
   * Warm up address.
   *
   * @param address the address
   * @return true if was cold (first access)
   */
  boolean warmUpAddress(Address address);

  /**
   * Is address warm.
   *
   * @param address the address
   * @return true if warm
   */
  boolean isAddressWarm(Address address);

  /**
   * Warm up storage.
   *
   * @param address the address
   * @param slot the slot
   * @return true if was cold (first access)
   */
  boolean warmUpStorage(Address address, Bytes32 slot);

  /**
   * Gets warmed up storage.
   *
   * @return the warmed up storage table
   */
  Table<Address, Bytes32, Boolean> getWarmedUpStorage();

  // ========== Transient Storage (EIP-1153) ==========

  /**
   * Gets transient storage value.
   *
   * @param accountAddress the account address
   * @param slot the slot
   * @return the value
   */
  Bytes32 getTransientStorageValue(Address accountAddress, Bytes32 slot);

  /**
   * Sets transient storage value.
   *
   * @param accountAddress the account address
   * @param slot the slot
   * @param value the value
   */
  void setTransientStorageValue(Address accountAddress, Bytes32 slot, Bytes32 value);

  // ========== Return Stack (EOF) ==========

  /**
   * Gets return stack.
   *
   * @return the return stack
   */
  ReturnStack getReturnStack();

  /**
   * Return stack size.
   *
   * @return the size
   */
  int returnStackSize();

  /**
   * Peek return stack.
   *
   * @return the return stack item
   */
  ReturnStack.ReturnStackItem peekReturnStack();

  /**
   * Push return stack item.
   *
   * @param returnStackItem the return stack item
   */
  void pushReturnStackItem(ReturnStack.ReturnStackItem returnStackItem);

  // ========== Exceptional Halt ==========

  /**
   * Sets exceptional halt reason.
   *
   * @param haltReason the halt reason
   */
  void setExceptionalHaltReason(Optional<ExceptionalHaltReason> haltReason);

  /**
   * Gets exceptional halt reason.
   *
   * @return the exceptional halt reason
   */
  Optional<ExceptionalHaltReason> getExceptionalHaltReason();

  // ========== Tracing Support ==========

  /**
   * Gets current operation (for tracing).
   *
   * @return the current operation
   */
  Operation getCurrentOperation();

  /**
   * Sets current operation (for tracing).
   *
   * @param currentOperation the current operation
   */
  void setCurrentOperation(Operation currentOperation);

  /**
   * Storage was updated (for tracing).
   *
   * @param storageAddress the storage address
   * @param value the value
   */
  void storageWasUpdated(UInt256 storageAddress, Bytes value);

  /**
   * Gets maybe updated memory (for tracing).
   *
   * @return the maybe updated memory
   */
  Optional<MemoryEntry> getMaybeUpdatedMemory();

  /**
   * Gets maybe updated storage (for tracing).
   *
   * @return the maybe updated storage
   */
  Optional<StorageEntry> getMaybeUpdatedStorage();

  // ========== Rollback ==========

  /** Rollback state changes. */
  void rollback();

  // ========== Context Variables (Extensibility) ==========

  /**
   * Gets context variable.
   *
   * @param <T> the type parameter
   * @param name the name
   * @return the context variable
   */
  <T> T getContextVariable(String name);

  /**
   * Gets context variable with default.
   *
   * @param <T> the type parameter
   * @param name the name
   * @param defaultValue the default value
   * @return the context variable
   */
  <T> T getContextVariable(String name, T defaultValue);

  /**
   * Has context variable.
   *
   * @param name the name
   * @return true if has context variable
   */
  boolean hasContextVariable(String name);

  // ========== Versioned Hashes (EIP-4844) ==========

  /**
   * Gets versioned hashes.
   *
   * @return the versioned hashes
   */
  Optional<List<VersionedHash>> getVersionedHashes();

  // ========== Completion ==========

  /** Notify completion (callback to parent). */
  void notifyCompletion();
}
