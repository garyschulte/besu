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
 * Interface for read-only observation of MessageFrame execution state.
 *
 * <p>This interface provides read-only access to EVM execution context for external observers
 * such as tracers and monitors. It contains only observation methods (getters), not mutations.
 *
 * <p>For EVM operations that need to mutate state (stack, memory, gas, etc.), use the concrete
 * {@link MessageFrame} class which provides full mutability.
 *
 * <p>This allows for efficient implementations like NativeMessageFrame that provide zero-copy
 * observation of native EVM execution without supporting mutations.
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
   * Gets section (for EOF support).
   *
   * @return the section
   */
  int getSection();

  // ========== Gas Management ==========

  /**
   * Gets remaining gas.
   *
   * @return the remaining gas
   */
  long getRemainingGas();

  /**
   * Gets gas refund.
   *
   * @return the gas refund
   */
  long getGasRefund();

  // ========== Stack Operations ==========

  /**
   * Gets stack item.
   *
   * @param offset the offset from top (0 = top)
   * @return the stack item
   */
  Bytes getStackItem(int offset);

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

  // ========== State and Context ==========

  /**
   * Gets state.
   *
   * @return the state
   */
  State getState();

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
   * Gets return data.
   *
   * @return the return data
   */
  Bytes getReturnData();

  /**
   * Gets revert reason.
   *
   * @return the revert reason
   */
  Optional<Bytes> getRevertReason();

  // ========== World State ==========

  /**
   * Gets world updater.
   *
   * @return the world updater
   */
  WorldUpdater getWorldUpdater();

  // ========== Logs ==========

  /**
   * Gets logs.
   *
   * @return the logs
   */
  List<Log> getLogs();

  // ========== Self Destructs ==========

  /**
   * Gets self destructs.
   *
   * @return the self destructs
   */
  Set<Address> getSelfDestructs();

  // ========== Creates ==========

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
   * Gets refunds.
   *
   * @return the refunds
   */
  Map<Address, Wei> getRefunds();

  // ========== EIP-2929 Access Lists (Warm/Cold Storage) ==========

  /**
   * Is address warm.
   *
   * @param address the address
   * @return true if warm
   */
  boolean isAddressWarm(Address address);

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

  // ========== Exceptional Halt ==========

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
}
