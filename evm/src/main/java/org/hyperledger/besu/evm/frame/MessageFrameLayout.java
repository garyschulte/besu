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

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * Memory layout for MessageFrame shared between Java and C++ via Panama FFM.
 *
 * <p>This struct layout must match the C++ MessageFrameMemory struct exactly.
 * Any changes to this layout MUST be synchronized with include/message_frame_memory.h
 *
 * <p>Total size: 384 bytes for metadata + variable data
 */
public class MessageFrameLayout {

  /** Size of the frame header (metadata) */
  public static final long HEADER_SIZE = 384;

  /** Size of each stack item in bytes */
  public static final int STACK_ITEM_SIZE = 32;

  /** Maximum number of stack items */
  public static final int MAX_STACK_SIZE = 1024;

  /** Memory layout definition */
  public static final StructLayout LAYOUT =
      MemoryLayout.structLayout(
              // Machine state (48 bytes)
              ValueLayout.JAVA_INT.withName("pc"),
              ValueLayout.JAVA_INT.withName("section"),
              ValueLayout.JAVA_LONG.withName("gas_remaining"),
              ValueLayout.JAVA_LONG.withName("gas_refund"),
              ValueLayout.JAVA_INT.withName("stack_size"),
              ValueLayout.JAVA_INT.withName("memory_size"),
              ValueLayout.JAVA_INT.withName("state"),
              ValueLayout.JAVA_INT.withName("type"),
              ValueLayout.JAVA_INT.withName("is_static"),
              ValueLayout.JAVA_INT.withName("depth"),

              // Pointers to variable-size data (64 bytes)
              ValueLayout.JAVA_LONG.withName("stack_ptr"),
              ValueLayout.JAVA_LONG.withName("memory_ptr"),
              ValueLayout.JAVA_LONG.withName("code_ptr"),
              ValueLayout.JAVA_LONG.withName("input_ptr"),
              ValueLayout.JAVA_LONG.withName("output_ptr"),
              ValueLayout.JAVA_LONG.withName("return_data_ptr"),
              ValueLayout.JAVA_LONG.withName("logs_ptr"),
              ValueLayout.JAVA_LONG.withName("warm_addresses_ptr"),

              // Sizes for variable data (32 bytes)
              ValueLayout.JAVA_INT.withName("code_size"),
              ValueLayout.JAVA_INT.withName("input_size"),
              ValueLayout.JAVA_INT.withName("output_size"),
              ValueLayout.JAVA_INT.withName("return_data_size"),
              ValueLayout.JAVA_INT.withName("logs_count"),
              ValueLayout.JAVA_INT.withName("warm_addresses_count"),
              ValueLayout.JAVA_INT.withName("warm_storage_count"),
              ValueLayout.JAVA_INT.withName("padding"),

              // Immutable context - addresses (100 bytes)
              MemoryLayout.sequenceLayout(20, ValueLayout.JAVA_BYTE).withName("recipient"),
              MemoryLayout.sequenceLayout(20, ValueLayout.JAVA_BYTE).withName("sender"),
              MemoryLayout.sequenceLayout(20, ValueLayout.JAVA_BYTE).withName("contract"),
              MemoryLayout.sequenceLayout(20, ValueLayout.JAVA_BYTE).withName("originator"),
              MemoryLayout.sequenceLayout(20, ValueLayout.JAVA_BYTE).withName("mining_beneficiary"),

              // Immutable context - values (96 bytes)
              MemoryLayout.sequenceLayout(32, ValueLayout.JAVA_BYTE).withName("value"),
              MemoryLayout.sequenceLayout(32, ValueLayout.JAVA_BYTE).withName("apparent_value"),
              MemoryLayout.sequenceLayout(32, ValueLayout.JAVA_BYTE).withName("gas_price"),

              // Halt reason (4 bytes)
              ValueLayout.JAVA_INT.withName("halt_reason"),

              // Reserved for future use and alignment (40 bytes)
              MemoryLayout.sequenceLayout(40, ValueLayout.JAVA_BYTE).withName("reserved"))
          .withName("MessageFrameMemory");

  // VarHandles for efficient field access

  /** Program counter */
  public static final VarHandle PC =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("pc"));

  /** Code section (for EOF) */
  public static final VarHandle SECTION =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("section"));

  /** Gas remaining */
  public static final VarHandle GAS_REMAINING =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("gas_remaining"));

  /** Gas refund */
  public static final VarHandle GAS_REFUND =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("gas_refund"));

  /** Stack size (current number of items on stack) */
  public static final VarHandle STACK_SIZE =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("stack_size"));

  /** Memory size in bytes */
  public static final VarHandle MEMORY_SIZE =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("memory_size"));

  /** Frame state (enum as int) */
  public static final VarHandle STATE =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("state"));

  /** Frame type (enum as int) */
  public static final VarHandle TYPE =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("type"));

  /** Is static call flag */
  public static final VarHandle IS_STATIC =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("is_static"));

  /** Call depth */
  public static final VarHandle DEPTH =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("depth"));

  /** Pointer to stack data */
  public static final VarHandle STACK_PTR =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("stack_ptr"));

  /** Pointer to memory data */
  public static final VarHandle MEMORY_PTR =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("memory_ptr"));

  /** Pointer to code bytes */
  public static final VarHandle CODE_PTR =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("code_ptr"));

  /** Pointer to input data */
  public static final VarHandle INPUT_PTR =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("input_ptr"));

  /** Pointer to output data */
  public static final VarHandle OUTPUT_PTR =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("output_ptr"));

  /** Pointer to return data */
  public static final VarHandle RETURN_DATA_PTR =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("return_data_ptr"));

  /** Code size */
  public static final VarHandle CODE_SIZE =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("code_size"));

  /** Input data size */
  public static final VarHandle INPUT_SIZE =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("input_size"));

  /** Output data size */
  public static final VarHandle OUTPUT_SIZE =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("output_size"));

  /** Return data size */
  public static final VarHandle RETURN_DATA_SIZE =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("return_data_size"));

  /** Halt reason */
  public static final VarHandle HALT_REASON =
      LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("halt_reason"));

  // Offsets for address fields (20 bytes each)
  public static final long OFFSET_RECIPIENT = 144;
  public static final long OFFSET_SENDER = 164;
  public static final long OFFSET_CONTRACT = 184;
  public static final long OFFSET_ORIGINATOR = 204;
  public static final long OFFSET_MINING_BENEFICIARY = 224;

  // Offsets for value fields (32 bytes each)
  public static final long OFFSET_VALUE = 244;
  public static final long OFFSET_APPARENT_VALUE = 276;
  public static final long OFFSET_GAS_PRICE = 308;

  /**
   * Estimate total memory size needed for a MessageFrame.
   *
   * @param stackSize Current stack size
   * @param memorySize Current memory size in bytes
   * @param codeSize Code size in bytes
   * @param inputSize Input data size in bytes
   * @return Total bytes needed
   */
  public static long estimateTotalSize(
      final int stackSize, final long memorySize, final int codeSize, final int inputSize) {
    long total = HEADER_SIZE;
    total += stackSize * (long) STACK_ITEM_SIZE; // Stack
    total += memorySize; // Memory
    total += codeSize; // Code
    total += inputSize; // Input
    total += 1024; // Output buffer
    total += 1024; // Return data buffer
    total += 4096; // Logs buffer
    total += 1024; // Warm addresses buffer
    return total;
  }

  private MessageFrameLayout() {
    // Utility class
  }
}
