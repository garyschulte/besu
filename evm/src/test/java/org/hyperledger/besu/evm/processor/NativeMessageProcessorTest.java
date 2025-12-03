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
package org.hyperledger.besu.evm.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.code.CodeV0;
import org.hyperledger.besu.evm.frame.IMessageFrame;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.LondonGasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for NativeMessageProcessor using Panama FFM.
 *
 * <p>These tests verify that the Java-C++ interop works correctly
 * using the mock native EVM implementation.
 */
public class NativeMessageProcessorTest {

  private NativeMessageProcessor processor;
  private EVM evm;
  private WorldUpdater worldUpdater;

  @BeforeEach
  public void setUp() {
    if (!NativeMessageProcessor.isAvailable()) {
      System.err.println(NativeMessageProcessor.getLoadError().getMessage());
    }
    // Skip if native library not available
    assumeTrue(
        NativeMessageProcessor.isAvailable(),
        "Native EVM library not available, skipping test");

    // Create EVM instance
    evm = MainnetEVMs.london(EvmConfiguration.DEFAULT);

    // Create processor
    processor = new NativeMessageProcessor(evm);

    // Mock world updater
    worldUpdater = Mockito.mock(WorldUpdater.class);
  }

  @Test
  public void testNativeLibraryAvailable() {
    assertThat(NativeMessageProcessor.isAvailable()).isTrue();
    assertThat(NativeMessageProcessor.getLoadError()).isNull();
  }

  @Test
  public void testSimpleExecution() {
    // Create a simple message frame
    MessageFrame frame = createTestFrame();

    // Record initial state
    int initialPC = frame.getPC();
    long initialGas = frame.getRemainingGas();

    // Execute with native processor
    processor.execute(frame, OperationTracer.NO_TRACING);

    // Verify execution completed
    assertThat(frame.getState()).isEqualTo(MessageFrame.State.COMPLETED_SUCCESS);

    // Verify PC is at STOP instruction (doesn't increment for STOP)
    assertThat(frame.getPC()).isEqualTo(initialPC);

    // Verify gas was consumed (STOP operation is free)
    assertThat(frame.getRemainingGas()).isEqualTo(initialGas);

    // Note: Simple STOP doesn't set output in this mock
  }

  @Test
  public void testStackOperation() {
    // Create frame with bytecode: ADD, STOP (0x01 0x00)
    Code code = new CodeV0(Bytes.fromHexString("0x0100"), Hash.ZERO);
    MessageFrame frame = createTestFrameWithCode(code, 1000000L);

    // Push two values onto stack
    frame.pushStackItem(Bytes32.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000005")); // 5
    frame.pushStackItem(Bytes32.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000003")); // 3

    int initialStackSize = frame.stackSize();
    assertThat(initialStackSize).isEqualTo(2);

    // Execute (ADD pops 2 and pushes sum)
    processor.execute(frame, OperationTracer.NO_TRACING);

    // Verify stack size decreased by 1 (popped 2, pushed 1)
    assertThat(frame.stackSize()).isEqualTo(1);

    // Verify result is sum (5 + 3 = 8)
    Bytes result = frame.popStackItem();
    assertThat(result.get(31)).isEqualTo((byte) 8);
  }

  @Test
  public void testOutOfGas() {
    // Create frame with insufficient gas
    MessageFrame frame = createTestFrameWithGas(2); // Only 2 gas, need 3

    // Execute
    processor.execute(frame, OperationTracer.NO_TRACING);

    // Verify exceptional halt
    assertThat(frame.getState()).isEqualTo(MessageFrame.State.EXCEPTIONAL_HALT);
  }

  @Test
  public void testWithTracerCallbacks() {
    // Create a counting tracer to verify callbacks work
    CountingTracer tracer = new CountingTracer();

    // Create frame with bytecode: PUSH1 5, PUSH1 3, ADD, STOP
    // 0x60 0x05  - PUSH1 5
    // 0x60 0x03  - PUSH1 3
    // 0x01       - ADD
    // 0x00       - STOP
    Bytes bytecode = Bytes.fromHexString("0x60056003010000");
    Code code = new CodeV0(bytecode, Hash.ZERO);
    MessageFrame frame = createTestFrameWithCode(code, 1000000L);

    System.out.println("Bytecode: " + bytecode.toHexString());
    System.out.println("Bytecode size: " + bytecode.size());

    // Execute with tracer
    long startNanos = System.nanoTime();
    processor.execute(frame, tracer);
    long durationNanos = System.nanoTime() - startNanos;

    // Verify execution succeeded
    assertThat(frame.getState()).isEqualTo(MessageFrame.State.COMPLETED_SUCCESS);

    // Verify callbacks were called
    System.out.println("Tracer callbacks:");
    System.out.println("  Pre-execution: " + tracer.preExecutionCount);
    System.out.println("  Post-execution: " + tracer.postExecutionCount);
    System.out.println("  Execution time: " + durationNanos + " ns");

    // Mock EVM executes 4 operations: PUSH1, PUSH1, ADD, STOP
    // Each operation should trigger pre and post callbacks
    assertThat(tracer.preExecutionCount).isEqualTo(4);
    assertThat(tracer.postExecutionCount).isEqualTo(4);
  }

  @Test
  public void testTracerCallbackPerformance() {
    // Create frame with bytecode that will execute multiple operations
    Code code = new CodeV0(Bytes.fromHexString("0x6005600301000000"), Hash.ZERO);

    // Warm up
    for (int i = 0; i < 100; i++) {
      MessageFrame warmupFrame = createTestFrameWithCode(code, 1000000L);
      CountingTracer warmupTracer = new CountingTracer();
      processor.execute(warmupFrame, warmupTracer);
    }

    // Measure performance
    int iterations = 10000;
    long totalNanos = 0;

    for (int i = 0; i < iterations; i++) {
      MessageFrame testFrame = createTestFrameWithCode(code, 1000000L);
      CountingTracer iterTracer = new CountingTracer();

      long startNanos = System.nanoTime();
      processor.execute(testFrame, iterTracer);
      long durationNanos = System.nanoTime() - startNanos;

      totalNanos += durationNanos;
    }

    long avgNanos = totalNanos / iterations;
    long avgCallbackNanos = avgNanos / 8; // 4 operations * 2 callbacks each

    System.out.println("\n=== Tracer Callback Performance ===");
    System.out.println("Iterations: " + iterations);
    System.out.println("Average execution time: " + avgNanos + " ns");
    System.out.println("Average per callback: " + avgCallbackNanos + " ns");
    System.out.println("Callbacks per second: " + (1_000_000_000L / avgCallbackNanos));
  }

  /**
   * Simple tracer that counts callback invocations for testing.
   */
  private static class CountingTracer implements OperationTracer {
    public int preExecutionCount = 0;
    public int postExecutionCount = 0;

    @Override
    public void tracePreExecution(final MessageFrame frame) {
      preExecutionCount++;
    }

    @Override
    public void tracePostExecution(
        final MessageFrame frame,
        final org.hyperledger.besu.evm.operation.Operation.OperationResult operationResult) {
      postExecutionCount++;
    }
  }

  private MessageFrame createTestFrame() {
    return createTestFrameWithGas(1000000L);
  }

  private MessageFrame createTestFrameWithGas(final long gas) {
    // Create simple bytecode (just a STOP instruction)
    Code code = new CodeV0(Bytes.of(0x00), Hash.ZERO);
    return createTestFrameWithCode(code, gas);
  }

  private MessageFrame createTestFrameWithCode(final Code code, final long gas) {
    Address sender = Address.fromHexString("0x1000000000000000000000000000000000000000");
    Address recipient = Address.fromHexString("0x2000000000000000000000000000000000000000");
    Address contract = recipient;

    return MessageFrame.builder()
        .type(IMessageFrame.Type.MESSAGE_CALL)
        .worldUpdater(worldUpdater)
        .initialGas(gas)
        .contract(contract)
        .address(recipient)
        .originator(sender)
        .sender(sender)
        .gasPrice(Wei.of(1))
        .value(Wei.ZERO)
        .apparentValue(Wei.ZERO)
        .code(code)
        .inputData(Bytes.EMPTY)
        .completer(__ -> {})
        .miningBeneficiary(Address.ZERO)
        .blockValues(Mockito.mock(org.hyperledger.besu.evm.frame.BlockValues.class))
        .blockHashLookup((_,__) -> Hash.ZERO)
        .build();
  }
}
