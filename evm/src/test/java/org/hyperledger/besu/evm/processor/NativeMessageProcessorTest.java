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

    // Calculate metrics
    int totalCallbacks = tracer.preExecutionCount + tracer.postExecutionCount;
    long callbacksPerSecond = (totalCallbacks * 1_000_000_000L) / durationNanos;

    // Verify callbacks were called
    System.out.println("Tracer callbacks:");
    System.out.println("  Pre-execution: " + tracer.preExecutionCount);
    System.out.println("  Post-execution: " + tracer.postExecutionCount);
    System.out.println("  Total callbacks: " + totalCallbacks);
    System.out.println("  Execution time: " + durationNanos + " ns");
    System.out.println("  Callbacks/second: " + String.format("%,d", callbacksPerSecond));

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

  @Test
  public void testCallbacksReceiveValidFrameData() {
    // Test that callbacks receive actual frame data, not null
    // Create same bytecode as testWithTracerCallbacks: PUSH1 5, PUSH1 3, ADD, STOP
    Bytes bytecode = Bytes.fromHexString("0x60056003010000");
    Code code = new CodeV0(bytecode, Hash.ZERO);
    MessageFrame testFrame = createTestFrameWithCode(code, 1000000L);
    ValidatingTracer tracer = new ValidatingTracer();

    processor.execute(testFrame, tracer);

    // Verify execution succeeded
    assertThat(testFrame.getState()).isEqualTo(MessageFrame.State.COMPLETED_SUCCESS);

    // Verify callbacks were invoked
    assertThat(tracer.preExecutionCount).isGreaterThan(0);
    assertThat(tracer.postExecutionCount).isGreaterThan(0);

    // Verify frame data was passed (not null)
    assertThat(tracer.receivedNonNullFrame).isTrue();
    assertThat(tracer.receivedValidPC).isTrue();
    assertThat(tracer.receivedValidGas).isTrue();

    System.out.println("\n=== Callback Frame Data Validation ===");
    System.out.println("Pre-execution callbacks: " + tracer.preExecutionCount);
    System.out.println("Post-execution callbacks: " + tracer.postExecutionCount);
    System.out.println("Received non-null frame: " + tracer.receivedNonNullFrame);
    System.out.println("Received valid PC: " + tracer.receivedValidPC);
    System.out.println("Received valid gas: " + tracer.receivedValidGas);
  }

  @Test
  public void testPerformanceComparisonJavaVsNative() {
    // Compare Java EVM vs Native EVM performance with tracer callbacks
    // Bytecode: PUSH1 5, PUSH1 3, ADD, STOP
    Bytes bytecode = Bytes.fromHexString("0x60056003010000");

    int iterations = 10000;
    int warmupRuns = 100;

    // === Java EVM Performance ===
    System.out.println("\n=== Java EVM vs Native EVM Performance Comparison ===");
    System.out.println("Bytecode: " + bytecode.toHexString() + " (PUSH1 5, PUSH1 3, ADD, STOP)");
    System.out.println("Iterations: " + iterations);

    // Warm up Java EVM
    for (int i = 0; i < warmupRuns; i++) {
      MessageFrame javaFrame = createTestFrameWithCode(new CodeV0(bytecode, Hash.ZERO), 1000000L);
      CountingTracer javaTracer = new CountingTracer();
      evm.runToHalt(javaFrame, javaTracer);
    }

    // Measure Java EVM
    long javaTotalNanos = 0;
    for (int i = 0; i < iterations; i++) {
      MessageFrame javaFrame = createTestFrameWithCode(new CodeV0(bytecode, Hash.ZERO), 1000000L);
      CountingTracer javaTracer = new CountingTracer();

      long startNanos = System.nanoTime();
      evm.runToHalt(javaFrame, javaTracer);
      long durationNanos = System.nanoTime() - startNanos;

      javaTotalNanos += durationNanos;
    }

    long javaAvgNanos = javaTotalNanos / iterations;

    // === Native EVM Performance ===

    // Warm up Native EVM
    for (int i = 0; i < warmupRuns; i++) {
      MessageFrame nativeFrame = createTestFrameWithCode(new CodeV0(bytecode, Hash.ZERO), 1000000L);
      CountingTracer nativeTracer = new CountingTracer();
      processor.execute(nativeFrame, nativeTracer);
    }

    // Measure Native EVM
    long nativeTotalNanos = 0;
    for (int i = 0; i < iterations; i++) {
      MessageFrame nativeFrame = createTestFrameWithCode(new CodeV0(bytecode, Hash.ZERO), 1000000L);
      CountingTracer nativeTracer = new CountingTracer();

      long startNanos = System.nanoTime();
      processor.execute(nativeFrame, nativeTracer);
      long durationNanos = System.nanoTime() - startNanos;

      nativeTotalNanos += durationNanos;
    }

    long nativeAvgNanos = nativeTotalNanos / iterations;

    // === Results ===
    double speedup = (double) javaAvgNanos / nativeAvgNanos;

    System.out.println("\n--- Results ---");
    System.out.println("Java EVM average:   " + String.format("%,d", javaAvgNanos) + " ns");
    System.out.println("Native EVM average: " + String.format("%,d", nativeAvgNanos) + " ns");
    System.out.println("Speedup: " + String.format("%.2fx", speedup));
    System.out.println();

    if (speedup > 1.0) {
      System.out.println("✓ Native EVM is " + String.format("%.2fx", speedup) + " faster than Java EVM");
    } else {
      System.out.println("⚠ Java EVM is " + String.format("%.2fx", 1.0 / speedup) + " faster than Native EVM");
    }
  }

  @Test
  public void testPerformanceComparisonNoTracer() {
    // Compare Java EVM vs Native EVM performance WITHOUT tracer callbacks
    // This measures pure execution overhead without callback costs
    // Bytecode: PUSH1 5, PUSH1 3, ADD, STOP
    Bytes bytecode = Bytes.fromHexString("0x60056003010000");

    int iterations = 10000;
    int warmupRuns = 100;

    System.out.println("\n=== Java EVM vs Native EVM Performance (NO_TRACING) ===");
    System.out.println("Bytecode: " + bytecode.toHexString() + " (PUSH1 5, PUSH1 3, ADD, STOP)");
    System.out.println("Iterations: " + iterations);
    System.out.println("Note: Using NO_TRACING to measure pure execution without callbacks");

    // === Java EVM Performance (NO_TRACING) ===

    // Warm up Java EVM
    for (int i = 0; i < warmupRuns; i++) {
      MessageFrame javaFrame = createTestFrameWithCode(new CodeV0(bytecode, Hash.ZERO), 1000000L);
      evm.runToHalt(javaFrame, OperationTracer.NO_TRACING);
    }

    // Measure Java EVM
    long javaTotalNanos = 0;
    for (int i = 0; i < iterations; i++) {
      MessageFrame javaFrame = createTestFrameWithCode(new CodeV0(bytecode, Hash.ZERO), 1000000L);

      long startNanos = System.nanoTime();
      evm.runToHalt(javaFrame, OperationTracer.NO_TRACING);
      long durationNanos = System.nanoTime() - startNanos;

      javaTotalNanos += durationNanos;
    }

    long javaAvgNanos = javaTotalNanos / iterations;

    // === Native EVM Performance (NO_TRACING) ===

    // Warm up Native EVM
    for (int i = 0; i < warmupRuns; i++) {
      MessageFrame nativeFrame = createTestFrameWithCode(new CodeV0(bytecode, Hash.ZERO), 1000000L);
      processor.execute(nativeFrame, OperationTracer.NO_TRACING);
    }

    // Measure Native EVM
    long nativeTotalNanos = 0;
    for (int i = 0; i < iterations; i++) {
      MessageFrame nativeFrame = createTestFrameWithCode(new CodeV0(bytecode, Hash.ZERO), 1000000L);

      long startNanos = System.nanoTime();
      processor.execute(nativeFrame, OperationTracer.NO_TRACING);
      long durationNanos = System.nanoTime() - startNanos;

      nativeTotalNanos += durationNanos;
    }

    long nativeAvgNanos = nativeTotalNanos / iterations;

    // === Results ===
    double speedup = (double) javaAvgNanos / nativeAvgNanos;

    System.out.println("\n--- Results (NO_TRACING) ---");
    System.out.println("Java EVM average:   " + String.format("%,d", javaAvgNanos) + " ns");
    System.out.println("Native EVM average: " + String.format("%,d", nativeAvgNanos) + " ns");
    System.out.println("Speedup: " + String.format("%.2fx", speedup));
    System.out.println();

    if (speedup > 1.0) {
      System.out.println("✓ Native EVM is " + String.format("%.2fx", speedup) + " faster than Java EVM");
    } else {
      System.out.println("⚠ Java EVM is " + String.format("%.2fx", 1.0 / speedup) + " faster than Native EVM");
    }

    System.out.println("\nNote: This measures pure execution + FFI overhead, without callback costs");
  }

  @Test
  public void testPerformanceLongBytecode() {
    // Test with longer bytecode to see if native EVM becomes competitive
    // Generate bytecode: PUSH1 1, [PUSH1 1, ADD] x 249, STOP = 500 operations
    int pushAddPairs = 249; // Will result in 1 + (249 * 2) + 1 = 500 operations

    // Build bytecode: PUSH1 1, [PUSH1 1, ADD] * 249, STOP
    byte[] bytecodeArray = new byte[1 + 2 + (pushAddPairs * 3) + 1];
    int idx = 0;
    bytecodeArray[idx++] = 0x60; // PUSH1
    bytecodeArray[idx++] = 0x01; // value 1

    for (int i = 0; i < pushAddPairs; i++) {
      bytecodeArray[idx++] = 0x60; // PUSH1
      bytecodeArray[idx++] = 0x01; // value 1
      bytecodeArray[idx++] = 0x01; // ADD
    }
    bytecodeArray[idx++] = 0x00; // STOP

    Bytes bytecode = Bytes.wrap(bytecodeArray);
    Code code = new CodeV0(bytecode, Hash.ZERO);

    int iterations = 1000; // Fewer iterations since bytecode is longer
    int warmupRuns = 1; // Minimal warmup to avoid JIT optimization skew

    System.out.println("\n=== Java EVM vs Native EVM Performance (Long Bytecode, Minimal Warmup) ===");
    System.out.println("Bytecode length: " + bytecode.size() + " bytes");
    System.out.println("Operations: ~500 (PUSH1 1, [PUSH1 1, ADD] x 249, STOP)");
    System.out.println("Warmup runs: " + warmupRuns + " (minimal to avoid JIT optimization)");
    System.out.println("Measurement iterations: " + iterations);

    // === Java EVM Performance (NO_TRACING) ===

    // Warm up
    for (int i = 0; i < warmupRuns; i++) {
      MessageFrame javaFrame = createTestFrameWithCode(code, 10000000L);
      evm.runToHalt(javaFrame, OperationTracer.NO_TRACING);
    }

    // Measure
    long javaTotalNanos = 0;
    for (int i = 0; i < iterations; i++) {
      MessageFrame javaFrame = createTestFrameWithCode(code, 10000000L);

      long startNanos = System.nanoTime();
      evm.runToHalt(javaFrame, OperationTracer.NO_TRACING);
      long durationNanos = System.nanoTime() - startNanos;

      javaTotalNanos += durationNanos;
    }

    long javaAvgNanos = javaTotalNanos / iterations;

    // === Native EVM Performance (NO_TRACING) ===

    // Warm up
    for (int i = 0; i < warmupRuns; i++) {
      MessageFrame nativeFrame = createTestFrameWithCode(code, 10000000L);
      processor.execute(nativeFrame, OperationTracer.NO_TRACING);
    }

    // Measure
    long nativeTotalNanos = 0;
    for (int i = 0; i < iterations; i++) {
      MessageFrame nativeFrame = createTestFrameWithCode(code, 10000000L);

      long startNanos = System.nanoTime();
      processor.execute(nativeFrame, OperationTracer.NO_TRACING);
      long durationNanos = System.nanoTime() - startNanos;

      nativeTotalNanos += durationNanos;
    }

    long nativeAvgNanos = nativeTotalNanos / iterations;

    // === Results ===
    double speedup = (double) javaAvgNanos / nativeAvgNanos;

    System.out.println("\n--- Results (NO_TRACING, 500 ops) ---");
    System.out.println("Java EVM average:   " + String.format("%,d", javaAvgNanos) + " ns (" + String.format("%.2f", javaAvgNanos / 500.0) + " ns/op)");
    System.out.println("Native EVM average: " + String.format("%,d", nativeAvgNanos) + " ns (" + String.format("%.2f", nativeAvgNanos / 500.0) + " ns/op)");
    System.out.println("Speedup: " + String.format("%.2fx", speedup));
    System.out.println();

    if (speedup > 1.0) {
      System.out.println("✓ Native EVM is " + String.format("%.2fx", speedup) + " faster than Java EVM");
    } else if (speedup < 1.0) {
      System.out.println("⚠ Java EVM is still " + String.format("%.2fx", 1.0 / speedup) + " faster than Native EVM");
    } else {
      System.out.println("≈ Performance is roughly equivalent");
    }

    // Calculate FFI overhead amortization
    double ffiOverhead = 12566.0; // From previous test
    double nativeExecOnly = nativeAvgNanos - ffiOverhead;
    System.out.println("\nEstimated FFI overhead: " + String.format("%.0f", ffiOverhead) + " ns");
    System.out.println("Estimated native execution: " + String.format("%.0f", nativeExecOnly) + " ns");
    System.out.println("FFI overhead as % of total: " + String.format("%.1f%%", (ffiOverhead / nativeAvgNanos) * 100));
  }

  @Test
  public void testReusableMemoryPerformance() {
    // Test the performance difference between allocating arenas vs reusing static memory
    // This isolates the arena allocation overhead
    int pushAddPairs = 249; // 500 operations total

    // Build bytecode: PUSH1 1, [PUSH1 1, ADD] * 249, STOP
    byte[] bytecodeArray = new byte[1 + 2 + (pushAddPairs * 3) + 1];
    int idx = 0;
    bytecodeArray[idx++] = 0x60; // PUSH1
    bytecodeArray[idx++] = 0x01; // value 1

    for (int i = 0; i < pushAddPairs; i++) {
      bytecodeArray[idx++] = 0x60; // PUSH1
      bytecodeArray[idx++] = 0x01; // value 1
      bytecodeArray[idx++] = 0x01; // ADD
    }
    bytecodeArray[idx++] = 0x00; // STOP

    Bytes bytecode = Bytes.wrap(bytecodeArray);
    Code code = new CodeV0(bytecode, Hash.ZERO);

    int iterations = 1000;
    int warmupRuns = 10;

    System.out.println("\n=== Arena Allocation Overhead Test ===");
    System.out.println("Bytecode: 500 operations");
    System.out.println("Warmup: " + warmupRuns + " iterations");
    System.out.println("Measurement: " + iterations + " iterations");

    // === Native EVM with arena allocation (current implementation) ===

    // Warm up
    for (int i = 0; i < warmupRuns; i++) {
      MessageFrame frame = createTestFrameWithCode(code, 10000000L);
      processor.execute(frame, OperationTracer.NO_TRACING);
    }

    // Measure
    long withArenaAllocTotalNanos = 0;
    for (int i = 0; i < iterations; i++) {
      MessageFrame frame = createTestFrameWithCode(code, 10000000L);

      long startNanos = System.nanoTime();
      processor.execute(frame, OperationTracer.NO_TRACING);
      long durationNanos = System.nanoTime() - startNanos;

      withArenaAllocTotalNanos += durationNanos;
    }

    long withArenaAllocAvgNanos = withArenaAllocTotalNanos / iterations;

    // === Native EVM with reusable memory (no arena allocation) ===

    // Warm up
    for (int i = 0; i < warmupRuns; i++) {
      MessageFrame frame = createTestFrameWithCode(code, 10000000L);
      processor.executeWithReusableMemory(frame, OperationTracer.NO_TRACING);
    }

    // Measure
    long withReusableTotalNanos = 0;
    for (int i = 0; i < iterations; i++) {
      MessageFrame frame = createTestFrameWithCode(code, 10000000L);

      long startNanos = System.nanoTime();
      processor.executeWithReusableMemory(frame, OperationTracer.NO_TRACING);
      long durationNanos = System.nanoTime() - startNanos;

      withReusableTotalNanos += durationNanos;
    }

    long withReusableAvgNanos = withReusableTotalNanos / iterations;

    // === Results ===
    long arenaOverhead = withArenaAllocAvgNanos - withReusableAvgNanos;
    double improvement = (double) withArenaAllocAvgNanos / withReusableAvgNanos;

    System.out.println("\n--- Results ---");
    System.out.println("Native with arena allocation:  " + String.format("%,d", withArenaAllocAvgNanos) + " ns");
    System.out.println("Native with reusable memory:   " + String.format("%,d", withReusableAvgNanos) + " ns");
    System.out.println("Arena allocation overhead:     " + String.format("%,d", arenaOverhead) + " ns (" + String.format("%.1f%%", (arenaOverhead * 100.0 / withArenaAllocAvgNanos)) + ")");
    System.out.println("Speedup with reusable memory:  " + String.format("%.2fx", improvement));

    if (improvement > 1.1) {
      System.out.println("\n✓ Reusable memory is " + String.format("%.2fx", improvement) + " faster");
    } else {
      System.out.println("\n≈ Minimal difference - arena allocation is not the bottleneck");
    }

    // === Compare optimized native to Java EVM ===

    // Warm up Java
    for (int i = 0; i < warmupRuns; i++) {
      MessageFrame frame = createTestFrameWithCode(code, 10000000L);
      evm.runToHalt(frame, OperationTracer.NO_TRACING);
    }

    // Measure Java
    long javaTotalNanos = 0;
    for (int i = 0; i < iterations; i++) {
      MessageFrame frame = createTestFrameWithCode(code, 10000000L);

      long startNanos = System.nanoTime();
      evm.runToHalt(frame, OperationTracer.NO_TRACING);
      long durationNanos = System.nanoTime() - startNanos;

      javaTotalNanos += durationNanos;
    }

    long javaAvgNanos = javaTotalNanos / iterations;
    double nativeVsJava = (double) withReusableAvgNanos / javaAvgNanos;

    System.out.println("\n--- Optimized Native vs Java EVM ---");
    System.out.println("Java EVM:                          " + String.format("%,d", javaAvgNanos) + " ns (" + String.format("%.2f", javaAvgNanos / 500.0) + " ns/op)");
    System.out.println("Native (reusable memory):          " + String.format("%,d", withReusableAvgNanos) + " ns (" + String.format("%.2f", withReusableAvgNanos / 500.0) + " ns/op)");

    if (nativeVsJava < 1.0) {
      System.out.println("✓ Native is " + String.format("%.2fx", 1.0 / nativeVsJava) + " faster than Java!");
    } else {
      System.out.println("⚠ Java is still " + String.format("%.2fx", nativeVsJava) + " faster than Native");
    }
  }

  /**
   * Simple tracer that counts callback invocations for testing.
   */
  private static class CountingTracer implements OperationTracer {
    public int preExecutionCount = 0;
    public int postExecutionCount = 0;

    @Override
    public void tracePreExecution(final IMessageFrame frame) {
      preExecutionCount++;
    }

    @Override
    public void tracePostExecution(
        final IMessageFrame frame,
        final org.hyperledger.besu.evm.operation.Operation.OperationResult operationResult) {
      postExecutionCount++;
    }
  }

  /**
   * Tracer that validates frame data is actually passed correctly to callbacks.
   */
  private static class ValidatingTracer implements OperationTracer {
    public boolean receivedNonNullFrame = false;
    public boolean receivedValidPC = false;
    public boolean receivedValidGas = false;
    public int preExecutionCount = 0;
    public int postExecutionCount = 0;

    @Override
    public void tracePreExecution(final IMessageFrame frame) {
      preExecutionCount++;
      if (frame != null) {
        receivedNonNullFrame = true;
        // Validate frame has reasonable data
        if (frame.getPC() >= 0) {
          receivedValidPC = true;
        }
        if (frame.getRemainingGas() > 0) {
          receivedValidGas = true;
        }
      }
    }

    @Override
    public void tracePostExecution(
        final IMessageFrame frame,
        final org.hyperledger.besu.evm.operation.Operation.OperationResult operationResult) {
      postExecutionCount++;
      if (frame != null && operationResult != null) {
        receivedNonNullFrame = true;
      }
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
