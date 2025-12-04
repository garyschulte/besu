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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.code.CodeV0;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.frame.IMessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import org.apache.tuweni.bytes.Bytes;
import org.mockito.Mockito;

/**
 * Standalone profiling program for NativeMessageProcessor.
 *
 * Run with async-profiler:
 * java --enable-preview --enable-native-access=ALL-UNNAMED \
 *   -Djava.library.path=/Users/garyschulte/dev/metaru \
 *   -agentpath:/tmp/async-profiler-3.0-macos/lib/libasyncProfiler.dylib=start,event=cpu,file=/tmp/native_profile_flamegraph.html \
 *   -cp evm/build/libs/evm-test.jar:... \
 *   org.hyperledger.besu.evm.processor.ProfileNativeProcessor
 */
public class ProfileNativeProcessor {

  public static void main(final String[] args) throws Exception {
    System.out.println("=== Native Processor Profiling ===");
    System.out.println("Warming up...");

    // Initialize EVM and processor
    final EVM evm = MainnetEVMs.cancun(EvmConfiguration.DEFAULT);
    final NativeMessageProcessor processor = new NativeMessageProcessor(evm);

    // Generate much longer bytecode: PUSH1 1, [PUSH1 1, ADD] x 4999, STOP = 10,000 operations
    final int pushAddPairs = 4999;
    final byte[] bytecodeArray = new byte[1 + 2 + (pushAddPairs * 3) + 1];
    int idx = 0;
    bytecodeArray[idx++] = 0x60; // PUSH1
    bytecodeArray[idx++] = 0x01; // value 1

    for (int i = 0; i < pushAddPairs; i++) {
      bytecodeArray[idx++] = 0x60; // PUSH1
      bytecodeArray[idx++] = 0x01; // value 1
      bytecodeArray[idx++] = 0x01; // ADD
    }
    bytecodeArray[idx++] = 0x00; // STOP

    final Bytes bytecode = Bytes.wrap(bytecodeArray);
    final Code code = new CodeV0(bytecode, Hash.ZERO);

    System.out.println("Bytecode: " + pushAddPairs + " PUSH1+ADD pairs = ~10,000 operations");

    // Pre-create frames to reuse (avoids frame creation overhead in profile)
    final int poolSize = 100;
    final MessageFrame[] framePool = new MessageFrame[poolSize];
    for (int i = 0; i < poolSize; i++) {
      framePool[i] = createTestFrame(code, 100000000L);
    }

    // Warmup: 1000 iterations
    for (int i = 0; i < 1000; i++) {
      final MessageFrame frame = framePool[i % poolSize];
      processor.executeWithReusableMemory(frame, OperationTracer.NO_TRACING);
      // Reset frame state for reuse
      frame.setPC(0);
      frame.setGasRemaining(100000000L);
      frame.setState(MessageFrame.State.CODE_EXECUTING);
    }

    System.out.println("Warmup complete. Starting profiled execution...");
    System.out.println("Running: Native (no trace) + Java (no trace) + Native (with trace) + Java (with trace)");

    // === NATIVE EVM EXECUTION (NO TRACING) ===
    final long nativeStartTime = System.nanoTime();

    for (int i = 0; i < 20000; i++) {
      final MessageFrame frame = framePool[i % poolSize];
      processor.executeWithReusableMemory(frame, OperationTracer.NO_TRACING);
      // Reset frame state for reuse
      frame.setPC(0);
      frame.setGasRemaining(100000000L);
      frame.setState(MessageFrame.State.CODE_EXECUTING);
    }

    final long nativeEndTime = System.nanoTime();
    final long nativeTotalNanos = nativeEndTime - nativeStartTime;
    final double nativeAvgNanos = nativeTotalNanos / 20000.0;

    // === JAVA EVM EXECUTION (NO TRACING) ===
    final long javaStartTime = System.nanoTime();

    for (int i = 0; i < 20000; i++) {
      final MessageFrame frame = framePool[i % poolSize];
      evm.runToHalt(frame, OperationTracer.NO_TRACING);
      // Reset frame state for reuse
      frame.setPC(0);
      frame.setGasRemaining(100000000L);
      frame.setState(MessageFrame.State.CODE_EXECUTING);
    }

    final long javaEndTime = System.nanoTime();
    final long javaTotalNanos = javaEndTime - javaStartTime;
    final double javaAvgNanos = javaTotalNanos / 20000.0;

    // === NATIVE EVM EXECUTION (WITH TRACING) ===
    final CountingTracer nativeTracer = new CountingTracer();
    final long nativeTracingStartTime = System.nanoTime();

    for (int i = 0; i < 5000; i++) {
      final MessageFrame frame = framePool[i % poolSize];
      processor.executeWithReusableMemory(frame, nativeTracer);
      // Reset frame state for reuse
      frame.setPC(0);
      frame.setGasRemaining(100000000L);
      frame.setState(MessageFrame.State.CODE_EXECUTING);
    }

    final long nativeTracingEndTime = System.nanoTime();
    final long nativeTracingTotalNanos = nativeTracingEndTime - nativeTracingStartTime;
    final double nativeTracingAvgNanos = nativeTracingTotalNanos / 5000.0;

    // === JAVA EVM EXECUTION (WITH TRACING) ===
    final CountingTracer javaTracer = new CountingTracer();
    final long javaTracingStartTime = System.nanoTime();

    for (int i = 0; i < 5000; i++) {
      final MessageFrame frame = framePool[i % poolSize];
      evm.runToHalt(frame, javaTracer);
      // Reset frame state for reuse
      frame.setPC(0);
      frame.setGasRemaining(100000000L);
      frame.setState(MessageFrame.State.CODE_EXECUTING);
    }

    final long javaTracingEndTime = System.nanoTime();
    final long javaTracingTotalNanos = javaTracingEndTime - javaTracingStartTime;
    final double javaTracingAvgNanos = javaTracingTotalNanos / 5000.0;

    System.out.println("\n=== Results ===");

    System.out.println("\n--- WITHOUT TRACING (NO_TRACING) ---");
    System.out.println("NATIVE EVM:");
    System.out.println("  Average per execution: " + String.format("%.2f", nativeAvgNanos) + " ns");
    System.out.println("  Average per operation: " + String.format("%.2f", nativeAvgNanos / 10000.0) + " ns/op");

    System.out.println("\nJAVA EVM:");
    System.out.println("  Average per execution: " + String.format("%.2f", javaAvgNanos) + " ns");
    System.out.println("  Average per operation: " + String.format("%.2f", javaAvgNanos / 10000.0) + " ns/op");

    System.out.println("\nCOMPARISON (no tracing):");
    if (nativeAvgNanos < javaAvgNanos) {
      System.out.println("  Native is " + String.format("%.2fx", javaAvgNanos / nativeAvgNanos) + " FASTER than Java");
    } else {
      System.out.println("  Java is " + String.format("%.2fx", nativeAvgNanos / javaAvgNanos) + " FASTER than Native");
    }

    System.out.println("\n--- WITH TRACING (CountingTracer, " + (nativeTracer.preExecutionCount / 5000) + " callbacks per execution) ---");
    System.out.println("NATIVE EVM:");
    System.out.println("  Average per execution: " + String.format("%.2f", nativeTracingAvgNanos) + " ns");
    System.out.println("  Average per operation: " + String.format("%.2f", nativeTracingAvgNanos / 10000.0) + " ns/op");
    System.out.println("  Callback overhead: " + String.format("%.2f", nativeTracingAvgNanos - nativeAvgNanos) + " ns (" + String.format("%.2f", ((nativeTracingAvgNanos - nativeAvgNanos) / nativeTracingAvgNanos) * 100) + "%)");

    System.out.println("\nJAVA EVM:");
    System.out.println("  Average per execution: " + String.format("%.2f", javaTracingAvgNanos) + " ns");
    System.out.println("  Average per operation: " + String.format("%.2f", javaTracingAvgNanos / 10000.0) + " ns/op");
    System.out.println("  Callback overhead: " + String.format("%.2f", javaTracingAvgNanos - javaAvgNanos) + " ns (" + String.format("%.2f", ((javaTracingAvgNanos - javaAvgNanos) / javaTracingAvgNanos) * 100) + "%)");

    System.out.println("\nCOMPARISON (with tracing):");
    if (nativeTracingAvgNanos < javaTracingAvgNanos) {
      System.out.println("  Native is " + String.format("%.2fx", javaTracingAvgNanos / nativeTracingAvgNanos) + " FASTER than Java");
    } else {
      System.out.println("  Java is " + String.format("%.2fx", nativeTracingAvgNanos / javaTracingAvgNanos) + " FASTER than Native");
    }

    System.out.println("\nProfile saved to: /tmp/native_profile_flamegraph.html");
    System.out.println("(Flame graph shows all execution paths)");
  }

  private static MessageFrame createTestFrame(final Code code, final long gas) {
    final Wei gasPrice = Wei.of(1);
    final Wei value = Wei.ZERO;
    final Address origin = Address.fromHexString("0x0000000000000000000000000000000000000001");
    final Address contract = Address.fromHexString("0x0000000000000000000000000000000000000100");
    final Address coinbase = Address.fromHexString("0x0000000000000000000000000000000000000000");
    final Bytes inputData = Bytes.EMPTY;

    final BlockValues blockValues = new BlockValues() {
      @Override
      public long getGasLimit() { return Long.MAX_VALUE; }
      @Override
      public long getTimestamp() { return 1; }
      @Override
      public java.util.Optional<Wei> getBaseFee() { return java.util.Optional.of(Wei.of(1)); }
      @Override
      public long getNumber() { return 1; }
      @Override
      public Bytes getDifficultyBytes() { return Bytes.EMPTY; }
    };

    return MessageFrame.builder()
        .type(MessageFrame.Type.MESSAGE_CALL)
        .worldUpdater(Mockito.mock(WorldUpdater.class))
        .initialGas(gas)
        .contract(contract)
        .address(contract)
        .originator(origin)
        .sender(origin)
        .gasPrice(gasPrice)
        .inputData(inputData)
        .value(value)
        .apparentValue(value)
        .code(code)
        .blockValues(blockValues)
        .completer(__ -> {})
        .miningBeneficiary(coinbase)
        .blockHashLookup((__, ___) -> Hash.ZERO)
        .build();
  }

  /**
   * Simple tracer that counts callback invocations.
   */
  @SuppressWarnings("UnusedVariable")
  private static class CountingTracer implements OperationTracer {
    public int preExecutionCount = 0;
    public int postExecutionCount = 0;

    @Override
    public void tracePreExecution(final IMessageFrame frame) {
      preExecutionCount++;
    }

    @Override
    public void tracePostExecution(final IMessageFrame frame, final Operation.OperationResult operationResult) {
      postExecutionCount++;
    }
  }
}
