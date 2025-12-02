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
package org.hyperledger.besu.evm;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.operation.OperationRegistry;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Native C++ implementation of the EVM via JNI.
 *
 * <p>This class provides a drop-in replacement for the Java EVM implementation by delegating
 * execution to a native C++ implementation for improved performance. The native library maintains
 * full compatibility with the standard Besu EVM interface.
 *
 * <p>The native library (libbesu_native_evm) must be available on the library path for this class
 * to function. The library is automatically loaded on class initialization.
 */
public class NativeEVM extends EVM {
  private static final Logger LOG = LoggerFactory.getLogger(NativeEVM.class);

  private static boolean nativeLibraryLoaded = false;
  private static String nativeLibraryLoadError = null;

  static {
    try {
      System.loadLibrary("besu_native_evm");
      nativeLibraryLoaded = true;
      LOG.info("Native EVM library loaded successfully");
    } catch (final UnsatisfiedLinkError e) {
      nativeLibraryLoadError = e.getMessage();
      LOG.warn(
          "Failed to load native EVM library. Falling back to Java implementation. Error: {}",
          e.getMessage());
    }
  }

  /**
   * Check if native library is available.
   *
   * @return true if native library loaded successfully
   */
  public static boolean isNativeLibraryLoaded() {
    return nativeLibraryLoaded;
  }

  /**
   * Get native library load error if any.
   *
   * @return the exception that occurred during loading, or null if loaded successfully
   */
  public static String getNativeLibraryLoadError() {
    return nativeLibraryLoadError;
  }

  /**
   * Instantiates a new Native EVM.
   *
   * @param operations the operations
   * @param gasCalculator the gas calculator
   * @param evmConfiguration the evm configuration
   * @param evmSpecVersion the evm spec version
   */
  public NativeEVM(
      final OperationRegistry operations,
      final GasCalculator gasCalculator,
      final EvmConfiguration evmConfiguration,
      final EvmSpecVersion evmSpecVersion) {
    super(operations, gasCalculator, evmConfiguration, evmSpecVersion);

    if (!nativeLibraryLoaded) {
      LOG.warn(
          "Native EVM created but library not loaded. Will use Java implementation via superclass.");
    }
  }

  /**
   * Run EVM execution to halt using native implementation.
   *
   * <p>This method delegates to the native C++ implementation via JNI. If the native library is not
   * available, it falls back to the Java implementation from the superclass.
   *
   * @param frame the message frame
   * @param operationTracer the operation tracer
   */
  @Override
  public void runToHalt(final MessageFrame frame, final OperationTracer operationTracer) {
    if (!nativeLibraryLoaded) {
      // Fallback to Java implementation
      super.runToHalt(frame, operationTracer);
      return;
    }

    try {
      // Call native implementation
      runToHaltNative(frame, operationTracer);
    } catch (final UnsatisfiedLinkError e) {
      // Native method not found - fallback to Java
      LOG.warn("Native method not available, falling back to Java implementation: {}", e.getMessage());
      super.runToHalt(frame, operationTracer);
    } catch (final Throwable e) {
      // Any other error from native code - propagate
      LOG.error("Error in native EVM execution", e);
      throw new RuntimeException("Native EVM execution failed", e);
    }
  }

  /**
   * Native method implementation of runToHalt.
   *
   * <p>This method is implemented in C++ and linked via JNI. It performs EVM execution in native
   * code for improved performance while maintaining full compatibility with the Java MessageFrame
   * and OperationTracer interfaces.
   *
   * <p>The native implementation will:
   *
   * <ul>
   *   <li>Execute opcodes using the provided MessageFrame
   *   <li>Call back to the Java OperationTracer for tracing operations
   *   <li>Handle all exceptional halt conditions
   *   <li>Update the MessageFrame state appropriately
   * </ul>
   *
   * @param frame the message frame containing execution context
   * @param operationTracer the operation tracer for execution tracing
   */
  private native void runToHaltNative(final MessageFrame frame, final OperationTracer operationTracer);

  /**
   * Initialize native EVM resources (called from C++ on first use).
   *
   * <p>This method is called from native code to perform any necessary initialization of native
   * resources. It receives configuration information to set up the native EVM properly.
   *
   * @return handle to native EVM context (opaque pointer cast to long)
   */
  private native long initializeNative(
      final int maxStackSize,
      final boolean jumpDestCacheWeightKB,
      final int evmSpecVersionOrdinal);

  /**
   * Destroy native EVM resources (should be called on shutdown).
   *
   * <p>This method cleans up any native resources allocated for this EVM instance.
   *
   * @param nativeHandle the native handle returned from initializeNative
   */
  private native void destroyNative(final long nativeHandle);
}
