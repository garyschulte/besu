/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.plugin.services.storage.rocksdbffm.segmented;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.Cleaner;

import io.github.dfa1.rocksdbffm.PinnableSlice;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes32;

/**
 * A {@link Bytes32} backed directly by a RocksDB block-cache pin, intended <em>solely</em> as an
 * optimised input to {@code UInt256.fromBytes()} in the SLOAD execution path.
 *
 * <p><strong>Scope of use:</strong> This class exists to feed the {@code UInt256.fromBytes(Bytes32)}
 * fast path. Because it implements {@link Bytes32}, {@code UInt256.fromBytes()} detects the {@code
 * instanceof Bytes32} condition and calls {@link #toArrayUnsafe()} directly into an {@code int[8]}
 * without calling {@code Bytes32.leftPad()}, eliminating one heap copy and leaving UInt256 with
 * the faster {@code int[8]}-backed representation. It is <em>not</em> a general-purpose {@link
 * Bytes32} and must not be used outside the storage read → UInt256 conversion pipeline.
 *
 * <p><strong>Experimental status:</strong> This implementation is experimental. It has not been
 * subjected to the full Besu test suite or production validation. If benchmarks confirm a
 * meaningful improvement to block execution throughput, this class and its callers should be
 * reviewed, hardened, and properly integrated before any production use.
 *
 * <p><strong>Single-use contract:</strong> The pin is destroyed <em>eagerly</em> inside {@link
 * #toArrayUnsafe()} as soon as the bytes have been read off the native segment — this is the call
 * that {@code UInt256.fromBytes} makes, and the pin is not needed afterward. After {@link
 * #toArrayUnsafe()} returns the backing native segment is invalid; the caller must not access this
 * object again. A {@link Cleaner} is registered as a safety net for any code path that does not go
 * through {@link #toArrayUnsafe()} (e.g. direct {@link #get} calls); because {@link
 * PinnableSlice#close()} is idempotent, double-close is safe.
 *
 * <p><strong>Block-cache pressure:</strong> Holding a {@link PinnableSlice} open prevents
 * eviction of the pinned RocksDB block-cache page. This class is designed for minimal hold time:
 * the pin is released as soon as {@code UInt256.fromBytes} drains the value via {@link
 * #toArrayUnsafe()}. Do not store instances of this class in caches or fields.
 */
final class PinnedBytes32 implements Bytes32 {

  private static final Cleaner CLEANER = Cleaner.create();

  /** The live native segment pointing into the pinned block-cache page. */
  private final MemorySegment data;

  /** The pin — closed deterministically in {@link #toArrayUnsafe()}, or by the Cleaner. */
  private final PinnableSlice slice;

  PinnedBytes32(final PinnableSlice slice) {
    this.slice = slice;
    this.data = slice.data();
    // Cleaner action must NOT reference 'this'; capturing 'slice' (a local) is safe.
    CLEANER.register(this, slice::close);
  }

  @Override
  public int size() {
    return 32;
  }

  @Override
  public byte get(final int i) {
    return data.get(ValueLayout.JAVA_BYTE, i);
  }

  /**
   * Copies 32 bytes from the pinned native segment into a heap array, then <em>immediately
   * destroys the pin</em>. After this method returns the backing native segment is invalid; the
   * caller must not access this {@link PinnedBytes32} object again.
   *
   * <p>When {@code UInt256.fromBytes} calls this method the JIT may apply escape analysis to the
   * returned array, scalar-replacing it and reading bytes directly from native memory into the
   * target {@code int[8]} without an intermediate heap allocation.
   *
   * @return a fresh {@code byte[32]} copied from the pinned native segment
   */
  @Override
  public byte[] toArrayUnsafe() {
    final byte[] arr = data.toArray(ValueLayout.JAVA_BYTE);
    slice.close(); // pin no longer needed; idempotent if Cleaner fires first
    return arr;
  }

  @Override
  public Bytes32 copy() {
    return Bytes32.wrap(toArrayUnsafe());
  }

  @Override
  public MutableBytes32 mutableCopy() {
    return MutableBytes32.wrap(toArrayUnsafe());
  }

  @Override
  public Bytes slice(final int i, final int length) {
    return Bytes.wrap(toArrayUnsafe(), i, length);
  }
}
