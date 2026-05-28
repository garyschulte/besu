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
package org.hyperledger.besu.plugin.services.storage.benchmark;

import org.hyperledger.besu.crypto.Hash;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Microbenchmark isolating Panama segment access cost and MessageDigest allocation strategy for
 * decode-like operations (Keccak hash, sequential byte scan).
 *
 * <p>Answers two questions:
 *
 * <ol>
 *   <li>Is reading from a native {@link MemorySegment} (byte-by-byte or via ByteBuffer) materially
 *       slower than reading from a heap {@code byte[]}?
 *   <li>Does ThreadLocal MessageDigest reuse (no clone per call) outperform the old clone-from-
 *       singleton approach, and does that hold under 8-thread concurrency?
 * </ol>
 *
 * <p>Keccak paths under test:
 *
 * <ul>
 *   <li>{@code keccakFromHeap} — {@code Hash.keccak256(Bytes.wrap(heapArray))}. Exercises
 *       whatever strategy {@code Hash} currently uses internally.
 *   <li>{@code keccakNativeCopy} — bulk-copies native → heap then hashes. Current FFM get() path.
 *   <li>{@code keccakNativeByteBufferTuweni} — {@code Bytes.wrapByteBuffer()} path. Appears
 *       zero-copy but internally calls {@code toArrayUnsafe()} → byte-by-byte copy.
 *   <li>{@code keccakCloneBaseline} — explicit clone-from-shared-prototype per call. Replicates
 *       the old {@code Hash.keccak256()} behavior for a direct comparison.
 *   <li>{@code keccakThreadLocalDirectUpdate} — per-thread digest reuse + {@code
 *       MessageDigest.update(ByteBuffer)} directly. Best-case for ThreadLocal + native memory.
 * </ul>
 *
 * <p>Each keccak benchmark has a {@code *MultiThread} variant at {@code @Threads(8)} so the two
 * strategies (clone vs ThreadLocal) can be compared under realistic concurrency.
 *
 * <p>Run: {@code ./gradlew :plugins:storage-benchmarks:jmh -Pincludes=PanamaDecodeBenchmark}
 */
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgs = {"--enable-native-access=ALL-UNNAMED"})
public class PanamaDecodeBenchmark {

  /**
   * Value size in bytes. 128 approximates a Bonsai leaf/account node; 600 approximates a full
   * branch node (16 × 32-byte child hashes + RLP framing).
   */
  @Param({"128", "600"})
  public int valueSize;

  // --- Shared benchmark-scoped state (all threads read; never mutated after setup) ---

  byte[] heapArray;
  MemorySegment nativeSegment;
  /** Read-only direct ByteBuffer view of nativeSegment — {@code duplicate()} before each use. */
  ByteBuffer nativeDirectBuffer;

  /**
   * Shared prototype digest for the clone-baseline benchmarks. Only ever read via {@code clone()};
   * never updated. Concurrent {@code clone()} calls on a never-mutated prototype are safe.
   */
  MessageDigest sharedPrototypeDigest;

  /** Allocates heap and native buffers with identical random content. */
  @Setup(Level.Trial)
  public void setup() {
    heapArray = new byte[valueSize];
    new Random(0xDEADBEEFCAFEBABEL).nextBytes(heapArray);

    final Arena arena = Arena.ofAuto();
    nativeSegment = arena.allocate(valueSize);
    MemorySegment.copy(MemorySegment.ofArray(heapArray), 0, nativeSegment, 0, valueSize);
    nativeDirectBuffer = nativeSegment.asByteBuffer().asReadOnlyBuffer();

    sharedPrototypeDigest = newKeccakDigest();
  }

  /**
   * Per-thread state holding a private MessageDigest instance. Used by benchmarks that measure
   * the ThreadLocal-reuse strategy under concurrency without sharing state between threads.
   */
  @State(Scope.Thread)
  public static class ThreadDigestState {
    MessageDigest keccakDigest;

    /** Initialises a fresh Keccak-256 digest owned exclusively by this thread. */
    @Setup(Level.Trial)
    public void setup() {
      keccakDigest = newKeccakDigest();
    }
  }

  // ---------------------------------------------------------------------------
  // Raw byte scan — isolates per-byte access cost independent of hash overhead.
  // ---------------------------------------------------------------------------

  /**
   * Baseline sequential read of a heap {@code byte[]}. Lower bound for byte access speed.
   *
   * @return XOR of all bytes (prevents dead-code elimination)
   */
  @Benchmark
  public int scanHeapArray() {
    int acc = 0;
    for (int i = 0; i < heapArray.length; i++) {
      acc ^= heapArray[i];
    }
    return acc;
  }

  /**
   * Panama segment byte access via {@link MemorySegment#get(ValueLayout.OfByte, long)}. Measures
   * raw FFM per-byte cost — what a hypothetical {@code PinnedBytes.get(int)} would pay.
   *
   * @return XOR of all bytes (prevents dead-code elimination)
   */
  @Benchmark
  public int scanNativeSegment() {
    int acc = 0;
    final long len = nativeSegment.byteSize();
    for (long i = 0; i < len; i++) {
      acc ^= nativeSegment.get(ValueLayout.JAVA_BYTE, i);
    }
    return acc;
  }

  /**
   * Direct ByteBuffer byte access. Measures the ByteBuffer abstraction overhead over native
   * memory — the path taken by {@code Bytes.wrapByteBuffer()}'s internal {@code get(i)} calls.
   *
   * @return XOR of all bytes (prevents dead-code elimination)
   */
  @Benchmark
  public int scanDirectByteBuffer() {
    final ByteBuffer buf = nativeDirectBuffer.duplicate();
    int acc = 0;
    while (buf.hasRemaining()) {
      acc ^= buf.get();
    }
    return acc;
  }

  // ---------------------------------------------------------------------------
  // Bulk copy cost — isolated from hash computation.
  // ---------------------------------------------------------------------------

  /**
   * Cost of {@link MemorySegment#toArray(ValueLayout.OfByte)} alone. Subtract this from {@link
   * #keccakNativeCopy()} to isolate hash computation cost.
   *
   * @return the copied heap array
   */
  @Benchmark
  public byte[] toArrayCost() {
    return nativeSegment.toArray(ValueLayout.JAVA_BYTE);
  }

  // ---------------------------------------------------------------------------
  // Single-threaded keccak benchmarks
  // ---------------------------------------------------------------------------

  /**
   * Hash data already on the heap. Exercises {@code Hash.keccak256()} with whatever digest
   * strategy it currently uses (ThreadLocal reuse or clone).
   *
   * @return Keccak-256 hash
   */
  @Benchmark
  public Bytes32 keccakFromHeap() {
    return Hash.keccak256(Bytes.wrap(heapArray));
  }

  /**
   * Bulk-copy native → heap then hash. The current RocksDB FFM {@code get()} path. Copy cost is
   * dominated by the hash — see {@link #toArrayCost()} for isolation.
   *
   * @return Keccak-256 hash of the copied data
   */
  @Benchmark
  public Bytes32 keccakNativeCopy() {
    final byte[] copy = nativeSegment.toArray(ValueLayout.JAVA_BYTE);
    return Hash.keccak256(Bytes.wrap(copy));
  }

  /**
   * {@code Bytes.wrapByteBuffer()} then hash. Appears zero-copy but is not: Tuweni's default
   * {@code Bytes.update(MessageDigest)} calls {@code toArrayUnsafe()}, which for a direct
   * ByteBuffer iterates {@code get(int)} byte-by-byte — slower than bulk {@code toArray()}.
   *
   * @return Keccak-256 hash via the Tuweni wrapByteBuffer path
   */
  @Benchmark
  public Bytes32 keccakNativeByteBufferTuweni() {
    return Hash.keccak256(Bytes.wrapByteBuffer(nativeDirectBuffer.duplicate()));
  }

  /**
   * Clone-from-shared-prototype per call. Replicates the <em>old</em> {@code Hash.keccak256()}
   * behavior: one memoized singleton is cloned on every call, producing a new {@link
   * MessageDigest} object that is used once and becomes garbage.
   *
   * @return Keccak-256 hash via the clone path
   * @throws CloneNotSupportedException if the digest does not support cloning (should not happen)
   */
  @Benchmark
  @SuppressWarnings("DoNotInvokeMessageDigestDirectly")
  public Bytes32 keccakCloneBaseline() throws CloneNotSupportedException {
    final MessageDigest clone = (MessageDigest) sharedPrototypeDigest.clone();
    Bytes.wrap(heapArray).update(clone);
    return Bytes32.wrap(clone.digest());
  }

  /**
   * Per-thread digest reuse via {@link MessageDigest#update(ByteBuffer)} directly on the native
   * buffer. Best-case combination: no copy, no clone, no Tuweni {@code toArrayUnsafe()} call.
   * Whether the JCA provider skips an internal copy for direct ByteBuffers is what this measures.
   *
   * @param tds per-thread digest state (injected by JMH)
   * @return raw digest bytes
   */
  @Benchmark
  public byte[] keccakThreadLocalDirectUpdate(final ThreadDigestState tds) {
    tds.keccakDigest.reset();
    tds.keccakDigest.update(nativeDirectBuffer.duplicate());
    return tds.keccakDigest.digest();
  }

  // ---------------------------------------------------------------------------
  // 8-thread concurrency variants — same logic, different thread count.
  // ---------------------------------------------------------------------------

  /**
   * {@link #keccakFromHeap()} under 8-thread concurrency. Measures {@code Hash.keccak256()}
   * throughput with the current digest strategy (ThreadLocal) across concurrent callers.
   *
   * @return Keccak-256 hash
   */
  @Benchmark
  @Threads(8)
  public Bytes32 keccakFromHeapMultiThread() {
    return Hash.keccak256(Bytes.wrap(heapArray));
  }

  /**
   * {@link #keccakNativeCopy()} under 8-thread concurrency.
   *
   * @return Keccak-256 hash of the copied data
   */
  @Benchmark
  @Threads(8)
  public Bytes32 keccakNativeCopyMultiThread() {
    final byte[] copy = nativeSegment.toArray(ValueLayout.JAVA_BYTE);
    return Hash.keccak256(Bytes.wrap(copy));
  }

  /**
   * {@link #keccakNativeByteBufferTuweni()} under 8-thread concurrency.
   *
   * @return Keccak-256 hash via the Tuweni wrapByteBuffer path
   */
  @Benchmark
  @Threads(8)
  public Bytes32 keccakNativeByteBufferTuweniMultiThread() {
    return Hash.keccak256(Bytes.wrapByteBuffer(nativeDirectBuffer.duplicate()));
  }

  /**
   * Clone-from-shared-prototype per call under 8-thread concurrency. Direct comparison against
   * {@link #keccakFromHeapMultiThread()} to measure the ThreadLocal vs clone trade-off under load.
   * All 8 threads clone from the same prototype (never mutated after setup — concurrent reads are
   * safe).
   *
   * @return Keccak-256 hash via the clone path
   * @throws CloneNotSupportedException if the digest does not support cloning
   */
  @Benchmark
  @Threads(8)
  @SuppressWarnings("DoNotInvokeMessageDigestDirectly")
  public Bytes32 keccakCloneBaselineMultiThread() throws CloneNotSupportedException {
    final MessageDigest clone = (MessageDigest) sharedPrototypeDigest.clone();
    Bytes.wrap(heapArray).update(clone);
    return Bytes32.wrap(clone.digest());
  }

  /**
   * Per-thread digest reuse + native ByteBuffer update under 8-thread concurrency. Each thread has
   * its own {@link MessageDigest} instance (via {@link ThreadDigestState}); no cloning, no Tuweni
   * {@code toArrayUnsafe()} call.
   *
   * @param tds per-thread digest state (injected by JMH)
   * @return raw digest bytes
   */
  @Benchmark
  @Threads(8)
  public byte[] keccakThreadLocalDirectUpdateMultiThread(final ThreadDigestState tds) {
    tds.keccakDigest.reset();
    tds.keccakDigest.update(nativeDirectBuffer.duplicate());
    return tds.keccakDigest.digest();
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  @SuppressWarnings("DoNotInvokeMessageDigestDirectly")
  static MessageDigest newKeccakDigest() {
    try {
      return MessageDigest.getInstance("KECCAK-256");
    } catch (final NoSuchAlgorithmException e) {
      try {
        return MessageDigest.getInstance("SHA3-256");
      } catch (final NoSuchAlgorithmException ex) {
        throw new RuntimeException("No Keccak or SHA3-256 provider found", ex);
      }
    }
  }
}
