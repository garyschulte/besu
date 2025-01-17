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
package org.hyperledger.besu.evm.precompile;

import org.hyperledger.besu.crypto.altbn128.AltBn128Point;
import org.hyperledger.besu.crypto.altbn128.Fq;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.nativelib.gnark.LibGnarkEIP196;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Stopwatch;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;

/** The AltBN128Add precompiled contract. */
public class AltBN128AddPrecompiledContract extends AbstractAltBnPrecompiledContract {

  private static final int PARAMETER_LENGTH = 128;
  public static final String PRECOMPILE_NAME = "AltBN128Add";

  private final long gasCost;
  private static final Cache<Integer, PrecompileInputResultTuple> bnAddCache =
      Caffeine.newBuilder().maximumSize(1000).build();

  private AltBN128AddPrecompiledContract(final GasCalculator gasCalculator, final long gasCost) {
    super(
        PRECOMPILE_NAME,
        gasCalculator,
        LibGnarkEIP196.EIP196_ADD_OPERATION_RAW_VALUE,
        PARAMETER_LENGTH);
    this.gasCost = gasCost;
  }

  /**
   * Create Byzantium AltBN128Add precompiled contract.
   *
   * @param gasCalculator the gas calculator
   * @return the AltBN128Add precompiled contract
   */
  public static AltBN128AddPrecompiledContract byzantium(final GasCalculator gasCalculator) {
    return new AltBN128AddPrecompiledContract(gasCalculator, 500L);
  }

  /**
   * Create Istanbul AltBN128Add precompiled contract.
   *
   * @param gasCalculator the gas calculator
   * @return the AltBN128Add precompiled contract
   */
  public static AltBN128AddPrecompiledContract istanbul(final GasCalculator gasCalculator) {
    return new AltBN128AddPrecompiledContract(gasCalculator, 150L);
  }

  @Override
  public long gasRequirement(final Bytes input) {
    return gasCost;
  }

  @Nonnull
  @Override
  public PrecompileContractResult computePrecompile(
      final Bytes input, @Nonnull final MessageFrame messageFrame) {
    final Stopwatch sw = Stopwatch.createStarted();

    PrecompileInputResultTuple res;
    Integer cacheKey = null;

    Stopwatch sw2 = Stopwatch.createStarted();
    if (enableResultCaching) {
      cacheKey = Arrays.hashCode(input.toArrayUnsafe());
      res = bnAddCache.getIfPresent(cacheKey);
      if (res != null) {
        if (res.cachedInput().equals(input)) {
          cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.HIT));
          System.err.printf(
              "\taltbn128add cache hit, lookup time %d ns, total time %d ns\n",
              sw2.elapsed(TimeUnit.NANOSECONDS), sw.elapsed(TimeUnit.NANOSECONDS));
          return res.cachedResult();
        } else {
          cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.FALSE_POSITIVE));
        }
      } else {
        cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.MISS));
      }
    }
    System.err.printf(
        "\taltbn128add cache miss, lookup time %d ns\n", sw2.elapsed(TimeUnit.NANOSECONDS));

    if (useNative) {
      res = new PrecompileInputResultTuple(input, computeNative(input, messageFrame));
    } else {
      res = new PrecompileInputResultTuple(input, computeDefault(input));
    }
    if (cacheKey != null) {
      sw2.reset();
      bnAddCache.put(cacheKey, res);
      System.err.printf(
          "\taltbn128add caching result, put time %d ns, total time %d ns\n",
          sw2.elapsed(TimeUnit.NANOSECONDS), sw.elapsed(TimeUnit.NANOSECONDS));
    }
    return res.cachedResult();
  }

  private static PrecompileContractResult computeDefault(final Bytes input) {
    final BigInteger x1 = extractParameter(input, 0, 32);
    final BigInteger y1 = extractParameter(input, 32, 32);
    final BigInteger x2 = extractParameter(input, 64, 32);
    final BigInteger y2 = extractParameter(input, 96, 32);

    final AltBn128Point p1 = new AltBn128Point(Fq.create(x1), Fq.create(y1));
    final AltBn128Point p2 = new AltBn128Point(Fq.create(x2), Fq.create(y2));
    if (!p1.isOnCurve() || !p2.isOnCurve()) {
      return PrecompileContractResult.halt(
          null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
    }
    final AltBn128Point sum = p1.add(p2);
    final Bytes x = sum.getX().toBytes();
    final Bytes y = sum.getY().toBytes();
    final MutableBytes result = MutableBytes.create(64);
    x.copyTo(result, 32 - x.size());
    y.copyTo(result, 64 - y.size());

    return PrecompileContractResult.success(result.copy());
  }

  private static BigInteger extractParameter(
      final Bytes input, final int offset, final int length) {
    if (offset > input.size() || length == 0) {
      return BigInteger.ZERO;
    }
    final byte[] raw = Arrays.copyOfRange(input.toArray(), offset, offset + length);
    return new BigInteger(1, raw);
  }
}
