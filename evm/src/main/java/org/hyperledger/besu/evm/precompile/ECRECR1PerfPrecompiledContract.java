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

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.nativelib.boringssl.BoringSSLPrecompiles;
import org.hyperledger.besu.nativelib.boringssl.BoringSSLPrecompiles.ECRecoverResult;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;
import org.apache.tuweni.bytes.MutableBytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The ECREC precompiled contract. */
public class ECRECR1PerfPrecompiledContract extends AbstractPrecompiledContract {

  private static final Logger LOG = LoggerFactory.getLogger(ECRECR1PerfPrecompiledContract.class);
  private static final String PRECOMPILE_NAME = "ECREC";
  private static final int V_BASE = 27;
  private static final Cache<Integer, PrecompileInputResultTuple> ecrecCache =
      Caffeine.newBuilder().maximumSize(1000).build();

  /**
   * Instantiates a new ECREC precompiled contract with the default signature algorithm.
   *
   * @param gasCalculator the gas calculator
   */
  public ECRECR1PerfPrecompiledContract(final GasCalculator gasCalculator) {
    super(PRECOMPILE_NAME, gasCalculator);
  }

  @Override
  public long gasRequirement(final Bytes input) {
    return gasCalculator().getEcrecPrecompiledContractGasCost();
  }

  @NotNull
  @Override
  public PrecompileContractResult computePrecompile(
      final Bytes input, @NotNull final MessageFrame messageFrame) {
    final int size = input.size();

    final Bytes safeInput =
        size >= 128 ? input : Bytes.wrap(input, MutableBytes.create(128 - size));
    final Bytes32 messageHash = Bytes32.wrap(safeInput, 0);
    if (!safeInput.slice(32, 31).isZero()) {
      // recId should be left padded with zeros
      return PrecompileContractResult.success(Bytes.EMPTY);
    }

    PrecompileInputResultTuple res;
    Integer cacheKey = null;
    if (enableResultCaching) {
      cacheKey = getCacheKey(input);
      res = ecrecCache.getIfPresent(cacheKey);

      if (res != null) {
        if (res.cachedInput().equals(input)) {
          cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.HIT));
          return res.cachedResult();
        } else {
          LOG.debug(
              "false positive ecrecover {}, cache key {}, cached input: {}, input: {}",
              input.getClass().getSimpleName(),
              cacheKey,
              res.cachedInput().toHexString(),
              input.toHexString());
          cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.FALSE_POSITIVE));
        }
      } else {
        cacheEventConsumer.accept(new CacheEvent(PRECOMPILE_NAME, CacheMetric.MISS));
      }
    }

    final int recId = safeInput.get(63) - V_BASE;
    final byte[] sigBytes = safeInput.slice(64, 64).toArrayUnsafe();

    try {
      final ECRecoverResult ecres =
          BoringSSLPrecompiles.ecrecover(messageHash.toArrayUnsafe(), sigBytes, recId);
      if (!(ecres.status() == 0) || ecres.publicKey().isEmpty()) {
        res =
            new PrecompileInputResultTuple(
                enableResultCaching ? input.copy() : input,
                PrecompileContractResult.success(Bytes.EMPTY));
        if (cacheKey != null) {
          ecrecCache.put(cacheKey, res);
        }
        return res.cachedResult();
      }

      final Bytes32 hashed = Hash.keccak256(Bytes.wrap(ecres.publicKey().get()));
      final MutableBytes32 result = MutableBytes32.create();
      hashed.slice(12).copyTo(result, 12);
      res =
          new PrecompileInputResultTuple(
              enableResultCaching ? input.copy() : input, PrecompileContractResult.success(result));
      if (enableResultCaching) {
        ecrecCache.put(cacheKey, res);
      }
      return res.cachedResult();
    } catch (final IllegalArgumentException e) {
      return PrecompileContractResult.success(Bytes.EMPTY);
    }
  }
}
