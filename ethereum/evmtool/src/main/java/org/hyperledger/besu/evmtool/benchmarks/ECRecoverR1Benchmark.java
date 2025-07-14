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
package org.hyperledger.besu.evmtool.benchmarks;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.crypto.SignatureAlgorithmType;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.fluent.EvmSpec;
import org.hyperledger.besu.evm.precompile.ECRECPrecompiledContract;
import org.hyperledger.besu.evm.precompile.PrecompiledContract;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hyperledger.besu.crypto.SignatureAlgorithmType.SECP_256_R1_CURVE_NAME;

/** Benchmark ECRecover precompile (ECDSA key extraction + keccak hash) */
public class ECRecoverR1Benchmark extends BenchmarkExecutor {

  /**
   * The constructor. Use default math based warmup and interations.
   *
   * @param output where to write the stats.
   * @param benchmarkConfig benchmark configurations.
   */
  public ECRecoverR1Benchmark(final PrintStream output, final BenchmarkConfig benchmarkConfig) {
    super(MATH_WARMUP, MATH_ITERATIONS, output, benchmarkConfig);
  }

  @Override
  public void runBenchmark(final Boolean attemptNative, final String fork) {
    EvmSpecVersion evmSpecVersion = EvmSpecVersion.fromName(fork);

    // openssl and bouncy castle implementations are very slow.  just use a few cases:
    final Map<String, Bytes> testCases = new LinkedHashMap<>();
    testCases.put("0x04e266ddfdc12668db30d4ca3e8f7749432c416044f2d2b8c10bf3d4012aeffa8abfa86404a2e9ffe67d47c587ef7a97a7f456b863b4d02cfc6928973ab5b1cb39", Bytes.fromHexString("0x9b2db89cb0e8fa3cc7608b4d6cc1dec0114e0b9ff4080bea12b134f489ab2bbc000000000000000000000000000000000000000000000000000000000000001b976d3a4e9d23326dc0baa9fa560b7c4e53f42864f508483a6473b6a11079b2db1b766e9ceb71ba6c01dcd46e0af462cd4cfa652ae5017d4555b8eeefe36e1932"));
    testCases.put("0x048984797b42905e8abd62068b8d54cc6d6916eda6793d559795332668a2cbc683d3479c408dad9d6f943f53c5e413288dbda46335999476313e046349ba73a57e", Bytes.fromHexString("0x86529e2290863b0ca6b51928809f02d419fec6feca28a2f54632af0a7232fd2d000000000000000000000000000000000000000000000000000000000000001bcac22d816e690cdf66e793cdec38b3abd0d1946ec710ec3a8964cb6490a63c725bbcc291c158077019c226625d51ac2213ac497043fbc45edd063efd0f7d6d11"));
    testCases.put("0x04a26a8f5ca01fe72696fc2e7b59bf81619f2db4d827f648effc3d9e6856871ff8a8d4c1bcd90910eb886bd0e39a7c3bf13090803729ecedd9d9cafed81d804a17", Bytes.fromHexString("0x2e1056bbc62f6f7321c88dabd840e7a8a09b337fc478cd5b31cd174a37c12b36000000000000000000000000000000000000000000000000000000000000001cb5f23dcee9e7bf5d993c764c8bc8e69edfdb80383c831e6f4429e0a412b262dc66548e449c7154b170fb2f9635a4d5130b5f5ea041b09d4291bde8777865a7cf"));

    SignatureAlgorithmFactory.setInstance(SignatureAlgorithmType.create(SECP_256_R1_CURVE_NAME));
    SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    if (attemptNative != null && (!attemptNative || !signatureAlgorithm.maybeEnableNative())) {
      signatureAlgorithm.disableNative();
    }
    output.println(signatureAlgorithm.isNative() ? "Native EcRecover" : "Java EcRecover");

    final PrecompiledContract contract = new ECRECPrecompiledContract(
        EvmSpec.evmSpec(evmSpecVersion).getEvm().getGasCalculator(),
        signatureAlgorithm);

    warmIterations = warmIterations / testCases.size();
    execIterations = execIterations / testCases.size();
    double execTime = Double.MIN_VALUE; // a way to dodge divide by zero
    long gasCost = 0;
    for (final Map.Entry<String, Bytes> testCase : testCases.entrySet()) {
      execTime += runPrecompileBenchmark(testCase.getKey(), testCase.getValue(), contract);
      gasCost += contract.gasRequirement(testCase.getValue());
    }
    execTime /= testCases.size();
    gasCost /= testCases.size();
    logPrecompilePerformance("ecrecover_r1", gasCost, execTime);
  }

  @Override
  public boolean isPrecompile() {
    return true;
  }
}
