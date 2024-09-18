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

import com.sun.jna.ptr.IntByReference;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.nativelib.constantine.LibConstantineEIP2537;
import org.hyperledger.besu.nativelib.gnark.LibGnarkEIP2537;

import org.apache.tuweni.bytes.Bytes;

import javax.annotation.Nonnull;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

/** The type BLS12_G1 MultiExp precompiled contract. */
public class BLS12G1MultiExpPrecompiledContract extends AbstractBLS12PrecompiledContract {

  private static final int PARAMETER_LENGTH = 160;

  /** Instantiates a new BLS12_G1 MultiExp precompiled contract. */
  public BLS12G1MultiExpPrecompiledContract() {
    super(
        "BLS12_G1MULTIEXP",
        LibGnarkEIP2537.BLS12_G1MULTIEXP_OPERATION_SHIM_VALUE,
        Integer.MAX_VALUE / PARAMETER_LENGTH * PARAMETER_LENGTH);
  }

  @Nonnull
  @Override
  public PrecompileContractResult computePrecompile(
      final Bytes input, @Nonnull final MessageFrame messageFrame) {

    final int inputSize = Math.min(this.inputLimit, input.size());
    try {
      final byte[] result = LibConstantineEIP2537.g1msm(input.slice(0, inputSize).toArrayUnsafe());
      return PrecompileContractResult.success(Bytes.wrap(result));
    } catch(RuntimeException ex) {
      final String errorMessage = ex.getMessage();
      messageFrame.setRevertReason(Bytes.wrap(errorMessage.getBytes(UTF_8)));
      return PrecompileContractResult.halt(
          null, Optional.of(ExceptionalHaltReason.PRECOMPILE_ERROR));
    }
  }

  @Override
  public long gasRequirement(final Bytes input) {
    final int k = input.size() / PARAMETER_LENGTH;
    return 12L * k * getDiscount(k);
  }
}
