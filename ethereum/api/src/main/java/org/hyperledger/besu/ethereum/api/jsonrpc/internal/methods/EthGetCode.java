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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameterOrBlockHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;

import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

public class EthGetCode extends AbstractBlockParameterOrBlockHashMethod {

  public EthGetCode(final BlockchainQueries blockchainQueries) {
    super(blockchainQueries);
  }

  public EthGetCode(final Supplier<BlockchainQueries> blockchainQueries) {
    super(blockchainQueries);
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_GET_CODE.getMethodName();
  }

  @Override
  protected BlockParameterOrBlockHash blockParameterOrBlockHash(
      final JsonRpcRequestContext request) {
    try {
      return request.getRequiredParameter(1, BlockParameterOrBlockHash.class);
    } catch (Exception e) { // TODO:replace with JsonRpcParameter.JsonRpcParameterException
      throw new InvalidJsonRpcParameters(
          "Invalid block or block hash parameter (index 1)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
  }

  @Override
  protected String resultByBlockHash(final JsonRpcRequestContext request, final Hash blockHash) {
    final Address address;
    try {
      address = request.getRequiredParameter(0, Address.class);
    } catch (Exception e) { // TODO:replace with JsonRpcParameter.JsonRpcParameterException
      throw new InvalidJsonRpcParameters(
          "Invalid address parameter (index 0)", RpcErrorType.INVALID_ADDRESS_PARAMS, e);
    }
    return getBlockchainQueries().getCode(address, blockHash).map(Bytes::toString).orElse(null);
  }
}
