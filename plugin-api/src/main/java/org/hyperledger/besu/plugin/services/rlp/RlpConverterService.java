/*
 * Copyright Hyperledger Besu Contributors.
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
package org.hyperledger.besu.plugin.services.rlp;

import org.hyperledger.besu.plugin.data.BlockBody;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.data.TransactionReceipt;
import org.hyperledger.besu.plugin.services.BesuService;

import org.apache.tuweni.bytes.Bytes;

/**
 * A service for converting RLP to and from block headers, block bodies, and transaction receipts.
 */
public interface RlpConverterService extends BesuService {

  /**
   * Builds a block header from RLP.
   * @param rlp bytes
   * @return header header
   */
  BlockHeader buildHeaderFromRlp(final Bytes rlp);

  /**
   * Builds a block body from RLP.
   * @param rlp bytes
   * @return body
   */
  BlockBody buildBodyFromRlp(final Bytes rlp);

  /**
   * Builds a transaction receipt from RLP.
   * @param rlp rlp vla
   * @return receipt
   */
  TransactionReceipt buildReceiptFromRlp(final Bytes rlp);

  /**
   * Builds RLP from a block header.
   * @param blockHeader hed
   * @return bytes
   */
  Bytes buildRlpFromHeader(final BlockHeader blockHeader);

  /**
   * Builds RLP from a block body.
   * @param blockBody bod
   * @return ret
   */
  Bytes buildRlpFromBody(final BlockBody blockBody);

  /**
   * Builds RLP from a transaction receipt.
   * @param receipt rec
   * @return bytes
   */
  Bytes buildRlpFromReceipt(final TransactionReceipt receipt);
}
