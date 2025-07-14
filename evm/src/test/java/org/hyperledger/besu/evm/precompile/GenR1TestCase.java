/*
 * Copyright contributors to Besu.
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

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.Security;
import java.security.Signature;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;

import org.apache.tuweni.bytes.Bytes;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.encoders.Hex;

public class GenR1TestCase {

  static SecureRandom random;
  static final BigInteger ORDER =
      new BigInteger(
          1, Hex.decodeStrict("FFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551"));

  public static void main(final String[] args) throws Exception {
    random = SecureRandom.getInstanceStrong();
    for (int i = 0; i < 20; i++) {
      blah(i);
    }
  }

  public static void blah(final int i) throws Exception {
    // Add BouncyCastle provider
    Security.addProvider(new BouncyCastleProvider());

    // Generate random 32-byte message hash
    byte[] messageHash = new byte[32];
    random.nextBytes(messageHash);

    // Generate P-256 key pair
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("EC", "BC");
    ECGenParameterSpec ecSpec = new ECGenParameterSpec("secp256r1");
    keyPairGenerator.initialize(ecSpec, random);
    KeyPair keyPair = keyPairGenerator.generateKeyPair();

    ECPrivateKey privateKey = (ECPrivateKey) keyPair.getPrivate();
    ECPublicKey publicKey = (ECPublicKey) keyPair.getPublic();

    // Sign the message hash
    Signature signer = Signature.getInstance("NONEwithECDSA", "BC");
    signer.initSign(privateKey, random);
    signer.update(messageHash);
    byte[] signature = signer.sign();

    // Parse DER signature to extract r and s
    BigInteger[] rs = parseDERSignature(signature);
    BigInteger r = rs[0];
    BigInteger s = rs[1];

    // Extract public key coordinates (without 0x04 prefix)
    var pubKeyPoint = publicKey.getW();
    BigInteger x = pubKeyPoint.getAffineX();
    BigInteger y = pubKeyPoint.getAffineY();

    // var malleatedSignatureS = ORDER.subtract(s);

    // System.out.printf("%s,%s,%s,%s%s,0,,malleated but valid signature %d\n",
    System.out.printf(
        "%s0000000000000000000000000000000000000000000000000000000000000001%s%s,04%s%s,1,wrong recovery id %d\n",
        Bytes.of(messageHash).toUnprefixedHexString(),
        safeBigIntTo32bytes(r),
        safeBigIntTo32bytes(s),
        // safeBigIntTo32bytes(malleatedSignatureS),
        safeBigIntTo32bytes(x),
        safeBigIntTo32bytes(y),
        i);
  }

  private static String safeBigIntTo32bytes(final BigInteger bigint) {
    var bytesVal = Bytes.of(bigint.toByteArray());
    return bytesVal.size() > 32
        ? bytesVal.slice(1, 32).toUnprefixedHexString()
        : bytesVal.toUnprefixedHexString();
  }

  private static BigInteger[] parseDERSignature(final byte[] derSignature) {
    int offset = 0;

    // Skip SEQUENCE tag and length
    if (derSignature[offset] != 0x30) {
      throw new IllegalArgumentException("Invalid DER signature format");
    }
    offset += 2; // Skip tag and length

    // Parse r
    if (derSignature[offset] != 0x02) {
      throw new IllegalArgumentException("Invalid DER signature format - r not INTEGER");
    }
    offset++;
    int rLength = derSignature[offset++] & 0xFF;
    byte[] rBytes = new byte[rLength];
    System.arraycopy(derSignature, offset, rBytes, 0, rLength);
    BigInteger r = new BigInteger(1, rBytes);
    offset += rLength;

    // Parse s
    if (derSignature[offset] != 0x02) {
      throw new IllegalArgumentException("Invalid DER signature format - s not INTEGER");
    }
    offset++;
    int sLength = derSignature[offset++] & 0xFF;
    byte[] sBytes = new byte[sLength];
    System.arraycopy(derSignature, offset, sBytes, 0, sLength);
    BigInteger s = new BigInteger(1, sBytes);

    return new BigInteger[] {r, s};
  }
}
