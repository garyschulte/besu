package org.hyperledger.besu.datatypes;

import java.util.List;

import org.apache.tuweni.units.bigints.UInt256;

public interface AccessWitness {

  List<Address> keys();

  long touchAndChargeProofOfAbsence(Address address);

  long touchAndChargeValueTransfer(Address caller, Address target);

  long touchAndChargeMessageCall(Address address);

  long touchTxOriginAndComputeGas(Address origin);

  long touchTxExistingAndComputeGas(Address target, boolean sendsValue);

  long touchAndChargeContractCreateInit(Address address, boolean createSendsValue);

  long touchAndChargeContractCreateCompleted(final Address address);

  long touchAddressOnWriteAndComputeGas(Address address, UInt256 treeIndex, UInt256 subIndex);

  long touchAddressOnReadAndComputeGas(Address address, UInt256 treeIndex, UInt256 subIndex);

  List<UInt256> getStorageSlotTreeIndexes(UInt256 storageKey);

  long touchCodeChunksUponContractCreation(Address address, long codeLength);

  long touchCodeChunks(Address address, long offset, long readSize, long codeLength);

  default long touchCodeChunksWithoutAccessCost(
      final Address address, final long offset, final long readSize, final long codeLength) {
    return touchCodeChunks(address, offset, readSize, codeLength);
  }
}
