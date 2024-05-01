package org.hyperledger.besu.evm.gascalculator.stateless;

import org.hyperledger.besu.datatypes.Address;

import org.apache.tuweni.units.bigints.UInt256;

public class DebugAccessWitness extends Eip4762AccessWitness {
  private long totalWitnessGas = 0l;

  @Override
  public long touchAndChargeProofOfAbsence(final Address address) {
    long gas = super.touchAndChargeProofOfAbsence(address);
    totalWitnessGas += gas;
    System.out.printf(
        "touchAndChargeProofOfAbsence address %s gas: %d acc: %d%n", address, gas, totalWitnessGas);
    return gas;
  }

  @Override
  public long touchAndChargeMessageCall(final Address address) {
    long gas = super.touchAndChargeMessageCall(address);
    totalWitnessGas += gas;
    System.out.printf(
        "touchAndChargeMessageCall address %s gas: %d acc: %d%n", address, gas, totalWitnessGas);
    return gas;
  }

  @Override
  public long touchAndChargeValueTransfer(final Address caller, final Address target) {
    long gas = super.touchAndChargeValueTransfer(caller, target);
    totalWitnessGas += gas;
    System.out.printf(
        "touchAndChargeValueTransfer caller: %s target: %s gas: %d acc: %d%n",
        caller, target, gas, totalWitnessGas);
    return gas;
  }

  @Override
  public long touchAndChargeContractCreateInit(
      final Address address, final boolean createSendsValue) {
    long gas = super.touchAndChargeContractCreateInit(address, createSendsValue);
    totalWitnessGas += gas;
    System.out.printf(
        "touchAndChargeContractCreateInit address: %s gas: %d acc: %d%n",
        address, gas, totalWitnessGas);
    return gas;
  }

  @Override
  public long touchAndChargeContractCreateCompleted(final Address address) {
    long gas = super.touchAndChargeContractCreateCompleted(address);
    totalWitnessGas += gas;
    System.out.printf(
        "touchAndChargeContractCreateCompleted address: %s gas: %d acc: %d%n",
        address, gas, totalWitnessGas);
    return gas;
  }

  @Override
  public long touchTxOriginAndComputeGas(final Address origin) {
    long gas = super.touchTxOriginAndComputeGas(origin);
    totalWitnessGas += gas;
    return gas;
  }

  @Override
  public long touchTxExistingAndComputeGas(final Address target, final boolean sendsValue) {
    long gas = super.touchTxExistingAndComputeGas(target, sendsValue);
    totalWitnessGas += gas;
    return gas;
  }

  @Override
  public long touchCodeChunksUponContractCreation(final Address address, final long codeLength) {
    // // note: NOT calling super so we can log the index keys
    // long gas = 0;
    // for (long i = 0; i < (codeLength + 30) / 31; i++) {
    //   var treeIndex = CODE_OFFSET.add(i).divide(VERKLE_NODE_WIDTH);
    //   var subIndex = CODE_OFFSET.add(i).mod(VERKLE_NODE_WIDTH);
    //   System.out.println("  treeIndex: " + treeIndex + " subIndex: " + subIndex);
    //   gas += touchAddressOnWriteAndComputeGas(address, treeIndex, subIndex);
    // }
    long gas = super.touchCodeChunksUponContractCreation(address, codeLength);
    System.out.printf(
        "touchCodeChunksUponContractCreation: address: %s codeLength: %d gas: %d acc: %d%n",
        address, codeLength, gas, totalWitnessGas);
    totalWitnessGas += gas;
    return gas;
  }

  @Override
  public long touchAddressOnWriteAndComputeGas(
      final Address address, final UInt256 treeIndex, final UInt256 subIndex) {
    long gas = super.touchAddressOnWriteAndComputeGas(address, treeIndex, subIndex);
    totalWitnessGas += gas;
    System.out.printf(
        "touchAddressOnWriteAndComputeGas: address: %s treeIndex: %s subIndex: %s gas: %d acc: %d%n",
        address,
        treeIndex.toEllipsisHexString(),
        subIndex.toEllipsisHexString(),
        gas,
        totalWitnessGas);
    return gas;
  }

  @Override
  public long touchAddressOnReadAndComputeGas(
      final Address address, final UInt256 treeIndex, final UInt256 subIndex) {
    long gas = super.touchAddressOnReadAndComputeGas(address, treeIndex, subIndex);
    totalWitnessGas += gas;
    System.out.printf(
        "touchAddressOnReadAndComputeGas: address: %s treeIndex: %s subIndex: %s gas: %d acc: %d%n",
        address,
        treeIndex.toEllipsisHexString(),
        subIndex.toEllipsisHexString(),
        gas,
        totalWitnessGas);
    return gas;
  }

  @Override
  public long touchCodeChunks(
      final Address address, final long offset, final long readSize, final long codeLength) {
    long gas = super.touchCodeChunks(address, offset, readSize, codeLength);
    totalWitnessGas += gas;
    System.out.printf(
        "touchCodeChunks: address: %s offset: %d readSize: %d codeLength: %d gas: %d acc: %d%n",
        address, offset, readSize, codeLength, gas, totalWitnessGas);
    return gas;
  }

  public long getTotalWitnessGas() {
    return totalWitnessGas;
  }
}
