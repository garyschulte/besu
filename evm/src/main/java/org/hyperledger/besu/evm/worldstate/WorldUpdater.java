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
package org.hyperledger.besu.evm.worldstate;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.MessageFrame;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;

/**
 * An object that buffers updates made over a particular {@link WorldView}.
 *
 * <p>All changes made to this object, being it account creation/deletion or account modifications
 * through {@link MutableAccount}, are immediately reflected on this object (so for instance,
 * deleting an account and trying to get it afterwards will return {@code null}) but do not impact
 * whichever {@link WorldView} this is an updater for until the {@link #commit} method is called.
 */
public interface WorldUpdater extends MutableWorldView {

  /**
   * Creates a new account, or reset it (that is, act as if it was deleted and created anew) if it
   * already exists.
   *
   * <p>After this call, the account will exists and will have the provided nonce and balance. The
   * code and storage will be empty.
   *
   * @param address the address of the account to create (or reset).
   * @param nonce the nonce for created/reset account.
   * @param balance the balance for created/reset account.
   * @return the account {@code address}, which will have nonce {@code nonce}, balance {@code
   *     balance} and empty code and storage.
   */
  MutableAccount createAccount(Address address, long nonce, Wei balance);

  /**
   * Creates a new account, or reset it (that is, act as if it was deleted and created anew) if it
   * already exists.
   *
   * <p>This call is equivalent to {@link #createAccount(Address, long, Wei)} but defaults both the
   * nonce and balance to zero.
   *
   * @param address the address of the account to create (or reset).
   * @return the account {@code address}, which will have 0 for the nonce and balance and empty code
   *     and storage.
   */
  default MutableAccount createAccount(final Address address) {
    return createAccount(address, Account.DEFAULT_NONCE, Account.DEFAULT_BALANCE);
  }

  /**
   * Retrieves the provided account if it exists, or create it if it doesn't.
   *
   * @param address the address of the account.
   * @return the account {@code address}. If that account exists, it is returned as if by {@link
   *     #getAccount(Address)}, otherwise, it is created and returned as if by {@link
   *     #createAccount(Address)} (and thus all his fields will be zero/empty).
   */
  default MutableAccount getOrCreate(final Address address) {
    MutableAccount account = getAccount(address);
    if (account == null) {
      account = createAccount(address);
      if (parentUpdater().isPresent() && parentUpdater().get().isDeleted(address)) {
        account.clearStorage();
        account.setCode(Bytes.EMPTY);
      }
    }
    return account;
  }

  /**
   * Check this and parent updaters to see if an address has been deleted since the last persist
   *
   * @param address address to check
   * @return true if any updaters have marked the address as deleted.
   */
  default boolean isDeleted(final Address address) {
    if (getDeletedAccountAddresses().contains(address)) {
      return true;
    } else {
      return parentUpdater().map(wu -> wu.isDeleted(address)).orElse(false);
    }
  }

  /**
   * Retrieves the provided account for a sender of a transaction if it exists, or creates it if it
   * doesn't.
   *
   * @param address the address of the account.
   * @return the account of the sender for {@code address}
   */
  default MutableAccount getOrCreateSenderAccount(final Address address) {
    return getOrCreate(address);
  }

  /**
   * Retrieves the provided account, returning a modifiable object (whose updates are accumulated by
   * this updater).
   *
   * @param address the address of the account.
   * @return the account {@code address}, or {@code null} if the account does not exist.
   */
  MutableAccount getAccount(Address address);

  /**
   * Retrieves the senders account, returning a modifiable object (whose updates are accumulated by
   * this updater).
   *
   * @param frame the current message frame.
   * @return the account {@code address}, or {@code null} if the account does not exist.
   */
  default MutableAccount getSenderAccount(final MessageFrame frame) {
    return getAccount(frame.getSenderAddress());
  }

  /**
   * Deletes the provided account.
   *
   * @param address the address of the account to delete. If that account doesn't exists prior to
   *     this call, this is a no-op.
   */
  void deleteAccount(Address address);

  /**
   * Returns the accounts that have been touched within the scope of this updater.
   *
   * @return the accounts that have been touched within the scope of this updater
   */
  Collection<? extends Account> getTouchedAccounts();

  /**
   * Returns the account addresses that have been deleted within the scope of this updater.
   *
   * @return the account addresses that have been deleted within the scope of this updater
   */
  Collection<Address> getDeletedAccountAddresses();

  /** Removes the changes that were made to this updater. */
  void revert();

  /**
   * Commits the changes made to this updater to the underlying {@link WorldView} this is an updater
   * of.
   */
  void commit();

  /**
   * The parent updater (if it exists).
   *
   * @return The parent WorldUpdater if this wraps another one, empty otherwise
   */
  Optional<WorldUpdater> parentUpdater();

  /** Clears any accounts that are empty */
  default void clearAccountsThatAreEmpty() {
    new ArrayList<>(getTouchedAccounts())
        .stream().filter(Account::isEmpty).forEach(a -> deleteAccount(a.getAddress()));
  }

  /**
   * Clears any accounts that are empty, excluding those in the provided set
   *
   * @param excludedAddresses the addresses to exclude from deletion
   */
  default void clearAccountsThatAreEmpty(final Set<Address> excludedAddresses) {
    new ArrayList<>(getTouchedAccounts())
        .stream()
            .filter(a -> !excludedAddresses.contains(a.getAddress()))
            .filter(Account::isEmpty)
            .forEach(a -> deleteAccount(a.getAddress()));
  }

  /** Mark transaction boundary. */
  default void markTransactionBoundary() {
    // default is to ignore
  }

  /**
   * Sets the {@link AuthorizedCodeService} associated with this {@link WorldUpdater}.
   *
   * @param authorizedCodeService the {@link AuthorizedCodeService} to associate with this {@link
   *     WorldUpdater}
   */
  void setAuthorizedCodeService(AuthorizedCodeService authorizedCodeService);
}
