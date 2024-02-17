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
package org.hyperledger.besu.cli.subcommands.storage;

import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.eth.sync.fastsync.FastSyncState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.RangeManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapWorldDownloadState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;

import java.time.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/** Heal Flat DB subcommand. */
@Command(
    name = "x-heal-flat-db",
    description = "heal flat db storage",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class HealFlatDbSubCommand implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(HealFlatDbSubCommand.class);

  @SuppressWarnings("UnusedVariable")
  @ParentCommand
  private static StorageSubCommand parentCommand;

  @SuppressWarnings("unused")
  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec; // Picocli injects reference to command spec

  @Override
  public void run() {
    try (final InMemoryTasksPriorityQueues<SnapDataRequest> taskQueue =
        new InMemoryTasksPriorityQueues<>()) {

      final BesuController besuController = parentCommand.parentCommand.buildController();
      final DataStorageConfiguration config = besuController.getDataStorageConfiguration();
      final StorageProvider storageProvider = besuController.getStorageProvider();
      final MutableBlockchain blockchain = besuController.getProtocolContext().getBlockchain();
      final BonsaiWorldStateKeyValueStorage rootWorldStateStorage =
          (BonsaiWorldStateKeyValueStorage) storageProvider.createWorldStateStorage(config);
      final SnapSyncProcessState snapSyncState = new SnapSyncProcessState(new FastSyncState());
      final SnapSyncMetricsManager snapSyncMetricsManager =
          new SnapSyncMetricsManager(
              new NoOpMetricsSystem(), besuController.getProtocolManager().ethContext());

      // init ranges
      snapSyncMetricsManager.initRange(RangeManager.generateAllRanges(16));

      final SnapWorldDownloadState snapWorldDownloadState =
          new SnapWorldDownloadState(
              rootWorldStateStorage,
              new SnapSyncStatePersistenceManager(storageProvider),
              blockchain,
              snapSyncState,
              taskQueue,
              1,
              5000,
              snapSyncMetricsManager,
              Clock.systemUTC());

      snapWorldDownloadState.startFlatDatabaseHeal(blockchain.getChainHeadHeader());

      while (!snapWorldDownloadState.allTasksComplete()) {
        Thread.sleep(1000);
      }

    } catch (final Exception e) {
      LOG.error("Error while healing flat db storage", e);
    }
  }
}
