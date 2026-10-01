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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

/**
 * Counts what happens to each preimage that block processing could have written, so the effect of
 * the write filters can be observed on a live node.
 */
public class PreimageWriteMetrics {

  /** What happened to a candidate preimage. */
  public enum Outcome {
    /** Staged for writing. */
    WRITTEN("written"),
    /** Skipped because the key already existed in state, so its preimage was already written. */
    SKIPPED_EXISTING("skipped_existing"),
    /** Skipped because the preimage is resolved from {@code KnownSlotPreimages}. */
    SKIPPED_KNOWN("skipped_known");

    private final String label;

    Outcome(final String label) {
      this.label = label;
    }
  }

  /** Metrics that record nothing. */
  public static final PreimageWriteMetrics NO_OP = new PreimageWriteMetrics(null);

  private final LabelledMetric<Counter> counter;

  public PreimageWriteMetrics(final MetricsSystem metricsSystem) {
    this.counter =
        metricsSystem == null
            ? null
            : metricsSystem.createLabelledCounter(
                BesuMetricCategory.BLOCKCHAIN,
                "hash_preimages_total",
                "Hash preimage candidates seen during block processing, by outcome",
                "type",
                "outcome");
  }

  /**
   * Record candidates of a type.
   *
   * @param type "account" or "slot"
   * @param outcome what happened to them
   * @param count how many
   */
  public void record(final String type, final Outcome outcome, final long count) {
    if (counter != null && count > 0) {
      counter.labels(type, outcome.label).inc(count);
    }
  }
}
