/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.plugin.services.storage.benchmark;

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.OptimisticRocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdbffm.segmented.RocksDBFfmColumnarKeyValueStorage;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * JMH benchmarks comparing read throughput of the rocksdbjni and rocksdbffm storage plugins using
 * Bonsai-shaped workloads. Populated with {@code NUM_ENTRIES} entries of 32-byte keys and 128-byte
 * values (approximate size of a Bonsai trie node), all stored in {@code TRIE_BRANCH_STORAGE}.
 *
 * <p>Run A/B comparison: {@code ./gradlew :plugins:storage-benchmarks:jmh
 * -Pincludes=StorageReadBenchmark -PgcProfiler=true}
 */
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 2, jvmArgs = {"--enable-native-access=ALL-UNNAMED"})
public class StorageReadBenchmark {

	private static final int NUM_ENTRIES = 100_000;
	private static final int KEY_SIZE = 32;
	private static final int VALUE_SIZE = 128;
	private static final SegmentIdentifier DEFAULT_SEGMENT = KeyValueSegmentIdentifier.DEFAULT;
	private static final SegmentIdentifier SEGMENT = KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
	private static final int BATCH_SIZE = 100;

	/** Selects which storage plugin to benchmark. */
	@Param({"rocksdb", "rocksdb-ffm"})
	public String plugin;

	private SegmentedKeyValueStorage storage;
	private byte[][] keys;
	private Path tempDir;

	/** Creates and populates the storage with {@code NUM_ENTRIES} entries. */
	@Setup(Level.Trial)
	public void setup() throws Exception {
		tempDir = Files.createTempDirectory("storage-bench");
		storage = buildStorage(plugin, tempDir);

		keys = new byte[NUM_ENTRIES][];
		final Random rng = new Random(0xdeadbeefL);
		var txn = storage.startTransaction();
		for (int i = 0; i < NUM_ENTRIES; i++) {
			keys[i] = new byte[KEY_SIZE];
			rng.nextBytes(keys[i]);
			final byte[] value = new byte[VALUE_SIZE];
			rng.nextBytes(value);
			txn.put(SEGMENT, keys[i], value);
			if (i % BATCH_SIZE == BATCH_SIZE - 1) {
				txn.commit();
				txn = storage.startTransaction();
			}
		}
		txn.commit();
	}

	private static SegmentedKeyValueStorage buildStorage(final String pluginName, final Path dir)
			throws Exception {
		return switch (pluginName) {
			case "rocksdb" -> new OptimisticRocksDBColumnarKeyValueStorage(
					new RocksDBConfigurationBuilder()
							.databaseDir(dir)
							.cacheCapacity(256L * 1024 * 1024)
							.build(),
					List.of(DEFAULT_SEGMENT, SEGMENT),
					List.of(),
					new NoOpMetricsSystem(),
					RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
			case "rocksdb-ffm" -> new RocksDBFfmColumnarKeyValueStorage(
					dir,
					List.of(DEFAULT_SEGMENT, SEGMENT),
					new RocksDBFactoryConfiguration(
							1024,
							4,
							256L * 1024 * 1024,
							false,
							false,
							false,
							Optional.empty(),
							Optional.empty()),
					new NoOpMetricsSystem());
			default -> throw new IllegalArgumentException("Unknown plugin: " + pluginName);
		};
	}

	/** Closes the storage and removes the temp directory. */
	@TearDown(Level.Trial)
	public void tearDown() throws IOException {
		try {
			storage.close();
		} catch (final Exception e) {
			throw new IOException("storage close failed", e);
		}
		try (var stream = Files.walk(tempDir)) {
			stream.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
		}
	}

	/**
	 * Hot read — always fetches {@code keys[0]}, which will be resident in the block cache after the
	 * first access. Measures the minimum achievable JNI/FFM overhead + RocksDB in-cache lookup cost.
	 *
	 * @return the value wrapped in Optional
	 */
	@Benchmark
	@Threads(1)
	public Optional<byte[]> getHotSingleThread() {
		return storage.get(SEGMENT, keys[0]);
	}

	/**
	 * Hot read under concurrency — same key fetched by multiple threads.
	 *
	 * @return the value wrapped in Optional
	 */
	@Benchmark
	@Threads(8)
	public Optional<byte[]> getHotMultiThread() {
		return storage.get(SEGMENT, keys[0]);
	}

	/**
	 * Cold read — fetches a uniformly random key from the corpus. With 100K × 128-byte values (~12
	 * MB) and a 256 MB cache, most reads will hit cache; lower the cache or raise NUM_ENTRIES to
	 * stress misses.
	 *
	 * @return the value wrapped in Optional
	 */
	@Benchmark
	@Threads(1)
	public Optional<byte[]> getColdSingleThread() {
		final int idx = ThreadLocalRandom.current().nextInt(NUM_ENTRIES);
		return storage.get(SEGMENT, keys[idx]);
	}

	/**
	 * Cold read under concurrency.
	 *
	 * @return the value wrapped in Optional
	 */
	@Benchmark
	@Threads(8)
	public Optional<byte[]> getColdMultiThread() {
		final int idx = ThreadLocalRandom.current().nextInt(NUM_ENTRIES);
		return storage.get(SEGMENT, keys[idx]);
	}

	/**
	 * Scoped hot read — wraps the stored bytes as Tuweni {@link Bytes} inside the reader without
	 * going through a heap {@code byte[]} intermediate. Models the scoped-reader path used by Bonsai
	 * trie-node reads.
	 *
	 * @return the value as Bytes, or empty
	 */
	@Benchmark
	@Threads(1)
	public Optional<Bytes> getScopedHotSingleThread() {
		return storage.getWithReader(
				SEGMENT, keys[0], seg -> Bytes.wrap(seg.toArray(ValueLayout.JAVA_BYTE)));
	}

	/**
	 * Scoped cold read under concurrency — exercises the FFM zero-copy path with concurrent callers.
	 *
	 * @return the value as Bytes, or empty
	 */
	@Benchmark
	@Threads(8)
	public Optional<Bytes> getScopedColdMultiThread() {
		final int idx = ThreadLocalRandom.current().nextInt(NUM_ENTRIES);
		return storage.getWithReader(
				SEGMENT, keys[idx], seg -> Bytes.wrap(seg.toArray(ValueLayout.JAVA_BYTE)));
	}

	/**
	 * Nearest-before lookup — models the {@code getNearestBefore} calls that Bonsai uses for archive
	 * reads. Each call opens an iterator, seeks, and closes it.
	 *
	 * @return the nearest key-value pair or empty
	 */
	@Benchmark
	@Threads(1)
	public Optional<SegmentedKeyValueStorage.NearestKeyValue> getNearestBefore() {
		final int idx = ThreadLocalRandom.current().nextInt(NUM_ENTRIES);
		return storage.getNearestBefore(SEGMENT, Bytes.wrap(keys[idx]));
	}

	/**
	 * Write throughput — commits a batch of {@code BATCH_SIZE} puts per invocation, modelling a
	 * Bonsai world-state commit.
	 *
	 * @throws Exception if the transaction fails
	 */
	@Benchmark
	@Threads(1)
	public void batchWrite() throws Exception {
		final var txn = storage.startTransaction();
		final Random rng = ThreadLocalRandom.current();
		for (int i = 0; i < BATCH_SIZE; i++) {
			final byte[] key = new byte[KEY_SIZE];
			final byte[] value = new byte[VALUE_SIZE];
			rng.nextBytes(key);
			rng.nextBytes(value);
			txn.put(SEGMENT, key, value);
		}
		txn.commit();
	}
}
