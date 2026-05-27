/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.plugin.services.storage.rocksdbffm.segmented;

import static com.google.common.base.Preconditions.checkState;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.github.dfa1.rocksdbffm.RocksIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Iterator adapter over rocksdbffm's {@link RocksIterator}. */
public class RocksDBFfmIterator implements Iterator<Pair<byte[], byte[]>>, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBFfmIterator.class);

  private final RocksIterator iter;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private RocksDBFfmIterator(final RocksIterator iter) {
    this.iter = iter;
  }

  /**
   * Creates a new iterator wrapping the given rocksdbffm iterator.
   *
   * @param iter the underlying rocksdbffm iterator, positioned at the start of the desired range
   * @return the new iterator
   */
  public static RocksDBFfmIterator create(final RocksIterator iter) {
    return new RocksDBFfmIterator(iter);
  }

  @Override
  public boolean hasNext() {
    assertOpen();
    return iter.isValid();
  }

  @Override
  public Pair<byte[], byte[]> next() {
    assertOpen();
    try {
      iter.checkError();
    } catch (final io.github.dfa1.rocksdbffm.RocksDBException e) {
      LOG.error("Iterator error on next()", e);
    }
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final byte[] key = iter.key();
    final byte[] value = iter.value();
    iter.next();
    return Pair.of(key, value);
  }

  /**
   * Advances the iterator and returns only the key.
   *
   * @return the next key
   */
  public byte[] nextKey() {
    assertOpen();
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final byte[] key = iter.key();
    iter.next();
    return key;
  }

  /**
   * Returns a key-value stream backed by this iterator. The stream closes the iterator on
   * termination.
   *
   * @return a sequential stream of key-value pairs
   */
  public Stream<Pair<byte[], byte[]>> toStream() {
    assertOpen();
    final Spliterator<Pair<byte[], byte[]>> spliterator =
        Spliterators.spliteratorUnknownSize(
            this,
            Spliterator.IMMUTABLE
                | Spliterator.DISTINCT
                | Spliterator.NONNULL
                | Spliterator.ORDERED
                | Spliterator.SORTED);
    return StreamSupport.stream(spliterator, false).onClose(this::close);
  }

  /**
   * Returns a keys-only stream backed by this iterator. The stream closes the iterator on
   * termination.
   *
   * @return a sequential stream of keys
   */
  public Stream<byte[]> toStreamKeys() {
    assertOpen();
    final Spliterator<byte[]> spliterator =
        Spliterators.spliteratorUnknownSize(
            new Iterator<>() {
              @Override
              public boolean hasNext() {
                return RocksDBFfmIterator.this.hasNext();
              }

              @Override
              public byte[] next() {
                return RocksDBFfmIterator.this.nextKey();
              }
            },
            Spliterator.IMMUTABLE
                | Spliterator.DISTINCT
                | Spliterator.NONNULL
                | Spliterator.ORDERED
                | Spliterator.SORTED);
    return StreamSupport.stream(spliterator, false).onClose(this::close);
  }

  private void assertOpen() {
    checkState(!closed.get(), "Attempt to read from a closed RocksDBFfmIterator");
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      iter.close();
    }
  }
}
