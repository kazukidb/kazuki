/**
 * Copyright 2014 Sunny Gleason
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kazuki.v0.store.index;

import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValueIterator;
import io.kazuki.v0.store.keyvalue.KeyValuePair;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

public class FilteredKeyValueIterable<U> implements KeyValueIterable<U> {
  private final KeyValueIterable<?> inner;
  private final Predicate<Object> filter;
  private final Function<KeyValuePair<?>, U> transform;
  private final Long offset;
  private final Long limit;

  public FilteredKeyValueIterable(KeyValueIterable<?> keyValueIterable, Predicate<Object> filter,
      Function<KeyValuePair<?>, U> transform, @Nullable final Long offset,
      @Nullable final Long limit) {
    this.inner = keyValueIterable;
    this.filter = filter;
    this.transform = transform;
    this.offset = offset;
    this.limit = limit;
  }

  @Override
  public KeyValueIterator<U> iterator() {
    return new FilteredKeyValueIterator<U>(inner.iterator(), filter, transform, offset, limit);
  }

  @Override
  public void close() {
    this.inner.close();
  }

  public static class FilteredKeyValueIterator<U> implements KeyValueIterator<U> {
    private final KeyValueIterator<?> innerIter;
    private final Predicate<Object> innerFilter;
    private final Function<KeyValuePair<?>, U> innerTransform;
    private final Long offset;
    private final AtomicLong toReturn;

    private volatile KeyValuePair<?> nextMatch = null;

    public FilteredKeyValueIterator(KeyValueIterator<?> innerIter, Predicate<Object> innerFilter,
        Function<KeyValuePair<?>, U> innerTransform, @Nullable Long offset, @Nullable Long limit) {
      this.innerIter = innerIter;
      this.innerFilter = innerFilter;
      this.innerTransform = innerTransform;
      this.offset = offset == null ? 0L : offset;
      this.toReturn = new AtomicLong(limit == null ? -1L : limit);

      for (long i = 0; i <= this.offset; i++) {
        this.nextMatch = (KeyValuePair<?>) advance();
        if (this.nextMatch == null) {
          break;
        }
      }
    }

    @Override
    public synchronized boolean hasNext() {
      Long remaining = toReturn.get();

      return nextMatch != null && (remaining == -1L || remaining > 0);
    }

    @Override
    public synchronized U next() {
      Preconditions.checkNotNull(nextMatch, "next");

      KeyValuePair<?> result = nextMatch;
      Long remaining = toReturn.get();

      if (remaining == 0L) {
        nextMatch = null;

        return null;
      } else if (remaining == -1L) {
        nextMatch = advance();
      } else if (remaining > 0L) {
        toReturn.decrementAndGet();
      }

      return innerTransform.apply(result);
    }

    @Override
    public synchronized void remove() {
      innerIter.remove();
    }

    @Override
    public synchronized void close() {
      innerIter.close();
    }

    private KeyValuePair<?> advance() {
      while (innerIter.hasNext()) {
        KeyValuePair<?> nextOne = (KeyValuePair<?>) innerIter.next();
        if (innerFilter.apply(nextOne.getValue())) {
          return nextOne;
        }
      }

      return null;
    }
  }
}
