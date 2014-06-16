/**
 * Copyright 2014 Sunny Gleason and original author or authors
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

import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValueIterator;
import io.kazuki.v0.store.keyvalue.KeyValuePair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryResultsPageImpl<T> implements QueryResultsPage<T> {
  private final List<Key> resultKeys;
  private final List<KeyValuePair<T>> results;
  private final PageToken currToken;
  private final PageToken prevToken;
  private final PageToken nextToken;

  public QueryResultsPageImpl(List<KeyValuePair<T>> results, boolean includesResults) {
    List<Key> newResultKeys = new ArrayList<Key>();
    for (KeyValuePair<T> kv : results) {
      newResultKeys.add(kv.getKey());
    }

    if (includesResults) {
      List<KeyValuePair<T>> newResults = new ArrayList<KeyValuePair<T>>();
      newResults.addAll(results);
      this.results = Collections.unmodifiableList(newResults);
    } else {
      this.results = null;
    }

    this.resultKeys = Collections.unmodifiableList(newResultKeys);

    this.currToken = null;
    this.nextToken = null;
    this.prevToken = null;
  }

  public QueryResultsPageImpl(KeyValueIterable<?> iterable, Long limit, boolean includeResults) {
    List<Key> newResultKeys = new ArrayList<Key>();
    List<KeyValuePair<T>> newResults = new ArrayList<KeyValuePair<T>>();

    if (includeResults) {
      try (KeyValueIterator<KeyValuePair<T>> iter =
          (KeyValueIterator<KeyValuePair<T>>) iterable.iterator()) {
        long found = 0L;

        while (iter.hasNext()) {
          KeyValuePair<T> theNext = iter.next();
          newResultKeys.add(theNext.getKey());
          newResults.add(theNext);

          found += 1;
          if (limit != null && limit > 0 && found >= limit) {
            break;
          }
        }
      }
    } else {
      try (KeyValueIterator<Key> iter = (KeyValueIterator<Key>) iterable.iterator()) {
        long found = 0L;

        while (iter.hasNext()) {
          Key theNext = iter.next();
          newResultKeys.add(theNext);

          found += 1;
          if (limit != null && limit > 0 && found >= limit) {
            break;
          }
        }
      }
    }

    this.resultKeys = Collections.unmodifiableList(newResultKeys);
    this.results = includeResults ? Collections.unmodifiableList(newResults) : null;

    this.currToken = null;
    this.nextToken = null;
    this.prevToken = null;
  }

  @Override
  public PageToken getCurrentPageToken() {
    return currToken;
  }

  @Override
  public List<Key> getResultKeys() {
    return resultKeys;
  }

  @Override
  public List<KeyValuePair<T>> getResultList() {
    return results;
  }

  @Override
  public boolean hasResults() {
    return results != null;
  }

  @Override
  public boolean hasPrevious() {
    return prevToken != null;
  }

  @Override
  public PageToken getPreviousPageToken() {
    return prevToken;
  }

  @Override
  public boolean hasNext() {
    return nextToken != null;
  }

  @Override
  public PageToken getNextPageToken() {
    return nextToken;
  }
}
