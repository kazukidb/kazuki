/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.kazuki.v0.store.index;

import io.kazuki.v0.internal.helper.EncodingHelper;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.index.query.QueryEvaluator;
import io.kazuki.v0.store.index.query.QueryHelper;
import io.kazuki.v0.store.index.query.QueryTerm;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;

public class SecondaryIndexBruteForceImpl implements SecondaryIndexStore {
  private final KeyValueStore kvStore;

  @Inject
  public SecondaryIndexBruteForceImpl(KeyValueStore kvStore) {
    this.kvStore = kvStore;
  }

  @Override
  public Map<UniqueEntityDescription<?>, ?> multiRetrieveUniqueEntities(
      Collection<UniqueEntityDescription<?>> entityDefinitions) {
    throw new UnsupportedOperationException("not supported - yet");
  }

  @Override
  public Map<UniqueEntityDescription<?>, Key> multiRetrieveUniqueKeys(
      Collection<UniqueEntityDescription<?>> entityDefinitions) {
    throw new UnsupportedOperationException("not supported - yet");
  }

  @Override
  public <T> KeyValueIterable<Key> queryWithoutPagination(final String type, final Class<T> clazz,
      final String indexName, final List<QueryTerm> query, final SortDirection sortDirection,
      final Long offset, final Long limit) {
    final QueryEvaluator eval = new QueryEvaluator();

    return new FilteredKeyValueIterable<Key>(kvStore.iterators().entries(type, LinkedHashMap.class,
        sortDirection), new Predicate<Object>() {
      @SuppressWarnings("unchecked")
      @Override
      public boolean apply(Object instance) {
        return eval.matches((LinkedHashMap<String, Object>) instance, query);
      }
    }, new Function<KeyValuePair<?>, Key>() {
      @Override
      public Key apply(KeyValuePair<?> instance) {
        return instance.getKey();
      }
    }, offset, limit);
  }

  @Override
  public <T> KeyValueIterable<Key> queryWithoutPagination(final String type, final Class<T> clazz,
      final String indexName, final String queryString, final SortDirection sortDirection,
      final Long offset, final Long limit) {
    Preconditions.checkNotNull(queryString, "query");

    return queryWithoutPagination(type, clazz, indexName, QueryHelper.parseQuery(queryString),
        sortDirection, offset, limit);
  }

  @Override
  public <T> QueryResultsPage<T> queryWithPagination(final String type, final Class<T> clazz,
      final String indexName, final List<QueryTerm> query, final SortDirection sortDirection,
      final Boolean loadResults, final PageToken token, final Long limit) {
    throw new UnsupportedOperationException("not supported - yet");

    /*
    if (loadResults != null && loadResults) {
      final QueryEvaluator eval = new QueryEvaluator();

      return new QueryResultsPageImpl<T>(new FilteredKeyValueIterable<T>(kvStore.iterators()
          .entries(type, LinkedHashMap.class, sortDirection), new Predicate<Object>() {
        @SuppressWarnings("unchecked")
        @Override
        public boolean apply(Object instance) {
          return eval.matches((LinkedHashMap<String, Object>) instance, query);
        }
      }, new Function<KeyValuePair<?>, T>() {
        @SuppressWarnings("unchecked")
        @Override
        public T apply(KeyValuePair<?> instance) {
          try {
            return (T) EncodingHelper.asValue((LinkedHashMap<String, Object>) instance.getValue(),
                clazz);
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }, 0L, limit), limit, true);
    }

    return new QueryResultsPageImpl<T>(queryWithoutPagination(type, clazz, indexName, query,
        sortDirection, 0L, limit), limit, false);
    */
  }

  @Override
  public <T> QueryResultsPage<T> queryWithPagination(String type, Class<T> clazz, String indexName,
      String queryString, SortDirection sortDirection, Boolean loadResults, PageToken token,
      Long limit) {
    throw new UnsupportedOperationException("not supported - yet");
    /*
    return queryWithPagination(type, clazz, indexName, QueryHelper.parseQuery(queryString),
        sortDirection, loadResults, token, limit);
    */
  }
}
