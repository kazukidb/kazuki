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
import io.kazuki.v0.store.index.query.QueryTerm;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.management.KazukiComponent;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

public interface SecondaryIndexStore extends KazukiComponent<SecondaryIndexStore> {
  <T> QueryResultsPage<T> queryWithPagination(String type, Class<T> clazz, String indexName,
      String queryString, SortDirection sortDirection, @Nullable Boolean loadResults,
      @Nullable PageToken token, @Nullable Long limit);

  <T> QueryResultsPage<T> queryWithPagination(String type, Class<T> clazz, String indexName,
      List<QueryTerm> query, SortDirection sortDirection, @Nullable Boolean loadResults,
      @Nullable PageToken token, @Nullable Long limit);

  <T> KeyValueIterable<Key> queryWithoutPagination(String type, Class<T> clazz, String indexName,
      String queryString, SortDirection sortDirection, @Nullable Long offset, @Nullable Long limit);

  <T> KeyValueIterable<Key> queryWithoutPagination(String type, Class<T> clazz, String indexName,
      List<QueryTerm> query, SortDirection sortDirection, @Nullable Long offset,
      @Nullable Long limit);

  Map<UniqueEntityDescription, Key> multiRetrieveUniqueKeys(
      Collection<UniqueEntityDescription> entityDefinitions);

  Map<UniqueEntityDescription, Object> multiRetrieveUniqueEntities(
      Collection<UniqueEntityDescription> entityDefinitions);
}
