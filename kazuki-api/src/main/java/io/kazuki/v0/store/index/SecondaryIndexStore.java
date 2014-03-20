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

import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.index.query.QueryTerm;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;

import java.util.List;

import javax.annotation.Nullable;

public interface SecondaryIndexStore {
  <T> QueryResultsPage<T> queryWithPagination(String type, Class<T> clazz, String indexName,
      String queryString, @Nullable Boolean loadResults, @Nullable PageToken token,
      @Nullable Long limit);

  <T> QueryResultsPage<T> queryWithPagination(String type, Class<T> clazz, String indexName,
      List<QueryTerm> query, @Nullable Boolean loadResults, @Nullable PageToken token,
      @Nullable Long limit);

  <T> KeyValueIterable<Key> queryWithoutPagination(String type, Class<T> clazz, String indexName,
      String queryString, @Nullable Long offset, @Nullable Long limit);

  <T> KeyValueIterable<Key> queryWithoutPagination(String type, Class<T> clazz, String indexName,
      List<QueryTerm> query, @Nullable Long offset, @Nullable Long limit);
}
