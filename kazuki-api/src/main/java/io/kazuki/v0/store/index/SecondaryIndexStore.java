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

/**
 * Interface for range-based Secondary Indexes. This type of secondary index roughly corresponds to
 * efficient B-Tree usage (although implementations do not necessarily use B-Trees). A Secondary Index
 * is modeled as a tree of non-nullable columns with left-to-right ordering. Queries over the secondary
 * index must specify columns from left to right (this is considered efficient because it avoids long
 * scans).  
 */
public interface SecondaryIndexStore extends KazukiComponent<SecondaryIndexStore> {
  /**
   * Paginated query access into a secondary index using the Kazuki Query Language.
   * 
   * @param type String containing the Kazuki type tag of items to query
   * @param clazz Class instance representing the type of returned results
   * @param indexName String name of the index
   * @param queryString String containing the search query (not necessarily in left-to-right order)
   * @param sortDirection SortDirection specifying sort order ascending / descending
   * @param loadResults boolean true if results should be loaded (as opposed to just keys)
   * @param token PageToken containing the offset of the page to load
   * @param limit Long maximum number of results to retrieve in this page
   * 
   * @return QueryResultsPage of results
   */
  <T> QueryResultsPage<T> queryWithPagination(String type, Class<T> clazz, String indexName,
      String queryString, SortDirection sortDirection, @Nullable Boolean loadResults,
      @Nullable PageToken token, @Nullable Long limit);

  /**
   * Paginated query access into a secondary index using an object-oriented list of QueryTerm instances.
   * 
   * @param type String containing the Kazuki type tag of items to query
   * @param clazz Class instance representing the type of returned results
   * @param indexName String name of the index
   * @param query List of QueryTerm instances (not necessarily in left-to-right order - they will be sorted "under the hood")
   * @param sortDirection SortDirection specifying sort order ascending / descending
   * @param loadResults boolean true if results should be loaded (as opposed to just keys)
   * @param token PageToken containing the offset of the page to load
   * @param limit Long maximum number of results to retrieve in this page
   * 
   * @return QueryResultsPage of results
   */
  <T> QueryResultsPage<T> queryWithPagination(String type, Class<T> clazz, String indexName,
      List<QueryTerm> query, SortDirection sortDirection, @Nullable Boolean loadResults,
      @Nullable PageToken token, @Nullable Long limit);

  /**
   * Non-paginated query access into a secondary index using the Kazuki Query Language.
   * 
   * @param type String containing the Kazuki type tag of items to query
   * @param clazz Class instance representing the type of returned results
   * @param indexName String name of the index
   * @param queryString String containing the search query (not necessarily in left-to-right order)
   * @param sortDirection SortDirection specifying sort order ascending / descending
   * @param offset Long offset into the index
   * @param limit Long maximum number of results to retrieve in this page
   * 
   * @return KeyValueIterable of Key instances identifying results
   */
  <T> KeyValueIterable<Key> queryWithoutPagination(String type, Class<T> clazz, String indexName,
      String queryString, SortDirection sortDirection, @Nullable Long offset, @Nullable Long limit);

  /**
   * Non-paginated query access into a secondary index using an object-oriented list of QueryTerm instances.

   * @param type String containing the Kazuki type tag of items to query
   * @param clazz Class instance representing the type of returned results
   * @param indexName String name of the index
   * @param query List of QueryTerm instances (not necessarily in left-to-right order - they will be sorted "under the hood")
   * @param sortDirection SortDirection specifying sort order ascending / descending
   * @param offset Long offset into the index
   * @param limit Long maximum number of results to retrieve in this page
   * 
   * @return KeyValueIterable of Key instances identifying results
   */
  <T> KeyValueIterable<Key> queryWithoutPagination(String type, Class<T> clazz, String indexName,
      List<QueryTerm> query, SortDirection sortDirection, @Nullable Long offset,
      @Nullable Long limit);

  /**
   * Multi-get operation for retrieving entity Keys from one or more unique indexes
   * 
   * @param entityDefinitions Collection of unique entity identifiers
   * 
   * @return Map of UniqueEntityDescription instances to (possibly null) Key identifiers
   */
  Map<UniqueEntityDescription, Key> multiRetrieveUniqueKeys(
      Collection<UniqueEntityDescription> entityDefinitions);

  /**
   * Multi-get operation for retrieving entity values from one or more unique indexes
   * 
   * @param entityDefinitions Collection of unique entity identifiers
   * 
   * @return Map of UniqueEntityDescription instances to (possibly null) entity classes
   */
  Map<UniqueEntityDescription, Object> multiRetrieveUniqueEntities(
      Collection<UniqueEntityDescription> entityDefinitions);
}
