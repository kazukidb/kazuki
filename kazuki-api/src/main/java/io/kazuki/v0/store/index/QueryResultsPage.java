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
import io.kazuki.v0.store.keyvalue.KeyValuePair;

import java.util.List;

import javax.annotation.Nullable;

/**
 * Encapsulates a page of query results, including prev/current/next tokens. A page may
 * or may not contain results, depending on whether the query requested results retrieval. 
 */
public interface QueryResultsPage<T> {
  /**
   * Returns the list of Key instances for this page of results
   */
  List<Key> getResultKeys();

  /**
   * Returns true if this page contains result entities
   */
  boolean hasResults();

  /**
   * Returns the list of KeyValuePairs for this page of results, or null if result entities were not requested
   */
  @Nullable List<KeyValuePair<T>> getResultList();

  /**
   * Returns true if there is a next page for this query
   */
  boolean hasNext();

  /**
   * Returns the PageToken representing the next page
   */
  @Nullable PageToken getNextPageToken();

  /**
   * Returns true if there is a previous page for this query
   */
  boolean hasPrevious();

  /**
   * Returns the PageToken representing the previous page
   */
  @Nullable PageToken getPreviousPageToken();

  /**
   * Returns the PageToken representing this current page
   */
  PageToken getCurrentPageToken();
}
