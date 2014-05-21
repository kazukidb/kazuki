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
package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.store.Key;

import javax.annotation.Nullable;

public interface KeyValueStoreIteration {
  public enum SortDirection {
    ASCENDING, DESCENDING
  };

  <T> KeyValueIterator<T> iterator(String type, Class<T> clazz, SortDirection sortDirection);

  <T> KeyValueIterator<T> iterator(String type, Class<T> clazz, SortDirection sortDirection,
      @Nullable Long offset, @Nullable Long limit);

  <T> KeyValueIterable<Key> keys(String type, Class<T> clazz, SortDirection sortDirection);

  <T> KeyValueIterable<Key> keys(String type, Class<T> clazz, SortDirection sortDirection,
      @Nullable Long offset, @Nullable Long limit);

  <T> KeyValueIterable<T> values(String type, Class<T> clazz, SortDirection sortDirection);

  <T> KeyValueIterable<T> values(String type, Class<T> clazz, SortDirection sortDirection,
      @Nullable Long offset, @Nullable Long limit);

  <T> KeyValueIterable<KeyValuePair<T>> entries(String type, Class<T> clazz,
      SortDirection sortDirection);

  <T> KeyValueIterable<KeyValuePair<T>> entries(String type, Class<T> clazz,
      SortDirection sortDirection, @Nullable Long offset, @Nullable Long limit);
}
