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
package io.kazuki.v0.store.journal;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.management.KazukiComponent;
import io.kazuki.v0.store.schema.TypeValidation;

import javax.annotation.Nullable;

public interface JournalStore extends KazukiComponent<JournalStore> {
  void initialize() throws KazukiException;

  <T> Key append(String type, Class<T> clazz, T inValue, TypeValidation typeSafety)
      throws KazukiException;

  <T> KeyValueIterable<KeyValuePair<T>> entriesRelative(String type, Class<T> clazz,
      SortDirection sortDirection, Long offset, Long limit) throws KazukiException;

  <T> KeyValueIterable<KeyValuePair<T>> entriesAbsolute(String type, Class<T> clazz,
      SortDirection sortDirection, Long offset, Long limit) throws KazukiException;

  KeyValueIterable<PartitionInfoSnapshot> getAllPartitions() throws KazukiException;

  KeyValueIterable<PartitionInfoSnapshot> getAllPartitions(SortDirection sortDirection)
      throws KazukiException;

  @Nullable
  PartitionInfo getActivePartition() throws KazukiException;

  boolean closeActivePartition() throws KazukiException;

  boolean dropPartition(String partitionId) throws KazukiException;

  void clear() throws KazukiException;

  Long approximateSize() throws KazukiException;
}
