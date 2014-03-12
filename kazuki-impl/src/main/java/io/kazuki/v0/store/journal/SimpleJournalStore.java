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
package io.kazuki.v0.store.journal;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.keyvalue.KeyValueStoreConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.schema.TypeValidation;

import javax.inject.Inject;

import com.google.common.base.Preconditions;


public class SimpleJournalStore implements JournalStore {
  private final KeyValueStore store;
  private final String dataType;

  @Inject
  public SimpleJournalStore(KeyValueStore store, KeyValueStoreConfiguration config) {
    Preconditions.checkNotNull(config.getDataType(), "dataType");

    this.store = store;
    this.dataType = config.getDataType();
  }

  @Override
  public void initialize() throws KazukiException {}

  @Override
  public <T> Key append(String type, Class<T> clazz, T inValue, TypeValidation typeSafety)
      throws KazukiException {
    return store.create(type, clazz, inValue, typeSafety);
  }

  @Override
  public <T> KeyValueIterable<KeyValuePair<T>> entriesAbsolute(String type, Class<T> clazz,
      SortDirection sortDirection, Long offset, Long limit) throws KazukiException {
    return store.iterators().entries(type, clazz, sortDirection, offset, limit);
  }

  @Override
  public <T> KeyValueIterable<KeyValuePair<T>> entriesRelative(String type, Class<T> clazz,
      SortDirection sortDirection, Long offset, Long limit) throws KazukiException {
    return entriesAbsolute(type, clazz, sortDirection, offset, limit);
  }

  @Override
  public Long approximateSize() throws KazukiException {
    return store.approximateSize(this.dataType);
  }

  @Override
  public void clear() throws KazukiException {
    store.clear(false, false);
  }

  @Override
  public boolean closeActivePartition() {
    throw new UnsupportedOperationException("closeActivePartition() not supported");
  }

  @Override
  public boolean dropPartition(String partitionId) {
    throw new UnsupportedOperationException("dropPartition() not yet supported");
  }

  @Override
  public PartitionInfo getActivePartition() {
    throw new UnsupportedOperationException("getActivePartition() not yet supported");
  }

  @Override
  public KeyValueIterable<PartitionInfoSnapshot> getAllPartitions() {
    throw new UnsupportedOperationException("getAllPartitions() not yet supported");
  }

  @Override
  public KeyValueIterable<PartitionInfoSnapshot> getAllPartitions(SortDirection sortDirection) {
    throw new UnsupportedOperationException("getAllPartitions() not yet supported");
  }
}
