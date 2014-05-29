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
package io.kazuki.v0.store.guice.impl;

import io.kazuki.v0.internal.helper.LockManager;
import io.kazuki.v0.store.journal.JournalStore;
import io.kazuki.v0.store.journal.PartitionedJournalStore;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.sequence.SequenceService;

import javax.sql.DataSource;

import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.name.Names;


public class JournalStoreModulePartitionedImpl extends KeyValueStoreModuleJdbiH2Impl {
  public JournalStoreModulePartitionedImpl(String name, Key<Lifecycle> lifecycleKey,
      Key<LockManager> lockManagerKey, Key<DataSource> dataSourceKey,
      Key<SequenceService> sequenceServiceKey) {
    super(name, lifecycleKey, lockManagerKey, dataSourceKey, sequenceServiceKey);
  }

  protected void includeInternal() {
    bind(JournalStore.class).annotatedWith(Names.named(name)).to(PartitionedJournalStore.class)
        .in(Scopes.SINGLETON);
  }

  @Override
  protected void includeExposures() {
    expose(Key.get(JournalStore.class, Names.named(name)));
  }
}
