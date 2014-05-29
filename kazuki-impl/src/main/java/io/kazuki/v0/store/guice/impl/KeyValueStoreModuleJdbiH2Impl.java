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

import io.kazuki.v0.internal.availability.AvailabilityManager;
import io.kazuki.v0.internal.helper.H2TypeHelper;
import io.kazuki.v0.internal.helper.LockManager;
import io.kazuki.v0.internal.helper.SqlTypeHelper;
import io.kazuki.v0.store.index.SecondaryIndexStore;
import io.kazuki.v0.store.index.SecondaryIndexStoreProvider;
import io.kazuki.v0.store.index.SecondaryIndexTableHelper;
import io.kazuki.v0.store.jdbi.IdbiProvider;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.keyvalue.KeyValueStoreConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueStoreJdbiH2Impl;
import io.kazuki.v0.store.keyvalue.KeyValueStoreRegistration;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.SchemaStoreImpl;
import io.kazuki.v0.store.schema.SchemaStoreRegistration;
import io.kazuki.v0.store.sequence.SequenceService;

import javax.sql.DataSource;

import org.skife.jdbi.v2.IDBI;

import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

public class KeyValueStoreModuleJdbiH2Impl extends PrivateModule {
  protected final String name;
  protected final Key<Lifecycle> lifecycleKey;
  protected final Key<LockManager> lockManagerKey;
  protected final Key<DataSource> dataSourceKey;
  protected final Key<SequenceService> sequenceServiceKey;

  public KeyValueStoreModuleJdbiH2Impl(String name, Key<Lifecycle> lifecycleKey,
      Key<LockManager> lockManagerKey, Key<DataSource> dataSourceKey,
      Key<SequenceService> sequenceServiceKey) {
    this.name = name;
    this.lifecycleKey = lifecycleKey;
    this.lockManagerKey = lockManagerKey;
    this.dataSourceKey = dataSourceKey;
    this.sequenceServiceKey = sequenceServiceKey;
  }

  protected void includeInternal() {}

  protected void includeExposures() {}

  @Override
  public void configure() {
    binder().requireExplicitBindings();

    bind(Lifecycle.class).to(lifecycleKey);

    bind(KeyValueStoreConfiguration.class).to(
        Key.get(KeyValueStoreConfiguration.class, Names.named(name)));

    Provider<DataSource> dsProvider = binder().getProvider(dataSourceKey);

    bind(DataSource.class).to(dataSourceKey);
    bind(IDBI.class).toProvider(new IdbiProvider(KeyValueStore.class, dsProvider)).in(
        Scopes.SINGLETON);

    bind(LockManager.class).to(lockManagerKey);
    bind(SequenceService.class).to(sequenceServiceKey);

    bind(SqlTypeHelper.class).to(H2TypeHelper.class).in(Scopes.SINGLETON);
    bind(AvailabilityManager.class).in(Scopes.SINGLETON);


    bind(KeyValueStoreJdbiH2Impl.class).in(Scopes.SINGLETON);
    bind(KeyValueStore.class).to(KeyValueStoreJdbiH2Impl.class).in(Scopes.SINGLETON);
    bind(KeyValueStoreRegistration.class).to(Key.get(KeyValueStoreJdbiH2Impl.class)).in(
        Scopes.SINGLETON);
    bind(KeyValueStore.class).annotatedWith(Names.named(name))
        .toProvider(binder().getProvider(Key.get(KeyValueStore.class))).in(Scopes.SINGLETON);

    bind(SchemaStoreImpl.class).in(Scopes.SINGLETON);
    bind(SchemaStore.class).to(SchemaStoreImpl.class).in(Scopes.SINGLETON);
    bind(SchemaStoreRegistration.class).to(Key.get(SchemaStoreImpl.class)).in(Scopes.SINGLETON);
    bind(SchemaStore.class).annotatedWith(Names.named(name)).to(Key.get(SchemaStore.class));

    bind(SecondaryIndexTableHelper.class).in(Scopes.SINGLETON);
    bind(SecondaryIndexStore.class).toProvider(SecondaryIndexStoreProvider.class).in(
        Scopes.SINGLETON);
    bind(SecondaryIndexStore.class).annotatedWith(Names.named(name)).to(
        Key.get(SecondaryIndexStore.class));

    includeInternal();

    expose(Key.get(SchemaStore.class, Names.named(name)));
    expose(Key.get(KeyValueStore.class, Names.named(name)));
    expose(Key.get(SecondaryIndexStore.class, Names.named(name)));

    includeExposures();
  }
}
