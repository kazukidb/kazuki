/**
 * Copyright 2014 the original author or authors
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

import io.kazuki.v0.internal.availability.AvailabilityManager;
import io.kazuki.v0.internal.helper.H2TypeHelper;
import io.kazuki.v0.internal.helper.LockManager;
import io.kazuki.v0.internal.helper.SqlTypeHelper;
import io.kazuki.v0.store.config.ConfigurationProvider;
import io.kazuki.v0.store.index.SecondaryIndexStore;
import io.kazuki.v0.store.index.SecondaryIndexStoreProvider;
import io.kazuki.v0.store.index.SecondaryIndexTableHelper;
import io.kazuki.v0.store.jdbi.IdbiProvider;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.SchemaStoreImpl;
import io.kazuki.v0.store.schema.SchemaStoreRegistration;
import io.kazuki.v0.store.sequence.SequenceService;

import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.skife.jdbi.v2.IDBI;

import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

public class KeyValueStoreJdbiH2Module extends PrivateModule {
  protected final String name;
  protected final String propertiesPath;
  private final AtomicReference<KeyValueStoreConfiguration> config;

  public KeyValueStoreJdbiH2Module(String name, @Nullable String propertiesPath) {
    this.name = name;
    this.propertiesPath = propertiesPath;
    this.config = new AtomicReference<KeyValueStoreConfiguration>();
  }

  public KeyValueStoreJdbiH2Module withConfiguration(KeyValueStoreConfiguration config) {
    this.config.set(config);

    return this;
  }

  protected void includeInternal() {}

  protected void includeExposures() {}

  @Override
  public void configure() {
    bind(Lifecycle.class).to(Key.get(Lifecycle.class, Names.named(name))).in(Scopes.SINGLETON);

    KeyValueStoreConfiguration theConfig = this.config.get();

    if (theConfig != null) {
      bind(KeyValueStoreConfiguration.class).toInstance(theConfig);
    } else if (propertiesPath != null) {
      bind(KeyValueStoreConfiguration.class).toProvider(
          new ConfigurationProvider<KeyValueStoreConfiguration>(name,
              KeyValueStoreConfiguration.class, propertiesPath, true));
    } else {
      bind(KeyValueStoreConfiguration.class).to(
          Key.get(KeyValueStoreConfiguration.class, Names.named(name)));
    }

    bind(SqlTypeHelper.class).to(H2TypeHelper.class).in(Scopes.SINGLETON);
    bind(AvailabilityManager.class).in(Scopes.SINGLETON);

    Provider<SequenceService> seqProvider =
        binder().getProvider(Key.<SequenceService>get(SequenceService.class, Names.named(name)));
    Provider<DataSource> dsProvider =
        binder().getProvider(Key.get(DataSource.class, Names.named(name)));

    bind(DataSource.class).toProvider(dsProvider);
    bind(IDBI.class).toProvider(new IdbiProvider(KeyValueStore.class, dsProvider)).in(
        Scopes.SINGLETON);

    bind(SequenceService.class).toProvider(seqProvider).in(Scopes.SINGLETON);
    bind(LockManager.class).to(Key.get(LockManager.class, Names.named(name)));

    bind(KeyValueStoreJdbiH2Impl.class).in(Scopes.SINGLETON);
    bind(KeyValueStore.class).to(KeyValueStoreJdbiH2Impl.class).in(Scopes.SINGLETON);
    bind(KeyValueStoreRegistration.class).to(Key.get(KeyValueStoreJdbiH2Impl.class)).in(
        Scopes.SINGLETON);;
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
