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
package io.kazuki.v0.store.sequence;

import io.kazuki.v0.internal.availability.AvailabilityManager;
import io.kazuki.v0.internal.helper.H2TypeHelper;
import io.kazuki.v0.internal.helper.LockManager;
import io.kazuki.v0.internal.helper.LockManagerImpl;
import io.kazuki.v0.internal.helper.SqlTypeHelper;
import io.kazuki.v0.store.config.ConfigurationProvider;
import io.kazuki.v0.store.jdbi.IdbiProvider;
import io.kazuki.v0.store.lifecycle.Lifecycle;

import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.skife.jdbi.v2.IDBI;

import com.google.common.base.Preconditions;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

public class H2SequenceServiceModule extends PrivateModule {
  private final String name;
  private final String propertiesPath;
  private final AtomicReference<SequenceServiceConfiguration> config;

  public H2SequenceServiceModule(String name, @Nullable String propertiesPath) {
    Preconditions.checkNotNull(name, "name");

    this.name = name;
    this.propertiesPath = propertiesPath;
    this.config = new AtomicReference<SequenceServiceConfiguration>();
  }

  public H2SequenceServiceModule withConfiguration(SequenceServiceConfiguration config) {
    this.config.set(config);

    return this;
  }

  @Override
  protected void configure() {
    bind(Lifecycle.class).to(Key.get(Lifecycle.class, Names.named(name))).in(Scopes.SINGLETON);

    Provider<DataSource> provider =
        binder().getProvider(Key.get(DataSource.class, Names.named(name)));

    bind(IDBI.class).toProvider(new IdbiProvider(SequenceService.class, provider)).in(
        Scopes.SINGLETON);
    bind(SqlTypeHelper.class).to(H2TypeHelper.class).in(Scopes.SINGLETON);

    SequenceServiceConfiguration theConfig = config.get();

    if (theConfig != null) {
      bind(SequenceServiceConfiguration.class).toInstance(theConfig);
    } else if (propertiesPath != null) {
      bind(SequenceServiceConfiguration.class).toProvider(
          new ConfigurationProvider<SequenceServiceConfiguration>(name,
              SequenceServiceConfiguration.class, propertiesPath, true));
    } else {
      bind(SequenceServiceConfiguration.class).to(
          Key.get(SequenceServiceConfiguration.class, Names.named(name)));
    }

    bind(SequenceHelper.class).in(Scopes.SINGLETON);
    bind(AvailabilityManager.class).in(Scopes.SINGLETON);

    bind(LockManager.class).annotatedWith(Names.named(name)).to(LockManagerImpl.class)
        .in(Scopes.SINGLETON);
    bind(LockManager.class).to(Key.get(LockManager.class, Names.named(name)));
    expose(LockManager.class).annotatedWith(Names.named(name));

    bind(SequenceService.class).annotatedWith(Names.named(name)).to(SequenceServiceJdbiImpl.class)
        .in(Scopes.SINGLETON);

    expose(SequenceService.class).annotatedWith(Names.named(name));
  }
}
