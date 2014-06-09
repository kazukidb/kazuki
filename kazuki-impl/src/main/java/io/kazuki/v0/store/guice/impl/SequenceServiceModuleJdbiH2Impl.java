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
import io.kazuki.v0.store.jdbi.IdbiProvider;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.management.ComponentRegistrar;
import io.kazuki.v0.store.management.KazukiComponent;
import io.kazuki.v0.store.sequence.SequenceHelper;
import io.kazuki.v0.store.sequence.SequenceService;
import io.kazuki.v0.store.sequence.SequenceServiceConfiguration;
import io.kazuki.v0.store.sequence.SequenceServiceJdbiImpl;

import javax.sql.DataSource;

import org.skife.jdbi.v2.IDBI;

import com.google.common.base.Preconditions;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

public class SequenceServiceModuleJdbiH2Impl extends PrivateModule {
  private final String name;
  private final Key<ComponentRegistrar> registrarKey;
  private final Key<Lifecycle> lifecycleKey;
  private final Key<DataSource> dataSourceKey;
  private final Key<LockManager> lockManagerKey;

  public SequenceServiceModuleJdbiH2Impl(String name, Key<ComponentRegistrar> registrarKey,
      Key<Lifecycle> lifecycleKey, Key<DataSource> dataSourceKey, Key<LockManager> lockManagerKey) {
    Preconditions.checkNotNull(name, "name");
    Preconditions.checkNotNull(registrarKey, "registrarKey");
    Preconditions.checkNotNull(lifecycleKey, "lifecycleKey");
    Preconditions.checkNotNull(dataSourceKey, "dataSourceKey");
    Preconditions.checkNotNull(lockManagerKey, "lockManagerKey");

    this.name = name;
    this.registrarKey = registrarKey;
    this.lifecycleKey = lifecycleKey;
    this.dataSourceKey = dataSourceKey;
    this.lockManagerKey = lockManagerKey;
  }

  @Override
  protected void configure() {
    // TODO: re-enable ASAP
    // binder().requireExplicitBindings();

    bind(ComponentRegistrar.class).to(registrarKey);
    bind(Lifecycle.class).to(lifecycleKey);

    Provider<DataSource> provider = binder().getProvider(dataSourceKey);

    Key<KazukiComponent<DataSource>> kcKey =
        Key.get(new TypeLiteral<KazukiComponent<DataSource>>() {});
    Key<KazukiComponent<DataSource>> kcKeyNamed =
        Key.get(new TypeLiteral<KazukiComponent<DataSource>>() {}, Names.named(name));
    bind(kcKey).to(kcKeyNamed);

    bind(IDBI.class).toProvider(new IdbiProvider(SequenceService.class, provider)).in(
        Scopes.SINGLETON);
    bind(SqlTypeHelper.class).to(H2TypeHelper.class).in(Scopes.SINGLETON);
    bind(SequenceServiceConfiguration.class).to(
        Key.get(SequenceServiceConfiguration.class, Names.named(name)));

    bind(SequenceHelper.class).in(Scopes.SINGLETON);
    bind(AvailabilityManager.class).in(Scopes.SINGLETON);
    bind(LockManager.class).to(lockManagerKey);

    bind(SequenceService.class).annotatedWith(Names.named(name)).to(SequenceServiceJdbiImpl.class)
        .in(Scopes.SINGLETON);

    expose(SequenceService.class).annotatedWith(Names.named(name));
  }
}
