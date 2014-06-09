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
package io.kazuki.v0.store.guice;

import io.kazuki.v0.internal.helper.LockManager;
import io.kazuki.v0.store.guice.impl.DataSourceModuleH2Impl;
import io.kazuki.v0.store.guice.impl.JournalStoreModulePartitionedImpl;
import io.kazuki.v0.store.guice.impl.KeyValueStoreModuleJdbiH2Impl;
import io.kazuki.v0.store.guice.impl.LifecycleModuleDefaultImpl;
import io.kazuki.v0.store.guice.impl.LockManagerModuleImpl;
import io.kazuki.v0.store.guice.impl.SequenceServiceModuleJdbiH2Impl;
import io.kazuki.v0.store.jdbi.JdbiDataSourceConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueStoreConfiguration;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.management.ComponentRegistrar;
import io.kazuki.v0.store.management.KazukiManager;
import io.kazuki.v0.store.management.impl.KazukiManagerImpl;
import io.kazuki.v0.store.sequence.SequenceService;
import io.kazuki.v0.store.sequence.SequenceServiceConfiguration;

import javax.sql.DataSource;

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

public class KazukiModule extends AbstractModule {
  private final String name;
  private final String managerName;
  private final BindingConfig jdbiConfig;
  private final BindingConfig seqConfig;
  private final BindingConfig kvConfig;
  private final BindingConfig jsConfig;

  private KazukiModule(String name, String managerName, BindingConfig jdbiConfig,
      BindingConfig seqConfig, BindingConfig kvConfig, BindingConfig jsConfig) {
    Preconditions.checkNotNull(name, "name");
    Preconditions.checkNotNull(jdbiConfig, "jdbiConfig");
    Preconditions.checkNotNull(seqConfig, "seqConfig");
    Preconditions.checkArgument(kvConfig == null || jsConfig == null,
        "must specify one KeyValueStore, one JournalStore, or neither");

    this.name = name;
    this.jdbiConfig = jdbiConfig;
    this.seqConfig = seqConfig;
    this.kvConfig = kvConfig;
    this.jsConfig = jsConfig;
    this.managerName = managerName;
  }

  @Override
  protected void configure() {
    // TODO: re-enable ASAP
    // binder().requireExplicitBindings();

    Key<Lifecycle> lifecycleKey = Key.get(Lifecycle.class, Names.named(name));
    Key<LockManager> lockManagerKey = Key.get(LockManager.class, Names.named(name));
    Key<JdbiDataSourceConfiguration> jdbiConfigKey =
        Key.get(JdbiDataSourceConfiguration.class, Names.named(jdbiConfig.getName()));
    Key<DataSource> dataSourceKey = Key.get(DataSource.class, Names.named(jdbiConfig.getName()));
    Key<SequenceService> sequenceServiceKey =
        Key.get(SequenceService.class, Names.named(seqConfig.getName()));

    // bind the manager & component registrar
    Key<KazukiManager> managerKey = null;
    Key<ComponentRegistrar> registrarKey = null;

    if (this.managerName != null) {
      managerKey = Key.get(KazukiManager.class, Names.named(this.managerName));
      registrarKey = Key.get(ComponentRegistrar.class, Names.named(this.managerName));

      bindObject(new BindingConfig(this.managerName, KazukiManager.class, managerKey));
      bindObject(new BindingConfig(this.managerName, ComponentRegistrar.class, registrarKey));
    } else {
      managerKey = Key.get(KazukiManager.class, Names.named(this.name));
      registrarKey = Key.get(ComponentRegistrar.class, Names.named(this.name));

      KazukiManagerImpl manager = new KazukiManagerImpl();

      bindObject(new BindingConfig(this.name, KazukiManager.class, manager));
      bindObject(new BindingConfig(this.name, ComponentRegistrar.class, manager));
    }

    // install Lifecycle
    install(new LifecycleModuleDefaultImpl(name));

    // bind lock manager
    install(new LockManagerModuleImpl(name, registrarKey));

    // bind JDBI config
    bindObject(jdbiConfig);
    install(new DataSourceModuleH2Impl(name, registrarKey, lifecycleKey, jdbiConfigKey));

    // bind SequenceService
    bindObject(seqConfig);
    install(new SequenceServiceModuleJdbiH2Impl(seqConfig.getName(), registrarKey, lifecycleKey,
        dataSourceKey, lockManagerKey));

    // bind KeyValueStore (if applicable)
    if (kvConfig != null) {
      bindObject(kvConfig);
      install(new KeyValueStoreModuleJdbiH2Impl(name, registrarKey, lifecycleKey, lockManagerKey,
          dataSourceKey, sequenceServiceKey));
    }

    // bind JournalStore (if applicable)
    if (jsConfig != null) {
      bindObject(jsConfig);
      install(new JournalStoreModulePartitionedImpl(name, registrarKey, lifecycleKey,
          lockManagerKey, dataSourceKey, sequenceServiceKey));
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected void bindObject(BindingConfig config) {
    Key bindKey = Key.get(config.getClazz(), Names.named(config.getName()));
    Object instance = config.getInstance();

    if (instance instanceof Key) {
      // sweet - it's already bound
    } else if (instance instanceof Provider) {
      bind(bindKey).toProvider((Provider) instance).in(Scopes.SINGLETON);
    } else {
      bind(bindKey).toInstance(instance);
    }
  }

  public static class Builder {
    private final String name;
    private String managerName;
    private BindingConfig jdbiConfig;
    private BindingConfig seqConfig;
    private BindingConfig kvConfig;
    private BindingConfig jsConfig;

    public Builder(String name) {
      this.name = name;
    }

    public Builder withComponentManager(String managerName) {
      this.managerName = managerName;

      return this;
    }

    public Builder withJdbiConfiguration(String name, JdbiDataSourceConfiguration jdbiConfig) {
      this.jdbiConfig = new BindingConfig(name, JdbiDataSourceConfiguration.class, jdbiConfig);

      return this;
    }

    public Builder withJdbiConfiguration(String name,
        Provider<JdbiDataSourceConfiguration> jdbiConfig, boolean exposed) {
      this.jdbiConfig = new BindingConfig(name, JdbiDataSourceConfiguration.class, jdbiConfig);

      return this;
    }

    public Builder withJdbiConfiguration(String name, Key<JdbiDataSourceConfiguration> jdbiConfig) {
      this.jdbiConfig = new BindingConfig(name, JdbiDataSourceConfiguration.class, jdbiConfig);

      return this;
    }

    public Builder withSequenceServiceConfiguration(String name,
        SequenceServiceConfiguration seqConfig) {
      this.seqConfig = new BindingConfig(name, SequenceServiceConfiguration.class, seqConfig);

      return this;
    }

    public Builder withSequenceServiceConfiguration(String name,
        Provider<SequenceServiceConfiguration> seqConfig) {
      this.seqConfig = new BindingConfig(name, SequenceServiceConfiguration.class, seqConfig);

      return this;
    }

    public Builder withSequenceServiceConfiguration(String name,
        Key<SequenceServiceConfiguration> seqConfig) {
      this.seqConfig = new BindingConfig(name, SequenceServiceConfiguration.class, seqConfig);

      return this;
    }

    public Builder withKeyValueStoreConfiguration(String name, KeyValueStoreConfiguration kvConfig) {
      this.kvConfig = new BindingConfig(name, KeyValueStoreConfiguration.class, kvConfig);

      return this;
    }

    public Builder withKeyValueStoreConfiguration(String name,
        Provider<KeyValueStoreConfiguration> kvConfig) {
      this.kvConfig = new BindingConfig(name, KeyValueStoreConfiguration.class, kvConfig);

      return this;
    }

    public Builder withKeyValueStoreConfiguration(String name,
        Key<KeyValueStoreConfiguration> kvConfig) {
      this.kvConfig = new BindingConfig(name, KeyValueStoreConfiguration.class, kvConfig);

      return this;
    }

    public Builder withJournalStoreConfiguration(String name, KeyValueStoreConfiguration jsConfig) {
      this.jsConfig = new BindingConfig(name, KeyValueStoreConfiguration.class, jsConfig);

      return this;
    }

    public Builder withJournalStoreConfiguration(String name,
        Provider<KeyValueStoreConfiguration> jsConfig) {
      this.jsConfig = new BindingConfig(name, KeyValueStoreConfiguration.class, jsConfig);

      return this;
    }

    public Builder withJournalStoreConfiguration(String name,
        Key<KeyValueStoreConfiguration> jsConfig) {
      this.jsConfig = new BindingConfig(name, KeyValueStoreConfiguration.class, jsConfig);

      return this;
    }

    public KazukiModule build() {
      return new KazukiModule(this.name, this.managerName, this.jdbiConfig, this.seqConfig,
          this.kvConfig, this.jsConfig);
    }
  }

  public static class BindingConfig {
    private final String name;
    private final Class<?> clazz;
    private final Object instance;

    public BindingConfig(String name, Class<?> clazz, Object instance) {
      this.name = name;
      this.clazz = clazz;
      this.instance = instance;
    }

    public String getName() {
      return name;
    }

    public Class<?> getClazz() {
      return clazz;
    }

    public Object getInstance() {
      return instance;
    }
  }
}
