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

import io.kazuki.v0.internal.helper.MaskProxy;
import io.kazuki.v0.internal.helper.ResourceHelper;
import io.kazuki.v0.store.jdbi.JdbiDataSourceConfiguration;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleRegistration;
import io.kazuki.v0.store.lifecycle.LifecycleSupportBase;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;

import com.google.common.base.Throwables;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

public class DataSourceModuleH2Impl extends PrivateModule {
  private final String name;
  private final Key<Lifecycle> lifecycleKey;
  private final Key<JdbiDataSourceConfiguration> configKey;

  public DataSourceModuleH2Impl(String name, Key<Lifecycle> lifecycleKey,
      Key<JdbiDataSourceConfiguration> configKey) {
    this.name = name;
    this.lifecycleKey = lifecycleKey;
    this.configKey = configKey;
  }

  @Override
  protected void configure() {
    // TODO: re-enable ASAP
    // binder().requireExplicitBindings();

    bind(JdbiDataSourceConfiguration.class).to(configKey);
    bind(Lifecycle.class).to(lifecycleKey);

    Key<DataSource> dsKey = Key.get(DataSource.class, Names.named(name));

    bind(dsKey).toProvider(H2DataSourceProvider.class).in(Scopes.SINGLETON);
    expose(dsKey);
  }

  private static class H2DataSourceProvider implements Provider<DataSource>, LifecycleRegistration {
    private final JdbiDataSourceConfiguration config;
    private final MaskProxy<DataSource, JdbcConnectionPool> instance;
    private volatile Lifecycle lifecycle;

    @Inject
    public H2DataSourceProvider(JdbiDataSourceConfiguration config) {
      this.config = config;
      this.instance = new MaskProxy<DataSource, JdbcConnectionPool>(DataSource.class, null);
    }

    @Override
    public Lifecycle getLifecycle() {
      return this.lifecycle;
    }

    @Override
    @Inject
    public void register(Lifecycle lifecycle) {
      if (this.lifecycle != null && !this.lifecycle.equals(lifecycle)) {
        throw new IllegalStateException("lifecycle already registered with "
            + System.identityHashCode(this.lifecycle));
      }

      this.lifecycle = lifecycle;

      this.lifecycle.register(new LifecycleSupportBase() {
        @Override
        public void init() {
          instance.getAndSet(createDataSource());
        }

        @Override
        public void shutdown() {
          JdbcConnectionPool oldInstance = instance.getAndSet(null);

          try {
            oldInstance.dispose();
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      });
    }

    @Override
    public DataSource get() {
      return instance.asProxyInstance();
    }

    private JdbcConnectionPool createDataSource() {
      ResourceHelper.forName(config.getJdbcDriver(), getClass());

      JdbcDataSource datasource = new JdbcDataSource();

      datasource.setURL(config.getJdbcUrl());
      datasource.setUser(config.getJdbcUser());
      datasource.setPassword(config.getJdbcPassword());

      JdbcConnectionPool pooledDatasource = JdbcConnectionPool.create(datasource);

      pooledDatasource.setMaxConnections(config.getPoolMaxConnections());

      return pooledDatasource;
    }
  }
}
