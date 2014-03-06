package io.kazuki.v0.store.jdbi;

import io.kazuki.v0.internal.helper.MaskProxy;
import io.kazuki.v0.internal.helper.ResourceHelper;
import io.kazuki.v0.store.config.ConfigurationProvider;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleRegistration;
import io.kazuki.v0.store.lifecycle.LifecycleSupportBase;

import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.sql.DataSource;

import com.google.common.base.Throwables;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.jolbox.bonecp.BoneCPDataSource;

public class BoneCpDataSourceModule extends PrivateModule {
  private final String name;
  private final String propertiesPath;
  private final String[] additionalNames;
  private final AtomicReference<JdbiDataSourceConfiguration> config;

  public BoneCpDataSourceModule(String name, @Nullable String propertiesPath,
      String... additionalNames) {
    this.name = name;
    this.propertiesPath = propertiesPath;
    this.additionalNames = additionalNames;
    this.config = new AtomicReference<JdbiDataSourceConfiguration>();
  }

  public BoneCpDataSourceModule withConfiguration(JdbiDataSourceConfiguration config) {
    this.config.set(config);

    return this;
  }

  @Override
  protected void configure() {
    JdbiDataSourceConfiguration theConfig = this.config.get();

    if (theConfig != null) {
      bind(JdbiDataSourceConfiguration.class).toInstance(theConfig);
    } else if (propertiesPath != null) {
      bind(JdbiDataSourceConfiguration.class).toProvider(
          new ConfigurationProvider<JdbiDataSourceConfiguration>(name,
              JdbiDataSourceConfiguration.class, propertiesPath, true));
    } else {
      bind(JdbiDataSourceConfiguration.class).to(
          Key.get(JdbiDataSourceConfiguration.class, Names.named(name)));
    }

    bind(Lifecycle.class).to(Key.get(Lifecycle.class, Names.named(name)));

    bind(DataSource.class).annotatedWith(Names.named(name))
        .toProvider(BoneCPDataSourceProvider.class).in(Scopes.SINGLETON);
    expose(DataSource.class).annotatedWith(Names.named(name));

    if (additionalNames != null) {
      for (String otherName : additionalNames) {
        bind(DataSource.class).annotatedWith(Names.named(otherName)).to(
            Key.get(DataSource.class, Names.named(name)));
        expose(DataSource.class).annotatedWith(Names.named(otherName));
      }
    }
  }

  private static class BoneCPDataSourceProvider
      implements
        Provider<DataSource>,
        LifecycleRegistration {
    private final JdbiDataSourceConfiguration config;
    private final MaskProxy<DataSource, BoneCPDataSource> instance;

    @Inject
    public BoneCPDataSourceProvider(JdbiDataSourceConfiguration config) {
      this.config = config;
      this.instance = new MaskProxy<DataSource, BoneCPDataSource>(DataSource.class, null);
    }

    @Override
    @Inject
    public void register(Lifecycle lifecycle) {
      lifecycle.register(new LifecycleSupportBase() {
        @Override
        public void init() {
          instance.getAndSet(createDataSource());
        }

        @Override
        public void shutdown() {
          BoneCPDataSource oldInstance = instance.getAndSet(null);

          try {
            oldInstance.close();
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

    private BoneCPDataSource createDataSource() {
      ResourceHelper.forName(config.getJdbcDriver(), getClass());

      BoneCPDataSource datasource = new BoneCPDataSource();

      datasource.setDriverClass(config.getJdbcDriver());
      datasource.setJdbcUrl(config.getJdbcUrl());
      datasource.setUsername(config.getJdbcUser());
      datasource.setPassword(config.getJdbcPassword());

      datasource.setMinConnectionsPerPartition(config.getPoolMinConnections());
      datasource.setMaxConnectionsPerPartition(config.getPoolMaxConnections());

      return datasource;
    }
  }
}
