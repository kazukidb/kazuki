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

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;

import com.google.common.base.Throwables;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

public class H2DataSourceModule extends PrivateModule {
  private final String name;
  private final String propertiesPath;
  private final String[] additionalNames;
  private final AtomicReference<JdbiDataSourceConfiguration> config;

  public H2DataSourceModule(String name, @Nullable String propertiesPath, String... additionalNames) {
    this.name = name;
    this.propertiesPath = propertiesPath;
    this.additionalNames = additionalNames;
    this.config = new AtomicReference<JdbiDataSourceConfiguration>();
  }

  public H2DataSourceModule withConfiguration(JdbiDataSourceConfiguration config) {
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

    bind(DataSource.class).annotatedWith(Names.named(name)).toProvider(H2DataSourceProvider.class)
        .in(Scopes.SINGLETON);
    expose(DataSource.class).annotatedWith(Names.named(name));

    if (additionalNames != null) {
      for (String otherName : additionalNames) {
        bind(DataSource.class).annotatedWith(Names.named(otherName)).to(
            Key.get(DataSource.class, Names.named(name)));
        expose(DataSource.class).annotatedWith(Names.named(otherName));
      }
    }
  }

  private static class H2DataSourceProvider implements Provider<DataSource>, LifecycleRegistration {
    private final JdbiDataSourceConfiguration config;
    private final MaskProxy<DataSource, JdbcConnectionPool> instance;

    @Inject
    public H2DataSourceProvider(JdbiDataSourceConfiguration config) {
      this.config = config;
      this.instance = new MaskProxy<DataSource, JdbcConnectionPool>(DataSource.class, null);
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
