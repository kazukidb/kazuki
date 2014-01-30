package io.kazuki.v0.store.jdbi;

import io.kazuki.v0.store.config.ConfigurationProvider;

import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.inject.Inject;

import com.google.common.base.Throwables;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.jolbox.bonecp.BoneCPDataSource;

public class JdbiDataSourceModule extends PrivateModule {
  private final String name;
  private final String propertiesPath;
  private final String[] additionalNames;
  private final AtomicReference<JdbiDataSourceConfiguration> config;

  public JdbiDataSourceModule(String name, @Nullable String propertiesPath,
      String... additionalNames) {
    this.name = name;
    this.propertiesPath = propertiesPath;
    this.additionalNames = additionalNames;
    this.config = new AtomicReference<JdbiDataSourceConfiguration>();
  }

  public JdbiDataSourceModule withConfiguration(JdbiDataSourceConfiguration config) {
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

    bind(BoneCPDataSource.class).annotatedWith(Names.named(name))
        .toProvider(BoneCPDataSourceProvider.class).in(Scopes.SINGLETON);
    expose(BoneCPDataSource.class).annotatedWith(Names.named(name));

    if (additionalNames != null) {
      for (String otherName : additionalNames) {
        bind(BoneCPDataSource.class).annotatedWith(Names.named(otherName)).to(
            Key.get(BoneCPDataSource.class, Names.named(name)));
        expose(BoneCPDataSource.class).annotatedWith(Names.named(otherName));
      }
    }
  }

  private static class BoneCPDataSourceProvider implements Provider<BoneCPDataSource> {
    private final JdbiDataSourceConfiguration config;

    @Inject
    public BoneCPDataSourceProvider(JdbiDataSourceConfiguration config) {
      this.config = config;
    }

    @Override
    public BoneCPDataSource get() {
      try {
        Class.forName(config.getJdbcDriver());
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }

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
