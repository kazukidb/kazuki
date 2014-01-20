package io.kazuki.v0.store.jdbi;

import io.kazuki.v0.store.config.ConfigurationProvider;

import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.base.Throwables;
import com.google.inject.PrivateModule;
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
    if (theConfig == null) {
      theConfig =
          new ConfigurationProvider<JdbiDataSourceConfiguration>(name,
              JdbiDataSourceConfiguration.class, propertiesPath, true).get();
    }

    try {
      Class.forName(theConfig.getJdbcDriver());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    BoneCPDataSource datasource = new BoneCPDataSource();

    datasource.setDriverClass(theConfig.getJdbcDriver());
    datasource.setJdbcUrl(theConfig.getJdbcUrl());
    datasource.setUsername(theConfig.getJdbcUser());
    datasource.setPassword(theConfig.getJdbcPassword());

    datasource.setMinConnectionsPerPartition(theConfig.getPoolMinConnections());
    datasource.setMaxConnectionsPerPartition(theConfig.getPoolMaxConnections());

    bind(BoneCPDataSource.class).annotatedWith(Names.named(name)).toInstance(datasource);
    expose(BoneCPDataSource.class).annotatedWith(Names.named(name));

    if (additionalNames != null) {
      for (String otherName : additionalNames) {
        bind(BoneCPDataSource.class).annotatedWith(Names.named(otherName)).toInstance(datasource);
        expose(BoneCPDataSource.class).annotatedWith(Names.named(otherName));
      }
    }
  }
}
