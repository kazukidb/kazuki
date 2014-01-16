package io.kazuki.v0.store.jdbi;

import io.kazuki.v0.store.config.ConfigurationProvider;

import javax.annotation.Nullable;

import com.google.common.base.Throwables;
import com.google.inject.PrivateModule;
import com.google.inject.name.Names;
import com.jolbox.bonecp.BoneCPDataSource;

public class JdbiDataSourceModule extends PrivateModule {
  private final String name;
  private final String propertiesPath;
  private final String[] additionalNames;

  public JdbiDataSourceModule(String name, @Nullable String propertiesPath,
      String... additionalNames) {
    this.name = name;
    this.propertiesPath = propertiesPath;
    this.additionalNames = additionalNames;
  }

  @Override
  protected void configure() {
    JdbiDataSourceConfiguration config =
        new ConfigurationProvider<JdbiDataSourceConfiguration>(name,
            JdbiDataSourceConfiguration.class, propertiesPath, true).get();

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
