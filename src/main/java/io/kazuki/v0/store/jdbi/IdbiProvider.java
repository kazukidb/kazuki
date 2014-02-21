package io.kazuki.v0.store.jdbi;

import io.kazuki.v0.internal.helper.JDBIHelper;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.skife.jdbi.v2.IDBI;

import com.google.inject.Provider;

public class IdbiProvider implements Provider<IDBI> {
  private final Class<?> clazz;
  private final Provider<DataSource> dataSourceProvider;

  @Inject
  public IdbiProvider(Class<?> clazz, Provider<DataSource> dataSourceProvider) {
    this.clazz = clazz;
    this.dataSourceProvider = dataSourceProvider;
  }

  @Override
  public IDBI get() {
    return JDBIHelper.getDBI(clazz, dataSourceProvider.get());
  }
}
