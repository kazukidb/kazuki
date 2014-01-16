package io.kazuki.v0.store.jdbi;

import io.kazuki.v0.internal.helper.JDBIHelper;

import org.skife.jdbi.v2.IDBI;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.jolbox.bonecp.BoneCPDataSource;

public class IdbiProvider implements Provider<IDBI> {
  private final Class<?> clazz;
  private final Provider<BoneCPDataSource> dataSourceProvider;

  @Inject
  public IdbiProvider(Class<?> clazz, Provider<BoneCPDataSource> dataSourceProvider) {
    this.clazz = clazz;
    this.dataSourceProvider = dataSourceProvider;
  }

  @Override
  public IDBI get() {
    return JDBIHelper.getDBI(clazz, dataSourceProvider.get());
  }
}
