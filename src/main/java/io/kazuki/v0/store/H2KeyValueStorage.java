package io.kazuki.v0.store;

import io.kazuki.v0.internal.helper.H2TypeHelper;
import io.kazuki.v0.internal.helper.JDBIHelper;
import io.kazuki.v0.internal.helper.SqlTypeHelper;
import io.kazuki.v0.internal.sequence.SequenceHelper;
import io.kazuki.v0.internal.sequence.SequenceService;
import io.kazuki.v0.internal.sequence.SequenceServiceDatabaseImpl;

import org.h2.Driver;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import com.jolbox.bonecp.BoneCPDataSource;

/**
 * H2 SQL implementation of key-value storage using JDBI.
 */
public class H2KeyValueStorage extends JDBIKeyValueStorage {
  protected String getPrefix() {
    return H2TypeHelper.DATABASE_PREFIX;
  }

  public static class H2KeyValueStorageModule extends AbstractModule {
    @Override
    public void configure() {
      Binder binder = binder();

      BoneCPDataSource datasource = new BoneCPDataSource();

      datasource.setDriverClass(Driver.class.getName());
      datasource.setJdbcUrl(System.getProperty("jdbc.url",
          "jdbc:h2:mem:thedb;DB_CLOSE_ON_EXIT=FALSE"));
      datasource.setUsername(System.getProperty("jdbc.user", "root"));
      datasource.setPassword(System.getProperty("jdbc.password", "notreallyused"));

      DBI dbi = JDBIHelper.getDBI(datasource);
      binder.bind(IDBI.class).toInstance(dbi);

      binder.bind(String.class).annotatedWith(Names.named("db.prefix"))
          .toInstance(H2TypeHelper.DATABASE_PREFIX);

      binder.bind(Boolean.class).annotatedWith(Names.named("nuke.allowed"))
          .toInstance(Boolean.valueOf(System.getProperty("nuke.allowed", "false")));

      SequenceServiceDatabaseImpl sequenceService =
          new SequenceServiceDatabaseImpl(new SequenceHelper(Boolean.valueOf(System.getProperty(
              "strict.type.creation", "true"))), dbi, H2TypeHelper.DATABASE_PREFIX,
              SequenceServiceDatabaseImpl.DEFAULT_INCREMENT);
      binder.bind(SequenceService.class).toInstance(sequenceService);
      binder.bind(SequenceServiceDatabaseImpl.class).toInstance(sequenceService);
      binder.bind(SqlTypeHelper.class).to(H2TypeHelper.class).asEagerSingleton();

      binder.bind(KeyValueStorage.class).to(H2KeyValueStorage.class).asEagerSingleton();
    }
  }
}
