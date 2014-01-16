package io.kazuki.v0.internal.helper;

import org.h2.Driver;
import org.skife.jdbi.v2.IDBI;

import com.jolbox.bonecp.BoneCPDataSource;

public class TestHelper {
  public static IDBI getTestDataSource(Class<?> clazz) {
    BoneCPDataSource datasource = new BoneCPDataSource();

    datasource.setDriverClass(Driver.class.getName());
    datasource.setJdbcUrl(System
        .getProperty("jdbc.url", "jdbc:h2:mem:thedb;DB_CLOSE_ON_EXIT=FALSE"));
    datasource.setUsername(System.getProperty("jdbc.user", "root"));
    datasource.setPassword(System.getProperty("jdbc.password", "notreallyused"));

    return JDBIHelper.getDBI(clazz, datasource);
  }
}
