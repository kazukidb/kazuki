package io.kazuki.v0.internal.helper;

import java.util.Iterator;

import org.h2.Driver;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.skife.jdbi.v2.IDBI;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jolbox.bonecp.BoneCPDataSource;

public class TestHelper {
  private static final ObjectMapper mapper = new ObjectMapper();

  public static IDBI getTestDataSource(Class<?> clazz) {
    BoneCPDataSource datasource = new BoneCPDataSource();

    datasource.setDriverClass(Driver.class.getName());
    datasource.setJdbcUrl(System
        .getProperty("jdbc.url", "jdbc:h2:mem:thedb;DB_CLOSE_ON_EXIT=FALSE"));
    datasource.setUsername(System.getProperty("jdbc.user", "root"));
    datasource.setPassword(System.getProperty("jdbc.password", "notreallyused"));

    return JDBIHelper.getDBI(clazz, datasource);
  }

  public static <T> Matcher<Iterator<T>> isEmptyIter(Class<T> clazz) {
    return new BaseMatcher<Iterator<T>>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("is not empty");
      }

      @Override
      public boolean matches(Object target) {
        return (target instanceof Iterator) && !((Iterator<?>) target).hasNext();
      }
    };
  }

  public static <T> Matcher<Iterator<T>> isNotEmptyIter(Class<T> clazz) {
    return new BaseMatcher<Iterator<T>>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("is empty");
      }

      @Override
      public boolean matches(Object target) {
        return (target instanceof Iterator) && ((Iterator<?>) target).hasNext();
      }
    };
  }

  public static <T> Matcher<Iterator<T>> isIterOfLength(Class<T> clazz, final int targetSize) {
    return new BaseMatcher<Iterator<T>>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("is iter of length " + targetSize);
      }

      @Override
      public boolean matches(Object target) {
        if (!(target instanceof Iterator)) {
          return false;
        }

        Iterator<T> targetIter = (Iterator<T>) target;
        int count = 0;
        while (targetIter.hasNext()) {
          targetIter.next();
          count += 1;
        }

        return count == targetSize;
      }
    };
  }

  public static String dump(Object object) throws Exception {
    return mapper.writeValueAsString(object);
  }
}
