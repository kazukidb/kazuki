/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.kazuki.v0.internal.helper;

import java.sql.Connection;
import java.util.Iterator;

import javax.sql.DataSource;

import org.h2.Driver;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.skife.jdbi.v2.IDBI;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.jolbox.bonecp.BoneCPDataSource;

public class TestHelper {
  private static final ObjectMapper mapper = new ObjectMapper();

  public static void dropSchema(DataSource database) {
    Connection conn = null;
    try {
      conn = database.getConnection();
      conn.prepareStatement("DROP ALL OBJECTS").execute();
      conn.commit();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }

  }

  public static IDBI getTestDataSource(Class<?> clazz) {
    BoneCPDataSource datasource = new BoneCPDataSource();

    datasource.setDriverClass(Driver.class.getName());
    datasource.setJdbcUrl(System
        .getProperty("jdbc.url", "jdbc:h2:mem:thedb;DB_CLOSE_ON_EXIT=FALSE"));
    datasource.setUsername(System.getProperty("jdbc.user", "root"));
    datasource.setPassword(System.getProperty("jdbc.password", "notreallyused"));

    return JDBIHelper.getDBI(clazz, datasource);
  }

  public static Matcher<Iterator<?>> isEmptyIter() {
    return new BaseMatcher<Iterator<?>>() {
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

  public static <T> Matcher<Iterator<?>> isNotEmptyIter() {
    return new BaseMatcher<Iterator<?>>() {
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

  public static <T> Matcher<Iterator<?>> isIterOfLength(final int targetSize) {
    return new BaseMatcher<Iterator<?>>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("is iter of length " + targetSize);
      }

      @Override
      public boolean matches(Object target) {
        if (!(target instanceof Iterator)) {
          return false;
        }

        Iterator<?> targetIter = (Iterator<?>) target;
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
