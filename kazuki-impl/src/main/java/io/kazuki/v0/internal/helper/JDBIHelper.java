/**
 * Copyright 2014 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kazuki.v0.internal.helper;

import java.util.Map;
import java.util.StringTokenizer;

import javax.sql.DataSource;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.antlr.stringtemplate.StringTemplateGroupLoader;
import org.antlr.stringtemplate.language.AngleBracketTemplateLexer;
import org.skife.jdbi.v2.ClasspathStatementLocator;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.exceptions.UnableToCreateStatementException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.tweak.StatementLocator;

public class JDBIHelper {
  public static DBI getDBI(Class<?> clazz, DataSource datasource) {
    DBI dbi = new DBI(datasource);

    final ClasspathGroupLoader theLoader =
        new ClasspathGroupLoader(AngleBracketTemplateLexer.class, clazz.getPackage().getName()
            .replaceAll("\\.", "/"));

    dbi.setStatementLocator(new StatementLocator() {
      private final StringTemplateGroupLoader loader = theLoader;

      public String locate(String name, StatementContext ctx) throws Exception {
        if (ClasspathStatementLocator.looksLikeSql(name)) {
          return name;
        }
        final StringTokenizer tok = new StringTokenizer(name, ":");
        final String group_name = tok.nextToken();
        final String template_name = tok.nextToken();
        final StringTemplateGroup group = loader.loadGroup(group_name);
        final StringTemplate template = group.getInstanceOf(template_name);

        template.setAttributes(ctx.getAttributes());
        return template.toString();
      }
    });

    return dbi;
  }

  public static Query<Map<String, Object>> getBoundQuery(Handle handle, String dbPrefix,
      String tableParameterName, String tableName, String queryName) {
    return handle.createQuery(dbPrefix + queryName).define(tableParameterName, tableName);
  }

  public static Update getBoundStatement(Handle handle, String dbPrefix, String tableParameterName,
      String tableName, String queryName) {
    return handle.createStatement(dbPrefix + queryName).define(tableParameterName, tableName);
  }

  public static void createTable(IDBI database, final String tableDrop, final String tableDefinition) {
    database.inTransaction(new TransactionCallback<Void>() {
      @Override
      public Void inTransaction(Handle handle, TransactionStatus arg1) throws Exception {
        handle.createStatement(tableDrop).execute();

        handle.createStatement(tableDefinition).execute();

        return null;
      }
    });
  }

  public static void dropTable(IDBI database, final String tableDrop) {
    database.inTransaction(new TransactionCallback<Void>() {
      @Override
      public Void inTransaction(Handle handle, TransactionStatus arg1) throws Exception {
        dropTable(handle, tableDrop);

        return null;
      }
    });
  }

  public static void dropTable(Handle handle, final String tableDrop) {
    handle.createStatement(tableDrop).execute();
  }

  public static boolean tableExists(IDBI database, final String prefix, final String tableName) {
    if (tableName == null) {
      return false;
    }

    return database.inTransaction(new TransactionCallback<Boolean>() {
      @Override
      public Boolean inTransaction(Handle handle, TransactionStatus arg1) throws Exception {
        try {
          handle.createStatement(prefix + "table_exists").define("table_name", tableName).execute();

          return true;
        } catch (UnableToExecuteStatementException e) {
          // expected in missing case
        } catch (UnableToCreateStatementException e) {
          // expected in missing case
        }

        return false;
      }
    });
  }
}
