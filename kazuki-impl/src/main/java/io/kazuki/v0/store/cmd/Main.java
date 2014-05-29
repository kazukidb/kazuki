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
package io.kazuki.v0.store.cmd;

import io.airlift.command.Arguments;
import io.airlift.command.Cli;
import io.airlift.command.Cli.CliBuilder;
import io.airlift.command.Command;
import io.airlift.command.Help;
import io.airlift.command.Option;
import io.airlift.command.OptionType;
import io.kazuki.v0.internal.helper.EncodingHelper;
import io.kazuki.v0.store.cmd.impl.SqlCommandHelper;
import io.kazuki.v0.store.cmd.impl.SqlCommandHelper.ResultHandler;
import io.kazuki.v0.store.cmd.impl.SqlCommandHelper.RowHandler;
import io.kazuki.v0.store.guice.KazukiModule;
import io.kazuki.v0.store.guice.impl.DataSourceModuleH2Impl;
import io.kazuki.v0.store.guice.impl.LifecycleModuleDefaultImpl;
import io.kazuki.v0.store.jdbi.IdbiProvider;
import io.kazuki.v0.store.jdbi.JdbiDataSourceConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.keyvalue.KeyValueStoreConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.sequence.KeyImpl;
import io.kazuki.v0.store.sequence.SequenceService;
import io.kazuki.v0.store.sequence.SequenceServiceConfiguration;

import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.skife.jdbi.v2.IDBI;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

/**
 * Entry point for Kazuki command-line utils
 */
public class Main {
  public static final String STORE_NAME = "db";

  public static void main(String[] args) {
    CliBuilder<Runnable> builder =
        Cli.<Runnable>builder("kazuki")
            .withDescription("the Kazuki command-line interface")
            .withDefaultCommand(Help.class)
            .withCommands(Help.class, ListStores.class, RawExport.class, DropTable.class,
                KeyValueExport.class, KeyValueDelete.class, KeyValueDeleteHard.class);

    Cli<Runnable> gitParser = builder.build();

    Runnable cmd = gitParser.parse(args);

    if (cmd instanceof KazukiCommand) {
      Injector inject = Guice.createInjector(((KazukiCommand) cmd).getModules());
      inject.injectMembers(cmd);
      ((KazukiCommand) cmd).init();
    }

    cmd.run();
  }

  public abstract static class KazukiCommand implements Runnable {
    @Option(type = OptionType.GLOBAL, name = "-v", description = "Verbose mode", required = false)
    public boolean verbose;

    @Option(type = OptionType.GLOBAL, name = "-u", description = "JDBC user", required = true)
    public String user;

    @Option(type = OptionType.GLOBAL, name = "-p", description = "JDBC password", required = true)
    public String password;

    @Option(type = OptionType.GLOBAL, name = "-f", description = "H2 database file", required = true)
    public String h2file;

    protected IDBI database;

    public KazukiCommand() {}

    public void init() {
      Injector inject = Guice.createInjector(getModules());

      inject.getBinding(Key.get(DataSource.class, Names.named(STORE_NAME))).getProvider().get();

      this.database = inject.getBinding(IDBI.class).getProvider().get();

      Lifecycle life =
          inject.getBinding(Key.get(Lifecycle.class, Names.named(STORE_NAME))).getProvider().get();

      life.init();
      life.start();
    }

    public List<Module> getModules() {
      return ImmutableList.<Module>of(
          new LifecycleModuleDefaultImpl(STORE_NAME),
          new DataSourceModuleH2Impl(STORE_NAME, Key.get(Lifecycle.class, Names.named(STORE_NAME)), Key
              .get(JdbiDataSourceConfiguration.class, Names.named(STORE_NAME))),
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(Key.get(JdbiDataSourceConfiguration.class, Names.named(STORE_NAME))).toInstance(
                  getJdbiConfig());

              Provider<DataSource> provider =
                  binder().getProvider(Key.get(DataSource.class, Names.named(STORE_NAME)));

              bind(IDBI.class).toProvider(new IdbiProvider(SequenceService.class, provider)).in(
                  Scopes.SINGLETON);
            }
          });
    }

    public JdbiDataSourceConfiguration getJdbiConfig() {
      return new JdbiDataSourceConfiguration.Builder().withJdbcDriver("org.h2.Driver")
          .withJdbcUrl("jdbc:h2:file:" + h2file).withJdbcUser(user).withJdbcPassword(password)
          .withPoolMaxConnections(10).withPoolMinConnections(10).build();
    }
  }

  @Command(name = "list", description = "list Kazuki stores")
  public static class ListStores extends KazukiCommand {
    @Override
    public void run() {
      try (PrintWriter output = new PrintWriter(System.out)) {
        SqlCommandHelper.outputResults(database,
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '\\_%'",
            new RowHandler() {
              @Override
              public void handleRow(Map<String, Object> row) {
                try {
                  String table = (String) row.get("table_name");
                  String[] parts = table.split("__");
                  String[] names = parts[0].split("_");
                  String group = names[1];
                  String store = names[2];
                  String type = parts[1];

                  ImmutableMap.Builder<String, String> result =
                      ImmutableMap.<String, String>builder();

                  result.put("group", group);
                  result.put("store", store);

                  if (parts.length > 2) {
                    result.put("partition", parts[2]);
                  }

                  result.put("$type", type);
                  result.put("$table_name", table);

                  output.println(EncodingHelper.convertToJson(result.build()));
                } catch (Exception e) {
                  throw Throwables.propagate(e);
                }
              }
            });
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  @Command(name = "raw_export", description = "export raw Kazuki tables as JSON")
  public static class RawExport extends KazukiCommand {
    @Arguments(description = "names of tables to be exported")
    public List<String> tableNames;

    @Override
    public void run() {
      try (PrintWriter output = new PrintWriter(System.out)) {
        for (String tableName : tableNames) {
          SqlCommandHelper.outputResults(this.database, "SELECT * FROM \"" + tableName + "\"",
              new RowHandler() {
                @Override
                public void handleRow(Map<String, Object> row) {
                  try {
                    if (row.containsKey("_value")) {
                      Object val = row.remove("_value");
                      Object newVal = EncodingHelper.parseSmile((byte[]) val, Object.class);
                      row.put("_value", newVal);
                    }

                    output.println(EncodingHelper.convertToJson(row));
                  } catch (Exception e) {
                    throw Throwables.propagate(e);
                  }
                }
              });
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  @Command(name = "drop_tables", description = "Drops specified tables")
  public static class DropTable extends KazukiCommand {
    @Arguments(description = "names of tables to be dropped")
    public List<String> tableNames;

    @Override
    public void run() {
      try (PrintWriter output = new PrintWriter(System.out)) {
        for (final String tableName : tableNames) {
          SqlCommandHelper.executeStatement(database, "drop table \"" + tableName + "\"",
              new ResultHandler() {
                @Override
                public void handleResult(int result) {
                  try {
                    output.println(EncodingHelper.convertToJson(ImmutableMap.of("drop", tableName,
                        "result", result)));
                  } catch (Exception e) {
                    throw Throwables.propagate(e);
                  }
                }
              });
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public abstract static class KeyValueStoreCommand extends KazukiCommand {
    @Option(type = OptionType.GLOBAL, name = "-g", description = "Kazuki store Group name", required = true)
    public String group;

    @Option(type = OptionType.GLOBAL, name = "-s", description = "Kazuki store Store name", required = true)
    public String store;

    @Option(type = OptionType.GLOBAL, name = "-t", description = "Kazuki store Partition name", required = false)
    public String partition = "default";

    protected SequenceService sequence;

    protected KeyValueStore keyValueStore;

    public void init() {
      Injector inject = Guice.createInjector(getModules());

      inject.getBinding(Key.get(DataSource.class, Names.named(STORE_NAME))).getProvider().get();

      this.sequence =
          inject.getBinding(Key.get(SequenceService.class, Names.named(STORE_NAME))).getProvider()
              .get();

      this.keyValueStore =
          inject.getBinding(Key.get(KeyValueStore.class, Names.named(STORE_NAME))).getProvider()
              .get();

      Lifecycle life =
          inject.getBinding(Key.get(Lifecycle.class, Names.named(STORE_NAME))).getProvider().get();

      life.init();
      life.start();
    }

    @Override
    public List<Module> getModules() {
      return ImmutableList.<Module>of(new KazukiModule.Builder(STORE_NAME)
          .withJdbiConfiguration(STORE_NAME, getJdbiConfig())
          .withSequenceServiceConfiguration(STORE_NAME, getSequenceConfig())
          .withKeyValueStoreConfiguration(STORE_NAME, getKeyValueConfig()).build());
    }

    public KeyValueStoreConfiguration getKeyValueConfig() {
      return new KeyValueStoreConfiguration.Builder().withDbType("h2").withGroupName(group)
          .withStoreName(store).withPartitionName(partition).withStrictTypeCreation(false).build();
    }

    public SequenceServiceConfiguration getSequenceConfig() {
      return new SequenceServiceConfiguration.Builder().withDbType("h2").withGroupName(group)
          .withStoreName(store).withIncrementBlockSize(100_000L).withStrictTypeCreation(false)
          .build();
    }
  }

  @Command(name = "kv_export", description = "export key value entries")
  public static class KeyValueExport extends KeyValueStoreCommand {
    @Override
    public void run() {
      try (PrintWriter output = new PrintWriter(System.out)) {
        int i = 1;
        String type = "$schema";

        for (;;) {
          try (KeyValueIterable<KeyValuePair<LinkedHashMap>> iter =
              keyValueStore.iterators().entries(type, LinkedHashMap.class, SortDirection.ASCENDING)) {
            for (KeyValuePair<LinkedHashMap> entry : iter) {
              output.println(EncodingHelper.convertToJson(ImmutableMap.of("type", type, "key",
                  entry.getKey().getIdentifier(), "value", entry.getValue())));
              output.flush();
            }
          }

          i += 1;
          try {
            type = sequence.getTypeName(i);
          } catch (Exception e) {
            break;
          }
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  @Command(name = "kv_delete", description = "delete (soft) one or more KV entries by identifier")
  public static class KeyValueDelete extends KeyValueStoreCommand {
    @Arguments(description = "keys to delete")
    public List<String> toDelete;

    @Override
    public void run() {
      try (PrintWriter output = new PrintWriter(System.out)) {
        for (String key : toDelete) {
          boolean result = keyValueStore.delete(KeyImpl.valueOf(key));

          output
              .println(EncodingHelper.convertToJson(ImmutableMap.of("key", key, "result", result)));
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  @Command(name = "kv_delete_hard", description = "delete (hard) one or more KV entries by identifier")
  public static class KeyValueDeleteHard extends KeyValueStoreCommand {
    @Arguments(description = "keys to delete")
    public List<String> toDelete;

    @Override
    public void run() {
      try (PrintWriter output = new PrintWriter(System.out)) {
        for (String key : toDelete) {
          boolean result = keyValueStore.deleteHard(KeyImpl.valueOf(key));

          output
              .println(EncodingHelper.convertToJson(ImmutableMap.of("key", key, "result", result)));
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
