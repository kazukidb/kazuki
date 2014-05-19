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
package io.kazuki.v0.store.keyvalue;

import static java.util.Arrays.asList;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.Version;
import io.kazuki.v0.store.easy.EasyKeyValueStoreModule;
import io.kazuki.v0.store.index.SecondaryIndexStore;
import io.kazuki.v0.store.index.query.QueryOperator;
import io.kazuki.v0.store.index.query.QueryTerm;
import io.kazuki.v0.store.index.query.ValueHolder;
import io.kazuki.v0.store.index.query.ValueType;
import io.kazuki.v0.store.jdbi.JdbiDataSourceConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.schema.model.Attribute.Type;
import io.kazuki.v0.store.schema.model.AttributeTransform;
import io.kazuki.v0.store.schema.model.IndexAttribute;
import io.kazuki.v0.store.schema.model.Schema.Builder;
import io.kazuki.v0.store.sequence.SequenceServiceConfiguration;

import java.io.File;
import java.util.Iterator;

import javax.inject.Inject;
import javax.inject.Named;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

public class KazukiEnumTest {
  public static final String ENUM_SMOKE_TEST = "enumsmoketest";

  @Inject
  @Named(ENUM_SMOKE_TEST)
  private Lifecycle lifecycle;

  @Inject
  @Named(ENUM_SMOKE_TEST)
  private KeyValueStore kvStore;

  @Inject
  @Named(ENUM_SMOKE_TEST)
  private SchemaStore schemaStore;

  @Inject
  @Named(ENUM_SMOKE_TEST)
  private SecondaryIndexStore secondaryIndexStore;

  @BeforeMethod
  public void init() {
    Injector injector = Guice.createInjector(new EnumSmokeTestModule());

    injector.injectMembers(this);

    lifecycle.init();
    lifecycle.start();
  }

  @AfterMethod
  public void stop() {
    lifecycle.stop();
    lifecycle.shutdown();
  }

  @Test
  public void enumSmokeTest() throws Exception {

    final String powerIndex = "powerIndex";
    final Version schema =
        schemaStore.createSchema(
            MyEntity.TYPE_NAME,
            new Builder()
                .addAttribute("name", Type.UTF8_SMALLSTRING, false)
                .addAttribute("power", Type.ENUM,
                    asList((Object) Power.ON.name(), (Object) Power.OFF.name()), false)
                .addIndex(
                    powerIndex,
                    asList(new IndexAttribute("power", SortDirection.ASCENDING,
                        AttributeTransform.NONE)), false).build());

    final KeyValuePair<MyEntity> keyValue =
        kvStore.create(MyEntity.TYPE_NAME, MyEntity.class, new MyEntity("zigglet", Power.OFF),
            TypeValidation.STRICT);

    final Key key = keyValue.getKey();

    // Search for enums that have power=OFF
    final KeyValueIterable<Key> keys =
        secondaryIndexStore.queryWithoutPagination(MyEntity.TYPE_NAME, MyEntity.class, powerIndex,
            asList(new QueryTerm(QueryOperator.EQ, "power", new ValueHolder(ValueType.STRING,
                Power.OFF.toString()))), SortDirection.ASCENDING, null, null);

    final Key firstSearchResult = keys.iterator().next();

    Assert.assertEquals(key, firstSearchResult);

    final MyEntity retrievedEntity = kvStore.retrieve(firstSearchResult, MyEntity.class);

    Assert.assertEquals("zigglet", retrievedEntity.getName());

    retrievedEntity.setPower(Power.ON);

    Assert.assertTrue(kvStore.update(firstSearchResult, MyEntity.class, retrievedEntity));

    final MyEntity retrievedEntity2 = kvStore.retrieve(firstSearchResult, MyEntity.class);

    Assert.assertEquals(retrievedEntity2.getPower(), Power.ON);

    // Search for enums that have power=ON
    final KeyValueIterable<Key> keys2 =
        secondaryIndexStore.queryWithoutPagination(MyEntity.TYPE_NAME, MyEntity.class, powerIndex,
            asList(new QueryTerm(QueryOperator.EQ, "power", new ValueHolder(ValueType.STRING,
                Power.ON.toString()))), SortDirection.ASCENDING, null, null);

    final Iterator<Key> firstSearchResult2 = keys2.iterator();
    Assert.assertTrue(firstSearchResult2.hasNext());
    Assert.assertEquals(key, firstSearchResult2.next());

    // Search for enums that have power=OFF
    final KeyValueIterable<Key> keys3 =
        secondaryIndexStore.queryWithoutPagination(MyEntity.TYPE_NAME, MyEntity.class, powerIndex,
            asList(new QueryTerm(QueryOperator.EQ, "power", new ValueHolder(ValueType.STRING,
                Power.OFF.toString()))), SortDirection.ASCENDING, null, null);

    final Iterator<Key> firstSearchResult3 = keys3.iterator();
    Assert.assertFalse(firstSearchResult3.hasNext());
  }

  public static enum Power {
    ON, OFF
  }

  public static class MyEntity {
    public static final String TYPE_NAME = "myentity";

    private Power power;

    private String name;

    public MyEntity() {

    }

    public MyEntity(final String name, final Power power) {
      this.name = name;
      this.power = power;
    }

    public Power getPower() {
      return power;
    }

    public void setPower(final Power power) {
      this.power = power;
    }

    public String getName() {
      return name;
    }

    public void setName(final String name) {
      this.name = name;
    }
  }

  public static class EnumSmokeTestModule extends AbstractModule {
    @Override
    protected void configure() {
      bind(JdbiDataSourceConfiguration.class).annotatedWith(Names.named(ENUM_SMOKE_TEST))
          .toProvider(JdbiConfigurationProvider.class).in(Scopes.SINGLETON);

      // Kazuki lifecycle management
      install(new LifecycleModule(ENUM_SMOKE_TEST));

      // Kazuki key-value store
      install(new EasyKeyValueStoreModule(ENUM_SMOKE_TEST, null).withSequenceConfig(
          getSequenceServiceConfiguration()).withKeyValueStoreConfig(
          getKeyValueStoreConfiguration()));
    }

    private SequenceServiceConfiguration getSequenceServiceConfiguration() {
      SequenceServiceConfiguration.Builder builder = new SequenceServiceConfiguration.Builder();

      builder.withDbType("h2");
      builder.withGroupName("example");
      builder.withStoreName(ENUM_SMOKE_TEST);
      builder.withStrictTypeCreation(true);

      return builder.build();
    }

    private KeyValueStoreConfiguration getKeyValueStoreConfiguration() {
      KeyValueStoreConfiguration.Builder builder = new KeyValueStoreConfiguration.Builder();

      builder.withDbType("h2");
      builder.withGroupName("example");
      builder.withStoreName(ENUM_SMOKE_TEST);
      builder.withPartitionName("default");
      builder.withPartitionSize(100_000L);
      builder.withStrictTypeCreation(true);
      builder.withDataType(MyEntity.TYPE_NAME);
      builder.withSecondaryIndex(true);

      return builder.build();
    }
  }

  private static class JdbiConfigurationProvider implements Provider<JdbiDataSourceConfiguration> {
    @Override
    public JdbiDataSourceConfiguration get() {
      JdbiDataSourceConfiguration.Builder builder = new JdbiDataSourceConfiguration.Builder();

      builder.withJdbcDriver("org.h2.Driver");
      File file = Files.createTempDir();
      builder.withJdbcUrl("jdbc:h2:" + file.getAbsolutePath());

      builder.withJdbcUser("root");
      builder.withJdbcPassword("not_really_used");
      builder.withPoolMinConnections(25);
      builder.withPoolMaxConnections(25);

      return builder.build();
    }
  }
}
