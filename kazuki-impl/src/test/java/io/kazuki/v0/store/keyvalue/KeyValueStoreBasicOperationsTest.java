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

import static com.google.common.base.Preconditions.checkNotNull;
import io.kazuki.v0.internal.helper.Configurations;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.guice.KazukiModule;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.model.Attribute.Type;
import io.kazuki.v0.store.schema.model.AttributeTransform;
import io.kazuki.v0.store.schema.model.IndexAttribute;
import io.kazuki.v0.store.schema.model.Schema;
import io.kazuki.v0.store.schema.model.Schema.Builder;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

/**
 * Basic operations smoke test.
 */
@Test
public class KeyValueStoreBasicOperationsTest {
  protected Lifecycle lifecycle;
  protected SchemaStore schema;
  protected KeyValueStore kvStore;

  @BeforeMethod
  public void prepare() throws Exception {
    Injector injector =
        Guice.createInjector(new KazukiModule.Builder(ExampleStore.STORE_NAME)
            .withJdbiConfiguration(ExampleStore.STORE_NAME, Configurations.getJdbi().build())
            .withSequenceServiceConfiguration(
                ExampleStore.STORE_NAME,
                Configurations.getSequence(ExampleStore.GROUP_NAME, ExampleStore.STORE_NAME)
                    .build())
            .withKeyValueStoreConfiguration(
                ExampleStore.STORE_NAME,
                Configurations.getKeyValue(ExampleStore.GROUP_NAME, ExampleStore.STORE_NAME)
                    .build()).build());

    lifecycle =
        injector.getBinding(Key.get(Lifecycle.class, Names.named(ExampleStore.STORE_NAME)))
            .getProvider().get();

    schema =
        injector.getBinding(Key.get(SchemaStore.class, Names.named(ExampleStore.STORE_NAME)))
            .getProvider().get();

    kvStore =
        injector.getBinding(Key.get(KeyValueStore.class, Names.named(ExampleStore.STORE_NAME)))
            .getProvider().get();

    lifecycle.init();
    lifecycle.start();
  }

  @AfterMethod
  public void cleanup() {
    lifecycle.stop();
    lifecycle.shutdown();
  }

  public static class Record {
    private static final Schema SCHEMA = new Builder()
        .addAttribute("key", Type.UTF8_SMALLSTRING, false)
        .addAttribute("value", Type.UTF8_SMALLSTRING, false)
        .addIndex(
            "keyIdx",
            ImmutableList.of(new IndexAttribute("key", SortDirection.ASCENDING,
                AttributeTransform.NONE)), true).build();

    private String key;

    private String value;

    @JsonCreator
    public Record(@JsonProperty("key") final String key, @JsonProperty("value") final String value) {
      setKey(key);
      setValue(value);
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    public void setKey(final String key) {
      this.key = checkNotNull(key);
    }

    public void setValue(final String value) {
      this.value = checkNotNull(value);
    }
  }

  public static class ExampleStore {
    public static final String GROUP_NAME = "example";
    public static final String STORE_NAME = "example";
  }

  @Test
  public void emptySchema() throws Exception {
    final Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          schema.createSchema("record", Record.SCHEMA);
        } catch (KazukiException e) {
          e.printStackTrace();
        }
      }
    });
    t.run();
    t.join();
    try (final KeyValueIterable<KeyValuePair<Record>> kvi =
        kvStore.iterators().entries("record", Record.class, SortDirection.ASCENDING)) {
      for (KeyValuePair<Record> kv : kvi) {
        System.out.println(kv.getValue().getKey() + " = " + kv.getValue().getValue());
      }
    }
  }
}
