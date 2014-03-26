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
package io.kazuki.v0.store.index;


import io.kazuki.v0.internal.helper.Configurations;
import io.kazuki.v0.internal.helper.TestSupport;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.easy.EasyKeyValueStoreModule;
import io.kazuki.v0.store.index.query.QueryBuilder;
import io.kazuki.v0.store.index.query.QueryOperator;
import io.kazuki.v0.store.index.query.ValueType;
import io.kazuki.v0.store.keyvalue.KeyValueIterator;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.AttributeTransform;
import io.kazuki.v0.store.schema.model.IndexAttribute;
import io.kazuki.v0.store.schema.model.IndexDefinition;
import io.kazuki.v0.store.schema.model.Schema;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class SecondaryIndexBruteForceImplTest extends TestSupport {
  private final Injector inject = Guice.createInjector(new LifecycleModule("foo"),
      new EasyKeyValueStoreModule("foo", "test/io/kazuki/v0/store/sequence")
          .withJdbiConfig(Configurations.getJdbi().build()));

  @Test
  public void testDemo() throws Exception {
    final Lifecycle lifecycle =
        inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));

    KeyValueStore store =
        inject.getInstance(com.google.inject.Key.get(KeyValueStore.class, Names.named("foo")));

    SchemaStore manager =
        inject.getInstance(com.google.inject.Key.get(SchemaStore.class, Names.named("foo")));

    SecondaryIndexStore index = new SecondaryIndexBruteForceImpl(store, manager);

    lifecycle.init();
    lifecycle.start();

    Assert.assertFalse(store.iterators().iterator("$schema", Schema.class, SortDirection.ASCENDING)
        .hasNext());

    Schema schema =
        new Schema(ImmutableList.of(new Attribute("fooKey", Attribute.Type.UTF8_SMALLSTRING, null,
            true), new Attribute("fooValue", Attribute.Type.UTF8_SMALLSTRING, null, true)),
            ImmutableList.<IndexDefinition>of(new IndexDefinition("fooKey",
                ImmutableList.of(new IndexAttribute("fooKey", SortDirection.ASCENDING,
                    AttributeTransform.NONE)), false)));

    manager.createSchema("foo", schema);

    Key k0 = store.create("foo", Foo.class, new Foo("k00", "v99"), TypeValidation.STRICT);
    Key k1 = store.create("foo", Foo.class, new Foo("k11", "v88"), TypeValidation.STRICT);
    Key k2 = store.create("foo", Foo.class, new Foo("k22", "v77"), TypeValidation.STRICT);
    Key k3 = store.create("foo", Foo.class, new Foo("k33", "v66"), TypeValidation.STRICT);
    Key k4 = store.create("foo", Foo.class, new Foo("k44", "v55"), TypeValidation.STRICT);
    Key k5 = store.create("foo", Foo.class, new Foo("k00", "v55"), TypeValidation.STRICT);

    try (KeyValueIterator<Key> iter =
        index.queryWithoutPagination(
            "foo",
            Foo.class,
            "fooKey",
            new QueryBuilder()
                .andMatchesSingle("fooKey", QueryOperator.EQ, ValueType.STRING, "k00").build(),
            SortDirection.ASCENDING, null, null).iterator()) {

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals(k0, iter.next());

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals(k5, iter.next());

      Assert.assertFalse(iter.hasNext());
    }

    try (KeyValueIterator<Key> iter =
        index.queryWithoutPagination(
            "foo",
            Foo.class,
            "fooKey",
            new QueryBuilder()
                .andMatchesSingle("fooKey", QueryOperator.EQ, ValueType.STRING, "k00").build(),
            SortDirection.DESCENDING, null, null).iterator()) {

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals(k5, iter.next());

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals(k0, iter.next());

      Assert.assertFalse(iter.hasNext());
    }

    for (QueryOperator op : ImmutableList.of(QueryOperator.NE, QueryOperator.GT)) {
      try (KeyValueIterator<Key> iter =
          index.queryWithoutPagination("foo", Foo.class, "fooKey",
              new QueryBuilder().andMatchesSingle("fooKey", op, ValueType.STRING, "k00").build(),
              SortDirection.ASCENDING, null, null).iterator()) {

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(k1, iter.next());

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(k2, iter.next());

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(k3, iter.next());

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(k4, iter.next());

        Assert.assertFalse(iter.hasNext());
      }
    }

    for (QueryOperator op : ImmutableList.of(QueryOperator.NE, QueryOperator.GT)) {
      try (KeyValueIterator<Key> iter =
          index.queryWithoutPagination("foo", Foo.class, "fooKey",
              new QueryBuilder().andMatchesSingle("fooKey", op, ValueType.STRING, "k00").build(),
              SortDirection.DESCENDING, null, null).iterator()) {

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(k4, iter.next());

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(k3, iter.next());

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(k2, iter.next());

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(k1, iter.next());

        Assert.assertFalse(iter.hasNext());
      }
    }

    store.clear(false, false);
  }
}
