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
package io.kazuki.v0.store.journal;


import static io.kazuki.v0.internal.helper.TestHelper.dump;
import io.kazuki.v0.internal.helper.Configurations;
import io.kazuki.v0.internal.helper.TestHelper;
import io.kazuki.v0.internal.helper.TestSupport;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.easy.EasyJournalStoreModule;
import io.kazuki.v0.store.keyvalue.KeyValueIterator;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.Schema;
import io.kazuki.v0.store.sequence.KeyImpl;

import javax.sql.DataSource;

import junit.framework.Assert;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class SimpleJournalStoreTest extends TestSupport {
  private final Injector inject = Guice.createInjector(new LifecycleModule("foo"),
      new EasyJournalStoreModule("foo", "test/io/kazuki/v0/store/sequence")
          .withJdbiConfig(Configurations.getJdbi().build()));

  @Test
  public void testDemo() throws Exception {
    final Lifecycle lifecycle =
        inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));

    DataSource database =
        inject.getInstance(com.google.inject.Key.get(DataSource.class, Names.named("foo")));

    SchemaStore manager =
        inject.getInstance(com.google.inject.Key.get(SchemaStore.class, Names.named("foo")));

    JournalStore journal =
        inject.getInstance(com.google.inject.Key.get(JournalStore.class, Names.named("foo")));

    lifecycle.init();

    TestHelper.dropSchema(database);

    lifecycle.stop();
    lifecycle.shutdown();
    lifecycle.init();
    lifecycle.start();

    Schema schema =
        new Schema(ImmutableList.of(new Attribute("fooKey", Attribute.Type.UTF8_SMALLSTRING, null,
            true), new Attribute("fooValue", Attribute.Type.UTF8_SMALLSTRING, null, true)));

    Assert.assertEquals(manager.createSchema("foo", schema), KeyImpl.valueOf("$schema:2"));
    Assert.assertNotNull(manager.retrieveSchema("foo"));

    for (int i = 0; i < 100; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);
    }

    log.info("ITER TEST:");
    for (int i = 0; i < 10; i++) {
      try (KeyValueIterator<KeyValuePair<Foo>> iter =
          journal.entriesAbsolute("foo", Foo.class, SortDirection.ASCENDING, Long.valueOf(i * 10),
              10L).iterator()) {
        Assert.assertTrue(iter.hasNext());
        int j = 0;
        while (iter.hasNext()) {
          Foo foo = iter.next().getValue();
          Assert.assertNotNull(foo);
          log.info("i=" + i + ",j=" + j + ",foo=" + dump(foo));
          j += 1;
        }
      }
    }
  }
}
