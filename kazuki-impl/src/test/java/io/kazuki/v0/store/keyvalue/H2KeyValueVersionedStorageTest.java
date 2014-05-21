/**
 * Copyright 2014 Sunny Gleason
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
package io.kazuki.v0.store.keyvalue;


import io.kazuki.v0.internal.helper.Configurations;
import io.kazuki.v0.internal.helper.TestSupport;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.Version;
import io.kazuki.v0.store.easy.EasyKeyValueStoreModule;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.schema.model.Schema;

import java.util.Map;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class H2KeyValueVersionedStorageTest extends TestSupport {
  private final Injector inject = Guice.createInjector(new LifecycleModule("foo"),
      new EasyKeyValueStoreModule("foo", "test/io/kazuki/v0/store/sequence")
          .withJdbiConfig(Configurations.getJdbi().build()));

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testDemo() throws Exception {
    final Lifecycle lifecycle =
        inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));

    KeyValueStore store =
        inject.getInstance(com.google.inject.Key.get(KeyValueStore.class, Names.named("foo")));

    SchemaStore manager =
        inject.getInstance(com.google.inject.Key.get(SchemaStore.class, Names.named("foo")));

    lifecycle.init();

    store.clear(false, false);

    lifecycle.stop();
    lifecycle.shutdown();
    lifecycle.init();
    lifecycle.start();

    Assert.assertFalse(store.iterators().iterator("$schema", Schema.class, SortDirection.ASCENDING)
        .hasNext());

    manager.createSchema("foo", Foo.FOO_SCHEMA);

    try (KeyValueIterator<Schema> sIter =
        store.iterators().iterator("$schema", Schema.class, SortDirection.ASCENDING)) {
      Assert.assertTrue(sIter.hasNext());
      sIter.next();
      Assert.assertFalse(sIter.hasNext());
    }

    KeyValuePair<Foo> foo1KeyValuePair =
        store.create("foo", Foo.class, new Foo("k", "v"), TypeValidation.STRICT);
    Key foo1Key = foo1KeyValuePair.getKey();
    log.info("created key = " + foo1Key);
    Assert.assertNotNull(store.retrieve(foo1Key, Foo.class));

    KeyValuePair<Foo> foo2KeyValuePair =
        store.create("foo", Foo.class, new Foo("a", "b"), TypeValidation.STRICT);
    Key foo2Key = foo2KeyValuePair.getKey();
    log.info("created key = " + foo2Key);
    Assert.assertNotNull(store.retrieve(foo2Key, Foo.class));

    try (KeyValueIterator<Foo> iter =
        store.iterators().iterator("foo", Foo.class, SortDirection.ASCENDING)) {
      Assert.assertTrue(iter.hasNext());
      while (iter.hasNext()) {
        Foo theNext = iter.next();
        Assert.assertNotNull(theNext);
        log.info("dump all : " + dump(theNext));
      }
    }

    KeyValuePair<Foo> foo1Found = store.retrieveVersioned(foo1Key, Foo.class);
    log.info("retrieved value 1 = " + dump(foo1Found));
    KeyValuePair<Foo> foo2Found = store.retrieveVersioned(foo2Key, Foo.class);
    log.info("retrieved value 2 = " + dump(foo2Found));

    Map<Key, KeyValuePair<Foo>> multiFound =
        store.multiRetrieveVersioned(ImmutableList.of(foo1Key, foo2Key), Foo.class);
    log.info("multi-retrieved values = " + dump(multiFound));
    Assert.assertEquals(multiFound.size(), 2);
    Assert.assertEquals(multiFound.get(foo1Key).getValue(), foo1Found.getValue());
    Assert.assertEquals(multiFound.get(foo2Key).getValue(), foo2Found.getValue());

    Version version =
        store.updateVersioned(foo1Key, foo1Found.getVersion(), Foo.class, new Foo("x", "y"));
    log.info("updated? " + (version != null));
    Assert.assertNotNull(version);

    Version versionBad =
        store.updateVersioned(foo1Key, foo1Found.getVersion(), Foo.class, new Foo("x", "y"));

    log.info("updated? " + (versionBad != null));
    Assert.assertNull(versionBad);

    KeyValuePair<Foo> foo1FoundAgain = store.retrieveVersioned(foo1Key, Foo.class);
    log.info("retrieved value = " + dump(foo1FoundAgain));
    Assert.assertNotSame(foo1FoundAgain.getVersion(), foo1Found.getVersion());

    boolean deleted = store.delete(foo1Key);
    log.info("deleted? " + deleted);
    Assert.assertTrue(deleted);

    foo1Found = store.retrieveVersioned(foo1Key, Foo.class);
    log.info("retrieved value = " + dump(foo1Found));
    Assert.assertNull(foo1Found);

    lifecycle.stop();
    lifecycle.shutdown();
    lifecycle.init();
    lifecycle.start();

    foo1Found = store.retrieveVersioned(foo1Key, Foo.class);
    log.info("retrieved value = " + dump(foo1Found));
    Assert.assertNull(foo1Found);

    foo1Found = store.retrieveVersioned(foo2Key, Foo.class);
    log.info("retrieved value = " + dump(foo2Found));
    Assert.assertNotNull(foo2Found);

    store.clear(false, false);
  }

  public String dump(Object object) throws Exception {
    return mapper.writeValueAsString(object);
  }
}
