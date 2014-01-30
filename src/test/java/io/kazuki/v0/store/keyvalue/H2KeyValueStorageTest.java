package io.kazuki.v0.store.keyvalue;


import io.kazuki.v0.internal.helper.Configurations;
import io.kazuki.v0.internal.v2schema.Attribute;
import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.easy.EasyKeyValueStoreModule;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;

import java.util.Iterator;
import java.util.Map;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class H2KeyValueStorageTest {
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

    Assert.assertFalse(store.iterator("$schema", Schema.class).hasNext());

    Schema schema =
        new Schema(ImmutableList.of(new Attribute("fooKey", Attribute.Type.UTF8_SMALLSTRING, null,
            true), new Attribute("fooValue", Attribute.Type.UTF8_SMALLSTRING, null, true)));

    manager.createSchema("foo", schema);
    Iterator<Schema> sIter = store.iterator("$schema", Schema.class);
    Assert.assertTrue(sIter.hasNext());
    sIter.next();
    Assert.assertFalse(sIter.hasNext());

    Key foo1Key = store.create("foo", Foo.class, new Foo("k", "v"), TypeValidation.STRICT);
    System.out.println("created key = " + foo1Key);
    Assert.assertNotNull(store.retrieve(foo1Key, Foo.class));

    Key foo2Key = store.create("foo", Foo.class, new Foo("a", "b"), TypeValidation.STRICT);
    System.out.println("created key = " + foo2Key);
    Assert.assertNotNull(store.retrieve(foo2Key, Foo.class));

    Iterator<Foo> iter = store.iterator("foo", Foo.class);
    Assert.assertTrue(iter.hasNext());
    while (iter.hasNext()) {
      Foo theNext = iter.next();
      Assert.assertNotNull(theNext);
      System.out.println("dump all : " + dump(theNext));
    }

    Foo foo1Found = store.retrieve(foo1Key, Foo.class);
    System.out.println("retrieved value 1 = " + dump(foo1Found));
    Foo foo2Found = store.retrieve(foo2Key, Foo.class);
    System.out.println("retrieved value 2 = " + dump(foo2Found));

    Map<Key, Foo> multiFound = store.multiRetrieve(ImmutableList.of(foo1Key, foo2Key), Foo.class);
    System.out.println("multi-retrieved values = " + dump(multiFound));
    Assert.assertEquals(multiFound.size(), 2);
    Assert.assertEquals(multiFound.get(foo1Key), foo1Found);
    Assert.assertEquals(multiFound.get(foo2Key), foo2Found);

    boolean updated = store.update(foo1Key, Foo.class, new Foo("x", "y"));
    System.out.println("updated? " + updated);
    Assert.assertTrue(updated);

    Foo foo1FoundAgain = store.retrieve(foo1Key, Foo.class);
    System.out.println("retrieved value = " + dump(foo1FoundAgain));
    Assert.assertNotSame(foo1FoundAgain, foo1Found);

    boolean deleted = store.delete(foo1Key);
    System.out.println("deleted? " + deleted);
    Assert.assertTrue(deleted);

    foo1Found = store.retrieve(foo1Key, Foo.class);
    System.out.println("retrieved value = " + dump(foo1Found));
    Assert.assertNull(foo1Found);

    store.clear(false, false);
  }

  public String dump(Object object) throws Exception {
    return mapper.writeValueAsString(object);
  }
}
