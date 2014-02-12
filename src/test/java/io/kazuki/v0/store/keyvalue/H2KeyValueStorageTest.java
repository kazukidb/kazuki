package io.kazuki.v0.store.keyvalue;


import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import io.kazuki.v0.internal.helper.Configurations;
import io.kazuki.v0.internal.helper.TestSupport;
import io.kazuki.v0.internal.v2schema.Attribute;
import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.easy.EasyKeyValueStoreModule;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.kazuki.v0.internal.helper.TestHelper.dump;

public class H2KeyValueStorageTest
    extends TestSupport
{
  private Injector inject;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    inject = Guice.createInjector(new LifecycleModule("foo"),
        new EasyKeyValueStoreModule("foo", "test/io/kazuki/v0/store/sequence")
            .withJdbiConfig(Configurations.getJdbi().build()));
  }

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

    Assert.assertFalse(store.iterators().iterator("$schema", Schema.class).hasNext());

    Schema schema =
        new Schema(ImmutableList.of(new Attribute("fooKey", Attribute.Type.UTF8_SMALLSTRING, null,
            true), new Attribute("fooValue", Attribute.Type.UTF8_SMALLSTRING, null, true)));

    manager.createSchema("foo", schema);
    Iterator<Schema> sIter = store.iterators().iterator("$schema", Schema.class);
    Assert.assertTrue(sIter.hasNext());
    sIter.next();
    Assert.assertFalse(sIter.hasNext());

    Key foo1Key = store.create("foo", Foo.class, new Foo("k", "v"), TypeValidation.STRICT);
    log.info("created key = " + foo1Key);
    Assert.assertNotNull(store.retrieve(foo1Key, Foo.class));

    Key foo2Key = store.create("foo", Foo.class, new Foo("a", "b"), TypeValidation.STRICT);
    log.info("created key = " + foo2Key);
    Assert.assertNotNull(store.retrieve(foo2Key, Foo.class));

    Iterator<Foo> iter = store.iterators().iterator("foo", Foo.class);
    Assert.assertTrue(iter.hasNext());
    while (iter.hasNext()) {
      Foo theNext = iter.next();
      Assert.assertNotNull(theNext);
      log.info("dump all : " + dump(theNext));
    }

    Foo foo1Found = store.retrieve(foo1Key, Foo.class);
    log.info("retrieved value 1 = " + dump(foo1Found));
    Foo foo2Found = store.retrieve(foo2Key, Foo.class);
    log.info("retrieved value 2 = " + dump(foo2Found));

    Map<Key, Foo> multiFound = store.multiRetrieve(ImmutableList.of(foo1Key, foo2Key), Foo.class);
    log.info("multi-retrieved values = " + dump(multiFound));
    Assert.assertEquals(multiFound.size(), 2);
    Assert.assertEquals(multiFound.get(foo1Key), foo1Found);
    Assert.assertEquals(multiFound.get(foo2Key), foo2Found);

    boolean updated = store.update(foo1Key, Foo.class, new Foo("x", "y"));
    log.info("updated? " + updated);
    Assert.assertTrue(updated);

    Foo foo1FoundAgain = store.retrieve(foo1Key, Foo.class);
    log.info("retrieved value = " + dump(foo1FoundAgain));
    Assert.assertNotSame(foo1FoundAgain, foo1Found);

    boolean deleted = store.delete(foo1Key);
    log.info("deleted? " + deleted);
    Assert.assertTrue(deleted);

    foo1Found = store.retrieve(foo1Key, Foo.class);
    log.info("retrieved value = " + dump(foo1Found));
    Assert.assertNull(foo1Found);

    store.clear(false, false);
  }
}
