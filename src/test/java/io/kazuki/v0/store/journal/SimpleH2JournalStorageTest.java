package io.kazuki.v0.store.journal;


import io.kazuki.v0.internal.v2schema.Attribute;
import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.easy.EasyJournalStoreModule;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;

import java.util.Iterator;

import junit.framework.Assert;

import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class SimpleH2JournalStorageTest {
  private final Injector inject = Guice.createInjector(new LifecycleModule(),
      new EasyJournalStoreModule("foo", "test/io/kazuki/v0/store/sequence"));
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testDemo() throws Exception {
    final Lifecycle lifecycle = inject.getInstance(Lifecycle.class);

    KeyValueStore store =
        inject.getInstance(com.google.inject.Key.get(KeyValueStore.class, Names.named("foo")));

    SchemaStore manager =
        inject.getInstance(com.google.inject.Key.get(SchemaStore.class, Names.named("foo")));

    JournalStore journal =
        inject.getInstance(com.google.inject.Key.get(JournalStore.class, Names.named("foo")));

    lifecycle.init();

    store.clear(false, false);

    Schema schema =
        new Schema(ImmutableList.of(new Attribute("fooKey", Attribute.Type.UTF8_SMALLSTRING, null,
            true), new Attribute("fooValue", Attribute.Type.UTF8_SMALLSTRING, null, true)));

    Assert.assertEquals(manager.createSchema("foo", schema), 2L);
    Assert.assertNotNull(manager.retrieveSchema("foo"));

    for (int i = 0; i < 100; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);
    }

    System.out.println("ITER TEST:");
    for (int i = 0; i < 10; i++) {
      Iterator<Foo> iter = journal.getIteratorAbsolute("foo", Foo.class, Long.valueOf(i + 10), 10L);
      Assert.assertTrue(iter.hasNext());
      int j = 0;
      while (iter.hasNext()) {
        Foo foo = iter.next();
        Assert.assertNotNull(foo);
        System.out.println("i=" + i + ",j=" + j + ",foo=" + dump(foo));
        j += 1;
      }
    }

    store.clear(false, false);
  }

  public String dump(Object object) throws Exception {
    return mapper.writeValueAsString(object);
  }
}
