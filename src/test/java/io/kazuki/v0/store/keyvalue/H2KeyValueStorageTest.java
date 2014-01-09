package io.kazuki.v0.store.keyvalue;


import io.kazuki.v0.internal.v2schema.Attribute;
import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.SchemaManager;
import io.kazuki.v0.store.keyvalue.H2KeyValueStorage;
import io.kazuki.v0.store.keyvalue.KeyValueStorage;

import java.util.Iterator;
import java.util.Map;

import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;

@Test
public class H2KeyValueStorageTest {
  private final Injector inject = Guice
      .createInjector(new H2KeyValueStorage.H2KeyValueStorageModule());
  private final ObjectMapper mapper = new ObjectMapper();

  public void testDemo() throws Exception {
    KeyValueStorage store = inject.getInstance(KeyValueStorage.class);
    store.initialize();
    SchemaManager manager = inject.getInstance(SchemaManager.class);

    Schema schema =
        new Schema(ImmutableList.of(new Attribute("fooKey", Attribute.Type.UTF8_SMALLSTRING, null,
            true), new Attribute("fooValue", Attribute.Type.UTF8_SMALLSTRING, null, true)));

    manager.createSchema("foo", schema);

    Key foo1Key = store.create("foo", Foo.class, new Foo("k", "v"), false);
    System.out.println("created key = " + foo1Key);

    Key foo2Key = store.create("foo", Foo.class, new Foo("a", "b"), false);
    System.out.println("created key = " + foo2Key);

    Iterator<Foo> iter = store.iterator("foo", Foo.class);
    while (iter.hasNext()) {
      System.out.println("dump all : " + dump(iter.next()));
    }

    Foo foo1Found = store.retrieve(foo1Key, Foo.class);
    System.out.println("retrieved value = " + dump(foo1Found));

    Map<Key, Foo> multiFound = store.multiRetrieve(ImmutableList.of(foo1Key, foo2Key), Foo.class);
    System.out.println("multi-retrieved values = " + dump(multiFound));

    boolean updated = store.update(foo1Key, Foo.class, new Foo("x", "y"));
    System.out.println("updated? " + updated);

    foo1Found = store.retrieve(foo1Key, Foo.class);
    System.out.println("retrieved value = " + dump(foo1Found));

    boolean deleted = store.delete(foo1Key);
    System.out.println("deleted? " + deleted);


    foo1Found = store.retrieve(foo1Key, Foo.class);
    System.out.println("retrieved value = " + dump(foo1Found));
  }

  public String dump(Object object) throws Exception {
    return mapper.writeValueAsString(object);
  }

}
