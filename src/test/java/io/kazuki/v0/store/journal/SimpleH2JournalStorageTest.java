package io.kazuki.v0.store.journal;


import io.kazuki.v0.internal.v2schema.Attribute;
import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.SchemaManager;
import io.kazuki.v0.store.keyvalue.H2KeyValueStorage;
import io.kazuki.v0.store.keyvalue.KeyValueStorage;

import java.util.Iterator;

import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;

@Test
public class SimpleH2JournalStorageTest {
  private final Injector inject = Guice
      .createInjector(new H2KeyValueStorage.H2KeyValueStorageModule());
  private final ObjectMapper mapper = new ObjectMapper();

  public void testDemo() throws Exception {
    KeyValueStorage _underlying = inject.getInstance(KeyValueStorage.class);
    _underlying.clear(false);

    SimpleH2JournalStorage store = inject.getInstance(SimpleH2JournalStorage.class);
    store.initialize();

    SchemaManager manager = inject.getInstance(SchemaManager.class);

    Schema schema =
        new Schema(ImmutableList.of(new Attribute("fooKey", Attribute.Type.UTF8_SMALLSTRING, null,
            true), new Attribute("fooValue", Attribute.Type.UTF8_SMALLSTRING, null, true)));

    manager.createSchema("foo", schema);

    for (int i = 0; i < 100; i++) {
      store.append("foo", Foo.class, new Foo("k" + i, "v" + i), false);
    }

    System.out.println("ITER TEST:");
    for (int i = 0; i < 10; i++) {
      Iterator<Foo> iter = store.getIteratorAbsolute("foo", Foo.class, Long.valueOf(i + 10), 10L);
      int j = 0;
      while (iter.hasNext()) {
        Foo foo = iter.next();
        System.out.println("i=" + i + ",j=" + j + ",foo=" + foo);
        j += 1;
      }
    }
    
    _underlying.clear(false);
  }

  public String dump(Object object) throws Exception {
    return mapper.writeValueAsString(object);
  }
}
