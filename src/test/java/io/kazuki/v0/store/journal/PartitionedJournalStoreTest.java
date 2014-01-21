package io.kazuki.v0.store.journal;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import io.kazuki.v0.internal.v2schema.Attribute;
import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.easy.EasyPartitionedJournalStoreModule;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;

import java.util.Iterator;

import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class PartitionedJournalStoreTest {
  private final Injector inject = Guice.createInjector(new LifecycleModule("foo"),
      new EasyPartitionedJournalStoreModule("foo", "test/io/kazuki/v0/store/sequence"));
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testDemo() throws Exception {
    final Lifecycle lifecycle =
        inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));

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

    assertThat(manager.createSchema("foo", schema), is(2L));
    assertThat(manager.retrieveSchema("foo"), notNullValue());

    assertThat(journal.getActivePartition(), nullValue());
    assertThat("no partitions yet", !journal.getAllPartitions().hasNext());

    for (int i = 0; i < 100; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);
      assertThat(journal.getActivePartition(), notNullValue());
      assertThat("active partition has insert", journal.getActivePartition().getMinId() <= i + 1);
      assertThat("active partition has insert", journal.getActivePartition().getMaxId() == i + 1);
    }

    long partitionCount = 0L;
    Iterator<PartitionInfoSnapshot> piter = journal.getAllPartitions();

    System.out.println("PARTITIONS:");
    while (piter.hasNext()) {
      System.out.println(" - part - " + dump(piter.next()));
      partitionCount += 1;
    }

    assertThat(partitionCount, is(10L));
    assertThat(journal.getActivePartition().getPartitionId(), is("PartitionInfo-foo-foostore:10"));

    System.out.println("RELATIVE ITER TEST:");
    for (int i = 0; i < 10; i++) {
      Iterator<Foo> iter = journal.getIteratorRelative("foo", Foo.class, Long.valueOf(i * 10), 10L);
      assertThat("iter is not empty", iter.hasNext());
      int j = 0;
      while (iter.hasNext()) {
        Foo foo = iter.next();
        assertThat(foo, notNullValue());
        System.out.println("i=" + i + ",j=" + j + ",foo=" + dump(foo));
        j += 1;
      }
    }

    System.out.println("ABSOLUTE ITER TEST:");
    for (int i = 0; i < 10; i++) {
      Iterator<Foo> iter = journal.getIteratorAbsolute("foo", Foo.class, Long.valueOf(i * 10), 10L);
      assertThat("iter is not empty", iter.hasNext());
      int j = 0;
      while (iter.hasNext()) {
        Foo foo = iter.next();
        assertThat(foo, notNullValue());
        System.out.println("i=" + i + ",j=" + j + ",foo=" + dump(foo));
        j += 1;
      }
    }

    assertThat("dropped", journal.dropPartition(journal.getAllPartitions().next().getPartitionId()));

    System.out.println("PARTITIONS:");
    partitionCount = 0L;
    piter = journal.getAllPartitions();
    while (piter.hasNext()) {
      System.out.println(" - part - " + dump(piter.next()));
      partitionCount += 1;
    }

    assertThat(partitionCount, is(9L));

    assertThat("iter empty", !journal.getIteratorAbsolute("foo", Foo.class, 0L, 10L).hasNext());
    assertThat("iter not empty", journal.getIteratorAbsolute("foo", Foo.class, 10L, 10L).hasNext());
    assertThat("iter not empty", journal.getIteratorAbsolute("foo", Foo.class, 90L, 10L).hasNext());

    assertThat("iter not empty", journal.getIteratorRelative("foo", Foo.class, 0L, 10L).hasNext());
    assertThat("iter not empty", journal.getIteratorRelative("foo", Foo.class, 10L, 10L).hasNext());
    assertThat("iter not empty", journal.getIteratorRelative("foo", Foo.class, 80L, 10L).hasNext());
    assertThat("iter empty", !journal.getIteratorRelative("foo", Foo.class, 90L, 10L).hasNext());

    store.clear(false, false);
  }

  public String dump(Object object) throws Exception {
    return mapper.writeValueAsString(object);
  }
}
