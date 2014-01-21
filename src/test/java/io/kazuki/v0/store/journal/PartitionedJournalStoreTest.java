package io.kazuki.v0.store.journal;


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

import junit.framework.Assert;

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

    Assert.assertEquals(manager.createSchema("foo", schema), 2L);
    Assert.assertNotNull(manager.retrieveSchema("foo"));

    Assert.assertNull(journal.getActivePartition());
    Assert.assertFalse(journal.getAllPartitions().hasNext());

    for (int i = 0; i < 100; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);
      Assert.assertNotNull(journal.getActivePartition());
      Assert.assertTrue(journal.getActivePartition().getMinId() <= i + 1);
      Assert.assertTrue(journal.getActivePartition().getMaxId() == i + 1);
    }

    long partitionCount = 0L;
    Iterator<PartitionInfoSnapshot> piter = journal.getAllPartitions();

    System.out.println("PARTITIONS:");
    while (piter.hasNext()) {
      System.out.println(" - part - " + dump(piter.next()));
      partitionCount += 1;
    }

    Assert.assertEquals(10L, partitionCount);

    System.out.println("RELATIVE ITER TEST:");
    for (int i = 0; i < 10; i++) {
      Iterator<Foo> iter = journal.getIteratorRelative("foo", Foo.class, Long.valueOf(i * 10), 10L);
      Assert.assertTrue(iter.hasNext());
      int j = 0;
      while (iter.hasNext()) {
        Foo foo = iter.next();
        Assert.assertNotNull(foo);
        System.out.println("i=" + i + ",j=" + j + ",foo=" + dump(foo));
        j += 1;
      }
    }

    System.out.println("ABSOLUTE ITER TEST:");
    for (int i = 0; i < 10; i++) {
      Iterator<Foo> iter = journal.getIteratorAbsolute("foo", Foo.class, Long.valueOf(i * 10), 10L);
      Assert.assertTrue(iter.hasNext());
      int j = 0;
      while (iter.hasNext()) {
        Foo foo = iter.next();
        Assert.assertNotNull(foo);
        System.out.println("i=" + i + ",j=" + j + ",foo=" + dump(foo));
        j += 1;
      }
    }

    Assert.assertTrue(journal.dropPartition(journal.getAllPartitions().next().getPartitionId()));

    System.out.println("PARTITIONS:");
    partitionCount = 0L;
    piter = journal.getAllPartitions();
    while (piter.hasNext()) {
      System.out.println(" - part - " + dump(piter.next()));
      partitionCount += 1;
    }

    Assert.assertEquals(9L, partitionCount);

    Assert.assertFalse(journal.getIteratorAbsolute("foo", Foo.class, 0L, 10L).hasNext());
    Assert.assertTrue(journal.getIteratorAbsolute("foo", Foo.class, 10L, 10L).hasNext());
    Assert.assertTrue(journal.getIteratorAbsolute("foo", Foo.class, 90L, 10L).hasNext());

    Assert.assertTrue(journal.getIteratorRelative("foo", Foo.class, 0L, 10L).hasNext());
    Assert.assertTrue(journal.getIteratorRelative("foo", Foo.class, 10L, 10L).hasNext());
    Assert.assertTrue(journal.getIteratorRelative("foo", Foo.class, 80L, 10L).hasNext());
    Assert.assertFalse(journal.getIteratorRelative("foo", Foo.class, 90L, 10L).hasNext());

    store.clear(false, false);
  }

  public String dump(Object object) throws Exception {
    return mapper.writeValueAsString(object);
  }
}
