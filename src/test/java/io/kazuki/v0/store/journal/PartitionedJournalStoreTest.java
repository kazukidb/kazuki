package io.kazuki.v0.store.journal;


import static io.kazuki.v0.internal.helper.TestHelper.dump;
import static io.kazuki.v0.internal.helper.TestHelper.isEmptyIter;
import static io.kazuki.v0.internal.helper.TestHelper.isIterOfLength;
import static io.kazuki.v0.internal.helper.TestHelper.isNotEmptyIter;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;
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

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class PartitionedJournalStoreTest {
  private Injector inject;
  private Lifecycle lifecycle;
  private KeyValueStore store;
  private SchemaStore manager;
  private JournalStore journal;

  @BeforeTest
  public void setUp() throws Exception {
    inject =
        Guice.createInjector(new LifecycleModule("foo"), new EasyPartitionedJournalStoreModule(
            "foo", "test/io/kazuki/v0/store/sequence"));

    lifecycle = inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));
    store = inject.getInstance(com.google.inject.Key.get(KeyValueStore.class, Names.named("foo")));
    manager = inject.getInstance(com.google.inject.Key.get(SchemaStore.class, Names.named("foo")));
    journal = inject.getInstance(com.google.inject.Key.get(JournalStore.class, Names.named("foo")));

    lifecycle.init();
    store.clear(false, false);
  }

  @Test
  public void testDemo() throws Exception {
    Schema schema =
        new Schema(ImmutableList.of(new Attribute("fooKey", Attribute.Type.UTF8_SMALLSTRING, null,
            true), new Attribute("fooValue", Attribute.Type.UTF8_SMALLSTRING, null, true)));

    assertThat(manager.createSchema("foo", schema), is(2L));
    assertThat(manager.retrieveSchema("foo"), notNullValue());

    assertThat(journal.getActivePartition(), nullValue());
    assertThat(journal.getAllPartitions(), isEmptyIter(PartitionInfoSnapshot.class));

    for (int i = 0; i < 100; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);
      assertThat(journal.getActivePartition(), notNullValue());
      assertThat(journal.getActivePartition().getMinId(), lessThanOrEqualTo(i + 1L));
      assertThat(journal.getActivePartition().getMaxId(), is(i + 1L));
    }

    Iterator<PartitionInfoSnapshot> piter = journal.getAllPartitions();
    assertThat(piter, isNotEmptyIter(PartitionInfoSnapshot.class));

    System.out.println("PARTITIONS:");
    while (piter.hasNext()) {
      System.out.println(" - part - " + dump(piter.next()));
    }

    assertThat(journal.getIteratorAbsolute("foo", Foo.class, 0L, null),
        isIterOfLength(Foo.class, 100));
    assertThat(journal.getIteratorAbsolute("foo", Foo.class, 0L, 0L), isEmptyIter(Foo.class));
    assertThat(journal.getIteratorAbsolute("foo", Foo.class, 0L, 10L),
        isIterOfLength(Foo.class, 10));
    assertThat(journal.getIteratorAbsolute("foo", Foo.class, 0L, 20L),
        isIterOfLength(Foo.class, 20));
    assertThat(journal.getIteratorAbsolute("foo", Foo.class, 11L, 19L),
        isIterOfLength(Foo.class, 19));
    assertThat(journal.getIteratorAbsolute("foo", Foo.class, 89L, 12L),
        isIterOfLength(Foo.class, 11));
    assertThat(journal.getIteratorAbsolute("foo", Foo.class, 89L, null),
        isIterOfLength(Foo.class, 11));
    assertThat(journal.getIteratorAbsolute("foo", Foo.class, 89L, 0L), isEmptyIter(Foo.class));

    assertThat(journal.getAllPartitions(), isIterOfLength(PartitionInfoSnapshot.class, 10));
    assertThat(journal.getActivePartition().getPartitionId(), is("PartitionInfo-foo-foostore:10"));

    System.out.println("RELATIVE ITER TEST:");
    for (int i = 0; i < 10; i++) {
      Iterator<Foo> iter = journal.getIteratorRelative("foo", Foo.class, Long.valueOf(i * 10), 10L);
      assertThat(iter, isNotEmptyIter(Foo.class));
      int j = 0;
      while (iter.hasNext()) {
        Foo foo = iter.next();
        assertThat(foo, notNullValue());
        System.out.println("i=" + i + ",j=" + j + ",foo=" + dump(foo));
        j += 1;
      }
      assertThat(j, is(10));
    }

    System.out.println("ABSOLUTE ITER TEST:");
    for (int i = 0; i < 10; i++) {
      Iterator<Foo> iter = journal.getIteratorAbsolute("foo", Foo.class, Long.valueOf(i * 10), 10L);
      assertThat(iter, isNotEmptyIter(Foo.class));
      int j = 0;
      while (iter.hasNext()) {
        Foo foo = iter.next();
        assertThat(foo, notNullValue());
        System.out.println("i=" + i + ",j=" + j + ",foo=" + dump(foo));
        j += 1;
      }
      assertThat(j, is(10));
    }

    assertThat(journal.dropPartition(journal.getAllPartitions().next().getPartitionId()), is(true));

    System.out.println("PARTITIONS:");
    piter = journal.getAllPartitions();
    while (piter.hasNext()) {
      System.out.println(" - part - " + dump(piter.next()));
    }

    assertThat(journal.getAllPartitions(), isIterOfLength(PartitionInfoSnapshot.class, 9));

    assertThat(journal.getIteratorAbsolute("foo", Foo.class, 0L, 10L), isEmptyIter(Foo.class));
    assertThat(journal.getIteratorAbsolute("foo", Foo.class, 10L, 10L),
        isIterOfLength(Foo.class, 10));
    assertThat(journal.getIteratorAbsolute("foo", Foo.class, 10L, 20L),
        isIterOfLength(Foo.class, 20));
    assertThat(journal.getIteratorAbsolute("foo", Foo.class, 90L, 10L),
        isIterOfLength(Foo.class, 10));
    assertThat(journal.getIteratorAbsolute("foo", Foo.class, 90L, 20L),
        isIterOfLength(Foo.class, 10));

    assertThat(journal.getIteratorRelative("foo", Foo.class, 0L, null),
        isIterOfLength(Foo.class, 90));
    assertThat(journal.getIteratorRelative("foo", Foo.class, 0L, 10L),
        isIterOfLength(Foo.class, 10));
    assertThat(journal.getIteratorRelative("foo", Foo.class, 0L, 20L),
        isIterOfLength(Foo.class, 20));
    assertThat(journal.getIteratorRelative("foo", Foo.class, 11L, 79L),
        isIterOfLength(Foo.class, 79));
    assertThat(journal.getIteratorRelative("foo", Foo.class, 11L, null),
        isIterOfLength(Foo.class, 79));
    assertThat(journal.getIteratorRelative("foo", Foo.class, 10L, 10L),
        isIterOfLength(Foo.class, 10));
    assertThat(journal.getIteratorRelative("foo", Foo.class, 5L, 10L),
        isIterOfLength(Foo.class, 10));
    assertThat(journal.getIteratorRelative("foo", Foo.class, 80L, 10L), isNotEmptyIter(Foo.class));
    assertThat(journal.getIteratorRelative("foo", Foo.class, 90L, 10L), isEmptyIter(Foo.class));

    store.clear(false, false);
  }
}
