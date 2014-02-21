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
import io.kazuki.v0.internal.helper.Configurations;
import io.kazuki.v0.internal.helper.TestHelper;
import io.kazuki.v0.internal.v2schema.Attribute;
import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.easy.EasyPartitionedJournalStoreModule;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValueIterator;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.sequence.KeyImpl;

import javax.sql.DataSource;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class PartitionedJournalStoreSmokeTest {
  private Injector inject;
  private DataSource database;
  private Lifecycle lifecycle;
  private SchemaStore manager;
  private JournalStore journal;

  @BeforeTest(alwaysRun = true)
  public void setUp() throws Exception {
    inject =
        Guice.createInjector(new LifecycleModule("foo"), new EasyPartitionedJournalStoreModule(
            "foo", "test/io/kazuki/v0/store/sequence").withJdbiConfig(Configurations.getJdbi()
            .build()));

    lifecycle = inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));
    database = inject.getInstance(com.google.inject.Key.get(DataSource.class, Names.named("foo")));
    manager = inject.getInstance(com.google.inject.Key.get(SchemaStore.class, Names.named("foo")));
    journal = inject.getInstance(com.google.inject.Key.get(JournalStore.class, Names.named("foo")));

    TestHelper.dropSchema(database);

    lifecycle.init();
  }

  @Test(singleThreaded = true)
  public void testDemo() throws Exception {
    assertThat(journal.getAllPartitions().iterator(), isEmptyIter());

    Schema schema =
        new Schema(ImmutableList.of(new Attribute("fooKey", Attribute.Type.UTF8_SMALLSTRING, null,
            true), new Attribute("fooValue", Attribute.Type.UTF8_SMALLSTRING, null, true)));

    assertThat(manager.createSchema("foo", schema), is(KeyImpl.valueOf("$schema:3")));
    assertThat(manager.retrieveSchema("foo"), notNullValue());

    System.out.println(dump(journal.getActivePartition()));
    assertThat(journal.getActivePartition(), nullValue());

    try (KeyValueIterator<PartitionInfoSnapshot> theIter = journal.getAllPartitions().iterator()) {
      assertThat(theIter, isEmptyIter());
    }

    for (int i = 0; i < 100; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);
      assertThat(journal.getActivePartition(), notNullValue());
      assertThat(journal.getActivePartition().getMinId(), lessThanOrEqualTo(i + 1L));
      assertThat(journal.getActivePartition().getMaxId(), is(i + 1L));
    }

    try (KeyValueIterator<PartitionInfoSnapshot> piter = journal.getAllPartitions().iterator()) {
      assertThat(piter, isNotEmptyIter());

      System.out.println("PARTITIONS:");
      while (piter.hasNext()) {
        PartitionInfoSnapshot snap = piter.next();
        System.out.println(" - part - " + dump(snap) + " " + snap.getSize());
        assertThat(snap.getSize(), is(10L));
      }
    }

    Long[][] configs =
        { {0L, null, 100L}, {0L, 0L, 0L}, {0L, 10L, 10L}, {0L, 20L, 20L}, {11L, 19L, 19L},
            {89L, 12L, 11L}, {89L, null, 11L}, {89L, 0L, 0L}};

    for (Long[] config : configs) {
      try (KeyValueIterator<KeyValuePair<Foo>> theIter =
          journal.entriesAbsolute("foo", Foo.class, config[0], config[1]).iterator()) {
        assertThat(theIter, isIterOfLength(config[2].intValue()));
      }
    }

    try (KeyValueIterator<PartitionInfoSnapshot> theIter = journal.getAllPartitions().iterator()) {
      assertThat(theIter, isIterOfLength(10));
    }

    assertThat(journal.getActivePartition().getPartitionId(), is("PartitionInfo-foo-foostore:10"));

    System.out.println("RELATIVE ITER TEST:");
    for (int i = 0; i < 10; i++) {
      try (KeyValueIterator<KeyValuePair<Foo>> iter =
          journal.entriesRelative("foo", Foo.class, Long.valueOf(i * 10), 10L).iterator()) {
        assertThat(iter, isNotEmptyIter());
        int j = 0;
        while (iter.hasNext()) {
          Foo foo = iter.next().getValue();
          assertThat(foo, notNullValue());
          System.out.println("i=" + i + ",j=" + j + ",foo=" + dump(foo));
          j += 1;
        }
        assertThat(j, is(10));
      }
    }

    System.out.println("ABSOLUTE ITER TEST:");
    for (int i = 0; i < 10; i++) {
      try (KeyValueIterator<KeyValuePair<Foo>> iter =
          journal.entriesAbsolute("foo", Foo.class, Long.valueOf(i * 10), 10L).iterator()) {
        assertThat(iter, isNotEmptyIter());
        int j = 0;
        while (iter.hasNext()) {
          Foo foo = iter.next().getValue();
          assertThat(foo, notNullValue());
          System.out.println("i=" + i + ",j=" + j + ",foo=" + dump(foo));
          j += 1;
        }
        assertThat(j, is(10));
      }
    }

    try (KeyValueIterator<PartitionInfoSnapshot> theIter = journal.getAllPartitions().iterator()) {
      assertThat(journal.dropPartition(theIter.next().getPartitionId()), is(true));
    }

    System.out.println("PARTITIONS:");

    try (KeyValueIterable<PartitionInfoSnapshot> piter2 = journal.getAllPartitions()) {
      for (PartitionInfoSnapshot snap : piter2) {
        System.out.println(" - part - " + dump(snap));
      }
    }

    try (KeyValueIterator<PartitionInfoSnapshot> theIter = journal.getAllPartitions().iterator()) {
      assertThat(theIter, isIterOfLength(9));
    }

    Long[][] absConfigs =
        { {0L, 10L, 0L}, {10L, 10L, 10L}, {10L, 20L, 20L}, {90L, 10L, 10L}, {90L, 20L, 10L}};

    for (Long[] config : absConfigs) {
      try (KeyValueIterator<KeyValuePair<Foo>> theIter =
          journal.entriesAbsolute("foo", Foo.class, config[0], config[1]).iterator()) {
        assertThat(theIter, isIterOfLength(config[2].intValue()));
      }
    }

    Long[][] relConfigs =
        { {0L, null, 90L}, {0L, 10L, 10L}, {0L, 20L, 20L}, {11L, 79L, 79L}, {11L, null, 79L},
            {10L, 10L, 10L}, {5L, 10L, 10L}, {80L, 10L, 10L}, {90L, 10L, 0L},};

    for (Long[] config : relConfigs) {
      try (KeyValueIterator<KeyValuePair<Foo>> theIter =
          journal.entriesRelative("foo", Foo.class, config[0], config[1]).iterator()) {
        assertThat(theIter, isIterOfLength(config[2].intValue()));
      }
    }

    lifecycle.stop();
    lifecycle.shutdown();
    lifecycle.init();
    lifecycle.start();

    for (Long[] config : absConfigs) {
      try (KeyValueIterator<KeyValuePair<Foo>> theIter =
          journal.entriesAbsolute("foo", Foo.class, config[0], config[1]).iterator()) {
        assertThat(theIter, isIterOfLength(config[2].intValue()));
      }
    }

    for (Long[] config : relConfigs) {
      try (KeyValueIterator<KeyValuePair<Foo>> theIter =
          journal.entriesRelative("foo", Foo.class, config[0], config[1]).iterator()) {
        assertThat(theIter, isIterOfLength(config[2].intValue()));
      }
    }

    journal.append("foo", Foo.class, new Foo("ab", "ac"), TypeValidation.STRICT);

    try (KeyValueIterator<KeyValuePair<Foo>> theIter =
        journal.entriesAbsolute("foo", Foo.class, 10L, null).iterator()) {
      assertThat(theIter, isIterOfLength(91));
    }

    try (KeyValueIterator<KeyValuePair<Foo>> theIter =
        journal.entriesRelative("foo", Foo.class, 0L, null).iterator()) {
      assertThat(theIter, isIterOfLength(91));
    }
  }
}
