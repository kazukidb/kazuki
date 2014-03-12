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
import io.kazuki.v0.internal.helper.TestSupport;
import io.kazuki.v0.internal.v2schema.Attribute;
import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.easy.EasyPartitionedJournalStoreModule;
import io.kazuki.v0.store.jdbi.JdbiDataSourceConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValueIterator;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.sequence.KeyImpl;

import java.io.File;

import org.hamcrest.Matchers;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class PartitionedJournalStoreSmokeTest extends TestSupport {
  private Injector inject;
  private JdbiDataSourceConfiguration config;
  private String dbName;
  private Lifecycle lifecycle;
  private SchemaStore manager;
  private JournalStore journal;

  @BeforeTest(alwaysRun = true)
  public void setUp() throws Exception {
    config = Configurations.getJdbi().build();
    dbName = config.getJdbcUrl().substring("jdbc:h2:".length());

    inject =
        Guice.createInjector(new LifecycleModule("foo"), new EasyPartitionedJournalStoreModule(
            "foo", "test/io/kazuki/v0/store/sequence").withJdbiConfig(config));

    lifecycle = inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));
    manager = inject.getInstance(com.google.inject.Key.get(SchemaStore.class, Names.named("foo")));
    journal = inject.getInstance(com.google.inject.Key.get(JournalStore.class, Names.named("foo")));

    new File(dbName + ".h2.db").delete();
    new File(dbName + ".trace.db").delete();
    new File(dbName + ".lock.db").delete();

    lifecycle.init();
    lifecycle.start();
    lifecycle.stop();
    lifecycle.shutdown();
    lifecycle.init();
    lifecycle.start();
  }

  @AfterTest(alwaysRun = true)
  public void tearDown() throws Exception {
    lifecycle.stop();
    lifecycle.shutdown();

    String dbName = config.getJdbcUrl().substring("jdbc:h2:".length());

    new File(dbName + ".h2.db").delete();
    new File(dbName + ".trace.db").delete();
    new File(dbName + ".lock.db").delete();
  }

  @Test(singleThreaded = true)
  public void testDemo() throws Exception {
    assertThat(manager.retrieveSchema("foo"), Matchers.nullValue());
    assertThat(journal.getAllPartitions().iterator(), isEmptyIter());

    Schema schema =
        new Schema(ImmutableList.of(new Attribute("fooKey", Attribute.Type.UTF8_SMALLSTRING, null,
            true), new Attribute("fooValue", Attribute.Type.UTF8_SMALLSTRING, null, true)));

    assertThat(manager.createSchema("foo", schema), is(KeyImpl.valueOf("$schema:3")));
    assertThat(manager.retrieveSchema("foo"), notNullValue());

    log.info(dump(journal.getActivePartition()));
    assertThat(journal.getActivePartition(), nullValue());

    try (KeyValueIterator<PartitionInfoSnapshot> theIter = journal.getAllPartitions().iterator()) {
      assertThat(theIter, isEmptyIter());
    }

    for (int i = 0; i < 100; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);
      if ((i + 1) % 10 != 0) {
        assertThat(journal.getActivePartition(), notNullValue());
        assertThat(journal.getActivePartition().getMinId(), lessThanOrEqualTo(i + 1L));
        assertThat(journal.getActivePartition().getMaxId(), is(i + 1L));
      } else {
        assertThat(journal.getActivePartition(), nullValue());
      }
    }

    try (KeyValueIterator<PartitionInfoSnapshot> piter = journal.getAllPartitions().iterator()) {
      assertThat(piter, isNotEmptyIter());

      log.info("PARTITIONS:");
      while (piter.hasNext()) {
        PartitionInfoSnapshot snap = piter.next();
        log.info(" - part - " + dump(snap) + " " + snap.getSize());
        assertThat(snap.getSize(), is(10L));
      }
    }

    Long[][] configs =
        { {0L, null, 100L}, {0L, 0L, 0L}, {0L, 10L, 10L}, {0L, 20L, 20L}, {11L, 19L, 19L},
            {89L, 12L, 11L}, {89L, null, 11L}, {89L, 0L, 0L}};

    for (Long[] config : configs) {
      try (KeyValueIterator<KeyValuePair<Foo>> theIter =
          journal.entriesAbsolute("foo", Foo.class, SortDirection.ASCENDING, config[0], config[1])
              .iterator()) {
        assertThat(theIter, isIterOfLength(config[2].intValue()));
      }
    }

    try (KeyValueIterator<PartitionInfoSnapshot> theIter = journal.getAllPartitions().iterator()) {
      assertThat(theIter, isIterOfLength(10));
    }

    journal.append("foo", Foo.class, new Foo("k100", "v100"), TypeValidation.STRICT);
    assertThat(journal.getActivePartition().getPartitionId(), is("PartitionInfo-foo-foostore:11"));

    log.info("RELATIVE ITER TEST:");
    for (int i = 0; i < 10; i++) {
      try (KeyValueIterator<KeyValuePair<Foo>> iter =
          journal.entriesRelative("foo", Foo.class, SortDirection.ASCENDING, Long.valueOf(i * 10),
              10L).iterator()) {
        assertThat(iter, isNotEmptyIter());
        int j = 0;
        while (iter.hasNext()) {
          Foo foo = iter.next().getValue();
          assertThat(foo, notNullValue());
          log.info("i=" + i + ",j=" + j + ",foo=" + dump(foo));
          j += 1;
        }
        assertThat(j, is(10));
      }

      try (KeyValueIterator<KeyValuePair<Foo>> iter =
          journal.entriesRelative("foo", Foo.class, SortDirection.DESCENDING, Long.valueOf(i * 10),
              10L).iterator()) {
        assertThat(iter, isNotEmptyIter());
        int j = 10;
        while (iter.hasNext()) {
          Foo foo = iter.next().getValue();
          assertThat(foo, notNullValue());
          log.info("i=" + i + ",j=" + j + ",foo=" + dump(foo));
          j -= 1;
        }
        assertThat(j, is(0));
      }
    }

    log.info("ABSOLUTE ITER TEST:");
    for (int i = 0; i < 10; i++) {
      try (KeyValueIterator<KeyValuePair<Foo>> iter =
          journal.entriesAbsolute("foo", Foo.class, SortDirection.ASCENDING, Long.valueOf(i * 10),
              10L).iterator()) {
        assertThat(iter, isNotEmptyIter());
        int j = 0;
        while (iter.hasNext()) {
          Foo foo = iter.next().getValue();
          assertThat(foo, notNullValue());
          log.info("i=" + i + ",j=" + j + ",foo=" + dump(foo));
          j += 1;
        }
        assertThat(j, is(10));
      }
    }

    try (KeyValueIterator<PartitionInfoSnapshot> theIter = journal.getAllPartitions().iterator()) {
      assertThat(journal.dropPartition(theIter.next().getPartitionId()), is(true));
    }

    log.info("PARTITIONS:");

    try (KeyValueIterable<PartitionInfoSnapshot> piter2 = journal.getAllPartitions()) {
      for (PartitionInfoSnapshot snap : piter2) {
        log.info(" - part - " + dump(snap));
      }
    }

    try (KeyValueIterator<PartitionInfoSnapshot> theIter = journal.getAllPartitions().iterator()) {
      assertThat(theIter, isIterOfLength(10));
    }

    Long[][] absConfigs =
        { {0L, 10L, 0L}, {10L, 10L, 10L}, {10L, 20L, 20L}, {90L, 10L, 10L}, {90L, 20L, 11L}};

    for (Long[] config : absConfigs) {
      try (KeyValueIterator<KeyValuePair<Foo>> theIter =
          journal.entriesAbsolute("foo", Foo.class, SortDirection.ASCENDING, config[0], config[1])
              .iterator()) {
        assertThat(theIter, isIterOfLength(config[2].intValue()));
      }
    }

    Long[][] relConfigs =
        { {0L, null, 91L}, {1L, null, 90L}, {0L, 10L, 10L}, {0L, 20L, 20L}, {11L, 79L, 79L},
            {11L, null, 80L}, {10L, 10L, 10L}, {5L, 10L, 10L}, {80L, 10L, 10L}, {90L, 10L, 1L},};

    for (Long[] config : relConfigs) {
      try (KeyValueIterator<KeyValuePair<Foo>> theIter =
          journal.entriesRelative("foo", Foo.class, SortDirection.ASCENDING, config[0], config[1])
              .iterator()) {
        assertThat(theIter, isIterOfLength(config[2].intValue()));
      }

      try (KeyValueIterator<KeyValuePair<Foo>> theIter =
          journal.entriesRelative("foo", Foo.class, SortDirection.DESCENDING, config[2], config[1])
              .iterator()) {
        int limit = 91 - config[2].intValue();

        if (config[1] != null) {
          limit = Math.min(limit, config[1].intValue());
        }

        assertThat(theIter, isIterOfLength(limit));
      }
    }

    lifecycle.stop();
    lifecycle.shutdown();
    lifecycle.init();
    lifecycle.start();

    for (Long[] config : absConfigs) {
      try (KeyValueIterator<KeyValuePair<Foo>> theIter =
          journal.entriesAbsolute("foo", Foo.class, SortDirection.ASCENDING, config[0], config[1])
              .iterator()) {
        assertThat(theIter, isIterOfLength(config[2].intValue()));
      }
    }

    for (Long[] config : relConfigs) {
      try (KeyValueIterator<KeyValuePair<Foo>> theIter =
          journal.entriesRelative("foo", Foo.class, SortDirection.ASCENDING, config[0], config[1])
              .iterator()) {
        assertThat(theIter, isIterOfLength(config[2].intValue()));
      }

      try (KeyValueIterator<KeyValuePair<Foo>> theIter =
          journal.entriesRelative("foo", Foo.class, SortDirection.DESCENDING, config[2], config[1])
              .iterator()) {
        int limit = 91 - config[2].intValue();

        if (config[1] != null) {
          limit = Math.min(limit, config[1].intValue());
        }

        assertThat(theIter, isIterOfLength(limit));
      }
    }

    journal.append("foo", Foo.class, new Foo("ab", "ac"), TypeValidation.STRICT);

    try (KeyValueIterator<KeyValuePair<Foo>> theIter =
        journal.entriesAbsolute("foo", Foo.class, SortDirection.ASCENDING, 10L, null).iterator()) {
      assertThat(theIter, isIterOfLength(92));
    }

    try (KeyValueIterator<KeyValuePair<Foo>> theIter =
        journal.entriesRelative("foo", Foo.class, SortDirection.ASCENDING, 0L, null).iterator()) {
      assertThat(theIter, isIterOfLength(92));
    }
  }
}
