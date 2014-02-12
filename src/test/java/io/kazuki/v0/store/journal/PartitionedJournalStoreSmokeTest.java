package io.kazuki.v0.store.journal;


import java.util.Iterator;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import com.jolbox.bonecp.BoneCPDataSource;
import io.kazuki.v0.internal.helper.Configurations;
import io.kazuki.v0.internal.helper.TestHelper;
import io.kazuki.v0.internal.helper.TestSupport;
import io.kazuki.v0.internal.v2schema.Attribute;
import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.easy.EasyPartitionedJournalStoreModule;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.kazuki.v0.internal.helper.TestHelper.dump;
import static io.kazuki.v0.internal.helper.TestHelper.isEmptyIter;
import static io.kazuki.v0.internal.helper.TestHelper.isIterOfLength;
import static io.kazuki.v0.internal.helper.TestHelper.isNotEmptyIter;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;

public class PartitionedJournalStoreSmokeTest
    extends TestSupport
{
  private Injector inject;

  private BoneCPDataSource database;

  private Lifecycle lifecycle;

  private SchemaStore manager;

  private JournalStore journal;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    inject =
        Guice.createInjector(new LifecycleModule("foo"), new EasyPartitionedJournalStoreModule(
            "foo", "test/io/kazuki/v0/store/sequence").withJdbiConfig(Configurations.getJdbi()
            .build()));

    lifecycle = inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));
    database =
        inject.getInstance(com.google.inject.Key.get(BoneCPDataSource.class, Names.named("foo")));
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

    assertThat(manager.createSchema("foo", schema), is(3L));
    assertThat(manager.retrieveSchema("foo"), notNullValue());

    log.info(dump(journal.getActivePartition()));
    assertThat(journal.getActivePartition(), nullValue());
    assertThat(journal.getAllPartitions().iterator(), isEmptyIter());

    for (int i = 0; i < 100; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);
      assertThat(journal.getActivePartition(), notNullValue());
      assertThat(journal.getActivePartition().getMinId(), lessThanOrEqualTo(i + 1L));
      assertThat(journal.getActivePartition().getMaxId(), is(i + 1L));
    }

    Iterator<PartitionInfoSnapshot> piter = journal.getAllPartitions().iterator();
    assertThat(piter, isNotEmptyIter());

    log.info("PARTITIONS:");
    while (piter.hasNext()) {
      PartitionInfoSnapshot snap = piter.next();
      log.info(" - part - " + dump(snap) + " " + snap.getSize());
      assertThat(snap.getSize(), is(10L));
    }

    assertThat(journal.entriesAbsolute("foo", Foo.class, 0L, null).iterator(), isIterOfLength(100));
    assertThat(journal.entriesAbsolute("foo", Foo.class, 0L, 0L).iterator(), isEmptyIter());
    assertThat(journal.entriesAbsolute("foo", Foo.class, 0L, 10L).iterator(), isIterOfLength(10));
    assertThat(journal.entriesAbsolute("foo", Foo.class, 0L, 20L).iterator(), isIterOfLength(20));
    assertThat(journal.entriesAbsolute("foo", Foo.class, 11L, 19L).iterator(), isIterOfLength(19));
    assertThat(journal.entriesAbsolute("foo", Foo.class, 89L, 12L).iterator(), isIterOfLength(11));
    assertThat(journal.entriesAbsolute("foo", Foo.class, 89L, null).iterator(), isIterOfLength(11));
    assertThat(journal.entriesAbsolute("foo", Foo.class, 89L, 0L).iterator(), isEmptyIter());

    assertThat(journal.getAllPartitions().iterator(), isIterOfLength(10));
    assertThat(journal.getActivePartition().getPartitionId(), is("PartitionInfo-foo-foostore:10"));

    log.info("RELATIVE ITER TEST:");
    for (int i = 0; i < 10; i++) {
      Iterator<KeyValuePair<Foo>> iter =
          journal.entriesRelative("foo", Foo.class, Long.valueOf(i * 10), 10L).iterator();
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

    log.info("ABSOLUTE ITER TEST:");
    for (int i = 0; i < 10; i++) {
      Iterator<KeyValuePair<Foo>> iter =
          journal.entriesAbsolute("foo", Foo.class, Long.valueOf(i * 10), 10L).iterator();
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

    assertThat(
        journal.dropPartition(journal.getAllPartitions().iterator().next().getPartitionId()),
        is(true));

    log.info("PARTITIONS:");
    piter = journal.getAllPartitions().iterator();
    while (piter.hasNext()) {
      log.info(" - part - " + dump(piter.next()));
    }

    assertThat(journal.getAllPartitions().iterator(), isIterOfLength(9));

    assertThat(journal.entriesAbsolute("foo", Foo.class, 0L, 10L).iterator(), isEmptyIter());
    assertThat(journal.entriesAbsolute("foo", Foo.class, 10L, 10L).iterator(), isIterOfLength(10));
    assertThat(journal.entriesAbsolute("foo", Foo.class, 10L, 20L).iterator(), isIterOfLength(20));
    assertThat(journal.entriesAbsolute("foo", Foo.class, 90L, 10L).iterator(), isIterOfLength(10));
    assertThat(journal.entriesAbsolute("foo", Foo.class, 90L, 20L).iterator(), isIterOfLength(10));

    assertThat(journal.entriesRelative("foo", Foo.class, 0L, null).iterator(), isIterOfLength(90));
    assertThat(journal.entriesRelative("foo", Foo.class, 0L, 10L).iterator(), isIterOfLength(10));
    assertThat(journal.entriesRelative("foo", Foo.class, 0L, 20L).iterator(), isIterOfLength(20));
    assertThat(journal.entriesRelative("foo", Foo.class, 11L, 79L).iterator(), isIterOfLength(79));
    assertThat(journal.entriesRelative("foo", Foo.class, 11L, null).iterator(), isIterOfLength(79));
    assertThat(journal.entriesRelative("foo", Foo.class, 10L, 10L).iterator(), isIterOfLength(10));
    assertThat(journal.entriesRelative("foo", Foo.class, 5L, 10L).iterator(), isIterOfLength(10));
    assertThat(journal.entriesRelative("foo", Foo.class, 80L, 10L).iterator(), isNotEmptyIter());
    assertThat(journal.entriesRelative("foo", Foo.class, 90L, 10L).iterator(), isEmptyIter());
  }
}
