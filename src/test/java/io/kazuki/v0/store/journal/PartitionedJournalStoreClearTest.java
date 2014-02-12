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
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.kazuki.v0.internal.helper.TestHelper.dump;
import static io.kazuki.v0.internal.helper.TestHelper.isEmptyIter;
import static io.kazuki.v0.internal.helper.TestHelper.isNotEmptyIter;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;

public class PartitionedJournalStoreClearTest
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
        Guice.createInjector(new LifecycleModule("bar"), new EasyPartitionedJournalStoreModule(
            "bar", "test/io/kazuki/v0/store/sequence").withJdbiConfig(Configurations.getJdbi()
            .build()));

    lifecycle = inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("bar")));
    database =
        inject.getInstance(com.google.inject.Key.get(BoneCPDataSource.class, Names.named("bar")));
    manager = inject.getInstance(com.google.inject.Key.get(SchemaStore.class, Names.named("bar")));
    journal = inject.getInstance(com.google.inject.Key.get(JournalStore.class, Names.named("bar")));

    TestHelper.dropSchema(database);

    lifecycle.init();
  }

  @Test
  public void testClear() throws Exception {
    Schema schema =
        new Schema(ImmutableList.of(new Attribute("fooKey", Attribute.Type.UTF8_SMALLSTRING, null,
            true), new Attribute("fooValue", Attribute.Type.UTF8_SMALLSTRING, null, true)));

    assertThat(manager.createSchema("foo", schema), is(3L));
    assertThat(manager.retrieveSchema("foo"), notNullValue());

    assertThat(journal.getActivePartition(), nullValue());
    assertThat(journal.getAllPartitions().iterator(), isEmptyIter());

    for (int i = 0; i < 100; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);
      assertThat(journal.getActivePartition(), notNullValue());
      assertThat(journal.getActivePartition().getMinId(), lessThanOrEqualTo(i + 1L));
      assertThat(journal.getActivePartition().getMaxId(), is(i + 1L));
    }

    assertThat(journal.getActivePartition().getPartitionId(),
        equalTo("PartitionInfo-bar-barstore:10"));

    Iterator<PartitionInfoSnapshot> piter = journal.getAllPartitions().iterator();
    assertThat(piter, isNotEmptyIter());

    log.info("PARTITIONS PRE:");
    while (piter.hasNext()) {
      log.info(" - part - " + dump(piter.next()));
    }

    journal.clear();

    assertThat(manager.createSchema("foo", schema), is(3L));
    assertThat(manager.retrieveSchema("foo"), notNullValue());

    for (int i = 0; i < 100; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);
      assertThat(journal.getActivePartition(), notNullValue());
      assertThat(journal.getActivePartition().getMinId(), lessThanOrEqualTo(i + 1L));
      assertThat(journal.getActivePartition().getMaxId(), is(i + 1L));
    }

    assertThat(journal.getActivePartition().getPartitionId(),
        equalTo("PartitionInfo-bar-barstore:10"));

    Iterator<PartitionInfoSnapshot> piter2 = journal.getAllPartitions().iterator();
    assertThat(piter2, isNotEmptyIter());

    log.info("PARTITIONS POST:");
    while (piter2.hasNext()) {
      log.info(" - part - " + dump(piter2.next()));
    }
  }
}
