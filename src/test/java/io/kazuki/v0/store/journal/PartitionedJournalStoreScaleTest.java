package io.kazuki.v0.store.journal;


import java.io.File;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import com.jolbox.bonecp.BoneCPDataSource;
import io.kazuki.v0.internal.helper.TestHelper;
import io.kazuki.v0.internal.helper.TestSupport;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.easy.EasyPartitionedJournalStoreModule;
import io.kazuki.v0.store.jdbi.JdbiDataSourceConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueStoreConfiguration;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.TypeValidation;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.kazuki.v0.internal.helper.TestHelper.isIterOfLength;
import static org.hamcrest.MatcherAssert.*;

public class PartitionedJournalStoreScaleTest
    extends TestSupport
{
  private static final String dbName = "target/testdb.db";

  private Injector inject;

  private BoneCPDataSource database;

  private Lifecycle lifecycle;

  private JournalStore journal;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    KeyValueStoreConfiguration kvConfig =
        new KeyValueStoreConfiguration.Builder().withDbType("h2").withDataType("foo")
            .withGroupName("foo").withStoreName("foostore").withPartitionName("default")
            .withPartitionSize(1000L).withStrictTypeCreation(true).build();

    JdbiDataSourceConfiguration dbConfig =
        new JdbiDataSourceConfiguration("org.h2.Driver", "jdbc:h2:" + dbName
            + ";DB_CLOSE_ON_EXIT=TRUE", "sa", "not_really_used", 25, 25);

    inject =
        Guice.createInjector(new LifecycleModule("foo"), new EasyPartitionedJournalStoreModule(
            "foo", "test/io/kazuki/v0/store/sequence").withKeyValueStoreConfig(kvConfig)
            .withJdbiConfig(dbConfig));

    lifecycle = inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));
    database =
        inject.getInstance(com.google.inject.Key.get(BoneCPDataSource.class, Names.named("foo")));
    journal = inject.getInstance(com.google.inject.Key.get(JournalStore.class, Names.named("foo")));

    TestHelper.dropSchema(database);

    lifecycle.init();
    lifecycle.start();

    journal.clear();
  }

  @AfterClass(alwaysRun = true)
  public void shutDown() throws Exception {
    new File(dbName + ".h2.db").delete();
    new File(dbName + ".trace.db").delete();
    new File(dbName + ".lock.db").delete();
  }

  @Test
  public void testDemo() throws Exception {
    assertThat(journal.getAllPartitions().iterator(), isIterOfLength(0));

    for (int i = 1; i <= 50000; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);

      if (i % 10000 == 0) {
        log.info(new DateTime() + " " + i + " " + +(new File(dbName + ".h2.db")).length());
      }

      if (i % 10000 == 0 && i > 0) {
        journal.dropPartition(journal.getAllPartitions().iterator().next().getPartitionId());
        journal.dropPartition(journal.getAllPartitions().iterator().next().getPartitionId());
        journal.dropPartition(journal.getAllPartitions().iterator().next().getPartitionId());
        journal.dropPartition(journal.getAllPartitions().iterator().next().getPartitionId());
        journal.dropPartition(journal.getAllPartitions().iterator().next().getPartitionId());
      }
    }

    assertThat(journal.entriesRelative("foo", Foo.class, 0L, 1L).iterator().next().getValue()
        .getFooKey(), Matchers.is("k25001"));
    assertThat(journal.getAllPartitions().iterator(), isIterOfLength(25));

    journal.clear();

    assertThat(journal.getAllPartitions().iterator(), isIterOfLength(0));

    for (int i = 1; i <= 50000; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);

      if (i % 10000 == 0) {
        log.info(new DateTime() + " " + i + " " + +(new File(dbName + ".h2.db")).length());
      }

      if (i % 10000 == 0 && i > 0) {
        journal.dropPartition(journal.getAllPartitions().iterator().next().getPartitionId());
        journal.dropPartition(journal.getAllPartitions().iterator().next().getPartitionId());
        journal.dropPartition(journal.getAllPartitions().iterator().next().getPartitionId());
        journal.dropPartition(journal.getAllPartitions().iterator().next().getPartitionId());
        journal.dropPartition(journal.getAllPartitions().iterator().next().getPartitionId());
      }
    }

    assertThat(journal.entriesRelative("foo", Foo.class, 0L, 1L).iterator().next().getValue()
        .getFooKey(), Matchers.is("k25001"));
    assertThat(journal.getAllPartitions().iterator(), isIterOfLength(25));
  }
}
