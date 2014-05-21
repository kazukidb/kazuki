/**
 * Copyright 2014 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kazuki.v0.store.journal;


import static io.kazuki.v0.internal.helper.TestHelper.isIterOfLength;
import static org.hamcrest.MatcherAssert.assertThat;
import io.kazuki.v0.internal.helper.TestSupport;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.easy.EasyPartitionedJournalStoreModule;
import io.kazuki.v0.store.jdbi.JdbiDataSourceConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueIterator;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.keyvalue.KeyValueStoreConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.TypeValidation;

import java.io.File;

import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class PartitionedJournalStoreScaleTest extends TestSupport {
  private static final String dbName = "target/testdb.db";
  private Injector inject;
  private Lifecycle lifecycle;
  private JournalStore journal;

  @BeforeTest(alwaysRun = true)
  public void setUp() throws Exception {
    KeyValueStoreConfiguration kvConfig =
        new KeyValueStoreConfiguration.Builder().withDbType("h2").withDataType("foo")
            .withGroupName("foo").withStoreName("foostore").withPartitionName("default")
            .withPartitionSize(1000L).withStrictTypeCreation(true).build();

    JdbiDataSourceConfiguration dbConfig =
        new JdbiDataSourceConfiguration("org.h2.Driver", "jdbc:h2:" + dbName, "sa",
            "not_really_used", 25, 25);

    inject =
        Guice.createInjector(new LifecycleModule("foo"), new EasyPartitionedJournalStoreModule(
            "foo", "test/io/kazuki/v0/store/sequence").withKeyValueStoreConfig(kvConfig)
            .withJdbiConfig(dbConfig));

    lifecycle = inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));
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

    journal.clear();
  }

  @AfterTest(alwaysRun = true)
  public void tearDown() throws Exception {
    lifecycle.stop();
    lifecycle.shutdown();
    new File(dbName + ".h2.db").delete();
    new File(dbName + ".trace.db").delete();
    new File(dbName + ".lock.db").delete();
  }

  @Test
  public void testDemo() throws Exception {
    try (KeyValueIterator<PartitionInfoSnapshot> theIter = journal.getAllPartitions().iterator()) {
      assertThat(theIter, isIterOfLength(0));
    }

    for (int i = 1; i <= 50000; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);

      if (i % 10000 == 0) {
        log.info(new DateTime() + " " + i + " " + +(new File(dbName + ".h2.db")).length());
      }

      if (i % 10000 == 0 && i > 0) {
        for (int j = 0; j < 5; j++) {
          try (KeyValueIterator<PartitionInfoSnapshot> theIter =
              journal.getAllPartitions().iterator()) {
            journal.dropPartition(theIter.next().getPartitionId());
          }
        }
      }
    }

    try (KeyValueIterator<KeyValuePair<Foo>> theIter =
        journal.entriesRelative("foo", Foo.class, SortDirection.ASCENDING, 0L, 1L).iterator()) {
      assertThat(theIter.next().getValue().getFooKey(), Matchers.is("k25001"));
    }

    try (KeyValueIterator<PartitionInfoSnapshot> theIter = journal.getAllPartitions().iterator()) {
      assertThat(theIter, isIterOfLength(25));
    }

    journal.clear();

    try (KeyValueIterator<PartitionInfoSnapshot> theIter = journal.getAllPartitions().iterator()) {
      assertThat(theIter, isIterOfLength(0));
    }

    for (int i = 1; i <= 50000; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);

      if (i % 10000 == 0) {
        log.info(new DateTime() + " " + i + " " + +(new File(dbName + ".h2.db")).length());
      }

      if (i % 10000 == 0 && i > 0) {
        for (int j = 0; j < 5; j++) {
          try (KeyValueIterator<PartitionInfoSnapshot> theIter =
              journal.getAllPartitions().iterator()) {
            journal.dropPartition(theIter.next().getPartitionId());
          }
        }
      }
    }

    try (KeyValueIterator<KeyValuePair<Foo>> theIter =
        journal.entriesRelative("foo", Foo.class, SortDirection.ASCENDING, 0L, 1L).iterator()) {
      assertThat(theIter.next().getValue().getFooKey(), Matchers.is("k25001"));
    }

    try (KeyValueIterator<PartitionInfoSnapshot> theIter = journal.getAllPartitions().iterator()) {
      assertThat(theIter, isIterOfLength(25));
    }
  }
}
