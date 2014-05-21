/**
 * Copyright 2014 Sunny Gleason
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


import static io.kazuki.v0.internal.helper.TestHelper.dump;
import static io.kazuki.v0.internal.helper.TestHelper.isEmptyIter;
import static io.kazuki.v0.internal.helper.TestHelper.isNotEmptyIter;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;
import io.kazuki.v0.internal.helper.Configurations;
import io.kazuki.v0.internal.helper.TestSupport;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.Version;
import io.kazuki.v0.store.easy.EasyPartitionedJournalStoreModule;
import io.kazuki.v0.store.jdbi.JdbiDataSourceConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueIterator;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.sequence.VersionImpl;

import java.io.File;

import org.hamcrest.Matchers;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class PartitionedJournalStoreClearTest extends TestSupport {
  private JdbiDataSourceConfiguration config;
  private String dbName;
  private Injector inject;
  private Lifecycle lifecycle;
  private SchemaStore manager;
  private JournalStore journal;

  @BeforeTest(alwaysRun = true)
  public void setUp() throws Exception {
    config = Configurations.getJdbi().build();
    dbName = config.getJdbcUrl().substring("jdbc:h2:".length());

    inject =
        Guice.createInjector(new LifecycleModule("bar"), new EasyPartitionedJournalStoreModule(
            "bar", "test/io/kazuki/v0/store/sequence").withJdbiConfig(config));

    lifecycle = inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("bar")));
    manager = inject.getInstance(com.google.inject.Key.get(SchemaStore.class, Names.named("bar")));
    journal = inject.getInstance(com.google.inject.Key.get(JournalStore.class, Names.named("bar")));

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

    new File(dbName + ".h2.db").delete();
    new File(dbName + ".trace.db").delete();
    new File(dbName + ".lock.db").delete();
  }

  @Test
  public void testClear() throws Exception {
    assertThat(manager.retrieveSchema("foo"), Matchers.nullValue());
    assertThat(manager.createSchema("foo", Foo.FOO_SCHEMA), is(VersionImpl.valueOf("$schema:3#2f73aea89adc5337")));
    assertThat(manager.retrieveSchema("foo"), notNullValue());

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

    journal.append("foo", Foo.class, new Foo("k100", "v100"), TypeValidation.STRICT);
    assertThat(journal.getActivePartition().getPartitionId(),
        equalTo("PartitionInfo-bar-barstore:11"));

    try (KeyValueIterator<PartitionInfoSnapshot> piter = journal.getAllPartitions().iterator()) {
      assertThat(piter, isNotEmptyIter());
      log.info("PARTITIONS PRE:");

      while (piter.hasNext()) {
        log.info(" - part - " + dump(piter.next()));
      }
    }

    journal.clear();

    assertThat(manager.createSchema("foo", Foo.FOO_SCHEMA), is(VersionImpl.valueOf("$schema:3#2f73aea89adc5337")));
    assertThat(manager.retrieveSchema("foo"), notNullValue());

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

    journal.append("foo", Foo.class, new Foo("k100", "v100"), TypeValidation.STRICT);
    assertThat(journal.getActivePartition().getPartitionId(),
        equalTo("PartitionInfo-bar-barstore:11"));

    try (KeyValueIterator<PartitionInfoSnapshot> piter2 = journal.getAllPartitions().iterator()) {
      assertThat(piter2, isNotEmptyIter());

      log.info("PARTITIONS POST:");
      while (piter2.hasNext()) {
        log.info(" - part - " + dump(piter2.next()));
      }
    }
  }
}
