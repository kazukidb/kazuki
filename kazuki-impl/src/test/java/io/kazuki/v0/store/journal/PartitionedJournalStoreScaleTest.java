/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.kazuki.v0.store.journal;


import static io.kazuki.v0.internal.helper.TestHelper.isIterOfLength;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import io.kazuki.v0.internal.helper.Configurations;
import io.kazuki.v0.internal.helper.TestSupport;
import io.kazuki.v0.store.Foo;
import io.kazuki.v0.store.guice.KazukiModule;
import io.kazuki.v0.store.jdbi.JdbiDataSourceConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueIterator;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.sequence.VersionImpl;

import java.io.File;

import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class PartitionedJournalStoreScaleTest extends TestSupport {
  private static final String dbName = "target/testdb.db";
  private Injector inject;
  private Lifecycle lifecycle;
  private SchemaStore manager;
  private JournalStore journal;

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    JdbiDataSourceConfiguration dbConfig =
        new JdbiDataSourceConfiguration("org.h2.Driver", "jdbc:h2:" + dbName, "sa",
            "not_really_used", 25, 25);

    inject =
        Guice.createInjector(new KazukiModule.Builder("foo")
            .withJdbiConfiguration("foo", dbConfig)
            .withSequenceServiceConfiguration("foo",
                Configurations.getSequence("foo", "foostore").build())
            .withJournalStoreConfiguration(
                "foo",
                Configurations.getKeyValue("foo", "foostore").withDataType("foo")
                    .withPartitionSize(1000L).build()).build());

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

    journal.clear();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() throws Exception {
    lifecycle.stop();
    lifecycle.shutdown();
    new File(dbName + ".h2.db").delete();
    new File(dbName + ".trace.db").delete();
    new File(dbName + ".lock.db").delete();
  }

  @Test
  public void testDemo() throws Exception {
    assertThat(manager.retrieveSchema("foo"), Matchers.nullValue());
    assertThat(manager.createSchema("foo", Foo.FOO_SCHEMA),
        is(VersionImpl.valueOf("$schema:3#2f73aea89adc5337")));
    assertThat(manager.retrieveSchema("foo"), notNullValue());

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
