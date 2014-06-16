/**
 * Copyright 2014 Sunny Gleason and original author or authors
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
package io.kazuki.v0.store.sequence;


import io.kazuki.v0.internal.helper.Configurations;
import io.kazuki.v0.internal.helper.TestHelper;
import io.kazuki.v0.internal.helper.TestSupport;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.config.ConfigurationProvider;
import io.kazuki.v0.store.guice.KazukiModule;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.sequence.SequenceServiceJdbiImpl.Counter;

import java.util.Map;

import javax.sql.DataSource;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class SequenceServiceJdbiImplTest extends TestSupport {
  private final Injector inject;

  public SequenceServiceJdbiImplTest() {
    inject =
        Guice.createInjector(new KazukiModule.Builder("foo")
            .withJdbiConfiguration("foo", Configurations.getJdbi().build())
            .withSequenceServiceConfiguration("foo",
                Configurations.getSequence("foo", "foo").withIncrementBlockSize(100_000L).build())
            .build());
  }

  @Test
  public void testDemo() throws Exception {
    final Lifecycle lifecycle =
        inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));

    final SequenceServiceJdbiImpl seq =
        (SequenceServiceJdbiImpl) inject.getInstance(com.google.inject.Key.<SequenceService>get(
            SequenceService.class, Names.named("foo")));

    final SequenceServiceConfiguration cfg =
        new ConfigurationProvider<SequenceServiceConfiguration>("foo",
            SequenceServiceConfiguration.class,
            "test/io/kazuki/v0/store/sequence/sequence.properties", false).get();

    DataSource database =
        inject.getInstance(com.google.inject.Key.get(DataSource.class, Names.named("foo")));

    lifecycle.init();

    TestHelper.dropSchema(database);

    lifecycle.stop();
    lifecycle.shutdown();
    lifecycle.init();
    lifecycle.start();

    Map<String, Counter> counters = seq.getCurrentCounters();
    Assert.assertEquals(counters.toString(), "{}");

    Integer fooId = seq.getTypeId("foo", true);
    Assert.assertEquals(fooId, Integer.valueOf(2));
    Assert.assertEquals(seq.getTypeName(fooId), "foo");

    Integer barId = seq.getTypeId("bar", true);
    Assert.assertEquals(barId, Integer.valueOf(3));
    Assert.assertEquals(seq.getTypeName(barId), "bar");

    Assert.assertEquals(counters.toString(), "{}");

    for (int i = 0; i < 10; i++) {
      Key key = seq.nextKey("foo");
      ResolvedKey resolvedKey = seq.resolveKey(key);
      Assert.assertEquals(key.getTypePart(), "foo");
      Assert.assertEquals(resolvedKey.getIdentifierHi(), 0L);
      Assert.assertEquals(resolvedKey.getIdentifierLo(), (long) i + 1L);
    }

    Map<String, Counter> counters2 = seq.getCurrentCounters();
    Assert
        .assertEquals(counters2.toString(), "{foo=Counter[type=foo,base=0,offset=10,max=100000]}");

    seq.clear(false, false);

    for (int i = 0; i < (cfg.getIncrementBlockSize() * 3) + 1; i++) {
      Key key = seq.nextKey("foo");
      ResolvedKey resolvedKey = seq.resolveKey(key);
      Assert.assertEquals(key.getTypePart(), "foo");
      Assert.assertEquals(resolvedKey.getIdentifierHi(), 0L);
      Assert.assertEquals(resolvedKey.getIdentifierLo(), (long) i + 1L);
    }

    Map<String, Counter> counters3 = seq.getCurrentCounters();
    Assert.assertEquals(counters3.toString(),
        "{foo=Counter[type=foo,base=300000,offset=1,max=400000]}");

    for (int i = 0; i < (cfg.getIncrementBlockSize() * 3) + 1; i++) {
      Key key = seq.nextKey("bar");
      ResolvedKey resolvedKey = seq.resolveKey(key);
      Assert.assertEquals(key.getTypePart(), "bar");
      Assert.assertEquals(resolvedKey.getIdentifierHi(), 0L);
      Assert.assertEquals(resolvedKey.getIdentifierLo(), (long) i + 1L);
    }

    lifecycle.shutdown();
    lifecycle.init();

    Assert.assertEquals(seq.resolveKey(seq.nextKey("foo")).getIdentifierLo(), 300002L);
    Assert.assertEquals(seq.resolveKey(seq.nextKey("bar")).getIdentifierLo(), 300002L);

    seq.clear(true, true);
  }
}
