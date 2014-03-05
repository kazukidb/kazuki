package io.kazuki.v0.store.sequence;


import io.kazuki.v0.internal.helper.TestHelper;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.jdbi.H2DataSourceModule;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.sequence.SequenceServiceJdbiImpl.Counter;

import java.util.Map;

import javax.sql.DataSource;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

public class SequenceServiceJdbiImplTest {
  private final Injector inject;

  public SequenceServiceJdbiImplTest() {
    inject =
        Guice.createInjector(new LifecycleModule("foo"), new H2DataSourceModule("foo",
            "test/io/kazuki/v0/store/sequence/jdbi.properties"), new H2SequenceServiceModule("foo",
            "test/io/kazuki/v0/store/sequence/sequence.properties"));
  }

  @Test
  public void testDemo() throws Exception {
    final Lifecycle lifecycle =
        inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));

    final SequenceServiceJdbiImpl seq =
        (SequenceServiceJdbiImpl) inject.getInstance(com.google.inject.Key.<SequenceService>get(
            SequenceService.class, Names.named("foo")));

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

    for (int i = 0; i < 10; i++) {
      Key key = seq.nextKey("bar");
      ResolvedKey resolvedKey = seq.resolveKey(key);
      Assert.assertEquals(key.getTypePart(), "bar");
      Assert.assertEquals(resolvedKey.getIdentifierHi(), 0L);
      Assert.assertEquals(resolvedKey.getIdentifierLo(), (long) i + 1L);
    }

    lifecycle.shutdown();
    lifecycle.init();

    Assert.assertEquals(seq.resolveKey(seq.nextKey("foo")).getIdentifierLo(), 11L);
    Assert.assertEquals(seq.resolveKey(seq.nextKey("bar")).getIdentifierLo(), 11L);

    seq.clear(true, true);
  }
}
