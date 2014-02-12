package io.kazuki.v0.store.sequence;


import io.kazuki.v0.internal.helper.TestHelper;
import io.kazuki.v0.internal.helper.TestSupport;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.jdbi.JdbiDataSourceModule;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import com.jolbox.bonecp.BoneCPDataSource;

public class SequenceServiceJdbiImplTest extends TestSupport
{
  private final Injector inject =
      Guice.createInjector(new LifecycleModule("foo"), new JdbiDataSourceModule("foo",
          "test/io/kazuki/v0/store/sequence/jdbi.properties"), new H2SequenceServiceModule("foo",
          "test/io/kazuki/v0/store/sequence/sequence.properties"));

  @Test
  public void testDemo() throws Exception {
    final Lifecycle lifecycle =
        inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));

    final SequenceServiceJdbiImpl seq =
        (SequenceServiceJdbiImpl) inject.getInstance(com.google.inject.Key.<SequenceService>get(
            SequenceService.class, Names.named("foo")));

    BoneCPDataSource database =
        inject.getInstance(com.google.inject.Key.get(BoneCPDataSource.class, Names.named("foo")));

    TestHelper.dropSchema(database);

    lifecycle.init();

    Assert.assertEquals(seq.getCurrentCounters().toString(), "{}");

    Integer fooId = seq.getTypeId("foo", true);
    Assert.assertEquals(fooId, Integer.valueOf(2));
    Assert.assertEquals(seq.getTypeName(fooId), "foo");

    Integer barId = seq.getTypeId("bar", true);
    Assert.assertEquals(barId, Integer.valueOf(3));
    Assert.assertEquals(seq.getTypeName(barId), "bar");

    Assert.assertEquals(seq.getCurrentCounters().toString(), "{}");

    for (int i = 0; i < 10; i++) {
      Key key = seq.nextKey("foo");
      Assert.assertEquals(key.getType(), "foo");
      Assert.assertEquals(key.getId(), Long.valueOf(i + 1));
    }

    Assert.assertEquals(
        seq.getCurrentCounters().toString(),
        "{foo=Counter[type=foo,base=0,offset=10,max=100000]}"
    );

    for (int i = 0; i < 10; i++) {
      Key key = seq.nextKey("bar");
      Assert.assertEquals(key.getType(), "bar");
      Assert.assertEquals(key.getId(), Long.valueOf(i + 1));
    }

    Assert.assertEquals(
        seq.getCurrentCounters().toString(),
        "{foo=Counter[type=foo,base=0,offset=10,max=100000], bar=Counter[type=bar,base=0,offset=10,max=100000]}"
    );

    lifecycle.shutdown();
    lifecycle.init();

    Assert.assertEquals(seq.nextKey("foo").getId(), Long.valueOf(11));
    Assert.assertEquals(seq.nextKey("bar").getId(), Long.valueOf(11));

    seq.clear(true, true);
  }
}
