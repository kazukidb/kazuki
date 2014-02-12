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
import io.kazuki.v0.store.easy.EasyJournalStoreModule;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import junit.framework.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.kazuki.v0.internal.helper.TestHelper.dump;

public class SimpleJournalStoreTest
    extends TestSupport
{
  private Injector inject;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    inject = Guice.createInjector(new LifecycleModule("foo"),
        new EasyJournalStoreModule("foo", "test/io/kazuki/v0/store/sequence")
            .withJdbiConfig(Configurations.getJdbi().build()));
  }

  @Test
  public void testDemo() throws Exception {
    final Lifecycle lifecycle =
        inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));

    BoneCPDataSource database =
        inject.getInstance(com.google.inject.Key.get(BoneCPDataSource.class, Names.named("foo")));

    SchemaStore manager =
        inject.getInstance(com.google.inject.Key.get(SchemaStore.class, Names.named("foo")));

    JournalStore journal =
        inject.getInstance(com.google.inject.Key.get(JournalStore.class, Names.named("foo")));

    TestHelper.dropSchema(database);

    lifecycle.init();

    Schema schema =
        new Schema(ImmutableList.of(new Attribute("fooKey", Attribute.Type.UTF8_SMALLSTRING, null,
            true), new Attribute("fooValue", Attribute.Type.UTF8_SMALLSTRING, null, true)));

    Assert.assertEquals(manager.createSchema("foo", schema), 2L);
    Assert.assertNotNull(manager.retrieveSchema("foo"));

    for (int i = 0; i < 100; i++) {
      journal.append("foo", Foo.class, new Foo("k" + i, "v" + i), TypeValidation.STRICT);
    }

    log.info("ITER TEST:");
    for (int i = 0; i < 10; i++) {
      Iterator<KeyValuePair<Foo>> iter =
          journal.entriesAbsolute("foo", Foo.class, Long.valueOf(i * 10), 10L).iterator();
      Assert.assertTrue(iter.hasNext());
      int j = 0;
      while (iter.hasNext()) {
        Foo foo = iter.next().getValue();
        Assert.assertNotNull(foo);
        log.info("i=" + i + ",j=" + j + ",foo=" + dump(foo));
        j += 1;
      }
    }
  }
}
