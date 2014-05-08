package io.kazuki.v0.store.index;

import io.kazuki.v0.internal.helper.TestSupport;
import io.kazuki.v0.store.Everything;
import io.kazuki.v0.store.Everything.TestEnum;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.index.query.QueryBuilder;
import io.kazuki.v0.store.index.query.QueryOperator;
import io.kazuki.v0.store.index.query.ValueType;
import io.kazuki.v0.store.keyvalue.KeyValueIterator;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.schema.model.Schema;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.google.inject.Injector;
import com.google.inject.name.Names;

public abstract class SecondaryIndexStoreAttributesTestBase extends TestSupport {
  protected abstract Injector getInjector();

  @Test
  public void testDemo() throws Exception {
    Injector inject = getInjector();

    final Lifecycle lifecycle =
        inject.getInstance(com.google.inject.Key.get(Lifecycle.class, Names.named("foo")));

    KeyValueStore store =
        inject.getInstance(com.google.inject.Key.get(KeyValueStore.class, Names.named("foo")));

    SecondaryIndexStore index =
        inject
            .getInstance(com.google.inject.Key.get(SecondaryIndexStore.class, Names.named("foo")));

    SchemaStore manager =
        inject.getInstance(com.google.inject.Key.get(SchemaStore.class, Names.named("foo")));

    lifecycle.init();
    lifecycle.start();

    Assert.assertFalse(store.iterators().iterator("$schema", Schema.class, SortDirection.ASCENDING)
        .hasNext());

    manager.createSchema("everything", Everything.EVERYTHING_SCHEMA);

    Everything e1 = new Everything();
    e1.theEnum = TestEnum.FOUR;

    Key k0 = store.create("everything", Everything.class, e1, TypeValidation.STRICT).getKey();


    try (KeyValueIterator<Key> iter =
        index.queryWithoutPagination(
            "everything",
            Everything.class,
            "theEnum",
            new QueryBuilder().andMatchesSingle("theEnum", QueryOperator.EQ, ValueType.STRING,
                "FOUR").build(), SortDirection.ASCENDING, null, null).iterator()) {

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals(k0, iter.next());

      Assert.assertFalse(iter.hasNext());
    }

    try (KeyValueIterator<Key> iter =
        index.queryWithoutPagination(
            "everything",
            Everything.class,
            "theEnum",
            new QueryBuilder().andMatchesSingle("theEnum", QueryOperator.EQ, ValueType.STRING,
                "ONE").build(), SortDirection.ASCENDING, null, null).iterator()) {
      Assert.assertFalse(iter.hasNext());
    }

    store.clear(false, false);
  }
}
