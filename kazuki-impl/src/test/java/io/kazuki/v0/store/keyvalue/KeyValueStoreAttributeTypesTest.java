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
package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.internal.helper.Configurations;
import io.kazuki.v0.internal.helper.EncodingHelper;
import io.kazuki.v0.store.Everything;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Everything.TestEnum;
import io.kazuki.v0.store.Version;
import io.kazuki.v0.store.easy.EasyKeyValueStoreModule;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleModule;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;

import java.math.BigInteger;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

/**
 * Attribute Types smoke test.
 */
@Test
public class KeyValueStoreAttributeTypesTest {
  protected Lifecycle lifecycle;
  protected SchemaStore schema;
  protected KeyValueStore kvStore;

  @BeforeMethod
  public void prepare() throws Exception {
    Injector injector =
        Guice.createInjector(
            new LifecycleModule(ExampleStore.STORE_NAME),
            new EasyKeyValueStoreModule(ExampleStore.STORE_NAME, null)
                .withJdbiConfig(Configurations.getJdbi().build())
                .withKeyValueStoreConfig(
                    Configurations.getKeyValue(ExampleStore.GROUP_NAME, ExampleStore.STORE_NAME)
                        .build())
                .withSequenceConfig(
                    Configurations.getSequence(ExampleStore.GROUP_NAME, ExampleStore.STORE_NAME)
                        .build()));

    lifecycle =
        injector.getBinding(Key.get(Lifecycle.class, Names.named(ExampleStore.STORE_NAME)))
            .getProvider().get();

    schema =
        injector.getBinding(Key.get(SchemaStore.class, Names.named(ExampleStore.STORE_NAME)))
            .getProvider().get();

    kvStore =
        injector.getBinding(Key.get(KeyValueStore.class, Names.named(ExampleStore.STORE_NAME)))
            .getProvider().get();

    lifecycle.init();
    lifecycle.start();
  }

  @AfterMethod
  public void cleanup() {
    lifecycle.stop();
    lifecycle.shutdown();
  }

  public static class ExampleStore {
    public static final String GROUP_NAME = "example";
    public static final String STORE_NAME = "example";
  }

  @Test
  public void emptySchema() throws Exception {
    try {
      schema.createSchema("everything", Everything.EVERYTHING_SCHEMA);
    } catch (KazukiException e) {
      e.printStackTrace();
    }

    Everything e1 = new Everything();

    e1.theAny =
        new BigInteger(
            "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111");
    e1.theMap = ImmutableMap.of("foo", 1L, "bar", true);
    e1.theArray = ImmutableList.of("a", false, 6L);
    e1.theUtcDate = new DateTime();
    e1.theCharOne = 'C';
    e1.theEnum = Everything.TestEnum.FOUR;

    e1.theI8 = Long.valueOf((1 << 7) - 1);
    e1.theI16 = Long.valueOf((1 << 15) - 1);
    e1.theI32 = Long.valueOf((1L << 31) - 1L);
    e1.theI64 = Long.valueOf((1L << 63) - 1L);

    e1.theU8 = Long.valueOf((1 << 8) - 1);
    e1.theU16 = Long.valueOf((1 << 16) - 1);
    e1.theU32 = Long.valueOf((1L << 32) - 1L);
    e1.theU64 = new BigInteger("1").shiftLeft(64).subtract(new BigInteger("1"));

    e1.theUtf8SmallString =
        "okokokokokokokokokokokokokokokokokokokokokokokokokokokokokokokokokokokokokokokok";
    e1.theUtf8Text = "this would be a big string";

    KeyValuePair<Everything> k1 =
        kvStore.create("everything", Everything.class, e1, TypeValidation.STRICT);
    Assert.assertEquals(k1.getVersion().getIdentifier(),
        "@everything:bc3ac6131dd329f3#08b57d789951b7e3");

    Everything v1 = kvStore.retrieve(k1.getKey(), Everything.class);

    Assert.assertEquals(EncodingHelper.convertToJson(v1), EncodingHelper.convertToJson(e1));

    e1.theEnum = TestEnum.THREE;

    Version k2 = kvStore.updateVersioned(k1.getKey(), k1.getVersion(), Everything.class, e1);
    Assert.assertNotEquals(k1.getVersion(), k2);
    Assert.assertEquals(k2.getIdentifier(), "@everything:bc3ac6131dd329f3#022b4a6a38daf6c5");

    Assert.assertFalse(kvStore.deleteVersioned(k1.getKey(), k1.getVersion()));
    Assert.assertTrue(kvStore.deleteVersioned(k1.getKey(), k2));
  }
}
