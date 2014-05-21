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
package io.kazuki.v0.store.index;


import io.kazuki.v0.internal.helper.Configurations;
import io.kazuki.v0.store.easy.EasyKeyValueStoreModule;
import io.kazuki.v0.store.lifecycle.LifecycleModule;

import org.testng.annotations.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;

@Test
public class SecondaryIndexStoreJdbiImplAttributesTest
    extends SecondaryIndexStoreAttributesTestBase {
  @Override
  protected Injector getInjector() {
    return Guice.createInjector(
        new LifecycleModule("foo"),
        new EasyKeyValueStoreModule("foo", "test/io/kazuki/v0/store/sequence").withJdbiConfig(
            Configurations.getJdbi().build()).withKeyValueStoreConfig(
            Configurations.getKeyValue("foo", "foo").withSecondaryIndex(true).build()));
  }
}
