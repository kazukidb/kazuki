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

import io.kazuki.v0.internal.helper.LockManager;
import io.kazuki.v0.store.keyvalue.KeyValueStoreJdbiH2Module;

import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.name.Names;


public class SimpleJournalStoreModule extends KeyValueStoreJdbiH2Module {
  public SimpleJournalStoreModule(String name, String propertiesPath) {
    super(name, propertiesPath);
  }

  protected void includeInternal() {
    bind(JournalStore.class).annotatedWith(Names.named(name)).to(SimpleJournalStore.class)
        .in(Scopes.SINGLETON);
    bind(LockManager.class).to(Key.get(LockManager.class, Names.named(name)));
  }

  @Override
  protected void includeExposures() {
    expose(Key.get(JournalStore.class, Names.named(name)));
  }
}
