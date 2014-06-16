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
package io.kazuki.v0.store.guice.impl;

import io.kazuki.v0.internal.helper.LockManager;
import io.kazuki.v0.internal.helper.LockManagerImpl;
import io.kazuki.v0.store.management.ComponentRegistrar;

import com.google.common.base.Preconditions;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.name.Names;

public class LockManagerModuleImpl extends PrivateModule {
  private final String name;
  private final Key<ComponentRegistrar> registrarKey;

  public LockManagerModuleImpl(String name, Key<ComponentRegistrar> registrarKey) {
    Preconditions.checkNotNull(name, "name");
    Preconditions.checkNotNull(registrarKey, "registrarKey");

    this.name = name;
    this.registrarKey = registrarKey;
  }

  @Override
  protected void configure() {
    // TODO: re-enable ASAP
    // binder().requireExplicitBindings();

    bind(ComponentRegistrar.class).to(registrarKey);

    Key<LockManager> lockManagerKey = Key.get(LockManager.class, Names.named(name));

    bind(lockManagerKey).toInstance(new LockManagerImpl(name));
    expose(lockManagerKey);
  }
}
