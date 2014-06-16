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
package io.kazuki.v0.internal.helper;

import io.kazuki.v0.store.management.ComponentDescriptor;
import io.kazuki.v0.store.management.ComponentRegistrar;
import io.kazuki.v0.store.management.KazukiComponent;
import io.kazuki.v0.store.management.impl.ComponentDescriptorImpl;

import java.util.concurrent.locks.ReentrantLock;

import javax.inject.Inject;

import com.google.common.collect.ImmutableList;


public class LockManagerImpl implements LockManager, KazukiComponent<LockManager> {
  private final ReentrantLock lock = new ReentrantLock();
  private final String name;
  private final ComponentDescriptor<LockManager> componentDescriptor;

  public LockManagerImpl(String name) {
    this.name = name;
    this.componentDescriptor =
        new ComponentDescriptorImpl<LockManager>("KZ:LockManager:" + this.name, LockManager.class,
            (LockManager) this, new ImmutableList.Builder().build());
  }

  @Override
  public ComponentDescriptor<LockManager> getComponentDescriptor() {
    return this.componentDescriptor;
  }

  @Override
  @Inject
  public void registerAsComponent(ComponentRegistrar manager) {
    manager.register(this.componentDescriptor);
  }

  @Override
  public LockManagerImpl acquire() {
    lock.lock();

    return this;
  }

  @Override
  public void close() {
    lock.unlock();
  }
}
