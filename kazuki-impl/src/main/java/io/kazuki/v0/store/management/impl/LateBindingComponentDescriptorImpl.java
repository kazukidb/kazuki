/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.kazuki.v0.store.management.impl;

import io.kazuki.v0.store.management.ComponentDescriptor;
import io.kazuki.v0.store.management.KazukiComponent;

import java.util.Collection;

import com.google.inject.Provider;

public abstract class LateBindingComponentDescriptorImpl<T>
    implements
      ComponentDescriptor<T>,
      Provider<KazukiComponent<T>> {
  @Override
  public abstract KazukiComponent<T> get();

  @Override
  public String getName() {
    return this.get().getComponentDescriptor().getName();
  }

  @Override
  public Class<T> getClazz() {
    return this.get().getComponentDescriptor().getClazz();
  }

  @Override
  public T getInstance() {
    return this.get().getComponentDescriptor().getInstance();
  }

  @Override
  public Collection<ComponentDescriptor> getDependencies() {
    return this.get().getComponentDescriptor().getDependencies();
  }

  @Override
  public String toString() {
    if (this.get() == null) {
      return getClass().getName() + "@" + System.identityHashCode(this);
    }

    return this.get().toString();
  }
}
