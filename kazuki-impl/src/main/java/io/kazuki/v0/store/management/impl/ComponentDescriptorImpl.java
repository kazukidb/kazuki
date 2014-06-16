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
package io.kazuki.v0.store.management.impl;

import io.kazuki.v0.internal.helper.EncodingHelper;
import io.kazuki.v0.store.management.ComponentDescriptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

public class ComponentDescriptorImpl<T> implements ComponentDescriptor<T> {
  private final String name;
  private final Class<T> clazz;
  private final T instance;
  private final Collection<ComponentDescriptor> dependencies;

  public ComponentDescriptorImpl(String name, Class<T> clazz, T instance,
      Collection<ComponentDescriptor> dependencies) {
    this.name = name;
    this.clazz = clazz;
    this.instance = instance;
    this.dependencies = dependencies;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public Class<T> getClazz() {
    return this.clazz;
  }

  @Override
  public T getInstance() {
    return this.instance;
  }

  @Override
  public Collection<ComponentDescriptor> getDependencies() {
    return this.dependencies;
  }

  @Override
  public String toString() {
    try {
      List<Map<String, Object>> depStrings = new ArrayList<Map<String, Object>>();

      for (ComponentDescriptor dep : dependencies) {
        depStrings
            .add(ImmutableMap.of("name", dep.getName(), "clazz", dep.getClazz().getName(),
                "instance", (Object) (dep.getClazz().getName() + "@" + System.identityHashCode(dep
                    .getInstance()))));
      }

      return EncodingHelper.convertToJson(ImmutableMap.of("name", name, "clazz", clazz.getName(),
          "instance", clazz.getName() + "@" + System.identityHashCode(instance), "dependences",
          depStrings));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
