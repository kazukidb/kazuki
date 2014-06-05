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
package io.kazuki.v0.store.management;

import java.util.Collection;

/**
 * Provides metadata about a first-class Kazuki component.
 * 
 * @param <T> Service interface class for the component
 */
public interface ComponentDescriptor<T> {
  /**
   * Returns the management-friendly component name
   * 
   * @return a management-friendly component name
   */
  String getName();

  /**
   * Returns the service interface class of the component
   * 
   * @return Class for the service interface
   */
  Class<T> getClazz();

  /**
   * Returns the component instance
   * 
   * @return T component instance
   */
  T getInstance();

  /**
   * Returns an unmodifiable collection of the (direct) dependency component descriptors
   * 
   * @return Collection of dependency ComponentDescriptor instances
   */
  Collection<ComponentDescriptor> getDependencies();
}
