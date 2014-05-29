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
package io.kazuki.v0.store.guice.impl;

import io.kazuki.v0.store.lifecycle.Lifecycle;

import com.google.common.base.Preconditions;
import com.google.inject.PrivateModule;
import com.google.inject.name.Names;

/**
 * The LifecycleModule is a Guice module that installs the Lifecycle instance into the Guice
 * context.
 */
public class LifecycleModuleDefaultImpl extends PrivateModule {
  private final String name;

  public LifecycleModuleDefaultImpl(String name) {
    Preconditions.checkNotNull(name, "name");
    this.name = name;
  }

  @Override
  protected void configure() {
    binder().requireExplicitBindings();

    bind(Lifecycle.class).annotatedWith(Names.named(name)).toInstance(new Lifecycle(this.name));
    expose(Lifecycle.class).annotatedWith(Names.named(name));
  }
}
