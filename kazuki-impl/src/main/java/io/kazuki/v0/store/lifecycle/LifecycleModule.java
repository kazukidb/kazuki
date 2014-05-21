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
package io.kazuki.v0.store.lifecycle;

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.name.Names;

/**
 * The LifecycleModule is a Guice module that installs the Lifecycle instance into the Guice
 * context.
 */
public class LifecycleModule extends AbstractModule {
  private final String name;
  private final String[] additionalNames;

  public LifecycleModule(String name, String... additionalNames) {
    Preconditions.checkNotNull(name, "name");

    this.name = name;
    this.additionalNames = additionalNames;
  }

  @Override
  protected void configure() {
    bind(Lifecycle.class).annotatedWith(Names.named(name)).toInstance(new Lifecycle(this.name));

    if (additionalNames != null) {
      for (String additionalName : additionalNames) {
        bind(Lifecycle.class).annotatedWith(Names.named(additionalName)).to(
            Key.get(Lifecycle.class, Names.named(name)));
      }
    }
  }
}
