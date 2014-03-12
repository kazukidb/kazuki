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
