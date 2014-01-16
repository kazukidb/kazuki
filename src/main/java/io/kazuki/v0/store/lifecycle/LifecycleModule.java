package io.kazuki.v0.store.lifecycle;

import com.google.inject.AbstractModule;

/**
 * The LifecycleModule is a Guice module that installs the Lifecycle instance into the Guice
 * context.
 */
public class LifecycleModule extends AbstractModule {
  @Override
  protected void configure() {
    binder().bind(Lifecycle.class).asEagerSingleton();
  }
}
