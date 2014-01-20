package io.kazuki.v0.store.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class to make it easy to implement Lifecycle support by only overriding one or two
 * methods.
 */
public abstract class LifecycleSupportBase implements LifecycleRegistration, LifecycleAware {
  protected final Logger log = LoggerFactory.getLogger(getClass());

  @Override
  public void register(Lifecycle lifecycle) {
    lifecycle.register(this);
  }

  public void init() {}

  public void start() {}

  public void announce() {}

  public void unannounce() {}

  public void shutdown() {}

  public void stop() {}

  @Override
  public void eventFired(LifecycleEvent event) {
    switch (event) {
      case INIT:
        init();
        break;
      case START:
        start();
        break;
      case ANNOUNCE:
        announce();
        break;
      case UNANNOUNCE:
        unannounce();
        break;
      case SHUTDOWN:
        shutdown();
        break;
      case STOP:
        stop();
        break;
    }
  }
}
