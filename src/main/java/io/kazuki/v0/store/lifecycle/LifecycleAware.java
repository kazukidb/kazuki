package io.kazuki.v0.store.lifecycle;

/**
 * LifecycleAware instances receive LifecycleEvent notifications from the Lifecycle instance.
 */
public interface LifecycleAware {
  /** receive a LifecycleEvent */
  public void eventFired(LifecycleEvent event);
}
