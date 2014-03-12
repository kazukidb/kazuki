package io.kazuki.v0.store.lifecycle;


/**
 * Objects should implement LifecycleRegistration so that the container will inject the singleton
 * Lifecycle instance to them for registration. Typical usage is something like this:
 * 
 * <pre>
 * class Foo implements LifecycleRegistration, LifecycleAware {
 *     &#064;Override
 *     public void register(Lifecycle lifecycle) {
 *         lifecycle.register(this);
 *     }
 * 
 *     &#064;Override
 *     public void eventFired(LifecycleEvent event) {
 *         // do stuff
 *     }
 * }
 * </pre>
 */
public interface LifecycleRegistration {
  public void register(Lifecycle lifecycle);
}
