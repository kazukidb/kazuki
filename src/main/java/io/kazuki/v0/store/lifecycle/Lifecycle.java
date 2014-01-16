package io.kazuki.v0.store.lifecycle;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The Lifecycle is a singleton instance (as hopefully enforced by the DI container) through which
 * application lifecycle is controlled. This Lifecycle instance is used by a controller object in
 * the app that invokes Lifecycle methods to inform components of application state changes.
 */
public class Lifecycle {
  private final ConcurrentLinkedQueue<LifecycleAware> listeners =
      new ConcurrentLinkedQueue<LifecycleAware>();

  /** Registers a listener with the lifecycle, thus 'subscribing' to events */
  public void register(LifecycleAware listener) {
    listeners.add(listener);
  }

  /**
   * Init: Components should sanity check and prepare themselves for use.
   */
  public void init() {
    fireEvent(LifecycleEvent.INIT);
  }

  /**
   * Start: service is listening but not taking requests.
   */
  public void start() {
    fireEvent(LifecycleEvent.START);
  }

  /**
   * Announce: start taking requests.
   */
  public void announce() {
    fireEvent(LifecycleEvent.ANNOUNCE);
  }

  /**
   * Unannounce: stop advertising, stop taking requests.
   */
  public void unannounce() {
    fireEvent(LifecycleEvent.UNANNOUNCE);
  }

  /**
   * Shutdown: dispose resources in a quick and orderly fashion.
   */
  public void shutdown() {
    fireEvent(LifecycleEvent.SHUTDOWN);
  }

  /**
   * Stop: halting of JVM is imminent.
   */
  public void stop() {
    fireEvent(LifecycleEvent.STOP);
  }

  /**
   * Sends the event synchronously to each listener in order.
   */
  private void fireEvent(LifecycleEvent event) {
    for (LifecycleAware listener : listeners) {
      listener.eventFired(event);
    }
  }
}
