package io.kazuki.v0.store.lifecycle;

import io.kazuki.v0.internal.helper.LogTranslation;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.slf4j.Logger;


/**
 * The Lifecycle is a singleton instance (as hopefully enforced by the DI container) through which
 * application lifecycle is controlled. This Lifecycle instance is used by a controller object in
 * the app that invokes Lifecycle methods to inform components of application state changes.
 */
public class Lifecycle {
  private final Logger log = LogTranslation.getLogger(getClass());

  private static final EnumSet<LifecycleEvent> reverseOrder = EnumSet.of(LifecycleEvent.UNANNOUNCE,
      LifecycleEvent.STOP, LifecycleEvent.SHUTDOWN);

  private final ConcurrentLinkedDeque<LifecycleAware> listeners = new ConcurrentLinkedDeque<>();

  private final String name;

  public Lifecycle(String name) {
    this.name = name;
  }

  /** Registers a listener with the lifecycle, thus 'subscribing' to events */
  public void register(LifecycleAware listener) {
    log.debug("Registering Lifecycle listener {}", listener);

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

  public String getName() {
    return name;
  }

  /**
   * Sends the event synchronously to each listener in order.
   */
  private void fireEvent(LifecycleEvent event) {
    log.debug("Firing lifecycle event {} to all listeners", event.name());

    Iterator<LifecycleAware> iter =
        reverseOrder.contains(event) ? listeners.descendingIterator() : listeners.iterator();

    while (iter.hasNext()) {
      LifecycleAware listener = iter.next();

      log.trace("Firing lifecycle event {} to listener {}", event.name(), listener);

      listener.eventFired(event);
    }

    log.debug("Fired lifecycle event {}", event.name());
  }
}
