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
