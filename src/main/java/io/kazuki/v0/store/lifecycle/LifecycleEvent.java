package io.kazuki.v0.store.lifecycle;

/**
 * LifecycleEvent enumerates the events sent by Lifecycle.
 */
public enum LifecycleEvent {
  INIT, START, ANNOUNCE, UNANNOUNCE, SHUTDOWN, STOP;
}
