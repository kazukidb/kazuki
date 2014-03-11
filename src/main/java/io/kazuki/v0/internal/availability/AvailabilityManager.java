package io.kazuki.v0.internal.availability;

import io.kazuki.v0.internal.helper.LogTranslation;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

import com.google.common.base.Throwables;

public class AvailabilityManager implements Releasable {
  private final Logger log = LogTranslation.getLogger(getClass());

  private final AtomicBoolean available = new AtomicBoolean();

  public <T> T doProtected(ProtectedCommand<T> command) {
    log.debug("Executing protected command: {}", command);

    boolean allowed = available.compareAndSet(true, false);
    try {
      if (allowed) {
        T value = command.execute(AvailabilityManager.this);

        log.debug("Executed protected command: {}", command);

        return value;
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    throw new IllegalStateException("service unavailable");
  }

  public void assertAvailable() {
    if (!available.get()) {
      throw new IllegalStateException("service unavailable");
    }
  }

  public void setAvailable(boolean availability) {
    log.trace("Setting availability status to {}", availability);

    available.set(availability);
  }

  public boolean isAvailable() {
    return available.get();
  }

  public boolean release() {
    log.trace("Releasing availability manager flag");

    return available.compareAndSet(false, true);
  }

  public interface ProtectedCommand<T> {
    public T execute(Releasable resource) throws Exception;
  }
}
