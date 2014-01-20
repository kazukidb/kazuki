package io.kazuki.v0.internal.availability;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

public class AvailabilityManager implements Releasable {
  private final Logger log = LoggerFactory.getLogger(getClass());

  private final AtomicBoolean available = new AtomicBoolean();

  public <T> T doProtected(ProtectedCommand<T> command) {
    log.info("Executing protected command: {}", command.toString());

    boolean allowed = available.compareAndSet(true, false);
    try {
      if (allowed) {
        T value = command.execute(AvailabilityManager.this);

        if (log.isDebugEnabled()) {
          log.debug("Executed protected command: {}", command.toString());
        }

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
    if (log.isTraceEnabled()) {
      log.trace("Setting availability status to {}", availability);
    }

    available.set(availability);
  }

  public boolean isAvailable() {
    return available.get();
  }

  public boolean release() {
    if (log.isTraceEnabled()) {
      log.trace("Releasing availability manager flag");
    }

    return available.compareAndSet(false, true);
  }

  public interface ProtectedCommand<T> {
    public T execute(Releasable resource) throws Exception;
  }
}
