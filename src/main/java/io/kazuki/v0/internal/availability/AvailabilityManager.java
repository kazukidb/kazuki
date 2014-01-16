package io.kazuki.v0.internal.availability;

import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Throwables;

public class AvailabilityManager implements Releasable {
  private final AtomicBoolean available = new AtomicBoolean();

  public <T> T doProtected(ProtectedCommand<T> command) {
    boolean allowed = available.compareAndSet(true, false);
    try {
      if (allowed) {
        return command.execute(AvailabilityManager.this);
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
    available.set(availability);
  }

  public boolean isAvailable() {
    return available.get();
  }

  public boolean release() {
    return available.compareAndSet(false, true);
  }

  public interface ProtectedCommand<T> {
    public T execute(Releasable resource) throws Exception;
  }
}
