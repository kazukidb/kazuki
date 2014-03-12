/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
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
