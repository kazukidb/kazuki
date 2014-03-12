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
package io.kazuki.v0.store.lifecycle;

import io.kazuki.v0.internal.helper.LogTranslation;

import org.slf4j.Logger;

/**
 * Abstract base class to make it easy to implement Lifecycle support by only overriding one or two
 * methods.
 */
public abstract class LifecycleSupportBase implements LifecycleRegistration, LifecycleAware {
  protected final Logger log = LogTranslation.getLogger(getClass());

  @Override
  public void register(Lifecycle lifecycle) {
    lifecycle.register(this);
  }

  public void init() {}

  public void start() {}

  public void announce() {}

  public void unannounce() {}

  public void shutdown() {}

  public void stop() {}

  @Override
  public void eventFired(LifecycleEvent event) {
    switch (event) {
      case INIT:
        init();
        break;
      case START:
        start();
        break;
      case ANNOUNCE:
        announce();
        break;
      case UNANNOUNCE:
        unannounce();
        break;
      case SHUTDOWN:
        shutdown();
        break;
      case STOP:
        stop();
        break;
    }
  }
}
