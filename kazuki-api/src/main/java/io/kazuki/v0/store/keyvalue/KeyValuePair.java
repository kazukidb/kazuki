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
package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.Version;

import javax.annotation.Nullable;

public class KeyValuePair<T> {
  private final Key key;
  private final Version version;
  private final Version schemaVersion;
  private final T value;

  public KeyValuePair(Key key, Version version, @Nullable Version schemaVersion, T value) {
    this.key = key;
    this.version = version;
    this.schemaVersion = schemaVersion;
    this.value = value;
  }

  public Key getKey() {
    return this.key;
  }

  public Version getVersion() {
    return version;
  }

  public Version getSchemaVersion() {
    return schemaVersion;
  }

  public T getValue() {
    return this.value;
  }
}
