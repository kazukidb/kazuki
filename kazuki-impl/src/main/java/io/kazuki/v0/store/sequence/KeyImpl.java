/**
 * Copyright 2014 Sunny Gleason and original author or authors
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
package io.kazuki.v0.store.sequence;

import io.kazuki.v0.internal.helper.KeyObfuscator;
import io.kazuki.v0.store.Key;

/**
 * Putting the "Key" in Key-Value storage. This implementation class should only be used within
 * Kazuki itself.
 */
public class KeyImpl implements Key {
  private final String internalIdentifier;
  private volatile String encryptedIdentifier;
  private volatile String encryptedId;
  private final String type;
  private final Long internalId;

  protected KeyImpl(String type, Long internalId) {
    this.internalIdentifier = type + ":" + Long.toString(internalId);
    this.type = type;
    this.internalId = internalId;
  }

  public static KeyImpl createInternal(String type, Long id) {
    return new KeyImpl(type, id);
  }

  public static Key valueOf(String key) {
    if (key == null || key.length() == 0) {
      throw new IllegalArgumentException("Invalid key");
    }

    if (key.startsWith("@")) {
      return KeyObfuscator.decrypt(key);
    }

    String[] parts = key.split(":");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid key");
    }

    Long id = Long.parseLong(parts[1]);

    return new KeyImpl(parts[0], id);
  }

  @Override
  public String getTypePart() {
    return type;
  }

  @Override
  public String getIdPart() {
    computeEncryptedIds();

    return this.encryptedId;
  }

  @Override
  public String getIdentifier() {
    computeEncryptedIds();

    return encryptedIdentifier;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof KeyImpl && internalIdentifier.equals(((KeyImpl) obj).internalIdentifier);
  }

  @Override
  public int hashCode() {
    return internalIdentifier.hashCode();
  }

  @Override
  public String toString() {
    return getIdentifier();
  }

  public String getInternalIdentifier() {
    return internalIdentifier;
  }

  public Long getInternalId() {
    return internalId;
  }

  private void computeEncryptedIds() {
    if (this.encryptedIdentifier == null) {
      this.encryptedIdentifier = KeyObfuscator.encrypt(this.type, this.internalId);
      this.encryptedId = this.encryptedIdentifier.split(":")[1];
    }
  }
}
