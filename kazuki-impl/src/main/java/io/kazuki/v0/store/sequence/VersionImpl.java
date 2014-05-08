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
package io.kazuki.v0.store.sequence;

import io.kazuki.v0.internal.helper.VersionObfuscator;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.Version;

/**
 * Simple Version concept for Key-Value storage. This implementation class should only be used
 * within Kazuki itself.
 */
public class VersionImpl implements Version {
  private final Key key;
  private final Long internalIdentifier;
  private volatile String encryptedIdentifier;
  private volatile String encryptedId;

  protected VersionImpl(Key key, Long internalIdentifier) {
    this.key = key;
    this.internalIdentifier = internalIdentifier;
  }

  protected VersionImpl(Key key, Long internalIdentifier, String encryptedIdentifier,
      String encryptedId) {
    this.key = key;
    this.internalIdentifier = internalIdentifier;
    this.encryptedIdentifier = encryptedIdentifier;
    this.encryptedId = encryptedId;
  }

  public static VersionImpl createInternal(Key key, Long id) {
    return new VersionImpl(key, id);
  }

  public static VersionImpl createInternal(Key key, Long id, String encryptedIdentifier,
      String encryptedId) {
    return new VersionImpl(key, id, encryptedIdentifier, encryptedId);
  }

  public static Version valueOf(String identifier) {
    if (identifier == null || identifier.length() == 0) {
      throw new IllegalArgumentException("Invalid key");
    }

    return VersionObfuscator.decrypt(identifier);
  }

  @Override
  public String getVersionPart() {
    computeEncryptedIds();

    return encryptedId;
  }

  @Override
  public String getIdentifier() {
    computeEncryptedIds();

    return encryptedIdentifier;
  }


  @Override
  public boolean equals(Object obj) {
    return obj != null && obj instanceof VersionImpl && key.equals(((VersionImpl) obj).key)
        && internalIdentifier.equals(((VersionImpl) obj).internalIdentifier);
  }

  @Override
  public int hashCode() {
    return key.hashCode() ^ internalIdentifier.hashCode();
  }

  @Override
  public String toString() {
    return getIdentifier();
  }

  public Long getInternalIdentifier() {
    return internalIdentifier;
  }

  public Key getKey() {
    return key;
  }

  private void computeEncryptedIds() {
    if (this.encryptedIdentifier == null) {
      this.encryptedIdentifier = VersionObfuscator.encrypt(this.key, this.internalIdentifier);
      this.encryptedId = this.encryptedIdentifier.split("#")[1];
    }
  }
}
