package io.kazuki.v0.store;

import io.kazuki.v0.internal.helper.KeyObfuscator;

/**
 * Putting the "Key" in Key-Value storage.
 */
public class Key {
  private final String internalIdentifier;
  private volatile String encryptedIdentifier;
  private final String type;
  private final Long id;

  public Key(String type, Long id) {
    this.internalIdentifier = type + ":" + Long.toString(id);
    this.type = type;
    this.id = id;
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

    return new Key(parts[0], id);
  }

  public Long getId() {
    return id;
  }

  public String getType() {
    return type;
  }

  public String getIdentifier() {
    if (this.encryptedIdentifier == null) {
      this.encryptedIdentifier = KeyObfuscator.encrypt(this.type, this.id);
    }

    return encryptedIdentifier;
  }

  public String getInternalIdentifier() {
    return internalIdentifier;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Key && internalIdentifier.equals(((Key) obj).internalIdentifier);
  }

  @Override
  public int hashCode() {
    return internalIdentifier.hashCode();
  }

  @Override
  public String toString() {
    return getIdentifier();
  }
}
