package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.store.Key;

public class KeyValuePair<T> {
  private final Key key;
  private final T value;

  public KeyValuePair(Key key, T value) {
    this.key = key;
    this.value = value;
  }

  public Key getKey() {
    return this.key;
  }

  public T getValue() {
    return this.value;
  }
}
