package io.kazuki.v0.internal.helper;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;

public class KeyHelper {
  public static void validateKey(String key) throws KazukiException {
    if (key == null || key.length() == 0 || key.indexOf(":") == -1) {
      throw new KazukiException("Invalid key");
    }

    String[] parts = key.split(":");
    if (parts.length != 2) {
      throw new KazukiException("Invalid key");
    }

    try {
      Key.valueOf(key);
    } catch (Exception e) {
      throw new KazukiException("Invalid key");
    }
  }
}
