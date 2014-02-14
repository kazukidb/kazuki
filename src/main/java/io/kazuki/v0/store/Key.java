package io.kazuki.v0.store;


/**
 * Putting the "Key" in Key-Value storage.
 */
public interface Key {
  String getTypePart();

  String getIdPart();

  String getIdentifier();
}
