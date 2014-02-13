package io.kazuki.v0.store.journal;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.schema.TypeValidation;

public interface JournalStore {
  void initialize() throws KazukiException;

  <T> Key append(String type, Class<T> clazz, T inValue, TypeValidation typeSafety)
      throws KazukiException;

  <T> KeyValueIterable<KeyValuePair<T>> entriesRelative(String type, Class<T> clazz, Long offset,
      Long limit) throws KazukiException;

  <T> KeyValueIterable<KeyValuePair<T>> entriesAbsolute(String type, Class<T> clazz, Long offset,
      Long limit) throws KazukiException;

  KeyValueIterable<PartitionInfoSnapshot> getAllPartitions() throws KazukiException;

  PartitionInfo getActivePartition() throws KazukiException;

  boolean closeActivePartition() throws KazukiException;

  boolean dropPartition(String partitionId) throws KazukiException;

  void clear() throws KazukiException;

  Long approximateSize() throws KazukiException;
}
