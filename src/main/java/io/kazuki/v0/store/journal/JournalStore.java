package io.kazuki.v0.store.journal;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.schema.TypeValidation;

import java.util.Iterator;

public interface JournalStore {
  void initialize() throws KazukiException;

  <T> void append(String type, Class<T> clazz, T inValue, TypeValidation typeSafety)
      throws KazukiException;

  <T> Iterator<T> getIteratorRelative(String type, Class<T> clazz, Long offset, Long limit)
      throws KazukiException;

  <T> Iterator<T> getIteratorAbsolute(String type, Class<T> clazz, Long offset, Long limit)
      throws KazukiException;

  Iterator<PartitionInfoSnapshot> getAllPartitions() throws KazukiException;

  PartitionInfo getActivePartition() throws KazukiException;

  boolean closeActivePartition() throws KazukiException;

  boolean dropPartition(String partitionId) throws KazukiException;

  void clear() throws KazukiException;

  Long approximateSize() throws KazukiException;
}
