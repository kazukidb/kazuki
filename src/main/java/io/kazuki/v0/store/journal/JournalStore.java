package io.kazuki.v0.store.journal;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.schema.TypeValidation;

import java.util.Collection;
import java.util.Iterator;

public interface JournalStore {
  void initialize();

  <T> void append(String type, Class<T> clazz, T inValue, TypeValidation typeSafety)
      throws KazukiException;

  <T> Iterator<T> getIteratorRelative(String type, Class<T> clazz, Long offset, Long limit)
      throws KazukiException;

  <T> Iterator<T> getIteratorAbsolute(String type, Class<T> clazz, Long offset, Long limit)
      throws KazukiException;

  Collection<PartitionInfo> getAllPartitions() throws KazukiException;

  PartitionInfo getActivePartition() throws KazukiException;

  boolean close(String partitionId) throws KazukiException;

  boolean drop(String partitionId) throws KazukiException;

  void clear(boolean preserveTypes, boolean preserveCounters) throws KazukiException;

  Long approximateSize(String type) throws KazukiException;
}
