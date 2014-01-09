package io.kazuki.v0.store.journal;

import io.kazuki.v0.store.KazukiException;

import java.util.Collection;
import java.util.Iterator;

public interface JournalStorage {
  public abstract void initialize();

  public abstract <T> void append(String type, Class<T> clazz, T inValue, boolean strictType)
      throws KazukiException;

  public abstract <T> Iterator<T> getIteratorRelative(String type, Class<T> clazz, Long offset,
      Long limit) throws Exception;

  public abstract <T> Iterator<T> getIteratorAbsolute(String type, Class<T> clazz, Long offset,
      Long limit) throws Exception;

  public abstract Collection<PartitionInfo> getAllPartitions();

  public abstract PartitionInfo getActivePartition();

  public abstract boolean close(String partitionId);

  public abstract boolean drop(String partitionId);
}
