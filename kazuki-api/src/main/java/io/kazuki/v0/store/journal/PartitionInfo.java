package io.kazuki.v0.store.journal;


public interface PartitionInfo {
  String getPartitionId();

  long getMinId();

  long getMaxId();

  long getSize();

  boolean isClosed();
}
