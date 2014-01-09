package io.kazuki.v0.store.journal;

public interface PartitionInfo {
  public String getPartitionId();

  public long getMinId();

  public long getMaxId();

  public long getSize();
}
