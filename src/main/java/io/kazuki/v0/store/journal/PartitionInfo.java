package io.kazuki.v0.store.journal;

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface PartitionInfo {
  String getPartitionId();

  long getMinId();

  long getMaxId();

  @JsonIgnore
  long getSize();

  boolean isClosed();
}
