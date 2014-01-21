package io.kazuki.v0.store.journal;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PartitionInfoSnapshot implements PartitionInfo {
  private final String partitionId;
  private final long maxId;
  private final long minId;
  private final boolean closed;

  public PartitionInfoSnapshot(@JsonProperty("partitionId") String partitionId,
      @JsonProperty("minId") long minId, @JsonProperty("maxId") long maxId,
      @JsonProperty("closed") boolean closed) {
    this.partitionId = partitionId;
    this.minId = minId;
    this.maxId = maxId;
    this.closed = closed;
  }

  @Override
  public long getMaxId() {
    return this.maxId;
  }

  @Override
  public long getMinId() {
    return this.minId;
  }

  @Override
  public String getPartitionId() {
    return this.partitionId;
  }

  @Override
  public long getSize() {
    return this.maxId - this.minId;
  }

  @Override
  public boolean isClosed() {
    return this.closed;
  }
}
