/**
 * Copyright 2014 Sunny Gleason
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kazuki.v0.store.journal;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PartitionInfoSnapshot implements PartitionInfo {
  private final String partitionId;
  private final long maxId;
  private final long minId;
  private final long size;
  private final boolean closed;

  public PartitionInfoSnapshot(@JsonProperty("partitionId") String partitionId,
      @JsonProperty("minId") long minId, @JsonProperty("maxId") long maxId,
      @JsonProperty("size") long size, @JsonProperty("closed") boolean closed) {
    this.partitionId = partitionId;
    this.minId = minId;
    this.maxId = maxId;
    this.size = size;
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
    return this.size;
  }

  @Override
  public boolean isClosed() {
    return this.closed;
  }
}
