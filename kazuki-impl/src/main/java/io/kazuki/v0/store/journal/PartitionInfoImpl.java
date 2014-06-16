/**
 * Copyright 2014 Sunny Gleason and original author or authors
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PartitionInfoImpl implements PartitionInfo {
  private final String partitionId;
  private final long minId;
  private AtomicLong maxId;
  private AtomicLong size;
  private AtomicBoolean closed;

  public PartitionInfoImpl(@JsonProperty("partitionId") String partitionId,
      @JsonProperty("minId") long minId, @JsonProperty("maxId") long maxId,
      @JsonProperty("size") long size, @JsonProperty("closed") boolean closed) {
    this.partitionId = partitionId;
    this.minId = minId;
    this.maxId = new AtomicLong(maxId);
    this.size = new AtomicLong(size);
    this.closed = new AtomicBoolean(closed);
  }

  @Override
  public long getMaxId() {
    return this.maxId.get();
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
    return this.size.get();
  }

  @Override
  public boolean isClosed() {
    return this.closed.get();
  }

  public synchronized void setSize(long theSize) {
    this.size.set(theSize);
  }

  public synchronized void setClosed(boolean theClosed) {
    this.closed.set(theClosed);
  }

  public synchronized void setMaxId(long theMaxId) {
    this.maxId.set(theMaxId);
  }

  public PartitionInfo snapshot() {
    return new PartitionInfoSnapshot(this.partitionId, this.minId, this.maxId.get(),
        this.size.get(), this.closed.get());
  }
}
