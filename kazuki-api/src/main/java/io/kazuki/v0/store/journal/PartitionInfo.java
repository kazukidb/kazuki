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

/**
 * Encapsulates metadata about a JournalStore partition.
 */
public interface PartitionInfo {
  /**
   * String opaque partition identifier
   */
  String getPartitionId();

  /**
   * The minimum identifier in the partition (inclusive)
   */
  long getMinId();

  /**
   * The maximum identifier in the partition (inclusive)
   */
  long getMaxId();

  /**
   * The approximate number of entries in the partition
   */
  long getSize();

  /**
   * True if this partition is closed (no longer active)
   */
  boolean isClosed();
}
