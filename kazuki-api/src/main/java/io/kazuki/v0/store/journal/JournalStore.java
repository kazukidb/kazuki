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

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.management.KazukiComponent;
import io.kazuki.v0.store.schema.TypeValidation;

import javax.annotation.Nullable;

/**
 * A JournalStore is optimized for sequential, write-once activity. Storage is implemented in large
 * chunks, called "partitions". There is at most one active partition at a time. Partitions may be
 * dropped, which deletes all entities that they contain (this is much more efficient than one-by-one
 * entity deletion). Read access into the JournalStore may be absolute (via an offset), or relative to
 * the earliest non-dropped partition.
 */
public interface JournalStore extends KazukiComponent<JournalStore> {
  /**
   * Initializes the JournalStore (not intended to be called directly by clients)
   */
  void initialize() throws KazukiException;

  /**
   * Appends an entry into the JournalStore
   * 
   * @param type String Kazuki type identifier
   * @param clazz Class representing the type of the value to append
   * @param inValue Object representing the value to append
   * @param typeSafety TypeSafety of schema validation of the given value
   * @return Key identifying the added entry
   * @throws KazukiException
   */
  <T> Key append(String type, Class<T> clazz, T inValue, TypeValidation typeSafety)
      throws KazukiException;

  /**
   * Returns a relative iterator over entries in the JournalStore. This means that offset zero
   * corresponds to the first entry of the first non-dropped partition.
   * 
   * @param type String Kazuki type identifier
   * @param clazz Class representing the value type
   * @param sortDirection SortDirection cursor sort direction
   * @param offset Long offset (null is zero)
   * @param limit Long max results to iterate over (null is unlimited)
   * @return KeyValueIterable of KeyValuePair entries to iterate over
   * @throws KazukiException
   */
  <T> KeyValueIterable<KeyValuePair<T>> entriesRelative(String type, Class<T> clazz,
      SortDirection sortDirection, Long offset, Long limit) throws KazukiException;

  /**
   * Returns an absolute iterator over entries in the JournalStore. This means that offset
   * zero corresponds to the first entry ever written (which may be a dropped partition, which
   * would produce an error).
   * 
   * @param type String Kazuki type identifier
   * @param clazz Class representing the value type
   * @param sortDirection SortDirection cursor sort direction
   * @param offset Long offset (null is zero)
   * @param limit Long max results to iterate over (null is unlimited)
   * @return KeyValueIterable of KeyValuePair entries to iterate over
   */
  <T> KeyValueIterable<KeyValuePair<T>> entriesAbsolute(String type, Class<T> clazz,
      SortDirection sortDirection, Long offset, Long limit) throws KazukiException;

  /**
   * Returns an iterator over all partition descriptors in ascending order
   * 
   * @throws KazukiException
   */
  KeyValueIterable<PartitionInfoSnapshot> getAllPartitions() throws KazukiException;

  /**
   * Returns an iterator over all partition descriptors using the specified SortDirection
   * 
   * @param sortDirection SortDirection cursor sort direction
   * @return
   * @throws KazukiException
   */
  KeyValueIterable<PartitionInfoSnapshot> getAllPartitions(SortDirection sortDirection)
      throws KazukiException;

  /**
   * Returns the active partition, or null if there is no active partition
   */
  @Nullable
  PartitionInfo getActivePartition() throws KazukiException;

  /**
   * Closes the current active partition (since there is at most one active partition at a time)
   */
  boolean closeActivePartition() throws KazukiException;

  /**
   * Drops a partition
   * 
   * @param partitionId String partition identifier obtained from a PartitionInfo instance
   * @return true if successful, false otherwise
   * @throws KazukiException
   */
  boolean dropPartition(String partitionId) throws KazukiException;

  /**
   * Clears all entries from the Journal Store
   */
  void clear() throws KazukiException;

  /**
   * Returns the approximate size of the store (across all partitions)
   */
  Long approximateSize() throws KazukiException;
}
