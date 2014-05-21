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
package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.Version;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.sequence.ResolvedKey;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nullable;

public interface KeyValueStore {
  void initialize();

  Key toKey(String keyString);

  <T> KeyValuePair<T> create(String type, Class<T> clazz, T inValue, TypeValidation typeSafety)
      throws KazukiException;

  <T> KeyValuePair<T> create(String type, Class<T> clazz, T inValue,
      @Nullable ResolvedKey keyOverride, TypeValidation typeSafety) throws KazukiException;

  <T> T retrieve(Key key, Class<T> clazz) throws KazukiException;

  <T> KeyValuePair<T> retrieveVersioned(Key key, Class<T> clazz) throws KazukiException;

  <T> Map<Key, T> multiRetrieve(Collection<Key> keys, Class<T> clazz) throws KazukiException;

  <T> Map<Key, KeyValuePair<T>> multiRetrieveVersioned(Collection<Key> keys, Class<T> clazz)
      throws KazukiException;

  <T> boolean update(Key key, Class<T> clazz, T inValue) throws KazukiException;

  <T> Version updateVersioned(Key key, Version version, Class<T> clazz, T inValue)
      throws KazukiException;

  boolean delete(Key key) throws KazukiException;

  boolean deleteVersioned(Key key, Version version) throws KazukiException;

  boolean deleteHard(Key key) throws KazukiException;

  boolean deleteHardVersioned(Key key, Version version) throws KazukiException;

  void clear(boolean preserveTypes, boolean preserveCounters) throws KazukiException;

  void clear(String type) throws KazukiException;

  Long approximateSize(String type) throws KazukiException;

  KeyValueStoreIteration iterators();

  void destroy() throws KazukiException;
}
