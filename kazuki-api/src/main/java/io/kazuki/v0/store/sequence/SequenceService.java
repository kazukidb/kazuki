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
package io.kazuki.v0.store.sequence;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;

public interface SequenceService {
  String getTypeName(final Integer id) throws KazukiException;

  Integer getTypeId(final String type, final boolean create) throws KazukiException;

  Key nextKey(String type) throws KazukiException;

  ResolvedKey resolveKey(Key key) throws KazukiException;

  Key unresolveKey(ResolvedKey key) throws KazukiException;

  void clear(boolean clearTypes, boolean clearCounters);

  void resetCounter(String type) throws KazukiException;
}
