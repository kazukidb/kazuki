/**
 * Copyright 2014 the original author or authors
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

import io.kazuki.v0.store.schema.model.Schema;
import io.kazuki.v0.store.sequence.ResolvedKey;

import java.util.Map;

import org.skife.jdbi.v2.Handle;

public interface KeyValueStoreListener {
  <T> void onCreate(Handle handle, String type, Class<T> clazz, Schema schema,
      ResolvedKey resolvedKey, Map<String, Object> instance);

  <T> void onUpdate(Handle handle, String type, Class<T> clazz, Schema schema,
      ResolvedKey resolvedKey, Map<String, Object> newInstance, Map<String, Object> oldInstance);

  <T> void onDelete(Handle handle, String type, Class<T> clazz, Schema schema,
      ResolvedKey resolvedKey, Map<String, Object> oldInstance);

  void clear(Handle handle, Map<String, Schema> typeToSchemaMap, boolean preserveSchema);
}
