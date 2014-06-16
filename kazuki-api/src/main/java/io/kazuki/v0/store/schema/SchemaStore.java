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
package io.kazuki.v0.store.schema;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Version;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.management.KazukiComponent;
import io.kazuki.v0.store.schema.model.Schema;

public interface SchemaStore extends KazukiComponent<SchemaStore> {
  KeyValuePair<Schema> retrieveSchema(String type) throws KazukiException;

  Version createSchema(String type, Schema value) throws KazukiException;

  Version updateSchema(final String type, final Version version, final Schema value)
      throws KazukiException;

  boolean deleteSchema(final String type, final Version version) throws KazukiException;

  void clear() throws KazukiException;
}
