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

/**
 * SchemaStore is a specialized Key-Value store that holds Schema instances for a Kazuki KeyValueStore
 * instance.
 */
public interface SchemaStore extends KazukiComponent<SchemaStore> {
  /**
   * Retrieves the Schema for a given Kazuki "type"
   * 
   * @param type String type instance
   * @return KeyValuePair containing the Schema (or null)
   * @throws KazukiException
   */
  KeyValuePair<Schema> retrieveSchema(String type) throws KazukiException;

  /**
   * Creates a schema for the given type.
   * 
   * @param type String type instance
   * @param value Schema to associate with the type
   * @return Version of the created Schema
   * @throws KazukiException
   */
  Version createSchema(String type, Schema value) throws KazukiException;

  /**
   * Updates a schema for the given type
   * 
   * @param type String type instance
   * @param version Version of the schema being modified
   * @param value Schema instance to replace the specified version
   * @return Version of the updated Schema, or null if not modified
   * @throws KazukiException
   */
  Version updateSchema(final String type, final Version version, final Schema value)
      throws KazukiException;

  /**
   * Deletes a schema for the given type
   * 
   * @param type String type instance
   * @param version Version of the schema being deleted
   * @return boolean true if the schema was deleted
   * @throws KazukiException
   */
  boolean deleteSchema(final String type, final Version version) throws KazukiException;

  /**
   * Clears all entries from the Schema store
   * 
   * @throws KazukiException
   */
  void clear() throws KazukiException;
}
