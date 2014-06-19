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
package io.kazuki.v0.store;


/**
 * Putting the "Key" in Key-Value storage. A Key instance consists of a "type" part, an "id" part,
 * and a full "identifier".
 */
public interface Key {
  /**
   * A String representing the "type" of entity. This is used in Kazuki to associate Schema instances with
   * entities of a given type.
   */
  String getTypePart();

  /**
   * A String representing the "id" of the entity. This corresponds to an opaque sequence for the type.
   * However, the "id" must only be used together with the "type" to fully identify an entity. 
   */
  String getIdPart();

  /**
   * A String representing the "full identity" of an entity, including its type and id. This is the only
   * String suitable for external references, unless the application has a convention/scheme for reconstructing
   * the identifier implicitly.
   */
  String getIdentifier();
}
