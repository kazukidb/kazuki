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
 * Opaque Version Identifier.
 */
public interface Version {
  /**
   * Returns a String representing the opaque version of the entity. This String is only meaningful within the
   * context of a single entity - it should not be used by itself except for informational purposes.
   */
  String getVersionPart();

  /**
   * The fully qualified version identifier. This is the String that should be used to fully identify a version
   * of a particular entity, unless the application has a mechanism to reconstruct it implicitly.
   */
  String getIdentifier();
}
