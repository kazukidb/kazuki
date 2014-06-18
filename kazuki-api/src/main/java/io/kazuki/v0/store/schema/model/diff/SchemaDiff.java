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
package io.kazuki.v0.store.schema.model.diff;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Representation of an individual difference between two Schema instances. This class is intended
 * to be used to allow more conservative and lazy migrations.
 */
public class SchemaDiff<T> {
  private final DiffType type;
  private final Class<T> clazz;
  private final T oldInstance;
  private final T newInstance;

  public SchemaDiff(@JsonProperty("type") DiffType type, @JsonProperty("clazz") Class<T> clazz,
      @JsonProperty("oldInstance") @Nullable T oldInstance,
      @JsonProperty("newInstance") @Nullable T newInstance) {
    this.type = type;
    this.clazz = clazz;
    this.oldInstance = oldInstance;
    this.newInstance = newInstance;
  }

  /**
   * Enumerated type for Schema operations: add/modify/rename/remove of attributes and indexes.
   */
  public enum DiffType {
    ATTRIBUTE_ADD, ATTRIBUTE_MODIFY, ATTRIBUTE_RENAME, ATTRIBUTE_REMOVE, INDEX_ADD, INDEX_MODIFY, INDEX_RENAME, INDEX_REMOVE;
  }

  public T getOldInstance() {
    return oldInstance;
  }

  public T getNewInstance() {
    return newInstance;
  }

  public DiffType getType() {
    return type;
  }

  public Class<T> getClazz() {
    return clazz;
  }
}
