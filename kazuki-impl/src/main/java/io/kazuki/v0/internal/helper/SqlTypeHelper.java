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
package io.kazuki.v0.internal.helper;

import io.kazuki.v0.store.schema.model.Attribute.Type;

public interface SqlTypeHelper {
  String getPrefix();

  String getSqlType(Type type);

  String getInsertIgnore();

  String getPKConflictResolve();

  boolean isTableAlreadyExistsException(Throwable t);

  boolean isDuplicateKeyException(Throwable t);

  String quote(String name);

  String getTableOptions();
}
