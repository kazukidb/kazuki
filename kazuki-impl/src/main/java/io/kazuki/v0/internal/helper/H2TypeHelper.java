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
package io.kazuki.v0.internal.helper;

import io.kazuki.v0.store.schema.model.Attribute.Type;

public class H2TypeHelper implements SqlTypeHelper {
  public static final String DATABASE_PREFIX = "h2:h2_";

  @Override
  public String getPrefix() {
    return DATABASE_PREFIX;
  }

  @Override
  public String getSqlType(Type type) {
    switch (type) {
      case BOOLEAN:
        return "BOOLEAN";
      case ENUM:
        return "SMALLINT";
      case I8:
        return "TINYINT";
      case I16:
        return "SMALLINT";
      case I32:
        return "INT";
      case I64:
        return "BIGINT";
      case U8:
        return "SMALLINT";
      case U16:
        return "INT";
      case U32:
        return "BIGINT";
      case U64:
        return "DECIMAL";
      case UTC_DATE_SECS:
        return "BIGINT";
      case UTF8_SMALLSTRING:
        return "VARCHAR(255)";
      case CHAR_ONE:
        return "CHAR(1)";
      default:
        throw new IllegalArgumentException("Unsupported type in index: " + type);
    }
  }

  @Override
  public String getInsertIgnore() {
    return "insert ";
  }

  @Override
  public String getPKConflictResolve() {
    return "";
  }

  @Override
  public boolean isDuplicateKeyException(Throwable t) {
    return t.getMessage().indexOf("Unique index or primary key violation") >= 0;
  }

  @Override
  public boolean isTableAlreadyExistsException(Throwable t) {
    return t.getMessage().indexOf(" already exists;") >= 0;
  }

  @Override
  public String quote(String name) {
    return "\"" + name + "\"";
  }

  @Override
  public String getTableOptions() {
    return "";
  }
}
