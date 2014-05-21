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
package io.kazuki.v0.store.index;

import io.kazuki.v0.store.index.query.QueryOperator;
import io.kazuki.v0.store.index.query.QueryTerm;
import io.kazuki.v0.store.index.query.ValueHolder;
import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.IndexDefinition;
import io.kazuki.v0.store.schema.model.Schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

public class UniqueEntityDescription<T> {
  private final String type;
  private final Class<T> clazz;
  private final String indexName;
  private final Map<String, QueryTerm> columnDefinitions;

  public UniqueEntityDescription(String type, Class<T> clazz, String indexName, Schema schema,
      Map<String, ValueHolder> values) {
    Preconditions.checkNotNull(type, "type");
    Preconditions.checkNotNull(clazz, "clazz");
    Preconditions.checkNotNull(indexName, "indexName");
    Preconditions.checkNotNull(schema, "schema");
    Preconditions.checkNotNull(values, "values");
    Preconditions.checkArgument(!values.isEmpty(), "values");

    IndexDefinition indexDef = schema.getIndex(indexName);

    Preconditions.checkNotNull(indexDef, "index");
    Preconditions.checkArgument(indexDef.isUnique(), "unique");

    this.type = type;
    this.clazz = clazz;
    this.indexName = indexName;

    Map<String, QueryTerm> newColumnDefinitions = new LinkedHashMap<String, QueryTerm>();

    for (Map.Entry<String, ValueHolder> def : values.entrySet()) {
      String attrName = def.getKey();
      ValueHolder value = def.getValue();

      Preconditions.checkNotNull(value, "value");

      Attribute attr = schema.getAttribute(attrName);
      Preconditions.checkNotNull(attr, "attr");

      newColumnDefinitions.put(attrName, new QueryTerm(QueryOperator.EQ, attrName, value));
    }

    for (String colName : indexDef.getAttributeNames()) {
      Preconditions.checkNotNull(newColumnDefinitions.get(colName), "column");
    }

    this.columnDefinitions = Collections.unmodifiableMap(newColumnDefinitions);
  }

  public String getType() {
    return type;
  }

  public Class<T> getClazz() {
    return clazz;
  }

  public String getIndexName() {
    return indexName;
  }

  public Map<String, QueryTerm> getColumnDefinitions() {
    return columnDefinitions;
  }

  // TODO : equals, hashCode, toString
}
