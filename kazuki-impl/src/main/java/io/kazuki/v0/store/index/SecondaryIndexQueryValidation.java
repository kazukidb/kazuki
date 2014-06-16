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
package io.kazuki.v0.store.index;

import io.kazuki.v0.store.index.query.QueryOperator;
import io.kazuki.v0.store.index.query.QueryTerm;
import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.IndexAttribute;
import io.kazuki.v0.store.schema.model.IndexDefinition;
import io.kazuki.v0.store.schema.model.Schema;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;

public class SecondaryIndexQueryValidation {
  public static void validateQuery(String indexName, List<QueryTerm> query, Schema schema) {
    Preconditions.checkNotNull(query, "query");
    Preconditions.checkNotNull(schema, "schema");

    IndexDefinition indexDef = schema.getIndex(indexName);
    Preconditions.checkNotNull(indexDef, "index");

    boolean containsFirst = false;
    boolean allEquality = true;

    Set<String> seenCols = new HashSet<String>();
    String firstIndexCol = indexDef.getAttributeNames().get(0);

    for (QueryTerm term : query) {
      String attrName = term.getField();

      Attribute existsSchema = schema.getAttribute(attrName);

      if (existsSchema == null) {
        throw new IllegalArgumentException("unknown schema attribute: " + attrName);
      }

      IndexAttribute existsIndex = indexDef.getIndexAttribute(attrName);
      if (existsIndex == null) {
        throw new IllegalArgumentException("unknown index attribute: " + attrName);
      }

      containsFirst = containsFirst || attrName.equals(firstIndexCol);
      allEquality = allEquality && term.getOperator().equals(QueryOperator.EQ);

      seenCols.add(attrName);
    }

    if (!containsFirst) {
      throw new IllegalArgumentException("query must contain first index attribute: "
          + firstIndexCol);
    }

    if (indexDef.isUnique()) {
      boolean containsAll = (seenCols.size() == indexDef.getIndexAttributeMap().size());

      if (!containsAll) {
        throw new IllegalArgumentException("unique index query must contain all index attributes");
      }

      if (!allEquality) {
        throw new IllegalArgumentException("unique index query must use equality operator only");
      }
    }
  }
}
