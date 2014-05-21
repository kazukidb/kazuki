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
package io.kazuki.v0.store.index.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryBuilder {
  public List<QueryTerm> query = new ArrayList<QueryTerm>();

  public QueryBuilder andMatchesSingle(String field, QueryOperator op, ValueType valueType,
      String literal) {
    query.add(new QueryTerm(op, field, new ValueHolder(valueType, literal)));

    return this;
  }

  public QueryBuilder andMatchesIn(String field, QueryOperator op, ValueType valueType,
      List<String> literalList) {
    List<ValueHolder> newValueHolders = new ArrayList<ValueHolder>();

    for (String literal : literalList) {
      newValueHolders.add(new ValueHolder(valueType, literal));
    }

    query.add(new QueryTerm(op, field, new ValueHolderList(newValueHolders)));

    return this;
  }

  public List<QueryTerm> build() {
    return Collections.unmodifiableList(query);
  }
}
