/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.kazuki.v0.store.index.query;

/**
 * A query term, representing one boolean clause in a conjunction of possibly several clauses.
 */
public class QueryTerm {
  private final QueryOperator operator;
  private final String field;
  private final ValueHolder value;
  private final QueryValueList valueList;

  public QueryTerm(QueryOperator operator, String field, ValueHolder value) {
    this.operator = operator;
    this.field = field;
    this.value = value;
    this.valueList = null;
  }

  public QueryTerm(QueryOperator operator, String field, QueryValueList valueList) {
    this.operator = operator;
    this.field = field;
    this.value = null;
    this.valueList = valueList;
  }

  public QueryOperator getOperator() {
    return operator;
  }

  public String getField() {
    return field;
  }

  public ValueHolder getValue() {
    if (valueList != null) {
      throw new IllegalStateException("value() not present in this term, use valueList() instead");
    }

    return value;
  }

  public QueryValueList getValueList() {
    if (value != null) {
      throw new IllegalStateException("valueList() not present in this term, use value() instead");
    }

    return valueList;
  }

  @Override
  public String toString() {
    return this.field + " " + this.operator.name() + " "
        + (this.value != null ? this.value : this.valueList);
  }

  // TODO : equals, hashCode, toString
}
