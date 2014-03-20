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

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A class that encapsulates a value and allows translations between types.
 */
public class ValueHolder {
  private final ValueType valueType;
  private final String literal;
  private final Object value;

  public ValueHolder(ValueType valueType, String literal) {
    this.valueType = valueType;
    this.literal = literal;
    switch (valueType) {
      case DECIMAL:
        this.value = new BigDecimal(literal);
        break;
      case INTEGER:
        this.value = new BigInteger(literal);
        break;
      case STRING:
        this.value = literal.substring(1, literal.length() - 1);
        break;
      case BOOLEAN:
        this.value = Boolean.valueOf(literal);
        break;
      case REFERENCE:
        this.value = literal.substring(1, literal.length() - 1);
        break;
      case NULL:
        this.value = "";
        break;
      default:
        throw new IllegalArgumentException("unknown type: " + valueType);
    }
  }

  public ValueType getValueType() {
    return valueType;
  }

  public String getLiteral() {
    return literal;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public String toString() {
    switch (valueType) {
      case DECIMAL:
      case INTEGER:
        return this.value.toString();
      case STRING:
        return literal;
      case REFERENCE:
        return literal;
      case BOOLEAN:
        return Boolean.valueOf(literal).toString();
      case NULL:
        return null;
      default:
        throw new IllegalArgumentException("unknown type: " + valueType);
    }
  }

  // TODO : equals, hashCode
}
