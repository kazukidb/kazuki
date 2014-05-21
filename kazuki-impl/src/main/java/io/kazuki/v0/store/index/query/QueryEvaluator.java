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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/**
 * QueryEvaluator class to serve as a mock implementation of secondary indexes. For database-backed
 * indexes, this will be implemented by the SQL engine. In the long run this implementation will
 * probably be most useful as a sanity check.
 */
public class QueryEvaluator {
  public boolean matches(Map<String, Object> instance, List<QueryTerm> query) {
    boolean matches = true;

    for (QueryTerm term : query) {
      String field = term.getField();
      QueryOperator operator = term.getOperator();
      ValueHolder targetValue = term.getValue();

      Object instanceValue = instance.get(field);

      if (instanceValue == null && !targetValue.getValueType().equals(ValueType.NULL)) {
        matches = false;
        break;
      }

      boolean satisfied = evaluate(operator, instanceValue, targetValue);

      if (!satisfied) {
        matches = false;
        break;
      }
    }

    return matches;
  }

  public boolean evaluate(QueryOperator operator, Object instanceValue, ValueHolder targetQueryValue) {
    Object targetValue = targetQueryValue.getValue();

    int comparison = 0;

    switch (targetQueryValue.getValueType()) {
      case INTEGER:
        try {
          comparison =
              -((BigInteger) targetValue).compareTo(new BigInteger(instanceValue.toString()));
        } catch (Exception e) {
          return false;
        }
        break;
      case DECIMAL:
        try {
          comparison =
              -((BigDecimal) targetValue).compareTo(new BigDecimal(instanceValue.toString()));
        } catch (Exception e) {
          return false;
        }
        break;
      case REFERENCE:
        if (!QueryOperator.EQ.equals(operator) && !QueryOperator.NE.equals(operator)) {
          throw new UnsupportedOperationException("Operator " + operator
              + " not supported for reference type");
        }
        comparison = instanceValue.toString().compareTo((String) targetValue);
        break;
      case STRING:
        comparison = instanceValue.toString().compareTo((String) targetValue);
        break;
      case BOOLEAN:
        try {
          comparison =
              -((Boolean) targetValue).compareTo(Boolean.parseBoolean(instanceValue.toString()));
        } catch (Exception e) {
          return false;
        }
        break;
      case NULL:
        comparison = (instanceValue == null) ? 0 : 1;
        break;
      default:
        throw new UnsupportedOperationException("Compare to " + targetValue.getClass().getName()
            + " not supported");
    }

    boolean matches = false;

    switch (operator) {
      case EQ:
        matches = (comparison == 0);
        break;
      case NE:
        matches = (comparison != 0);
        break;
      case GE:
        matches = (comparison >= 0);
        break;
      case GT:
        matches = (comparison > 0);
        break;
      case LE:
        matches = (comparison <= 0);
        break;
      case LT:
        matches = (comparison < 0);
        break;
      default:
        throw new UnsupportedOperationException("Operator " + operator + " not supported");
    }

    return matches;
  }
}
