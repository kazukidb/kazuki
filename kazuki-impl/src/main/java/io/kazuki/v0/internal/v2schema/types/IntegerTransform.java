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
package io.kazuki.v0.internal.v2schema.types;

import io.kazuki.v0.store.schema.model.Transform;
import io.kazuki.v0.store.schema.model.TransformException;

import java.math.BigInteger;



/**
 * Validates / transforms integer values.
 */
public class IntegerTransform implements Transform<Object, Number> {
  private final BigInteger min;
  private final BigInteger max;

  public IntegerTransform(String minString, String maxString) {
    this.min = new BigInteger(minString);
    this.max = new BigInteger(maxString);
  }

  public BigInteger pack(Object instance) throws TransformException {
    if (instance == null) {
      throw new TransformException("must not be null");
    }

    try {
      String instanceString = instance.toString();
      BigInteger instanceInteger = new BigInteger(instanceString);

      if (instanceInteger.compareTo(min) < 0) {
        throw new TransformException("must be greater than or equal to " + min);
      } else if (instanceInteger.compareTo(max) > 0) {
        throw new TransformException("must be less than or equal to " + max);
      }

      return instanceInteger;
    } catch (NumberFormatException e) {
      throw new TransformException("is not a valid integer");
    }
  }

  @Override
  public Object unpack(Number instance) throws TransformException {
    if (instance == null) {
      throw new TransformException("must not be null");
    }

    BigInteger bigInstance =
        (instance instanceof BigInteger) ? (BigInteger) instance : new BigInteger(
            instance.toString());
    Object result = instance;

    int compareMin = bigInstance.compareTo(BigInteger.valueOf(Long.MIN_VALUE));
    int compareMax = bigInstance.compareTo(BigInteger.valueOf(Long.MAX_VALUE));
    if (compareMin >= 0 && compareMax <= 0) {
      result = instance.longValue();
    }

    return result;
  }
}
