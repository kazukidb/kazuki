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
package io.kazuki.v0.internal.v2schema.compact;

import io.kazuki.v0.internal.v2schema.types.TypeTransforms;
import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.Schema;
import io.kazuki.v0.store.schema.model.Transform;
import io.kazuki.v0.store.schema.model.TransformException;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Type-based compaction for object instances. For example, boolean compacts to char, datetime
 * compacts to long, enum compacts to int. This results in more marshalling more compact values.
 */
public class FieldTransform implements Transform<Map<String, Object>, Map<String, Object>> {
  private final Schema schema;
  private final Map<String, Transform<?, ?>> fieldCompactions;

  public FieldTransform(Schema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("schema definition must not be null");
    }

    this.schema = schema;

    Map<String, Transform<?, ?>> newFieldCompactions = new LinkedHashMap<String, Transform<?, ?>>();

    for (Attribute attribute : schema.getAttributes()) {
      newFieldCompactions.put(attribute.getName(), TypeTransforms.validatorFor(attribute));
    }

    this.fieldCompactions = Collections.unmodifiableMap(newFieldCompactions);
  }

  @Override
  public Map<String, Object> pack(Map<String, Object> instance) throws TransformException {
    if (instance == null) {
      throw new TransformException("instance must not be null");
    }

    Map<String, Object> mindlessClone = new LinkedHashMap<String, Object>();

    for (Map.Entry<String, Transform<?, ?>> e : fieldCompactions.entrySet()) {
      String attrName = e.getKey();
      Transform validator = e.getValue();

      Attribute attribute = schema.getAttribute(attrName);
      Object inbound = instance.get(attrName);

      if (inbound == null) {
        if (attribute.isNullable()) {
          mindlessClone.put(attrName, null);
          continue;
        }

        throw new TransformException("attribute must not be null: " + attrName);
      }

      try {
        Object transformed = validator.pack(inbound);

        mindlessClone.put(attrName, transformed);
      } catch (ClassCastException ex) {
        throw new TransformException("invalid attribute value for '" + attrName + "'");
      }
    }

    Map<String, Object> mindlessCloneInOrder = new LinkedHashMap<String, Object>();

    for (Map.Entry<String, Object> e : instance.entrySet()) {
      String attrName = e.getKey();

      if (mindlessClone.containsKey(attrName)) {
        mindlessCloneInOrder.put(attrName, mindlessClone.get(attrName));
      } else {
        mindlessCloneInOrder.put(attrName, instance.get(attrName));
      }
    }

    return mindlessCloneInOrder;
  }

  @Override
  public Map<String, Object> unpack(Map<String, Object> instance) throws TransformException {
    if (instance == null) {
      throw new TransformException("instance must not be null");
    }

    Map<String, Object> mindlessClone = new LinkedHashMap<String, Object>();

    for (Map.Entry<String, Object> e : instance.entrySet()) {
      String attrName = e.getKey();

      Attribute attribute = schema.getAttribute(attrName);

      if (attribute == null) {
        mindlessClone.put(attrName, e.getValue());
        continue;
      }

      Object inbound = e.getValue();

      if (inbound == null) {
        if (attribute.isNullable()) {
          mindlessClone.put(attrName, null);
          continue;
        }

        throw new TransformException("attribute must not be null: " + attrName);
      }

      Transform validator = fieldCompactions.get(attrName);
      Object untransformed = validator.unpack(inbound);

      mindlessClone.put(attrName, untransformed);
    }

    return mindlessClone;
  }

  public Object transformValue(String attrName, Object value) throws TransformException {
    try {
      Transform validator = (Transform) fieldCompactions.get(attrName);

      return validator != null ? validator.pack(value) : value;
    } catch (ClassCastException e) {
      throw new TransformException("invalid attribute value for '" + attrName + "': "
          + value.toString());
    }
  }
}
