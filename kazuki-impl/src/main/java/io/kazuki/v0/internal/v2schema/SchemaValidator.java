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
package io.kazuki.v0.internal.v2schema;

import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.TransformException;
import io.kazuki.v0.store.schema.model.Attribute.Type;
import io.kazuki.v0.store.schema.model.Schema;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Validates schema instances for creation and upgrades. For validation of instances against a
 * schema, use TypeCompaction.
 */
public class SchemaValidator {
  private static final Pattern VALID_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");

  public static void validate(Schema schemaDefinition) throws TransformException {
    for (Attribute attribute : schemaDefinition.getAttributes()) {
      SchemaValidator.validateAttribute(attribute);
    }
  }

  public static void validateUpgrade(Schema oldSchema, Schema newSchema) {
    // validate existing attributes
    for (Attribute oldAttr : oldSchema.getAttributes()) {
      Attribute newAttr = newSchema.getAttribute(oldAttr.getName());
      SchemaValidator.validateAttributeUpgrade(oldAttr, newAttr);
    }

    // tricky note: 'new' attributes may already exist in the entities;
    // if those entities are incompatible, they will become irretrievable
  }

  public static void validateAttribute(Attribute attribute) {
    String attrName = attribute.getName();

    if (!VALID_NAME_PATTERN.matcher(attrName).matches()) {
      throw new TransformException("Invalid attribute name : " + attrName);
    }

    if (attribute.getType().equals(Type.ENUM)) {
      List<String> values = attribute.getValues();
      if (values == null || values.size() < 1) {
        throw new TransformException("Invalid enum attribute (contains no values) : " + attrName);
      }
    }
  }

  public static void validateAttributeUpgrade(Attribute oldAttr, Attribute newAttr) {
    String name = oldAttr.getName();
    Type oldType = oldAttr.getType();
    Type newType = (newAttr == null) ? null : newAttr.getType();

    switch (oldType) {
      case ANY:
      case ARRAY:
      case MAP:
      case BOOLEAN:
      case CHAR_ONE:
        assertAttributeTypeOneOf(name, newType, null, oldType);
        break;

      case ENUM:
        assertAttributeTypeOneOf(name, newType, Type.ENUM);
        assertEnumAttributeCompatible(oldAttr, newAttr);
        break;

      case I8:
        assertAttributeTypeOneOf(name, newType, null, Type.I8, Type.I16, Type.I32, Type.I64);
        break;
      case I16:
        assertAttributeTypeOneOf(name, newType, null, Type.I16, Type.I32, Type.I64);
        break;
      case I32:
        assertAttributeTypeOneOf(name, newType, null, Type.I32, Type.I64);
        break;
      case I64:
        assertAttributeTypeOneOf(name, newType, null, Type.I64);
        break;

      case U8:
        assertAttributeTypeOneOf(name, newType, null, Type.U8, Type.U16, Type.U32, Type.U64);
        break;
      case U16:
        assertAttributeTypeOneOf(name, newType, null, Type.U16, Type.U32, Type.U64);
        break;
      case U32:
        assertAttributeTypeOneOf(name, newType, null, Type.U32, Type.U64);
        break;
      case U64:
        assertAttributeTypeOneOf(name, newType, null, Type.U64);
        break;

      case UTC_DATE_SECS:
        assertAttributeTypeOneOf(name, newType, null, Type.UTC_DATE_SECS, Type.I64);
        break;

      case UTF8_SMALLSTRING:
        assertAttributeTypeOneOf(name, newType, null, Type.UTF8_SMALLSTRING, Type.UTF8_TEXT);
        break;

      case UTF8_TEXT:
        assertAttributeTypeOneOf(name, newType, null, Type.UTF8_TEXT);
        break;
    }
  }

  public static void assertEnumAttributeCompatible(Attribute oldAttr, Attribute newAttr) {
    if (newAttr == null) {
      throw new TransformException("enum attributes, such as '" + oldAttr + "' may not be removed");
    }

    List<String> newValues = newAttr.getValues();
    List<String> oldValues = oldAttr.getValues();

    if (newValues == null || oldValues == null) {
      throw new TransformException("enum attribute '" + oldAttr.getName() + "' has no values");
    }

    if (newValues.size() < oldValues.size()) {
      throw new TransformException("may not remove enum values from '" + oldAttr.getName() + "', "
          + newValues + " has fewer items than " + oldValues);
    }

    for (int i = 0; i < oldValues.size(); i++) {
      if (!oldValues.get(i).equals(newValues.get(i))) {
        throw new TransformException("illegal enum values update for '" + oldAttr.getName() + "', "
            + newValues + " updates in the front or middle of " + oldValues);
      }
    }
  }

  public static void assertAttributeTypeOneOf(String attrName, Type theType, Type... possibles) {
    for (Type candidate : possibles) {
      if (theType == null && candidate == null) {
        return;
      }

      if (theType != null && theType.equals(candidate)) {
        return;
      }
    }

    throw new TransformException("attribute '" + attrName + "' updated to '" + theType
        + "', must be one of " + Arrays.asList(possibles));
  }
}
