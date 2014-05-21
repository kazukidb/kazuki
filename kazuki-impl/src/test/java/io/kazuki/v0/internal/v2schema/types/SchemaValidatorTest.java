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
package io.kazuki.v0.internal.v2schema.types;

import io.kazuki.v0.internal.v2schema.SchemaValidator;
import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.TransformException;
import io.kazuki.v0.store.schema.model.Attribute.Type;

import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

@Test
public class SchemaValidatorTest {
  public void testAttributeUpgrades() {
    SchemaValidator.validateAttributeUpgrade(a("foo", Type.ENUM, "dude"),
        a("foo", Type.ENUM, "dude"));

    for (Type type : new Type[] {Type.ANY, Type.ARRAY, Type.BOOLEAN, Type.CHAR_ONE, Type.MAP}) {
      SchemaValidator.validateAttributeUpgrade(a("foo", type), a("foo", type));
      SchemaValidator.validateAttributeUpgrade(a("foo", type), null);
    }

    SchemaValidator.validateAttributeUpgrade(a("foo", Type.UTC_DATE_SECS),
        a("foo", Type.UTC_DATE_SECS));
    SchemaValidator.validateAttributeUpgrade(a("foo", Type.UTC_DATE_SECS), a("foo", Type.I64));
    SchemaValidator.validateAttributeUpgrade(a("foo", Type.UTC_DATE_SECS), null);

    SchemaValidator.validateAttributeUpgrade(a("foo", Type.UTF8_SMALLSTRING),
        a("foo", Type.UTF8_SMALLSTRING));
    SchemaValidator.validateAttributeUpgrade(a("foo", Type.UTF8_SMALLSTRING),
        a("foo", Type.UTF8_TEXT));
    SchemaValidator.validateAttributeUpgrade(a("foo", Type.UTF8_SMALLSTRING), null);

    SchemaValidator.validateAttributeUpgrade(a("foo", Type.UTF8_TEXT), a("foo", Type.UTF8_TEXT));
    SchemaValidator.validateAttributeUpgrade(a("foo", Type.UTF8_SMALLSTRING), null);
  }

  public void testNumericUpgrades() {
    validateNumericType(new Type[] {Type.I8, Type.I16, Type.I32, Type.I64});
    validateNumericType(new Type[] {Type.I16, Type.I32, Type.I64});
    validateNumericType(new Type[] {Type.I32, Type.I64});
    validateNumericType(new Type[] {Type.I64});

    validateNumericType(new Type[] {Type.U8, Type.U16, Type.U32, Type.U64});
    validateNumericType(new Type[] {Type.U16, Type.U32, Type.U64});
    validateNumericType(new Type[] {Type.U32, Type.U64});
    validateNumericType(new Type[] {Type.U64});
  }

  public void testEnumUpgrades() {
    SchemaValidator.assertEnumAttributeCompatible(a("foo", Type.ENUM, "dude"),
        a("foo", Type.ENUM, "dude"));
    SchemaValidator.assertEnumAttributeCompatible(a("foo", Type.ENUM, "dude"),
        a("foo", Type.ENUM, "dude", "dude2"));
  }

  @Test(expectedExceptions = TransformException.class)
  public void testIllegalEnumUpgradeFewer() {
    SchemaValidator.assertEnumAttributeCompatible(a("foo", Type.ENUM, "dude"), a("foo", Type.ENUM));
  }

  @Test(expectedExceptions = TransformException.class)
  public void testIllegalEnumUpgradeReorder() {
    SchemaValidator.assertEnumAttributeCompatible(a("foo", Type.ENUM, "dude", "dude2"),
        a("foo", Type.ENUM, "dude2", "dude"));
  }

  @Test(expectedExceptions = TransformException.class)
  public void testIllegalEnumUpgradeInsertAtFront() {
    SchemaValidator.assertEnumAttributeCompatible(a("foo", Type.ENUM, "dude", "dude2"),
        a("foo", Type.ENUM, "dude0", "dude", "dude2"));
  }

  @Test(expectedExceptions = TransformException.class)
  public void testIllegalEnumUpgradeInsertInMiddle() {
    SchemaValidator.assertEnumAttributeCompatible(a("foo", Type.ENUM, "dude", "dude2", "dude3"),
        a("foo", Type.ENUM, "dude", "dude2", "dude2.5", "dude3"));
  }

  private static Attribute a(String name, Type type, Object... values) {
    List<Object> valueList = values == null ? null : Arrays.asList(values);

    return new Attribute(name, type, valueList, true);
  }

  private void validateNumericType(Type[] possibleTypes) {
    Type baseType = possibleTypes[0];
    for (Type type : possibleTypes) {
      SchemaValidator.validateAttributeUpgrade(a("foo", baseType), a("foo", type));
    }
    SchemaValidator.validateAttributeUpgrade(a("foo", baseType), null);
  }

}
