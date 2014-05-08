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
package io.kazuki.v0.store;

import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.AttributeTransform;
import io.kazuki.v0.store.schema.model.IndexAttribute;
import io.kazuki.v0.store.schema.model.IndexDefinition;
import io.kazuki.v0.store.schema.model.Schema;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.google.common.collect.ImmutableList;

public class Everything {
  public enum TestEnum {
    ONE, TWO, THREE, FOUR
  };

  public static final Schema EVERYTHING_SCHEMA;
  static {
    List<Attribute> attrs = new ArrayList<Attribute>();

    attrs.add(new Attribute("theAny", Attribute.Type.ANY, null, true));
    attrs.add(new Attribute("theMap", Attribute.Type.MAP, null, true));
    attrs.add(new Attribute("theArray", Attribute.Type.ARRAY, null, true));
    attrs.add(new Attribute("theBoolean", Attribute.Type.BOOLEAN, null, true));
    attrs.add(new Attribute("theCharOne", Attribute.Type.CHAR_ONE, null, true));

    //
    // BEWARE: enums aren't auto linked to Java enums because it's important
    // that schema enum values are never removed!
    //
    attrs.add(new Attribute("theEnum", Attribute.Type.ENUM, ImmutableList.<Object>of("ZERO", "ONE",
        "TWO", "THREE", "FOUR"), false));

    attrs.add(new Attribute("theU8", Attribute.Type.U8, null, true));
    attrs.add(new Attribute("theU16", Attribute.Type.U16, null, true));
    attrs.add(new Attribute("theU32", Attribute.Type.U32, null, true));
    attrs.add(new Attribute("theU64", Attribute.Type.U64, null, true));

    attrs.add(new Attribute("theI8", Attribute.Type.I8, null, true));
    attrs.add(new Attribute("theI16", Attribute.Type.I16, null, true));
    attrs.add(new Attribute("theI32", Attribute.Type.I32, null, true));
    attrs.add(new Attribute("theI64", Attribute.Type.I64, null, true));

    attrs.add(new Attribute("theUtcDate", Attribute.Type.UTC_DATE_SECS, null, true));
    attrs.add(new Attribute("theUtf8SmallString", Attribute.Type.UTF8_SMALLSTRING, null, true));
    attrs.add(new Attribute("theUtf8Text", Attribute.Type.UTF8_SMALLSTRING, null, true));

    List<IndexDefinition> indexDefs = new ArrayList<IndexDefinition>();

    // indexDefs.add(new IndexDefinition("uniqueFooKeyValue", ImmutableList.of(new IndexAttribute(
    // "fooKey", SortDirection.ASCENDING, AttributeTransform.NONE), new IndexAttribute("fooValue",
    // SortDirection.ASCENDING, AttributeTransform.NONE)), true));

    indexDefs.add(new IndexDefinition("theEnum", ImmutableList.of(new IndexAttribute("theEnum",
        SortDirection.ASCENDING, AttributeTransform.NONE)), false));

    EVERYTHING_SCHEMA =
        new Schema(Collections.unmodifiableList(attrs), Collections.unmodifiableList(indexDefs));
  }

  public Object theAny;
  public Map theMap;
  public List theArray;
  public Boolean theBoolean;
  public Character theCharOne;
  public TestEnum theEnum;
  public Long theU8;
  public Long theU16;
  public Long theU32;
  public BigInteger theU64;
  public Long theI8;
  public Long theI16;
  public Long theI32;
  public Long theI64;
  public DateTime theUtcDate;
  public String theUtf8SmallString;
  public String theUtf8Text;
}
