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
package io.kazuki.v0.store.schema.model;

import io.kazuki.v0.store.schema.model.Schema.Builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Attribute class for properties of a user-defined Object.
 */
public class Attribute {
  public static Schema ATTRIBUTE_SCHEMA;
  static {
    Builder schema = new Builder();

    schema.addAttribute("name", Type.UTF8_SMALLSTRING, false);
    schema.addAttribute("type", Type.ENUM, Arrays.asList((Object[]) Type.values()), false);
    schema.addAttribute("nullable", Type.BOOLEAN, true);
    schema.addAttribute("values", Type.ARRAY, true);

    ATTRIBUTE_SCHEMA = schema.build();
  }

  public enum Type {
    // structured types
    ANY, MAP, ARRAY,
    // compact types
    BOOLEAN, CHAR_ONE,
    // enumeration types
    ENUM,
    // unsigned integer types
    U8, U16, U32, U64,
    // signed integer types
    I8, I16, I32, I64,
    // date types
    UTC_DATE_SECS,
    // string types
    UTF8_SMALLSTRING, UTF8_TEXT;
  }

  private final String name;
  private final Type type;
  private final List<String> values;
  private final boolean nullable;

  @JsonCreator
  public Attribute(@JsonProperty("name") String name, @JsonProperty("type") Type type,
      @JsonProperty("values") List<Object> values, @JsonProperty("nullable") Boolean nullable) {
    if (name == null) {
      throw new IllegalArgumentException("Attribute 'name' must not be null");
    }

    if (type == null) {
      throw new IllegalArgumentException("Attribute 'type' must be specified");
    }

    this.name = name;
    this.type = type;
    this.nullable = (nullable == null) || nullable;

    if (values != null) {
      List<String> newVals = new ArrayList<String>();
      for (Object object : values) {
        newVals.add(String.valueOf(object));
      }

      this.values = Collections.unmodifiableList(newVals);
    } else {
      this.values = null;
    }
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<String> getValues() {
    return values;
  }

  public boolean isNullable() {
    return nullable;
  }
}
