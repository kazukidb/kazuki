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
package io.kazuki.v0.internal.v2schema;

import io.kazuki.v0.internal.v2schema.Attribute.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Schema definition class - the main entry point for schema creation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Schema {
  public static Schema SCHEMA_SCHEMA = (new Builder())
      .addAttribute("attributes", Type.ARRAY, false).build();

  private final List<Attribute> attributes;
  private final Map<String, Attribute> attributeMap;

  @JsonCreator
  public Schema(@JsonProperty("attributes") List<Attribute> attributes) {
    if (attributes == null) {
      throw new IllegalArgumentException("'attributes' must be present");
    }

    this.attributes = Collections.unmodifiableList(attributes);

    Map<String, Attribute> newAttributes = new LinkedHashMap<String, Attribute>();
    for (Attribute attr : attributes) {
      newAttributes.put(attr.getName(), attr);
    }

    this.attributeMap = Collections.unmodifiableMap(newAttributes);
  }

  public List<Attribute> getAttributes() {
    return attributes;
  }

  @JsonIgnore
  public Attribute getAttribute(String name) {
    return attributeMap.get(name);
  }

  public static class Builder {
    private List<Attribute> attributes = new ArrayList<Attribute>();

    public Builder addAttribute(String name, Type type, boolean nullable) {
      this.attributes.add(new Attribute(name, type, null, nullable));

      return this;
    }

    public Builder addAttribute(String name, Type type, List<Object> values, boolean nullable) {
      this.attributes.add(new Attribute(name, type, values, nullable));

      return this;
    }

    public Schema build() {
      return new Schema(attributes);
    }
  }
}
