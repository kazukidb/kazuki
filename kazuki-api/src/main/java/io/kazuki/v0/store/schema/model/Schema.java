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
package io.kazuki.v0.store.schema.model;

import io.kazuki.v0.store.schema.model.Attribute.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Schema definition class - the main entry point for schema creation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Schema {
  public static Schema SCHEMA_SCHEMA = (new Builder())
      .addAttribute("attributes", Type.ARRAY, false).addAttribute("indexes", Type.ARRAY, true)
      .build();

  private final List<Attribute> attributes;
  private final Map<String, Attribute> attributeMap;
  private final List<IndexDefinition> indexes;
  private final Map<String, IndexDefinition> indexMap;

  @JsonCreator
  public Schema(@JsonProperty("attributes") List<Attribute> attributes,
      @JsonProperty("indexes") @Nullable List<IndexDefinition> indexes) {
    Preconditions.checkNotNull(attributes, "attributes");

    this.attributes = Collections.unmodifiableList(attributes);

    Map<String, Attribute> newAttributes = new LinkedHashMap<String, Attribute>();
    for (Attribute attr : attributes) {
      newAttributes.put(attr.getName(), attr);
    }

    this.attributeMap = Collections.unmodifiableMap(newAttributes);

    if (indexes == null || indexes.isEmpty()) {
      this.indexes = ImmutableList.of();
      this.indexMap = ImmutableMap.of();
    } else {
      this.indexes = Collections.unmodifiableList(indexes);

      Map<String, IndexDefinition> newIndexes = new LinkedHashMap<String, IndexDefinition>();
      for (IndexDefinition index : indexes) {
        String name = index.getName();

        if (newIndexes.containsKey(name)) {
          throw new IllegalArgumentException("duplicate index entry for '" + name + "'");
        }

        if (newIndexes.size() > 0 && index.isUnique()) {
          throw new IllegalArgumentException(
              "at most one unique 'index' may be present per schema and must be the first index listed in order");
        }

        for (IndexAttribute attr : index.getIndexAttributes()) {
          String attrName = attr.getName();

          if (!attributeMap.containsKey(attrName)) {
            throw new IllegalArgumentException("index '" + name
                + "' references unknown attribute '" + attrName);
          }
        }

        newIndexes.put(name, index);
      }

      this.indexMap = Collections.unmodifiableMap(newIndexes);
    }
  }

  public List<Attribute> getAttributes() {
    return attributes;
  }

  @JsonIgnore
  public Attribute getAttribute(String name) {
    return attributeMap.get(name);
  }

  @JsonIgnore
  public Map<String, Attribute> getAttributeMap() {
    return attributeMap;
  }

  public List<IndexDefinition> getIndexes() {
    return indexes;
  }

  @JsonIgnore
  public IndexDefinition getIndex(String name) {
    return indexMap.get(name);
  }

  @JsonIgnore
  public Map<String, IndexDefinition> getIndexMap() {
    return indexMap;
  }

  public static class Builder {
    private List<Attribute> attributes = new ArrayList<Attribute>();
    private List<IndexDefinition> indexes = new ArrayList<IndexDefinition>();

    public Builder addAttribute(String name, Type type, boolean nullable) {
      return this.addAttribute(name, type, nullable, null);
    }

    public Builder addAttribute(String name, Type type, boolean nullable, String renameOf) {
      this.attributes.add(new Attribute(name, type, null, nullable, renameOf));

      return this;
    }

    public Builder addAttribute(String name, Type type, List<Object> values, boolean nullable) {
      return this.addAttribute(name, type, values, nullable, null);
    }

    public Builder addAttribute(String name, Type type, List<Object> values, boolean nullable,
        String renameOf) {
      this.attributes.add(new Attribute(name, type, values, nullable, renameOf));

      return this;
    }

    public Builder addIndex(String name, List<IndexAttribute> cols, Boolean unique, String renameOf) {
      this.indexes.add(new IndexDefinition(name, cols, unique, renameOf));

      return this;
    }

    public Builder addIndex(String name, List<IndexAttribute> cols, Boolean unique) {
      return this.addIndex(name, cols, unique, null);
    }

    public Schema build() {
      return new Schema(attributes, indexes);
    }
  }
}
