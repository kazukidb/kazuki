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
package io.kazuki.v0.store.schema.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;


/**
 * Object class for an index definition, including name and column definitions.
 */
public class IndexDefinition {
  private final String name;
  private final List<IndexAttribute> indexColumns;
  private final List<String> attributeNames;
  private final boolean unique;

  @JsonCreator
  public IndexDefinition(@JsonProperty("name") String name,
      @JsonProperty("cols") List<IndexAttribute> cols,
      @JsonProperty("unique") @Nullable Boolean unique) {
    Preconditions.checkNotNull(name, "name");
    Preconditions.checkNotNull(cols, "cols");

    this.name = name;
    this.unique = (unique != null && unique);

    List<String> newAttributeNames = new ArrayList<String>();
    for (IndexAttribute attr : cols) {
      newAttributeNames.add(attr.getName());
    }

    this.indexColumns = Collections.unmodifiableList(cols);
    this.attributeNames = Collections.unmodifiableList(newAttributeNames);
  }

  public String getName() {
    return name;
  }

  public boolean isUnique() {
    return unique;
  }

  @JsonProperty("cols")
  public List<IndexAttribute> getIndexAttributes() {
    return indexColumns;
  }

  @JsonIgnore
  public List<String> getAttributeNames() {
    return attributeNames;
  }
}
