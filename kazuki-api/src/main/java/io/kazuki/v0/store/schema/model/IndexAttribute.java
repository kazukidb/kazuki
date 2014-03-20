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

import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * Class for index column specifications (for user-defined secondary indexes).
 */
public class IndexAttribute {
  public final String name;
  public final SortDirection sortDirection;
  public final AttributeTransform transform;

  @JsonCreator
  public IndexAttribute(@JsonProperty("name") String name,
      @JsonProperty("sort") SortDirection sortDirection,
      @JsonProperty("transform") @Nullable AttributeTransform transform) {
    Preconditions.checkNotNull(name, "name");
    Preconditions.checkNotNull(sortDirection, "sort");

    this.name = name;
    this.sortDirection = sortDirection;
    this.transform = (transform == null) ? AttributeTransform.NONE : transform;
  }

  public String getName() {
    return name;
  }

  public AttributeTransform getTransform() {
    return transform;
  }

  @JsonProperty("sort")
  public SortDirection getSortDirection() {
    return sortDirection;
  }
}
