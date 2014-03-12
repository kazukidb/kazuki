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
package io.kazuki.v0.store.sequence;

import io.kazuki.v0.store.config.ConfigurationBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class SequenceServiceConfiguration {
  private final String dbType;
  private final String dbPrefix;
  private final String groupName;
  private final String storeName;
  private final Long incrementBlockSize;
  private final boolean strictTypeCreation;

  public SequenceServiceConfiguration(@JsonProperty("dbType") String dbType,
      @JsonProperty("groupName") String groupName, @JsonProperty("storeName") String storeName,
      @JsonProperty("incrementBlockSize") Long incrementBlockSize,
      @JsonProperty("strict") boolean strictTypeCreation) {
    Preconditions.checkNotNull(dbType, "dbType");
    Preconditions.checkArgument(!dbType.contains("_") && !dbType.contains(":"), "invalid dbType");
    Preconditions.checkNotNull(groupName, "groupName");
    Preconditions.checkNotNull(storeName, "storeName");
    Preconditions.checkNotNull(incrementBlockSize, "incrementBlockSize");

    this.dbType = dbType;
    this.dbPrefix = dbType + ":" + dbType + "_";
    this.groupName = groupName;
    this.storeName = storeName;
    this.incrementBlockSize = incrementBlockSize;
    this.strictTypeCreation = strictTypeCreation;
  }

  public String getDbType() {
    return dbType;
  }

  public String getDbPrefix() {
    return dbPrefix;
  }

  public String getGroupName() {
    return groupName;
  }

  public Long getIncrementBlockSize() {
    return incrementBlockSize;
  }

  public String getStoreName() {
    return storeName;
  }

  public boolean isStrictTypeCreation() {
    return strictTypeCreation;
  }

  public static class Builder implements ConfigurationBuilder<SequenceServiceConfiguration> {
    private String dbType;
    private String groupName;
    private String storeName;
    private Long incrementBlockSize = SequenceServiceJdbiImpl.DEFAULT_INCREMENT_BLOCK_SIZE;
    private boolean strictTypeCreation = true;

    public Builder withDbType(String dbType) {
      this.dbType = dbType;

      return this;
    }

    public Builder withGroupName(String groupName) {
      this.groupName = groupName;

      return this;
    }

    public Builder withStoreName(String storeName) {
      this.storeName = storeName;

      return this;
    }

    public Builder withIncrementBlockSize(Long incrementBlockSize) {
      this.incrementBlockSize = incrementBlockSize;

      return this;
    }

    public Builder withStrictTypeCreation(boolean strictTypeCreation) {
      this.strictTypeCreation = strictTypeCreation;

      return this;
    }

    public SequenceServiceConfiguration build() {
      return new SequenceServiceConfiguration(dbType, groupName, storeName, incrementBlockSize,
          strictTypeCreation);
    }
  }
}
