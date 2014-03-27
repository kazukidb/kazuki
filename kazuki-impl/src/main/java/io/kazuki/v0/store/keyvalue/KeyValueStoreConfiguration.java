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
package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.store.config.ConfigurationBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class KeyValueStoreConfiguration {
  private final String dbType;
  private final String dbPrefix;
  private final String dataType;
  private final String groupName;
  private final String storeName;
  private final String partitionName;
  private final boolean strictTypeCreation;
  private final Long partitionSize;
  private final boolean secondaryIndex;

  public KeyValueStoreConfiguration(@JsonProperty("dbType") String dbType,
      @JsonProperty("dataType") String dataType, @JsonProperty("groupName") String groupName,
      @JsonProperty("storeName") String storeName,
      @JsonProperty("partitionName") String partitionName,
      @JsonProperty("partitionSize") Long partitionSize,
      @JsonProperty("strictTypeCreation") boolean strictTypeCreation,
      @JsonProperty("secondaryIndex") boolean secondaryIndex) {
    Preconditions.checkNotNull(dbType, "dbType");
    Preconditions.checkArgument(!dbType.contains("_") && !dbType.contains(":"), "invalid dbType");
    Preconditions.checkNotNull(groupName, "groupName");
    Preconditions.checkNotNull(storeName, "storeName");
    Preconditions.checkArgument(partitionName != null || partitionSize != null,
        "partitionName or partitionSize must be set");
    Preconditions
        .checkArgument(partitionSize == null || partitionSize > 1, "invalid partitionSize");

    this.dbType = dbType;
    this.dataType = dataType;
    this.dbPrefix = dbType + ":" + dbType + "_";
    this.groupName = groupName;
    this.storeName = storeName;
    this.partitionName = partitionName != null ? partitionName : String.format("%016x", 0L);
    this.partitionSize = partitionSize;
    this.strictTypeCreation = strictTypeCreation;
    this.secondaryIndex = secondaryIndex;
  }

  public String getDbType() {
    return dbType;
  }

  public String getDataType() {
    return dataType;
  }

  public String getDbPrefix() {
    return dbPrefix;
  }

  public String getGroupName() {
    return groupName;
  }

  public String getStoreName() {
    return storeName;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public Long getPartitionSize() {
    return partitionSize;
  }

  public boolean isStrictTypeCreation() {
    return strictTypeCreation;
  }

  public boolean isSecondaryIndex() {
    return secondaryIndex;
  }

  public static class Builder implements ConfigurationBuilder<KeyValueStoreConfiguration> {
    private String dbType;
    private String dataType;
    private String groupName;
    private String storeName;
    private String partitionName;
    private Long partitionSize;
    private boolean strictTypeCreation = true;
    private boolean secondaryIndex = false;

    public Builder withDbType(String dbType) {
      this.dbType = dbType;

      return this;
    }

    public Builder withDataType(String dataType) {
      this.dataType = dataType;

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

    public Builder withPartitionName(String partitionName) {
      this.partitionName = partitionName;

      return this;
    }

    public Builder withPartitionSize(Long partitionSize) {
      this.partitionSize = partitionSize;

      return this;
    }

    public Builder withStrictTypeCreation(boolean strictTypeCreation) {
      this.strictTypeCreation = strictTypeCreation;

      return this;
    }

    public Builder withSecondaryIndex(boolean secondaryIndex) {
      this.secondaryIndex = secondaryIndex;

      return this;
    }

    public KeyValueStoreConfiguration build() {
      return new KeyValueStoreConfiguration(dbType, dataType, groupName, storeName, partitionName,
          partitionSize, strictTypeCreation, secondaryIndex);
    }
  }
}
