package io.kazuki.v0.store.keyvalue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class KeyValueStoreConfiguration {
  private final String dbType;
  private final String dbPrefix;
  private final String groupName;
  private final String storeName;
  private final String partitionName;
  private final boolean strictTypeCreation;

  public KeyValueStoreConfiguration(@JsonProperty("dbType") String dbType,
      @JsonProperty("groupName") String groupName, @JsonProperty("storeName") String storeName,
      @JsonProperty("partitionName") String partitionName,
      @JsonProperty("strictTypeCreation") boolean strictTypeCreation) {
    Preconditions.checkNotNull(dbType, "dbType must not be null");
    Preconditions.checkArgument(!dbType.contains("_") && !dbType.contains(":"), "invalid dbType");
    Preconditions.checkNotNull(groupName, "groupName must not be null");
    Preconditions.checkNotNull(storeName, "storeName must not be null");
    Preconditions.checkNotNull(partitionName, "partitionName must not be null");

    this.dbType = dbType;
    this.dbPrefix = dbType + ":" + dbType + "_";
    this.groupName = groupName;
    this.storeName = storeName;
    this.partitionName = partitionName;
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

  public String getStoreName() {
    return storeName;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public boolean isStrictTypeCreation() {
    return strictTypeCreation;
  }
}
