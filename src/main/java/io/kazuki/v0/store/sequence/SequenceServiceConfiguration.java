package io.kazuki.v0.store.sequence;

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
    Preconditions.checkNotNull(dbType, "dbType must not be null");
    Preconditions.checkArgument(!dbType.contains("_") && !dbType.contains(":"), "invalid dbType");
    Preconditions.checkNotNull(groupName, "groupName must not be null");
    Preconditions.checkNotNull(storeName, "storeName must not be null");
    Preconditions.checkNotNull(incrementBlockSize, "incrementBlockSize must not be null");

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
}
