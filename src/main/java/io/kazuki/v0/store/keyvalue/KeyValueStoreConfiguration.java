package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.store.config.ConfigurationBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class KeyValueStoreConfiguration {
  private final String dbType;
  private final String dbPrefix;
  private final String groupName;
  private final String storeName;
  private final String partitionName;
  private final boolean strictTypeCreation;
  private final Long partitionSize;

  public KeyValueStoreConfiguration(@JsonProperty("dbType") String dbType,
      @JsonProperty("groupName") String groupName, @JsonProperty("storeName") String storeName,
      @JsonProperty("partitionName") String partitionName,
      @JsonProperty("partitionSize") Long partitionSize,
      @JsonProperty("strictTypeCreation") boolean strictTypeCreation) {
    Preconditions.checkNotNull(dbType, "dbType");
    Preconditions.checkArgument(!dbType.contains("_") && !dbType.contains(":"), "invalid dbType");
    Preconditions.checkNotNull(groupName, "groupName");
    Preconditions.checkNotNull(storeName, "storeName");
    Preconditions.checkArgument(partitionName != null || partitionSize != null,
        "partitionName or partitionSize must be set");
    Preconditions
        .checkArgument(partitionSize == null || partitionSize > 1, "invalid partitionSize");

    this.dbType = dbType;
    this.dbPrefix = dbType + ":" + dbType + "_";
    this.groupName = groupName;
    this.storeName = storeName;
    this.partitionName = partitionName != null ? partitionName : String.format("%016x", 0L);
    this.partitionSize = partitionSize;
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

  public Long getPartitionSize() {
    return partitionSize;
  }

  public boolean isStrictTypeCreation() {
    return strictTypeCreation;
  }

  public static class Builder implements ConfigurationBuilder<KeyValueStoreConfiguration> {
    private String dbType;
    private String groupName;
    private String storeName;
    private String partitionName;
    private Long partitionSize;
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

    public KeyValueStoreConfiguration build() {
      return new KeyValueStoreConfiguration(dbType, groupName, storeName, partitionName,
          partitionSize, strictTypeCreation);
    }
  }
}
