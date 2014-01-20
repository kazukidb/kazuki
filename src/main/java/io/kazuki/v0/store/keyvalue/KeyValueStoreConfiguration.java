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

  public KeyValueStoreConfiguration(@JsonProperty("dbType") String dbType,
      @JsonProperty("groupName") String groupName, @JsonProperty("storeName") String storeName,
      @JsonProperty("partitionName") String partitionName,
      @JsonProperty("strictTypeCreation") boolean strictTypeCreation) {
    Preconditions.checkNotNull(dbType, "dbType");
    Preconditions.checkArgument(!dbType.contains("_") && !dbType.contains(":"), "invalid dbType");
    Preconditions.checkNotNull(groupName, "groupName");
    Preconditions.checkNotNull(storeName, "storeName");
    Preconditions.checkNotNull(partitionName, "partitionName");

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

  public static class Builder implements ConfigurationBuilder<KeyValueStoreConfiguration> {
    private String dbType;
    private String groupName;
    private String storeName;
    private String partitionName;
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

    public Builder withStrictTypeCreation(boolean strictTypeCreation) {
      this.strictTypeCreation = strictTypeCreation;

      return this;
    }

    public KeyValueStoreConfiguration build() {
      return new KeyValueStoreConfiguration(dbType, groupName, storeName, partitionName,
          strictTypeCreation);
    }
  }
}
