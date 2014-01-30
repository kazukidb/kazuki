package io.kazuki.v0.internal.helper;

import java.util.UUID;

import io.kazuki.v0.store.jdbi.JdbiDataSourceConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueStoreConfiguration;
import io.kazuki.v0.store.sequence.SequenceServiceConfiguration;

public class Configurations {
  public static JdbiDataSourceConfiguration.Builder getJdbi() {
    return new JdbiDataSourceConfiguration.Builder().withJdbcDriver("org.h2.Driver")
        .withJdbcUrl("jdbc:h2:target/" + UUID.randomUUID()).withJdbcUser("root")
        .withJdbcPassword("not_really_used").withPoolMinConnections(25).withPoolMaxConnections(25);
  }

  public static SequenceServiceConfiguration.Builder getSequence(String groupName, String storeName) {
    return new SequenceServiceConfiguration.Builder().withDbType("h2").withGroupName(groupName)
        .withStoreName(storeName).withIncrementBlockSize(1000L).withStrictTypeCreation(true);
  }

  public static KeyValueStoreConfiguration.Builder getKeyValue(String groupName, String storeName) {
    return new KeyValueStoreConfiguration.Builder().withDbType("h2").withGroupName(groupName)
        .withStoreName(storeName).withPartitionName("default").withPartitionSize(1000L)
        .withStrictTypeCreation(true);
  }
}
