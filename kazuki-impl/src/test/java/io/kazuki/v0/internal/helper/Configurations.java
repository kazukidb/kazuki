/**
 * Copyright 2014 the original author or authors
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
        .withStrictTypeCreation(true).withSecondaryIndex(false);
  }
}
