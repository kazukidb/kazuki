/**
 * Copyright 2014 Sunny Gleason and original author or authors
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
package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.internal.availability.AvailabilityManager;
import io.kazuki.v0.internal.helper.H2TypeHelper;
import io.kazuki.v0.internal.helper.LockManager;
import io.kazuki.v0.internal.helper.SqlTypeHelper;
import io.kazuki.v0.store.management.KazukiComponent;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.sequence.SequenceService;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.skife.jdbi.v2.IDBI;

/**
 * H2 SQL implementation of key-value storage using JDBI.
 */
public class KeyValueStoreJdbiH2Impl extends KeyValueStoreJdbiBaseImpl {
  protected String getPrefix() {
    return H2TypeHelper.DATABASE_PREFIX;
  }

  @Inject
  public KeyValueStoreJdbiH2Impl(AvailabilityManager availability, LockManager lockManager,
      KazukiComponent<DataSource> dataSource, IDBI database, SqlTypeHelper typeHelper,
      SchemaStore schemaManager, SequenceService sequences, KeyValueStoreConfiguration config) {
    this(availability, lockManager, dataSource, database, typeHelper, schemaManager, sequences,
        config.getGroupName(), config.getStoreName(), config.getPartitionName());
  }

  public KeyValueStoreJdbiH2Impl(AvailabilityManager availability, LockManager lockManager,
      KazukiComponent<DataSource> dataSource, IDBI database, SqlTypeHelper typeHelper,
      SchemaStore schemaManager, SequenceService sequences, String groupName, String storeName,
      String partitionName) {
    super(availability, lockManager, dataSource, database, typeHelper, schemaManager, sequences,
        groupName, storeName, partitionName);
  }
}
