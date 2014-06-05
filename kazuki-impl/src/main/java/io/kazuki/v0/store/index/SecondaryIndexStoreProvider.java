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
package io.kazuki.v0.store.index;

import io.kazuki.v0.internal.availability.AvailabilityManager;
import io.kazuki.v0.internal.helper.LockManager;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.keyvalue.KeyValueStoreConfiguration;
import io.kazuki.v0.store.management.KazukiComponent;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.sequence.SequenceService;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;

import org.skife.jdbi.v2.IDBI;

import com.google.inject.Injector;
import com.google.inject.Provider;

@Singleton
public class SecondaryIndexStoreProvider implements Provider<SecondaryIndexSupport> {
  private final SecondaryIndexSupport instance;
  private final Injector inject;

  @Inject
  public SecondaryIndexStoreProvider(KeyValueStoreConfiguration kvConfig,
      AvailabilityManager availability, LockManager lockManager,
      KazukiComponent<DataSource> dataSource, IDBI database, SequenceService sequences,
      SchemaStore schemaStore, KeyValueStore kvStore, SecondaryIndexTableHelper tableHelper,
      Injector injector) {
    this.inject = injector;

    if (kvConfig.isSecondaryIndex()) {
      this.instance =
          new SecondaryIndexStoreJdbiImpl(availability, lockManager, dataSource, database,
              sequences, schemaStore, kvStore, tableHelper, kvConfig.getGroupName(),
              kvConfig.getStoreName(), kvConfig.getPartitionName());
    } else {
      this.instance =
          new SecondaryIndexStoreBruteForceImpl(kvConfig, sequences, kvStore, schemaStore);
    }

    this.inject.injectMembers(instance);
  }

  public SecondaryIndexSupport get() {
    return instance;
  }
}
