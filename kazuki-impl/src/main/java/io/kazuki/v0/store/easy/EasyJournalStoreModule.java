/**
 * Copyright 2014 Sunny Gleason
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
package io.kazuki.v0.store.easy;

import io.kazuki.v0.store.jdbi.H2DataSourceModule;
import io.kazuki.v0.store.jdbi.JdbiDataSourceConfiguration;
import io.kazuki.v0.store.journal.SimpleJournalStoreModule;
import io.kazuki.v0.store.keyvalue.KeyValueStoreConfiguration;
import io.kazuki.v0.store.sequence.H2SequenceServiceModule;
import io.kazuki.v0.store.sequence.SequenceServiceConfiguration;

import java.util.concurrent.atomic.AtomicReference;

import com.google.inject.AbstractModule;

public class EasyJournalStoreModule extends AbstractModule {
  private final String name;
  private final String propertiesBase;
  private final AtomicReference<JdbiDataSourceConfiguration> jdbiConfig;
  private final AtomicReference<SequenceServiceConfiguration> sequenceConfig;
  private final AtomicReference<KeyValueStoreConfiguration> keyValueConfig;

  public EasyJournalStoreModule(String name, String propertiesBase) {
    this.name = name;
    this.propertiesBase = propertiesBase;
    this.jdbiConfig = new AtomicReference<JdbiDataSourceConfiguration>();
    this.sequenceConfig = new AtomicReference<SequenceServiceConfiguration>();
    this.keyValueConfig = new AtomicReference<KeyValueStoreConfiguration>();
  }

  public EasyJournalStoreModule withJdbiConfig(JdbiDataSourceConfiguration config) {
    this.jdbiConfig.set(config);

    return this;
  }

  public EasyJournalStoreModule withSequenceConfig(SequenceServiceConfiguration config) {
    this.sequenceConfig.set(config);

    return this;
  }

  public EasyJournalStoreModule withKeyValueStoreConfig(KeyValueStoreConfiguration config) {
    this.keyValueConfig.set(config);

    return this;
  }

  @Override
  protected void configure() {
    install(new H2DataSourceModule(name, propertiesBase == null ? null : propertiesBase
        + "/jdbi.properties").withConfiguration(this.jdbiConfig.get()));
    install(new H2SequenceServiceModule(name, propertiesBase == null ? null : propertiesBase
        + "/sequence.properties").withConfiguration(this.sequenceConfig.get()));
    install(new SimpleJournalStoreModule(name, propertiesBase == null ? null : propertiesBase
        + "/keyvalue.properties").withConfiguration(this.keyValueConfig.get()));
  }
}
