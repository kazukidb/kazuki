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
package io.kazuki.v0.store.schema;

import io.kazuki.v0.internal.helper.LockManager;
import io.kazuki.v0.internal.helper.LogTranslation;
import io.kazuki.v0.internal.v2schema.SchemaValidator;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.Version;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.keyvalue.KeyValueStoreConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.management.ComponentDescriptor;
import io.kazuki.v0.store.management.ComponentRegistrar;
import io.kazuki.v0.store.management.KazukiComponent;
import io.kazuki.v0.store.management.impl.ComponentDescriptorImpl;
import io.kazuki.v0.store.management.impl.LateBindingComponentDescriptorImpl;
import io.kazuki.v0.store.schema.model.Schema;
import io.kazuki.v0.store.schema.model.TransformException;
import io.kazuki.v0.store.sequence.KeyImpl;
import io.kazuki.v0.store.sequence.ResolvedKey;
import io.kazuki.v0.store.sequence.SequenceService;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

public class SchemaStoreImpl implements SchemaStore, SchemaStoreRegistration {
  public static final String SCHEMA_PREFIX = "$schema";

  private final Logger log = LogTranslation.getLogger(getClass());

  private final LockManager lockManager;
  private final SequenceService sequences;
  private final List<SchemaStoreListener> ssListeners;
  private final ComponentDescriptor<SchemaStore> componentDescriptor;
  private KeyValueStore store;

  @Inject
  public SchemaStoreImpl(LockManager lockManager, SequenceService sequences,
      KeyValueStoreConfiguration config) {
    this.lockManager = lockManager;
    this.sequences = sequences;
    this.ssListeners = new ArrayList<SchemaStoreListener>();
    this.componentDescriptor =
        new ComponentDescriptorImpl<SchemaStore>("KZ:SchemaStore:" + config.getGroupName() + "-"
            + config.getStoreName(), SchemaStore.class, (SchemaStore) this,
            new ImmutableList.Builder().add(
                ((KazukiComponent) lockManager).getComponentDescriptor(),
                ((KazukiComponent) sequences).getComponentDescriptor(),
                (new LateBindingComponentDescriptorImpl<KeyValueStore>() {
                  @Override
                  public KazukiComponent<KeyValueStore> get() {
                    return (KazukiComponent<KeyValueStore>) SchemaStoreImpl.this.store;
                  }
                })).build());
  }

  @Inject
  public synchronized void setKeyValueStorage(KeyValueStore store) {
    this.store = store;
  }

  @Override
  public void addListener(SchemaStoreListener listener) {
    this.ssListeners.add(listener);
  }

  @Override
  public ComponentDescriptor<SchemaStore> getComponentDescriptor() {
    return this.componentDescriptor;
  }

  @Override
  @Inject
  public void registerAsComponent(ComponentRegistrar manager) {
    manager.register(this.componentDescriptor);
  }

  public Version createSchema(String type, Schema value) throws KazukiException {
    if (store == null) {
      throw new IllegalStateException("schemaManager not initialized with KV store");
    }

    try (LockManager toRelease = lockManager.acquire()) {
      Integer typeId = getTypeIdPossiblyNull(type, true);

      if (typeId == null) {
        throw new KazukiException("unable to allocate new type id for Schema type: " + type);
      }

      Key realKey = KeyImpl.createInternal(SCHEMA_PREFIX, typeId.longValue());

      KeyValuePair<Schema> existing = this.store.retrieveVersioned(realKey, Schema.class);
      if (existing != null) {
        return existing.getVersion();
      }

      try {
        SchemaValidator.validate(value);
      } catch (TransformException e) {
        throw new KazukiException("invalid schema definition for type: " + type, e);
      }

      for (SchemaStoreListener ssListener : ssListeners) {
        ssListener.onSchemaCreate(type, value);
      }

      ResolvedKey resolvedKey = sequences.resolveKey(realKey);
      KeyValuePair<Schema> schemaKv =
          store.create(SCHEMA_PREFIX, Schema.class, value, resolvedKey, TypeValidation.LAX);

      return schemaKv.getVersion();
    }
  }

  public KeyValuePair<Schema> retrieveSchema(String type) throws KazukiException {
    if (store == null) {
      throw new IllegalStateException("schemaManager not initialized with KV store");
    }

    try (LockManager toRelease = lockManager.acquire()) {
      Integer typeId = getTypeIdPossiblyNull(type, false);

      if (typeId == null || type.equals(SCHEMA_PREFIX)) {
        return null;
      }

      return store.retrieveVersioned(KeyImpl.createInternal(SCHEMA_PREFIX, typeId.longValue()),
          Schema.class);
    }
  }

  public Version updateSchema(final String type, final Version version, final Schema value)
      throws KazukiException {
    if (store == null) {
      throw new IllegalStateException("schemaManager not initialized with KV store");
    }

    try (LockManager toRelease = lockManager.acquire()) {
      final Integer typeId = getTypeIdPossiblyNull(type, false);

      if (typeId == null) {
        return null;
      }

      Key theKey = KeyImpl.createInternal(SCHEMA_PREFIX, typeId.longValue());

      final Schema original = store.retrieve(theKey, Schema.class);

      if (original == null) {
        return null;
      }

      try {
        SchemaValidator.validate(value);
        SchemaValidator.validateUpgrade(original, value);
      } catch (TransformException e) {
        throw new KazukiException("invalid Schema update for type: " + type, e);
      }

      for (SchemaStoreListener ssListener : ssListeners) {
        ssListener.onSchemaUpdate(type, value, original,
            this.store.iterators().entries(type, LinkedHashMap.class, SortDirection.ASCENDING));
      }

      return store.updateVersioned(theKey, version, Schema.class, value);
    }
  }

  public boolean deleteSchema(final String type, final Version version) throws KazukiException {
    if (store == null) {
      throw new IllegalStateException("schemaManager not initialized with KV store");
    }

    try (LockManager toRelease = lockManager.acquire()) {
      Integer typeId = getTypeIdPossiblyNull(type, true);

      if (typeId == null) {
        return false;
      }

      Schema original =
          store.retrieve(KeyImpl.createInternal(SCHEMA_PREFIX, Long.valueOf(typeId)), Schema.class);

      for (SchemaStoreListener ssListener : ssListeners) {
        ssListener.onSchemaDelete(type, original);
      }

      Key theKey = KeyImpl.createInternal(SCHEMA_PREFIX, typeId.longValue());

      return store.deleteHard(theKey);
    }
  }

  public void clear() throws KazukiException {
    try (LockManager toRelease = lockManager.acquire()) {
      this.store.clear(SCHEMA_PREFIX);
    }
  }

  private Integer getTypeIdPossiblyNull(String type, boolean val) {
    try {
      return sequences.getTypeId(type, val);
    } catch (KazukiException e) {
      return null;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
