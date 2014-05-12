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
package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.internal.availability.AvailabilityManager;
import io.kazuki.v0.internal.helper.EncodingHelper;
import io.kazuki.v0.internal.helper.IoHelper;
import io.kazuki.v0.internal.helper.JDBIHelper;
import io.kazuki.v0.internal.helper.LockManager;
import io.kazuki.v0.internal.helper.LogTranslation;
import io.kazuki.v0.internal.helper.SqlTypeHelper;
import io.kazuki.v0.internal.v2schema.compact.FieldTransform;
import io.kazuki.v0.internal.v2schema.compact.StructureTransform;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.Version;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteratorJdbiImpl.KeyValueIterableJdbiImpl;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleSupportBase;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.schema.model.Schema;
import io.kazuki.v0.store.sequence.KeyImpl;
import io.kazuki.v0.store.sequence.ResolvedKey;
import io.kazuki.v0.store.sequence.SequenceService;
import io.kazuki.v0.store.sequence.SequenceServiceJdbiImpl;
import io.kazuki.v0.store.sequence.VersionImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;
import javax.inject.Inject;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;


/**
 * Abstract implementation of key-value storage based on JDBI.
 */
public abstract class KeyValueStoreJdbiBaseImpl
    implements
      KeyValueStore,
      KeyValueStoreIteration,
      KeyValueStoreRegistration {
  public static int MULTIGET_MAX_KEYS = 3000;

  protected final Logger log = LogTranslation.getLogger(getClass());

  protected final AvailabilityManager availability;

  protected final IDBI database;

  protected final LockManager lockManager;

  protected final SchemaStore schemaService;

  protected final SequenceService sequences;

  protected final SqlTypeHelper typeHelper;

  protected final List<KeyValueStoreListener> kvListeners;

  protected abstract String getPrefix();

  protected final Lock nukeLock = new ReentrantLock();

  protected final String tableName;

  public KeyValueStoreJdbiBaseImpl(AvailabilityManager availability, LockManager lockManager,
      IDBI database, SqlTypeHelper typeHelper, SchemaStore schemaService,
      SequenceService sequences, String groupName, String storeName, String partitionName) {
    this.availability = availability;
    this.lockManager = lockManager;
    this.database = database;
    this.schemaService = schemaService;
    this.sequences = sequences;
    this.typeHelper = typeHelper;
    this.kvListeners = new ArrayList<KeyValueStoreListener>();
    this.tableName = "_" + groupName + "_" + storeName + "__kv__" + partitionName;
  }

  @Inject
  public void register(Lifecycle lifecycle) {
    lifecycle.register(new LifecycleSupportBase() {
      @Override
      public void init() {
        KeyValueStoreJdbiBaseImpl.this.initialize();
      }

      @Override
      public void stop() {
        availability.setAvailable(false);
      }
    });
  }

  @Override
  public void addListener(KeyValueStoreListener listener) {
    this.kvListeners.add(listener);
  }

  @Override
  public void initialize() {
    log.debug("Intitializing KeyValueStore {}", this);

    database.inTransaction(new TransactionCallback<Void>() {
      @Override
      public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
        performInitialization(handle, tableName);

        return null;
      }
    });

    availability.setAvailable(true);
    log.debug("Intitialized KeyValueStore {}", this);
  }

  @Override
  public Key toKey(String keyString) {
    return KeyImpl.valueOf(keyString);
  }

  @Override
  public <T> KeyValuePair<T> create(final String type, Class<T> clazz, final T inValue,
      TypeValidation typeSafety) throws KazukiException {
    return create(type, clazz, inValue, null, typeSafety);
  }

  @Override
  public <T> KeyValuePair<T> create(final String type, final Class<T> clazz, final T inValue,
      final ResolvedKey idOverride, TypeValidation typeSafety) throws KazukiException {
    availability.assertAvailable();

    if (type == null
        || (TypeValidation.STRICT.equals(typeSafety) && ((type.contains("@") || type.contains("$"))))) {
      throw new IllegalArgumentException("Invalid entity 'type'");
    }

    try (LockManager toRelease = lockManager.acquire()) {
      final Key newKey;
      final ResolvedKey resolvedKey;

      if (idOverride != null) {
        newKey = sequences.unresolveKey(idOverride);
        resolvedKey = idOverride;
      } else {
        newKey = sequences.nextKey(type);
        resolvedKey = sequences.resolveKey(newKey);
      }

      final Schema schema = schemaService.retrieveSchema(type);

      return database.inTransaction(new TransactionCallback<KeyValuePair<T>>() {
        @Override
        public KeyValuePair<T> inTransaction(Handle handle, TransactionStatus status)
            throws Exception {
          Object storeValue = EncodingHelper.asJsonMap(inValue);

          if (schema != null) {
            FieldTransform fieldTransform = new FieldTransform(schema);
            StructureTransform structureTransform = new StructureTransform(schema);

            Map<String, Object> fieldTransformed =
                fieldTransform.pack((Map<String, Object>) storeValue);

            for (KeyValueStoreListener kvListener : kvListeners) {
              kvListener.onCreate(handle, type, clazz, schema, resolvedKey, fieldTransformed);
            }

            storeValue = structureTransform.pack(fieldTransformed);
          }

          byte[] storeValueBytes = EncodingHelper.convertToSmile(storeValue);

          DateTime createdDate = new DateTime();
          int inserted = doInsert(handle, resolvedKey, storeValueBytes, createdDate);

          if (inserted < 1) {
            throw new KazukiException("Entity not created!");
          }

          return new KeyValuePair<T>(newKey, VersionImpl.createInternal(newKey, 1L), inValue);
        }
      });
    }
  }

  @Override
  public <T> T retrieve(final Key realKey, final Class<T> clazz) throws KazukiException {
    KeyValuePair<T> result = retrieveVersioned(realKey, clazz);

    return (result == null) ? null : result.getValue();
  }

  @Override
  public <T> KeyValuePair<T> retrieveVersioned(final Key realKey, final Class<T> clazz)
      throws KazukiException {
    availability.assertAvailable();

    final Schema schema = schemaService.retrieveSchema(realKey.getTypePart());
    final ResolvedKey resolvedKey = sequences.resolveKey(realKey);

    return database.inTransaction(new TransactionCallback<KeyValuePair<T>>() {
      @Override
      public KeyValuePair<T> inTransaction(Handle handle, TransactionStatus status)
          throws Exception {
        try {
          Map<String, Object> objectMap = loadObjectMap(handle, resolvedKey);

          byte[] objectBytes = getObjectBytes(objectMap);

          if (objectBytes == null) {
            return null;
          }

          Version version =
              VersionImpl.createInternal(realKey, ((Number) objectMap.get("_version")).longValue());

          Object storedValue = EncodingHelper.parseSmile(objectBytes, Object.class);

          if (schema != null && storedValue instanceof List) {
            FieldTransform fieldTransform = new FieldTransform(schema);
            StructureTransform structureTransform = new StructureTransform(schema);
            storedValue =
                fieldTransform.unpack(structureTransform.unpack((List<Object>) storedValue));
          }

          return new KeyValuePair<T>(realKey, version, EncodingHelper.asValue(
              (Map<String, Object>) storedValue, clazz));
        } catch (Exception e) {
          throw new KazukiException(e);
        }
      }
    });
  }

  @Override
  public <T> Map<Key, T> multiRetrieve(final Collection<Key> keys, final Class<T> clazz)
      throws KazukiException {
    availability.assertAvailable();

    if (keys == null || keys.isEmpty()) {
      return Collections.emptyMap();
    }

    Preconditions.checkArgument(keys.size() <= MULTIGET_MAX_KEYS, "Multiget max is %s keys",
        MULTIGET_MAX_KEYS);

    final Map<String, Schema> schemaMap = new HashMap<>(keys.size());

    for (Key realKey : keys) {
      String type = realKey.getTypePart();

      if (schemaMap.containsKey(type)) {
        continue;
      }

      schemaMap.put(type, schemaService.retrieveSchema(realKey.getTypePart()));
    }

    return database.inTransaction(new TransactionCallback<Map<Key, T>>() {
      @Override
      public Map<Key, T> inTransaction(Handle handle, TransactionStatus status) throws Exception {
        Map<Key, T> dbFound = new LinkedHashMap<Key, T>();

        for (Key realKey : keys) {
          final ResolvedKey resolvedKey = sequences.resolveKey(realKey);

          Query<Map<String, Object>> select =
              JDBIHelper.getBoundQuery(handle, getPrefix(), "kv_table_name", tableName,
                  "kv_retrieve");

          select.bind("key_type", resolvedKey.getTypeTag());
          select.bind("key_id_hi", resolvedKey.getIdentifierHi());
          select.bind("key_id_lo", resolvedKey.getIdentifierLo());

          List<Map<String, Object>> results = select.list();

          if (results == null || results.isEmpty()) {
            dbFound.put(realKey, null);

            continue;
          }

          Map<String, Object> first = results.iterator().next();

          Object storedValue =
              EncodingHelper.parseSmile((byte[]) first.get("_value"), Object.class);

          final Schema schema = schemaMap.get(realKey.getTypePart());

          if (schema != null && storedValue instanceof List) {
            FieldTransform fieldTransform = new FieldTransform(schema);
            StructureTransform structureTransform = new StructureTransform(schema);
            storedValue =
                fieldTransform.unpack(structureTransform.unpack((List<Object>) storedValue));
          }

          dbFound.put(realKey, EncodingHelper.asValue((Map<String, Object>) storedValue, clazz));
        }

        return dbFound;
      }
    });
  }

  @Override
  public <T> Map<Key, KeyValuePair<T>> multiRetrieveVersioned(final Collection<Key> keys,
      final Class<T> clazz) throws KazukiException {
    availability.assertAvailable();

    if (keys == null || keys.isEmpty()) {
      return Collections.emptyMap();
    }

    Preconditions.checkArgument(keys.size() <= MULTIGET_MAX_KEYS, "Multiget max is %s keys",
        MULTIGET_MAX_KEYS);

    final Map<String, Schema> schemaMap = new HashMap<>(keys.size());

    for (Key realKey : keys) {
      String type = realKey.getTypePart();

      if (schemaMap.containsKey(type)) {
        continue;
      }

      schemaMap.put(type, schemaService.retrieveSchema(realKey.getTypePart()));
    }

    return database.inTransaction(new TransactionCallback<Map<Key, KeyValuePair<T>>>() {
      @Override
      public Map<Key, KeyValuePair<T>> inTransaction(Handle handle, TransactionStatus status)
          throws Exception {
        Map<Key, KeyValuePair<T>> dbFound = new LinkedHashMap<Key, KeyValuePair<T>>();

        for (Key realKey : keys) {
          final ResolvedKey resolvedKey = sequences.resolveKey(realKey);

          Query<Map<String, Object>> select =
              JDBIHelper.getBoundQuery(handle, getPrefix(), "kv_table_name", tableName,
                  "kv_retrieve");

          select.bind("key_type", resolvedKey.getTypeTag());
          select.bind("key_id_hi", resolvedKey.getIdentifierHi());
          select.bind("key_id_lo", resolvedKey.getIdentifierLo());

          List<Map<String, Object>> results = select.list();

          if (results == null || results.isEmpty()) {
            dbFound.put(realKey, null);

            continue;
          }

          Map<String, Object> first = results.iterator().next();

          Version version =
              VersionImpl.createInternal(realKey, ((Number) first.get("_version")).longValue());

          Object storedValue =
              EncodingHelper.parseSmile((byte[]) first.get("_value"), Object.class);

          final Schema schema = schemaMap.get(realKey.getTypePart());

          if (schema != null && storedValue instanceof List) {
            FieldTransform fieldTransform = new FieldTransform(schema);
            StructureTransform structureTransform = new StructureTransform(schema);
            storedValue =
                fieldTransform.unpack(structureTransform.unpack((List<Object>) storedValue));
          }

          dbFound.put(
              realKey,
              new KeyValuePair<T>(realKey, version, EncodingHelper.asValue(
                  (Map<String, Object>) storedValue, clazz)));
        }

        return dbFound;
      }
    });
  }

  @Override
  public <T> boolean update(final Key realKey, final Class<T> clazz, final T inValue)
      throws KazukiException {
    availability.assertAvailable();

    try (LockManager toRelease = lockManager.acquire()) {
      final String type = realKey.getTypePart();
      final Schema schema = schemaService.retrieveSchema(type);
      final ResolvedKey resolvedKey = sequences.resolveKey(realKey);

      try {
        return database.inTransaction(new TransactionCallback<Boolean>() {
          @Override
          public Boolean inTransaction(Handle handle, TransactionStatus status) throws Exception {
            Map<String, Object> storeValueMap = EncodingHelper.asJsonMap(inValue);
            Object storeValue = storeValueMap;

            FieldTransform fieldTransform = null;
            StructureTransform structureTransform = null;
            Map<String, Object> fieldTransformed = null;

            if (schema != null) {
              fieldTransform = new FieldTransform(schema);
              structureTransform = new StructureTransform(schema);

              fieldTransformed = fieldTransform.pack((Map<String, Object>) storeValue);

              storeValue =
                  structureTransform.pack(fieldTransform.pack((Map<String, Object>) storeValue));
            }

            int updatedCount =
                doUpdate(handle, resolvedKey, EncodingHelper.convertToSmile(storeValue));
            boolean updated = (updatedCount == 1);

            if (updated && schema != null) {
              Map<String, Object> objectMap = loadObjectMap(handle, resolvedKey);

              Map<String, Object> oldInstance =
                  structureTransform.unpack((List<Object>) EncodingHelper.parseSmile(
                      getObjectBytes(objectMap), Object.class));

              for (KeyValueStoreListener kvListener : kvListeners) {
                kvListener.onUpdate(handle, type, clazz, schema, resolvedKey, fieldTransformed,
                    oldInstance);
              }
            }

            return updatedCount == 1;
          }
        });
      } catch (Exception e) {
        throw new KazukiException(e);
      }
    }
  }

  @Override
  public <T> Version updateVersioned(final Key realKey, final Version version,
      final Class<T> clazz, final T inValue) throws KazukiException {
    availability.assertAvailable();

    try (LockManager toRelease = lockManager.acquire()) {
      final String type = realKey.getTypePart();
      final Schema schema = schemaService.retrieveSchema(type);
      final ResolvedKey resolvedKey = sequences.resolveKey(realKey);

      try {
        return database.inTransaction(new TransactionCallback<Version>() {
          @Override
          public Version inTransaction(Handle handle, TransactionStatus status) throws Exception {
            Map<String, Object> storeValueMap = EncodingHelper.asJsonMap(inValue);
            Object storeValue = storeValueMap;

            FieldTransform fieldTransform = null;
            StructureTransform structureTransform = null;

            if (schema != null) {
              fieldTransform = new FieldTransform(schema);
              structureTransform = new StructureTransform(schema);

              storeValue =
                  structureTransform.pack(fieldTransform.pack((Map<String, Object>) storeValue));
            }

            int updatedCount =
                doUpdateVersioned(handle, resolvedKey, (VersionImpl) version,
                    EncodingHelper.convertToSmile(storeValue));

            boolean updated = (updatedCount == 1);

            if (updated && schema != null) {
              Map<String, Object> objectMap = loadObjectMap(handle, resolvedKey);

              Map<String, Object> oldInstance =
                  structureTransform.unpack((List<Object>) EncodingHelper.parseSmile(
                      getObjectBytes(objectMap), Object.class));

              for (KeyValueStoreListener kvListener : kvListeners) {
                kvListener.onUpdate(handle, type, clazz, schema, resolvedKey, storeValueMap,
                    oldInstance);
              }
            }

            return updated ? VersionImpl.createInternal(realKey,
                ((VersionImpl) version).getInternalIdentifier() + 1L) : null;
          }
        });
      } catch (Exception e) {
        throw new KazukiException(e);
      }
    }
  }

  @Override
  public boolean delete(final Key realKey) throws KazukiException {
    availability.assertAvailable();

    try (LockManager toRelease = lockManager.acquire()) {
      final ResolvedKey resolvedKey = sequences.resolveKey(realKey);
      final String type = realKey.getTypePart();
      final Schema schema = schemaService.retrieveSchema(type);

      return database.inTransaction(new TransactionCallback<Boolean>() {
        @Override
        public Boolean inTransaction(Handle handle, TransactionStatus status) throws Exception {
          if (schema != null && !kvListeners.isEmpty()) {
            StructureTransform structureTransform = new StructureTransform(schema);

            Map<String, Object> objectMap = loadObjectMap(handle, resolvedKey);

            Map<String, Object> oldInstance =
                structureTransform.unpack((List<Object>) EncodingHelper.parseSmile(
                    getObjectBytes(objectMap), Object.class));

            for (KeyValueStoreListener kvListener : kvListeners) {
              kvListener.onDelete(handle, type, LinkedHashMap.class, schema, resolvedKey,
                  oldInstance);
            }
          }

          Update delete =
              JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
                  "kv_delete");

          delete.bind("updated_dt", getEpochSecondsNow());
          delete.bind("key_type", resolvedKey.getTypeTag());
          delete.bind("key_id_hi", resolvedKey.getIdentifierHi());
          delete.bind("key_id_lo", resolvedKey.getIdentifierLo());

          int deletedCount = delete.execute();

          return (deletedCount == 1);
        }
      });
    }
  }

  @Override
  public boolean deleteVersioned(final Key realKey, final Version version) throws KazukiException {
    availability.assertAvailable();

    try (LockManager toRelease = lockManager.acquire()) {
      final ResolvedKey resolvedKey = sequences.resolveKey(realKey);
      final String type = realKey.getTypePart();
      final Schema schema = schemaService.retrieveSchema(type);

      return database.inTransaction(new TransactionCallback<Boolean>() {
        @Override
        public Boolean inTransaction(Handle handle, TransactionStatus status) throws Exception {
          if (schema != null && !kvListeners.isEmpty()) {
            StructureTransform structureTransform = new StructureTransform(schema);

            Map<String, Object> objectMap = loadObjectMap(handle, resolvedKey);

            Map<String, Object> oldInstance =
                structureTransform.unpack((List<Object>) EncodingHelper.parseSmile(
                    getObjectBytes(objectMap), Object.class));

            for (KeyValueStoreListener kvListener : kvListeners) {
              kvListener.onDelete(handle, type, LinkedHashMap.class, schema, resolvedKey,
                  oldInstance);
            }
          }

          Update delete =
              JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
                  "kv_delete_versioned");

          delete.bind("updated_dt", getEpochSecondsNow());
          delete.bind("key_type", resolvedKey.getTypeTag());
          delete.bind("key_id_hi", resolvedKey.getIdentifierHi());
          delete.bind("key_id_lo", resolvedKey.getIdentifierLo());
          delete.bind("old_version", ((VersionImpl) version).getInternalIdentifier());

          int deletedCount = delete.execute();

          return (deletedCount == 1);
        }
      });
    }
  }

  @Override
  public boolean deleteHard(final Key realKey) throws KazukiException {
    availability.assertAvailable();

    try (LockManager toRelease = lockManager.acquire()) {
      final ResolvedKey resolvedKey = sequences.resolveKey(realKey);
      final String type = realKey.getTypePart();
      final Schema schema = schemaService.retrieveSchema(type);

      return database.inTransaction(new TransactionCallback<Boolean>() {
        @Override
        public Boolean inTransaction(Handle handle, TransactionStatus status) throws Exception {
          if (schema != null && !kvListeners.isEmpty()) {
            StructureTransform structureTransform = new StructureTransform(schema);

            Map<String, Object> objectMap = loadObjectMap(handle, resolvedKey);

            Map<String, Object> oldInstance =
                structureTransform.unpack((List<Object>) EncodingHelper.parseSmile(
                    getObjectBytes(objectMap), Object.class));

            for (KeyValueStoreListener kvListener : kvListeners) {
              kvListener.onDelete(handle, type, LinkedHashMap.class, schema, resolvedKey,
                  oldInstance);
            }
          }

          Update delete =
              JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
                  "kv_delete_hard");

          delete.bind("updated_dt", getEpochSecondsNow());
          delete.bind("key_type", resolvedKey.getTypeTag());
          delete.bind("key_id_hi", resolvedKey.getIdentifierHi());
          delete.bind("key_id_lo", resolvedKey.getIdentifierLo());

          int deletedCount = delete.execute();

          return (deletedCount == 1);
        }
      });
    }
  }

  @Override
  public boolean deleteHardVersioned(final Key realKey, final Version version)
      throws KazukiException {
    availability.assertAvailable();

    try (LockManager toRelease = lockManager.acquire()) {
      final ResolvedKey resolvedKey = sequences.resolveKey(realKey);
      final String type = realKey.getTypePart();
      final Schema schema = schemaService.retrieveSchema(type);

      return database.inTransaction(new TransactionCallback<Boolean>() {
        @Override
        public Boolean inTransaction(Handle handle, TransactionStatus status) throws Exception {
          if (schema != null && !kvListeners.isEmpty()) {
            StructureTransform structureTransform = new StructureTransform(schema);

            Map<String, Object> objectMap = loadObjectMap(handle, resolvedKey);

            Map<String, Object> oldInstance =
                structureTransform.unpack((List<Object>) EncodingHelper.parseSmile(
                    getObjectBytes(objectMap), Object.class));

            for (KeyValueStoreListener kvListener : kvListeners) {
              kvListener.onDelete(handle, type, LinkedHashMap.class, schema, resolvedKey,
                  oldInstance);
            }
          }

          Update delete =
              JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
                  "kv_delete_hard_versioned");

          delete.bind("updated_dt", getEpochSecondsNow());
          delete.bind("key_type", resolvedKey.getTypeTag());
          delete.bind("key_id_hi", resolvedKey.getIdentifierHi());
          delete.bind("key_id_lo", resolvedKey.getIdentifierLo());
          delete.bind("old_version", ((VersionImpl) version).getInternalIdentifier());

          int deletedCount = delete.execute();

          return (deletedCount == 1);
        }
      });
    }
  }

  @Override
  public Long approximateSize(String type) throws KazukiException {
    availability.assertAvailable();

    Key nextId = ((SequenceServiceJdbiImpl) sequences).peekKey(type);
    ResolvedKey resolvedKey = sequences.resolveKey(nextId);

    return (nextId == null) ? 0L : resolvedKey.getIdentifierLo();
  }

  public void clear(final boolean preserveTypes, final boolean preserveCounters) {
    log.debug("Clearing KeyValueStore {} table {}", this, tableName);

    availability.assertAvailable();

    nukeLock.lock();

    try {
      database.inTransaction(new TransactionCallback<Void>() {
        @Override
        public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
          if (preserveTypes) {
            JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
                "kv_reset");
          } else {
            JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
                "kv_truncate").execute();

            performInitialization(handle, tableName);
          }

          return null;
        }
      });

      sequences.clear(preserveTypes, preserveCounters);
    } finally {
      nukeLock.unlock();
    }

    log.debug("Cleared KeyValueStore {} table {}", this, tableName);
  }

  public void clear(final String type) throws KazukiException {
    log.debug("Clearing KeyValueStore {} table {} type {}", this, tableName, type);

    availability.assertAvailable();

    nukeLock.lock();

    final int typeId = sequences.getTypeId(type, false);

    try {
      database.inTransaction(new TransactionCallback<Void>() {
        @Override
        public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
          JDBIHelper
              .getBoundStatement(handle, getPrefix(), "kv_table_name", tableName, "kv_clear_type")
              .bind("key_type", typeId).execute();

          return null;
        }
      });

      log.debug("Cleared KeyValueStore {} table {} type {}", this, tableName, type);
    } finally {
      nukeLock.unlock();
    }
  }

  public void destroy() {
    log.debug("Destroying KeyValueStore {} table {}", this, tableName);

    availability.assertAvailable();

    nukeLock.lock();

    try {
      database.inTransaction(new TransactionCallback<Void>() {
        @Override
        public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
          JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
              "kv_truncate").execute();
          JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
              "kv_destroy");

          return null;
        }
      });
    } finally {
      nukeLock.unlock();
    }

    log.debug("Destroyed KeyValueStore {} table {}", this, tableName);
  }

  @Override
  public KeyValueStoreIteration iterators() {
    return this;
  }

  @Override
  public <T> KeyValueIterator<T> iterator(String type, Class<T> clazz, SortDirection sortDirection) {
    return this.values(type, clazz, sortDirection).iterator();
  }

  @Override
  public <T> KeyValueIterator<T> iterator(String type, Class<T> clazz, SortDirection sortDirection,
      @Nullable Long offset, @Nullable Long limit) {
    return this.values(type, clazz, sortDirection, offset, limit).iterator();
  }

  @Override
  public <T> KeyValueIterable<KeyValuePair<T>> entries(String type, Class<T> clazz,
      SortDirection sortDirection) {
    return this.entries(type, clazz, sortDirection, null, null);
  }

  @Override
  public <T> KeyValueIterable<KeyValuePair<T>> entries(final String type, final Class<T> clazz,
      SortDirection sortDirection, @Nullable final Long offset, @Nullable final Long limit) {
    final Handle handle = database.open();

    try {
      return new KeyValueIterableJdbiImpl<T>(availability, sequences,
          KeyValueStoreJdbiBaseImpl.this, schemaService.retrieveSchema(type), handle,
          typeHelper.getPrefix(), "_key_id_lo", JDBIHelper.getBoundQuery(handle,
              KeyValueStoreJdbiBaseImpl.this.typeHelper.getPrefix(), "kv_table_name", tableName,
              "kv_key_values_of_type"), type, clazz, sortDirection, offset, limit, true, true);
    } catch (KazukiException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T> KeyValueIterable<Key> keys(String type, Class<T> clazz, SortDirection sortDirection) {
    return this.keys(type, clazz, sortDirection, null, null);
  }

  @Override
  public <T> KeyValueIterable<Key> keys(final String type, final Class<T> clazz,
      final SortDirection sortDirection, @Nullable final Long offset, @Nullable final Long limit) {
    try {
      return new KeyValueIterable<Key>() {
        private final Handle handle = database.open();

        private volatile KeyValueIterableJdbiImpl<T> inner = new KeyValueIterableJdbiImpl<T>(
            availability, sequences, KeyValueStoreJdbiBaseImpl.this,
            schemaService.retrieveSchema(type), handle, typeHelper.getPrefix(), "_key_id_lo",
            JDBIHelper.getBoundQuery(handle, typeHelper.getPrefix(), "kv_table_name", tableName,
                "kv_key_ids_of_type"), type, clazz, sortDirection, offset, limit, false, true);

        @Override
        public KeyValueIterator<Key> iterator() {
          return new KeyValueIterator<Key>() {
            volatile KeyValueIterator<KeyValuePair<T>> innerIter = inner.iterator();

            @Override
            public boolean hasNext() {
              if (innerIter == null) {
                return false;
              }

              return innerIter.hasNext();
            }

            @Override
            public Key next() {
              return innerIter.next().getKey();
            }

            @Override
            public void remove() {
              innerIter.remove();
            }

            @Override
            public void close() {
              IoHelper.closeQuietly(innerIter, log);
              innerIter = null;
            }
          };
        }

        @Override
        public void close() {
          IoHelper.closeQuietly(inner, log);
          inner = null;
        }
      };
    } catch (KazukiException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T> KeyValueIterable<T> values(String type, Class<T> clazz, SortDirection sortDirection) {
    return this.values(type, clazz, sortDirection, null, null);
  }

  @Override
  public <T> KeyValueIterable<T> values(final String type, final Class<T> clazz,
      final SortDirection sortDirection, @Nullable final Long offset, @Nullable final Long limit) {
    try {
      return new KeyValueIterable<T>() {
        private final Handle handle = database.open();

        private volatile KeyValueIterableJdbiImpl<T> inner = new KeyValueIterableJdbiImpl<T>(
            availability, sequences, KeyValueStoreJdbiBaseImpl.this,
            schemaService.retrieveSchema(type), handle, typeHelper.getPrefix(), "_key_id_lo",
            JDBIHelper.getBoundQuery(handle, KeyValueStoreJdbiBaseImpl.this.typeHelper.getPrefix(),
                "kv_table_name", tableName, "kv_key_values_of_type"), type, clazz, sortDirection,
            offset, limit, true, true);

        @Override
        public KeyValueIterator<T> iterator() {
          return new KeyValueIterator<T>() {
            volatile KeyValueIterator<KeyValuePair<T>> innerIter = inner.iterator();

            @Override
            public boolean hasNext() {
              if (innerIter == null) {
                return false;
              }

              return innerIter.hasNext();
            }

            @Override
            public T next() {
              return innerIter.next().getValue();
            }

            @Override
            public void remove() {
              innerIter.remove();
            }

            @Override
            public void close() {
              IoHelper.closeQuietly(innerIter, log);
              innerIter = null;
            }
          };
        }

        @Override
        public void close() {
          IoHelper.closeQuietly(inner, log);
          inner = null;
        }
      };
    } catch (KazukiException e) {
      throw Throwables.propagate(e);
    }
  }

  private byte[] getObjectBytes(Map<String, Object> objectMap) {
    if (objectMap == null) {
      return null;
    }

    return (byte[]) objectMap.get("_value");
  }

  private Map<String, Object> loadObjectMap(final Handle handle, final ResolvedKey key)
      throws KazukiException {
    Query<Map<String, Object>> select =
        JDBIHelper.getBoundQuery(handle, getPrefix(), "kv_table_name", tableName, "kv_retrieve");

    select.bind("key_type", key.getTypeTag());
    select.bind("key_id_hi", key.getIdentifierHi());
    select.bind("key_id_lo", key.getIdentifierLo());

    List<Map<String, Object>> results = select.list();

    if (results == null || results.isEmpty()) {
      return null;
    }

    return results.iterator().next();
  }

  private void performInitialization(Handle handle, String tableName) {
    log.debug("Creating table if not exist with name {} for KeyValueStore {}", tableName, this);

    try {
      JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
          "kv_create_table").execute();
    } catch (Throwable t) {
      if (!this.typeHelper.isTableAlreadyExistsException(t)) {
        throw Throwables.propagate(t);
      }
    }

    log.debug("Creating index if not exists on table {} for KeyValueStore {}", tableName, this);

    JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
        "kv_create_table_index").execute();
  }

  private int doInsert(Handle handle, final ResolvedKey resolvedKey, byte[] valueBytes,
      DateTime date) {
    Update update =
        JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName, "kv_create");
    update.bind("key_type", resolvedKey.getTypeTag());
    update.bind("key_id_hi", resolvedKey.getIdentifierHi());
    update.bind("key_id_lo", resolvedKey.getIdentifierLo());
    update.bind("created_dt", date.withZone(DateTimeZone.UTC).getMillis() / 1000);
    update.bind("version", 1L);
    update.bind("value", valueBytes);
    int inserted = update.execute();

    return inserted;
  }

  private int doUpdate(Handle handle, final ResolvedKey resolvedKey, byte[] valueBytes) {
    Update update =
        JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName, "kv_update");
    update.bind("key_type", resolvedKey.getTypeTag());
    update.bind("key_id_hi", resolvedKey.getIdentifierHi());
    update.bind("key_id_lo", resolvedKey.getIdentifierLo());
    update.bind("updated_dt", getEpochSecondsNow());
    update.bind("value", valueBytes);
    int updated = update.execute();

    return updated;
  }

  private int doUpdateVersioned(Handle handle, final ResolvedKey resolvedKey,
      final VersionImpl version, byte[] valueBytes) {
    Update update =
        JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
            "kv_update_versioned");

    update.bind("key_type", resolvedKey.getTypeTag());
    update.bind("key_id_hi", resolvedKey.getIdentifierHi());
    update.bind("key_id_lo", resolvedKey.getIdentifierLo());
    update.bind("updated_dt", getEpochSecondsNow());
    update.bind("old_version", version.getInternalIdentifier());
    update.bind("new_version", version.getInternalIdentifier() + 1L);
    update.bind("value", valueBytes);
    int updated = update.execute();

    return updated;
  }

  private int getEpochSecondsNow() {
    return (int) (new DateTime().withZone(DateTimeZone.UTC).getMillis() / 1000);
  }
}
