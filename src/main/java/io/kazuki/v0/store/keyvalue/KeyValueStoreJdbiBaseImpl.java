package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.internal.helper.EncodingHelper;
import io.kazuki.v0.internal.helper.JDBIHelper;
import io.kazuki.v0.internal.helper.SqlTypeHelper;
import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.internal.v2schema.compact.FieldTransform;
import io.kazuki.v0.internal.v2schema.compact.StructureTransform;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleSupportBase;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.sequence.SequenceService;
import io.kazuki.v0.store.sequence.SequenceServiceJdbiImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;


/**
 * Abstract implementation of key-value storage based on JDBI.
 */
public abstract class KeyValueStoreJdbiBaseImpl implements KeyValueStore {
  public static int MULTIGET_MAX_KEYS = 3000;

  protected final Logger log = LoggerFactory.getLogger(getClass());

  protected final IDBI database;

  protected final SchemaStore schemaService;

  protected final SequenceService sequences;

  protected final SqlTypeHelper typeHelper;

  protected abstract String getPrefix();

  protected final Lock nukeLock = new ReentrantLock();

  protected final String tableName;

  public KeyValueStoreJdbiBaseImpl(IDBI database, SqlTypeHelper typeHelper,
      SchemaStore schemaService, SequenceService sequences, String groupName, String storeName,
      String partitionName) {
    this.database = database;
    this.typeHelper = typeHelper;
    this.schemaService = schemaService;
    this.sequences = sequences;
    this.tableName = "_" + groupName + "_" + storeName + "__kv__" + partitionName;
  }

  @Inject
  public void register(Lifecycle lifecycle) {
    lifecycle.register(new LifecycleSupportBase() {
      @Override
      public void init() {
        KeyValueStoreJdbiBaseImpl.this.initialize();
      }
    });
  }

  @Override
  public void initialize() {
    log.info("Intitializing KeyValueStore {}", this);

    database.inTransaction(new TransactionCallback<Void>() {
      @Override
      public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
        performInitialization(handle, tableName);

        return null;
      }
    });

    log.debug("Intitialized KeyValueStore {}", this);
  }

  @Override
  public <T> Key create(final String type, Class<T> clazz, final T inValue,
      TypeValidation typeSafety) throws KazukiException {
    return create(type, clazz, inValue, null, typeSafety);
  }

  @Override
  public <T> Key create(final String type, Class<T> clazz, final T inValue, final Long idOverride,
      TypeValidation typeSafety) throws KazukiException {
    if (type == null
        || (TypeValidation.STRICT.equals(typeSafety) && ((type.contains("@") || type.contains("$"))))) {
      throw new IllegalArgumentException("Invalid entity 'type'");
    }

    final Key newKey = idOverride == null ? sequences.nextKey(type) : new Key(type, idOverride);
    final Schema schema = schemaService.retrieveSchema(type);

    return database.inTransaction(new TransactionCallback<Key>() {
      @Override
      public Key inTransaction(Handle handle, TransactionStatus status) throws Exception {
        Object storeValue = EncodingHelper.asJsonMap(inValue);

        if (schema != null) {
          FieldTransform fieldTransform = new FieldTransform(schema);
          StructureTransform structureTransform = new StructureTransform(schema);
          storeValue =
              structureTransform.pack(fieldTransform.pack((Map<String, Object>) storeValue));
        }

        byte[] storeValueBytes = EncodingHelper.convertToSmile(storeValue);

        DateTime createdDate = new DateTime();
        int inserted =
            doInsert(handle, sequences.getTypeId(type, false), newKey.getId(), storeValueBytes,
                createdDate);

        if (inserted < 1) {
          throw new KazukiException("Entity not created!");
        }

        return newKey;
      }
    });
  }

  @Override
  public <T> T retrieve(final Key realKey, Class<T> clazz) throws KazukiException {
    try {
      byte[] objectBytes = getObjectBytes(realKey);

      if (objectBytes == null) {
        return null;
      }

      final Schema schema = schemaService.retrieveSchema(realKey.getType());

      Object storedValue = EncodingHelper.parseSmile(objectBytes, Object.class);

      if (schema != null && storedValue instanceof List) {
        FieldTransform fieldTransform = new FieldTransform(schema);
        StructureTransform structureTransform = new StructureTransform(schema);
        storedValue = fieldTransform.unpack(structureTransform.unpack((List<Object>) storedValue));
      }

      return EncodingHelper.asValue((Map<String, Object>) storedValue, clazz);
    } catch (Exception e) {
      throw new KazukiException(e);
    }
  }

  @Override
  public <T> Map<Key, T> multiRetrieve(final Collection<Key> keys, final Class<T> clazz)
      throws KazukiException {
    if (keys == null || keys.isEmpty()) {
      return Collections.emptyMap();
    }

    Preconditions.checkArgument(keys.size() <= MULTIGET_MAX_KEYS, "Multiget max is %s keys",
        MULTIGET_MAX_KEYS);

    return database.inTransaction(new TransactionCallback<Map<Key, T>>() {
      @Override
      public Map<Key, T> inTransaction(Handle handle, TransactionStatus status) throws Exception {
        Map<Key, T> dbFound = new LinkedHashMap<Key, T>();

        for (Key realKey : keys) {
          final int typeId = sequences.getTypeId(realKey.getType(), false);
          final long keyId = realKey.getId();

          Query<Map<String, Object>> select =
              JDBIHelper.getBoundQuery(handle, getPrefix(), "kv_table_name", tableName,
                  "kv_retrieve");

          select.bind("key_type", typeId);
          select.bind("key_id", keyId);

          List<Map<String, Object>> results = select.list();

          if (results == null || results.isEmpty()) {
            dbFound.put(realKey, null);

            continue;
          }

          Map<String, Object> first = results.iterator().next();

          Object storedValue =
              EncodingHelper.parseSmile((byte[]) first.get("_value"), Object.class);

          final Schema schema = schemaService.retrieveSchema(realKey.getType());

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
  public <T> boolean update(final Key realKey, final Class<T> clazz, final T inValue)
      throws KazukiException {
    try {
      return database.inTransaction(new TransactionCallback<Boolean>() {
        @Override
        public Boolean inTransaction(Handle handle, TransactionStatus status) throws Exception {
          final int typeId = sequences.getTypeId(realKey.getType(), false);
          final long keyId = realKey.getId();

          byte[] objectBytes = getObjectBytes(realKey);

          if (objectBytes == null) {
            return false;
          }

          Object storeValue = EncodingHelper.asJsonMap(inValue);

          final Schema schema = schemaService.retrieveSchema(realKey.getType());

          if (schema != null) {
            FieldTransform fieldTransform = new FieldTransform(schema);
            StructureTransform structureTransform = new StructureTransform(schema);
            storeValue =
                structureTransform.pack(fieldTransform.pack((Map<String, Object>) storeValue));
          }

          int updated = doUpdate(handle, typeId, keyId, EncodingHelper.convertToSmile(storeValue));

          return updated == 1;
        }
      });
    } catch (Exception e) {
      throw new KazukiException(e);
    }
  }

  @Override
  public boolean delete(final Key realKey) throws KazukiException {
    return database.inTransaction(new TransactionCallback<Boolean>() {
      @Override
      public Boolean inTransaction(Handle handle, TransactionStatus status) throws Exception {
        final int typeId = sequences.getTypeId(realKey.getType(), false);
        final long keyId = realKey.getId();

        Update delete =
            JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
                "kv_delete");

        delete.bind("updated_dt", getEpochSecondsNow());
        delete.bind("key_type", typeId);
        delete.bind("key_id", keyId);

        int deleted = delete.execute();

        return deleted != 0;
      }
    });
  }

  @Override
  public boolean deleteHard(final Key realKey) throws KazukiException {
    return database.inTransaction(new TransactionCallback<Boolean>() {
      @Override
      public Boolean inTransaction(Handle handle, TransactionStatus status) throws Exception {
        final int typeId = sequences.getTypeId(realKey.getType(), false);
        final long keyId = realKey.getId();

        Update delete =
            JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
                "kv_delete_hard");

        delete.bind("updated_dt", getEpochSecondsNow());
        delete.bind("key_type", typeId);
        delete.bind("key_id", keyId);

        int deleted = delete.execute();

        return deleted != 0;
      }
    });
  }

  @Override
  public Long approximateSize(String type) throws KazukiException {
    Key nextId = ((SequenceServiceJdbiImpl) sequences).peekKey(type);

    return (nextId == null) ? 0L : nextId.getId();
  }

  public void clear(final boolean preserveTypes, final boolean preserveCounters) {
    log.debug("Clearing KeyValueStore {} table {}", this, tableName);

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

  public <T> Iterator<T> iterator(final String type, final Class<T> clazz) throws KazukiException {
    return iterator(type, clazz, 0L, null);
  }

  @Override
  public <T> Iterator<T> iterator(final String type, final Class<T> clazz, final Long offset,
      final Long limit) throws KazukiException {
    return new Iterator<T>() {
      private final Iterator<String> inner = createKeyIterator(type, offset, limit);
      private final Schema schema = schemaService.retrieveSchema(type);
      private String nextKey = advance();

      public String advance() {
        String key = null;

        while (key == null && inner.hasNext()) {
          String newKey = inner.next();

          final Key realKey;
          try {
            realKey = Key.valueOf(newKey);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

          if (type.equals(realKey.getType())) {
            key = newKey;
            break;
          }
        }

        return key;
      }

      @Override
      public boolean hasNext() {
        return nextKey != null;
      }

      @Override
      public T next() {
        try {
          Object result = loadFriendlyEntity(Key.valueOf(nextKey), Object.class);

          nextKey = advance();

          if (schema != null && result instanceof List) {
            FieldTransform fieldTransform = new FieldTransform(schema);
            StructureTransform structureTransform = new StructureTransform(schema);
            result = fieldTransform.unpack(structureTransform.unpack((List<Object>) result));
          }

          return EncodingHelper.asValue((Map<String, Object>) result, clazz);
        } catch (Exception e) {
          if (!(e instanceof RuntimeException)) {
            throw new RuntimeException(e);
          } else {
            throw (RuntimeException) e;
          }
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private byte[] getObjectBytes(final Key key) throws KazukiException {
    return database.inTransaction(new TransactionCallback<byte[]>() {
      @Override
      public byte[] inTransaction(Handle handle, TransactionStatus status) throws Exception {
        final int typeId = sequences.getTypeId(key.getType(), false);
        final long keyId = key.getId();

        Query<Map<String, Object>> select =
            JDBIHelper
                .getBoundQuery(handle, getPrefix(), "kv_table_name", tableName, "kv_retrieve");

        select.bind("key_type", typeId);
        select.bind("key_id", keyId);

        List<Map<String, Object>> results = select.list();

        if (results == null || results.isEmpty()) {
          return null;
        }

        Map<String, Object> first = results.iterator().next();

        byte[] foundBytes = (byte[]) first.get("_value");

        return foundBytes;
      }
    });
  }

  private Iterator<String> createKeyIterator(final String type, final Long offset, final Long limit) {
    return database.withHandle(new HandleCallback<Iterator<String>>() {
      @Override
      public Iterator<String> withHandle(Handle handle) throws Exception {
        final int typeId = sequences.getTypeId(type, false);

        Query<Map<String, Object>> select =
            JDBIHelper.getBoundQuery(handle, getPrefix(), "kv_table_name", tableName,
                "kv_key_ids_of_type");
        select.bind("key_type", typeId);
        select.bind("offset", offset);
        select.bind("limit", limit);

        List<String> inefficentButExpedient = new ArrayList<String>();

        Iterator<Map<String, Object>> iter = select.iterator();
        while (iter.hasNext()) {
          Map<String, Object> next = iter.next();
          inefficentButExpedient.add(Key.valueOf(type + ":" + next.get("_key_id"))
              .getEncryptedIdentifier());
        }

        return inefficentButExpedient.iterator();
      }
    });
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

  private int doInsert(Handle handle, final int typeId, final long nextId, byte[] valueBytes,
      DateTime date) {
    Update update =
        JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName, "kv_create");
    update.bind("key_type", typeId);
    update.bind("key_id", nextId);
    update.bind("created_dt", date.withZone(DateTimeZone.UTC).getMillis() / 1000);
    update.bind("version", 0L);
    update.bind("value", valueBytes);
    int inserted = update.execute();

    return inserted;
  }

  private int doUpdate(Handle handle, final int typeId, final long keyId, byte[] valueBytes) {
    Update update =
        JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName, "kv_update");
    update.bind("key_type", typeId);
    update.bind("key_id", keyId);
    update.bind("updated_dt", getEpochSecondsNow());
    update.bind("old_version", 0L);
    update.bind("new_version", 0L);
    update.bind("value", valueBytes);
    int updated = update.execute();

    return updated;
  }

  private int getEpochSecondsNow() {
    return (int) (new DateTime().withZone(DateTimeZone.UTC).getMillis() / 1000);
  }

  private <T> T loadFriendlyEntity(final Key realKey, Class<T> clazz) throws KazukiException {
    byte[] objectBytes = getObjectBytes(realKey);

    if (objectBytes == null) {
      return null;
    }

    try {
      return (T) EncodingHelper.parseSmile(objectBytes, clazz);
    } catch (Exception e) {
      throw new KazukiException(e);
    }
  }
}
