package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.internal.helper.EncodingHelper;
import io.kazuki.v0.internal.sequence.SequenceServiceDatabaseImpl;
import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.internal.v2schema.compact.FieldTransform;
import io.kazuki.v0.internal.v2schema.compact.StructureTransform;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.SchemaManager;

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

/**
 * Abstract implementation of key-value storage based on JDBI.
 */
public abstract class JDBIKeyValueStorage implements KeyValueStorage {
  public static int MULTIGET_MAX_KEYS = 3000;

  protected final IDBI database;

  protected final SchemaManager schemaManager;

  protected final SequenceServiceDatabaseImpl sequences;

  protected abstract String getPrefix();

  protected final Lock nukeLock = new ReentrantLock();

  @Inject
  public JDBIKeyValueStorage(IDBI database, SchemaManager schemaManager,
      SequenceServiceDatabaseImpl sequences) {
    this.database = database;
    this.schemaManager = schemaManager;
    this.sequences = sequences;
    this.initialize();
  }

  @Override
  public void initialize() {
    database.inTransaction(new TransactionCallback<Void>() {
      @Override
      public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
        performInitialization(handle);

        return null;
      }
    });

    sequences.initializeCountersFromDb();
  }

  @Override
  public <T> Key create(final String type, Class<T> clazz, final T inValue, boolean strictType)
      throws KazukiException {
    return create(type, clazz, inValue, null, strictType);
  }

  @Override
  public <T> Key create(final String type, Class<T> clazz, final T inValue, final Long idOverride,
      boolean strictType) throws KazukiException {
    if (type == null || (strictType && ((type.contains("@") || type.contains("$"))))) {
      throw new KazukiException("Invalid entity 'type'");
    }

    try {
      final Key newKey = idOverride == null ? sequences.nextKey(type) : new Key(type, idOverride);
      final Schema schema = schemaManager.retrieveSchema(type);

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
    } catch (Exception e) {
      throw new KazukiException(e);
    }
  }

  @Override
  public <T> T retrieve(final Key realKey, Class<T> clazz) throws KazukiException {
    try {
      byte[] objectBytes = getObjectBytes(realKey);

      if (objectBytes == null) {
        return null;
      }

      final Schema schema = schemaManager.retrieveSchema(realKey.getType());

      Object storedValue =
          EncodingHelper.parseSmile(objectBytes, schema != null ? List.class : Map.class);

      if (schema != null) {
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

    if (keys.size() > MULTIGET_MAX_KEYS) {
      throw new KazukiException("Multiget max is " + MULTIGET_MAX_KEYS + " keys");
    }

    return database.inTransaction(new TransactionCallback<Map<Key, T>>() {
      @Override
      public Map<Key, T> inTransaction(Handle handle, TransactionStatus status) throws Exception {
        Map<Key, T> dbFound = new LinkedHashMap<Key, T>();

        for (Key realKey : keys) {
          final int typeId = sequences.getTypeId(realKey.getType(), false);
          final long keyId = realKey.getId();

          String selectStmt = getPrefix() + "retrieve";

          Query<Map<String, Object>> select = handle.createQuery(selectStmt);

          select.bind("key_type", typeId);
          select.bind("key_id", keyId);

          List<Map<String, Object>> results = select.list();

          if (results == null || results.isEmpty()) {
            dbFound.put(realKey, null);

            continue;
          }

          Map<String, Object> first = results.iterator().next();

          Object storedValue = EncodingHelper.parseSmile((byte[]) first.get("_value"), List.class);

          final Schema schema = schemaManager.retrieveSchema(realKey.getType());
          if (schema != null) {
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

          final Schema schema = schemaManager.retrieveSchema(realKey.getType());

          if (schema != null) {
            FieldTransform fieldTransform = new FieldTransform(schema);
            StructureTransform structureTransform = new StructureTransform(schema);
            storeValue =
                structureTransform.pack(fieldTransform.pack((Map<String, Object>) storeValue));
          }

          int updated = doUpdate(handle, typeId, keyId, EncodingHelper.convertToSmile(storeValue));

          return updated == 0;
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

        Update delete = handle.createStatement(getPrefix() + "delete");
        delete.bind("updated_dt", getEpochSecondsNow());
        delete.bind("key_type", typeId);
        delete.bind("key_id", keyId);

        int deleted = delete.execute();

        return deleted != 0;
      }
    });
  }

  public void clear(final boolean preserveSchema) {
    nukeLock.lock();

    try {
      database.inTransaction(new TransactionCallback<Void>() {
        @Override
        public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
          try {
            if (preserveSchema) {
              handle.createStatement(getPrefix() + "reset_sequences").execute();
              handle.createStatement(getPrefix() + "reset_key_values").execute();
            } else {
              handle.createStatement(getPrefix() + "truncate_key_types").execute();
              handle.createStatement(getPrefix() + "truncate_sequences").execute();
              handle.createStatement(getPrefix() + "truncate_key_values").execute();

              performInitialization(handle);
            }

            return null;
          } catch (Exception e) {
            e.printStackTrace();

            throw new RuntimeException(e);
          }
        }
      });
      sequences.clear();
    } finally {
      nukeLock.unlock();
    }
  }

  public <T> Iterator<T> iterator(final String type, final Class<T> clazz) throws Exception {
    return iterator(type, clazz, 0L, null);
  }

  @Override
  public <T> Iterator<T> iterator(final String type, final Class<T> clazz, final Long offset,
      final Long limit) throws Exception {
    final SchemaManager schemas = schemaManager;

    return new Iterator<T>() {
      private final Iterator<String> inner = createKeyIterator(type, offset, limit);
      private final Schema schema = schemas.retrieveSchema(type);
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
          Object result =
              loadFriendlyEntity(Key.valueOf(nextKey), schema != null ? List.class : Map.class);

          nextKey = advance();

          if (schema != null) {
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

  private byte[] getObjectBytes(final Key key) throws Exception {
    return database.inTransaction(new TransactionCallback<byte[]>() {
      @Override
      public byte[] inTransaction(Handle handle, TransactionStatus status) throws Exception {
        final int typeId = sequences.getTypeId(key.getType(), false);
        final long keyId = key.getId();

        String queryName = getPrefix() + "retrieve";
        Query<Map<String, Object>> select = handle.createQuery(queryName);

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

        Query<Map<String, Object>> select = handle.createQuery(getPrefix() + "key_ids_of_type");
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

  private void performInitialization(Handle handle) {
    handle.createStatement(getPrefix() + "init_key_types").execute();
    handle.createStatement(getPrefix() + "init_sequences").execute();
    handle.createStatement(getPrefix() + "init_key_values").execute();
    handle.createStatement(getPrefix() + "init_key_values_index").execute();

    try {
      handle.createStatement(getPrefix() + "populate_key_types").execute();
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      handle.createStatement(getPrefix() + "populate_sequences").execute();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private int doInsert(Handle handle, final int typeId, final long nextId, byte[] valueBytes,
      DateTime date) {
    Update update = handle.createStatement(getPrefix() + "create");
    update.bind("key_type", typeId);
    update.bind("key_id", nextId);
    update.bind("created_dt", date.withZone(DateTimeZone.UTC).getMillis() / 1000);
    update.bind("version", 0L);
    update.bind("value", valueBytes);
    int inserted = update.execute();

    return inserted;
  }

  private int doUpdate(Handle handle, final int typeId, final long keyId, byte[] valueBytes) {
    Update update = handle.createStatement(getPrefix() + "update");
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

  private <T> T loadFriendlyEntity(final Key realKey, Class<T> clazz) throws Exception {
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
