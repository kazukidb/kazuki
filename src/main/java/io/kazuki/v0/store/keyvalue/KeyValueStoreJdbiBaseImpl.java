package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.internal.availability.AvailabilityManager;
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
import io.kazuki.v0.store.sequence.KeyImpl;
import io.kazuki.v0.store.sequence.ResolvedKey;
import io.kazuki.v0.store.sequence.SequenceService;
import io.kazuki.v0.store.sequence.SequenceServiceJdbiImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
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
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;


/**
 * Abstract implementation of key-value storage based on JDBI.
 */
public abstract class KeyValueStoreJdbiBaseImpl implements KeyValueStore, KeyValueStoreIteration {
  public static int MULTIGET_MAX_KEYS = 3000;

  protected final Logger log = LoggerFactory.getLogger(getClass());

  protected final AvailabilityManager availability;

  protected final IDBI database;

  protected final SchemaStore schemaService;

  protected final SequenceService sequences;

  protected final SqlTypeHelper typeHelper;

  protected abstract String getPrefix();

  protected final Lock nukeLock = new ReentrantLock();

  protected final String tableName;

  public KeyValueStoreJdbiBaseImpl(AvailabilityManager availability, IDBI database,
      SqlTypeHelper typeHelper, SchemaStore schemaService, SequenceService sequences,
      String groupName, String storeName, String partitionName) {
    this.availability = availability;
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

    availability.setAvailable(true);
    log.debug("Intitialized KeyValueStore {}", this);
  }

  @Override
  public Key toKey(String keyString) {
    return KeyImpl.valueOf(keyString);
  }

  @Override
  public <T> Key create(final String type, Class<T> clazz, final T inValue,
      TypeValidation typeSafety) throws KazukiException {
    return create(type, clazz, inValue, null, typeSafety);
  }

  @Override
  public <T> Key create(final String type, Class<T> clazz, final T inValue,
      final ResolvedKey idOverride, TypeValidation typeSafety) throws KazukiException {
    availability.assertAvailable();

    if (type == null
        || (TypeValidation.STRICT.equals(typeSafety) && ((type.contains("@") || type.contains("$"))))) {
      throw new IllegalArgumentException("Invalid entity 'type'");
    }

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
        int inserted = doInsert(handle, resolvedKey, storeValueBytes, createdDate);

        if (inserted < 1) {
          throw new KazukiException("Entity not created!");
        }

        return newKey;
      }
    });
  }

  @Override
  public <T> T retrieve(final Key realKey, Class<T> clazz) throws KazukiException {
    availability.assertAvailable();

    try {
      ResolvedKey resolvedKey = sequences.resolveKey(realKey);
      byte[] objectBytes = getObjectBytes(resolvedKey);

      if (objectBytes == null) {
        return null;
      }

      final Schema schema = schemaService.retrieveSchema(realKey.getTypePart());

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
    availability.assertAvailable();

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

          final Schema schema = schemaService.retrieveSchema(realKey.getTypePart());

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
    availability.assertAvailable();

    try {
      return database.inTransaction(new TransactionCallback<Boolean>() {
        @Override
        public Boolean inTransaction(Handle handle, TransactionStatus status) throws Exception {
          ResolvedKey resolvedKey = sequences.resolveKey(realKey);
          Object storeValue = EncodingHelper.asJsonMap(inValue);

          final Schema schema = schemaService.retrieveSchema(realKey.getTypePart());

          if (schema != null) {
            FieldTransform fieldTransform = new FieldTransform(schema);
            StructureTransform structureTransform = new StructureTransform(schema);
            storeValue =
                structureTransform.pack(fieldTransform.pack((Map<String, Object>) storeValue));
          }

          int updated = doUpdate(handle, resolvedKey, EncodingHelper.convertToSmile(storeValue));

          return updated == 1;
        }
      });
    } catch (Exception e) {
      throw new KazukiException(e);
    }
  }

  @Override
  public boolean delete(final Key realKey) throws KazukiException {
    availability.assertAvailable();

    return database.inTransaction(new TransactionCallback<Boolean>() {
      @Override
      public Boolean inTransaction(Handle handle, TransactionStatus status) throws Exception {
        ResolvedKey resolvedKey = sequences.resolveKey(realKey);

        Update delete =
            JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
                "kv_delete");

        delete.bind("updated_dt", getEpochSecondsNow());
        delete.bind("key_type", resolvedKey.getTypeTag());
        delete.bind("key_id_hi", resolvedKey.getIdentifierHi());
        delete.bind("key_id_lo", resolvedKey.getIdentifierLo());

        int deleted = delete.execute();

        return deleted == 1;
      }
    });
  }

  @Override
  public boolean deleteHard(final Key realKey) throws KazukiException {
    availability.assertAvailable();

    return database.inTransaction(new TransactionCallback<Boolean>() {
      @Override
      public Boolean inTransaction(Handle handle, TransactionStatus status) throws Exception {
        ResolvedKey resolvedKey = sequences.resolveKey(realKey);

        Update delete =
            JDBIHelper.getBoundStatement(handle, getPrefix(), "kv_table_name", tableName,
                "kv_delete_hard");

        delete.bind("updated_dt", getEpochSecondsNow());
        delete.bind("key_type", resolvedKey.getTypeTag());
        delete.bind("key_id_hi", resolvedKey.getIdentifierHi());
        delete.bind("key_id_lo", resolvedKey.getIdentifierLo());

        int deleted = delete.execute();

        return deleted != 0;
      }
    });
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
  public <T> KeyValueIterator<T> iterator(String type, Class<T> clazz) {
    return this.values(type, clazz).iterator();
  }

  @Override
  public <T> KeyValueIterator<T> iterator(String type, Class<T> clazz, @Nullable Long offset,
      @Nullable Long limit) {
    return this.values(type, clazz, offset, limit).iterator();
  }

  @Override
  public <T> KeyValueIterable<KeyValuePair<T>> entries(String type, Class<T> clazz) {
    return this.entries(type, clazz, null, null);
  }

  @Override
  public <T> KeyValueIterable<KeyValuePair<T>> entries(final String type, final Class<T> clazz,
      @Nullable final Long offset, @Nullable final Long limit) {
    return new KeyValueIterableJdbiImpl<T>(type, clazz, offset, limit, true);
  }

  @Override
  public <T> KeyValueIterable<Key> keys(String type, Class<T> clazz) {
    return this.keys(type, clazz, null, null);
  }

  @Override
  public <T> KeyValueIterable<Key> keys(final String type, final Class<T> clazz,
      @Nullable final Long offset, @Nullable final Long limit) {
    return new KeyValueIterable<Key>() {
      private final KeyValueIterableJdbiImpl<T> inner = new KeyValueIterableJdbiImpl<T>(type,
          clazz, offset, limit, false);

      @Override
      public KeyValueIterator<Key> iterator() {
        final KeyValueIterator<KeyValuePair<T>> innerIter = inner.iterator();

        return new KeyValueIterator<Key>() {
          @Override
          public boolean hasNext() {
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
            innerIter.close();
          }
        };
      }

      @Override
      public void close() {
        inner.close();
      }
    };
  }

  @Override
  public <T> KeyValueIterable<T> values(String type, Class<T> clazz) {
    return this.values(type, clazz, null, null);
  }

  @Override
  public <T> KeyValueIterable<T> values(final String type, final Class<T> clazz,
      @Nullable final Long offset, @Nullable final Long limit) {
    return new KeyValueIterable<T>() {
      private final KeyValueIterableJdbiImpl<T> inner = new KeyValueIterableJdbiImpl<T>(type,
          clazz, offset, limit, true);

      @Override
      public KeyValueIterator<T> iterator() {
        final KeyValueIterator<KeyValuePair<T>> innerIter = inner.iterator();

        return new KeyValueIterator<T>() {
          @Override
          public boolean hasNext() {
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
            innerIter.close();
          }
        };
      }

      @Override
      public void close() {
        inner.close();
      }
    };
  }

  private byte[] getObjectBytes(final ResolvedKey key) throws KazukiException {
    return database.inTransaction(new TransactionCallback<byte[]>() {
      @Override
      public byte[] inTransaction(Handle handle, TransactionStatus status) throws Exception {
        Query<Map<String, Object>> select =
            JDBIHelper
                .getBoundQuery(handle, getPrefix(), "kv_table_name", tableName, "kv_retrieve");

        select.bind("key_type", key.getTypeTag());
        select.bind("key_id_hi", key.getIdentifierHi());
        select.bind("key_id_lo", key.getIdentifierLo());

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

  private KeyValueIterator<Map<String, Object>> createKeyValueIterator(final String type,
      final Long offset, final Long limit, boolean hasValue) {
    final Integer typeId;
    try {
      typeId = sequences.getTypeId(type, false);
    } catch (KazukiException e) {
      throw Throwables.propagate(e);
    }

    if (typeId == null) {
      return null;
    }

    final Handle handle = database.open();

    String query = hasValue ? "kv_key_values_of_type" : "kv_key_ids_of_type";

    final Query<Map<String, Object>> select =
        JDBIHelper.getBoundQuery(handle, getPrefix(), "kv_table_name", tableName, query);

    select.bind("key_type", typeId);
    select.bind("offset", offset);
    select.bind("limit", limit);

    final Iterator<Map<String, Object>> iter = select.iterator();

    return new KeyValueIterator<Map<String, Object>>() {
      private Handle theHandle = handle;

      @Override
      public boolean hasNext() {
        boolean hasNext = iter.hasNext();

        if (!hasNext) {
          closeQuietly();
        }

        return hasNext;
      }

      @Override
      public Map<String, Object> next() {
        return iter.next();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() {
        closeQuietly();
      }

      @Override
      protected void finalize() throws Throwable {
        closeQuietly();
      }

      private void closeQuietly() {
        try {
          if (theHandle != null) {
            theHandle.close();
            theHandle = null;
          }
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
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
    update.bind("version", 0L);
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
    update.bind("old_version", 0L);
    update.bind("new_version", 0L);
    update.bind("value", valueBytes);
    int updated = update.execute();

    return updated;
  }

  private int getEpochSecondsNow() {
    return (int) (new DateTime().withZone(DateTimeZone.UTC).getMillis() / 1000);
  }

  class KeyValueIterableJdbiImpl<T> implements KeyValueIterable<KeyValuePair<T>> {
    private final String type;
    private final Class<T> clazz;
    private final Long offset;
    private final Long limit;
    private final boolean includeValues;
    private KeyValueIterator<KeyValuePair<T>> theIter = null;
    private boolean instantiated = false;

    public KeyValueIterableJdbiImpl(String type, Class<T> clazz, Long offset, Long limit,
        boolean includeValues) {
      this.type = type;
      this.clazz = clazz;
      this.offset = offset;
      this.limit = limit;
      this.includeValues = includeValues;
    }

    @Override
    public KeyValueIterator<KeyValuePair<T>> iterator() {
      if (instantiated) {
        throw new IllegalStateException("iterable may only be used once!");
      }

      try {
        theIter = new KeyValueIterator<KeyValuePair<T>>() {
          private volatile KeyValueIterator<Map<String, Object>> inner = createKeyValueIterator(
              type, offset, limit, includeValues);
          private final Schema schema = schemaService.retrieveSchema(type);
          private KeyValuePair<T> nextKv = advance();
          private KeyValuePair<T> currentKv = null;

          public KeyValuePair<T> advance() {
            Preconditions.checkNotNull(inner, "iterator");

            Map<String, Object> record = null;
            Key key = null;
            T value = null;

            while (key == null && inner.hasNext()) {
              record = inner.next();

              try {
                key = KeyImpl.createInternal(type, ((Number) record.get("_key_id_lo")).longValue());
              } catch (Exception e) {
                throw Throwables.propagate(e);
              }

              break;
            }

            if (key == null) {
              return null;
            }

            try {
              if (includeValues) {
                byte[] resultBytes = (byte[]) record.get("_value");
                Object result = EncodingHelper.parseSmile(resultBytes, Object.class);

                if (schema != null && result instanceof List) {
                  FieldTransform fieldTransform = new FieldTransform(schema);
                  StructureTransform structureTransform = new StructureTransform(schema);
                  result = fieldTransform.unpack(structureTransform.unpack((List<Object>) result));
                }

                value = EncodingHelper.asValue((Map<String, Object>) result, clazz);
              }
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }

            return new KeyValuePair<T>(key, value);
          }

          @Override
          public boolean hasNext() {
            Preconditions.checkNotNull(inner, "iterator");

            return nextKv != null;
          }

          @Override
          public KeyValuePair<T> next() {
            availability.assertAvailable();

            currentKv = nextKv;
            nextKv = advance();

            return currentKv;
          }

          @Override
          public void remove() {
            availability.assertAvailable();

            Preconditions.checkNotNull(inner, "iterator");
            Preconditions.checkNotNull(currentKv, "next");

            try {
              KeyValueStoreJdbiBaseImpl.this.delete(currentKv.getKey());
              currentKv = null;
            } catch (KazukiException e) {
              throw Throwables.propagate(e);
            }
          }

          @Override
          public void close() {
            if (inner != null) {
              inner.close();
              inner = null;
            }
          }

          @Override
          protected void finalize() throws Throwable {
            if (inner != null) {
              inner.close();
            }
          }
        };
      } catch (KazukiException e) {
        throw Throwables.propagate(e);
      }

      instantiated = true;

      return theIter;
    }

    @Override
    public void close() {
      if (theIter != null) {
        theIter.close();
        theIter = null;
      }
    }
  }
}
