package io.kazuki.v0.store.schema;

import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.internal.v2schema.SchemaValidator;
import io.kazuki.v0.internal.v2schema.types.TransformException;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.sequence.KeyImpl;
import io.kazuki.v0.store.sequence.ResolvedKey;
import io.kazuki.v0.store.sequence.SequenceService;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;


public class SchemaStoreImpl implements SchemaStore {
  public static final String SCHEMA_PREFIX = "$schema";

  private final Logger log = LoggerFactory.getLogger(getClass());

  private final SequenceService sequences;
  private KeyValueStore store;

  @Inject
  public SchemaStoreImpl(SequenceService sequences) {
    this.sequences = sequences;
  }

  @Inject
  public synchronized void setKeyValueStorage(KeyValueStore store) {
    log.debug("Setting schema KeyValueStore for {}", this);

    this.store = store;
  }

  public Key createSchema(String type, Schema value) throws KazukiException {
    if (store == null) {
      throw new IllegalStateException("schemaManager not initialized with KV store");
    }

    Integer typeId = getTypeIdPossiblyNull(type, true);

    if (typeId == null) {
      throw new KazukiException("unable to allocate new type id for Schema type: " + type);
    }

    Key realKey = KeyImpl.createInternal(SCHEMA_PREFIX, typeId.longValue());

    Schema existing = this.store.retrieve(realKey, Schema.class);
    if (existing != null) {
      return realKey;
    }

    try {
      SchemaValidator.validate(value);
    } catch (TransformException e) {
      throw new KazukiException("invalid schema definition for type: " + type, e);
    }

    ResolvedKey resolvedKey = sequences.resolveKey(realKey);
    store.create(SCHEMA_PREFIX, Schema.class, value, resolvedKey, TypeValidation.LAX);

    return realKey;
  }

  public Schema retrieveSchema(String type) throws KazukiException {
    if (store == null) {
      throw new IllegalStateException("schemaManager not initialized with KV store");
    }

    Integer typeId = getTypeIdPossiblyNull(type, false);

    if (typeId == null || type.equals(SCHEMA_PREFIX)) {
      return null;
    }

    return store.retrieve(KeyImpl.createInternal(SCHEMA_PREFIX, typeId.longValue()), Schema.class);
  }

  public boolean updateSchema(final String type, final Schema value) throws KazukiException {
    if (store == null) {
      throw new IllegalStateException("schemaManager not initialized with KV store");
    }

    final Integer typeId = getTypeIdPossiblyNull(type, false);

    if (typeId == null) {
      return false;
    }

    Key theKey = KeyImpl.createInternal(SCHEMA_PREFIX, typeId.longValue());

    final Schema original = store.retrieve(theKey, Schema.class);

    if (original == null) {
      return false;
    }

    try {
      SchemaValidator.validate(value);
      SchemaValidator.validateUpgrade(original, value);
    } catch (TransformException e) {
      throw new KazukiException("invalid Schema update for type: " + type, e);
    }

    return store.update(theKey, Schema.class, value);
  }

  public boolean deleteSchema(final String type) throws KazukiException {
    if (store == null) {
      throw new IllegalStateException("schemaManager not initialized with KV store");
    }

    Integer typeId = getTypeIdPossiblyNull(type, true);

    if (typeId == null) {
      return false;
    }

    Key theKey = KeyImpl.createInternal(SCHEMA_PREFIX, typeId.longValue());

    return store.deleteHard(theKey);
  }

  public void clear() throws KazukiException {
    this.store.clear(SCHEMA_PREFIX);
  }

  private Integer getTypeIdPossiblyNull(String type, boolean val) {
    try {
      return sequences.getTypeId(type, val);
    } catch (IllegalArgumentException e) {
      return null;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
