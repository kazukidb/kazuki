package io.kazuki.v0.store;

import io.kazuki.v0.internal.sequence.SequenceServiceDatabaseImpl;
import io.kazuki.v0.internal.v1schema.validator.ValidationException;
import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.internal.v2schema.SchemaValidator;
import io.kazuki.v0.store.keyvalue.KeyValueStorage;

import com.google.inject.Inject;


public class SchemaManager {
  private static final String SCHEMA_PREFIX = "$schema";

  @Inject
  private KeyValueStorage store;

  @Inject
  protected SequenceServiceDatabaseImpl sequences;

  public long createSchema(String type, Schema value) throws Exception {
    Integer typeId = getTypeIdPossiblyNull(type, true);

    if (typeId == null) {
      throw new KazukiException("unable to allocate new type id for Schema type: " + type);
    }

    Schema existing = this.store.retrieve(Key.valueOf(SCHEMA_PREFIX + ":" + typeId), Schema.class);
    if (existing != null) {
      return typeId.longValue();
    }

    try {
      SchemaValidator.validate(value);
    } catch (ValidationException e) {
      throw new KazukiException("invalid schema definition for type: " + type, e);
    }

    store.create(SCHEMA_PREFIX, Schema.class, value, typeId.longValue(), false);

    return typeId.longValue();
  }

  public Schema retrieveSchema(String type) throws Exception {
    Integer typeId = getTypeIdPossiblyNull(type, false);

    if (typeId == null) {
      return null;
    }

    return store.retrieve(new Key(SCHEMA_PREFIX, typeId.longValue()), Schema.class);
  }

  public boolean updateSchema(final String type, final Schema value) throws Exception {
    final Integer typeId = getTypeIdPossiblyNull(type, false);

    if (typeId == null) {
      return false;
    }

    Key theKey = new Key(SCHEMA_PREFIX, typeId.longValue());

    final Schema original = store.retrieve(theKey, Schema.class);

    if (original == null) {
      return false;
    }

    try {
      SchemaValidator.validate(value);
      SchemaValidator.validateUpgrade(original, value);
    } catch (ValidationException e) {
      throw new KazukiException("invalid Schema update for type: " + type, e);
    }

    return store.update(theKey, Schema.class, value);
  }

  public boolean deleteSchema(final String type) throws KazukiException {
    Integer typeId = getTypeIdPossiblyNull(type, true);

    if (typeId == null) {
      return false;
    }

    Key theKey = new Key(SCHEMA_PREFIX, typeId.longValue());

    // Schema existing = store.retrieve(theKey, Schema.class);
    //
    // if (existing == null) {
    // return false;
    // }

    return store.delete(theKey);
  }

  private Integer getTypeIdPossiblyNull(String type, boolean val) {
    try {
      return sequences.getTypeId(type, val);
    } catch (KazukiException e) {
      return null;
    }
  }
}
