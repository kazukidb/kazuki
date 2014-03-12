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
package io.kazuki.v0.store.schema;

import io.kazuki.v0.internal.helper.LogTranslation;
import io.kazuki.v0.internal.v2schema.SchemaValidator;
import io.kazuki.v0.internal.v2schema.types.TransformException;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.schema.model.Schema;
import io.kazuki.v0.store.sequence.KeyImpl;
import io.kazuki.v0.store.sequence.ResolvedKey;
import io.kazuki.v0.store.sequence.SequenceService;

import javax.inject.Inject;

import org.slf4j.Logger;

import com.google.common.base.Throwables;


public class SchemaStoreImpl implements SchemaStore {
  public static final String SCHEMA_PREFIX = "$schema";

  private final Logger log = LogTranslation.getLogger(getClass());

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

  public synchronized Key createSchema(String type, Schema value) throws KazukiException {
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

  public synchronized Schema retrieveSchema(String type) throws KazukiException {
    if (store == null) {
      throw new IllegalStateException("schemaManager not initialized with KV store");
    }

    Integer typeId = getTypeIdPossiblyNull(type, false);

    if (typeId == null || type.equals(SCHEMA_PREFIX)) {
      return null;
    }

    return store.retrieve(KeyImpl.createInternal(SCHEMA_PREFIX, typeId.longValue()), Schema.class);
  }

  public synchronized boolean updateSchema(final String type, final Schema value)
      throws KazukiException {
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

  public synchronized boolean deleteSchema(final String type) throws KazukiException {
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

  public synchronized void clear() throws KazukiException {
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
