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
import io.kazuki.v0.internal.helper.LogTranslation;
import io.kazuki.v0.internal.v2schema.compact.FieldTransform;
import io.kazuki.v0.internal.v2schema.compact.StructureTransform;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.schema.model.Schema;
import io.kazuki.v0.store.sequence.KeyImpl;
import io.kazuki.v0.store.sequence.SequenceService;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class KeyValueStoreIteratorJdbiImpl {
  private static final Logger log = LogTranslation.getLogger(KeyValueStoreIteratorJdbiImpl.class);


  public static class KeyValueIterableJdbiImpl<T> implements KeyValueIterable<KeyValuePair<T>> {
    private final AvailabilityManager availability;
    private final Handle handle;
    private final String prefix;
    private final String idColumn;
    private final Query<Map<String, Object>> query;
    private final SequenceService sequences;
    private final KeyValueStore kvStore;
    private final Schema schema;
    private final String type;
    private final Class<T> clazz;
    private final SortDirection sortDirection;
    private final Long offset;
    private final Long limit;
    private final boolean includeValues;
    private final boolean doBind;
    private volatile KeyValueIterator<KeyValuePair<T>> theIter = null;
    private boolean instantiated = false;

    public KeyValueIterableJdbiImpl(final AvailabilityManager availability,
        final SequenceService sequences, final KeyValueStore kvStore, final Schema schema,
        final Handle handle, final String prefix, final String idColumn,
        final Query<Map<String, Object>> query, String type, Class<T> clazz,
        SortDirection sortDirection, Long offset, Long limit, boolean includeValues, boolean doBind) {
      this.availability = availability;
      this.sequences = sequences;
      this.handle = handle;
      this.prefix = prefix;
      this.idColumn = idColumn;
      this.query = query;
      this.kvStore = kvStore;
      this.schema = schema;
      this.type = type;
      this.clazz = clazz;
      this.sortDirection = sortDirection;
      this.offset = offset;
      this.limit = limit;
      this.includeValues = includeValues;
      this.doBind = doBind;
    }

    @Override
    public KeyValueIterator<KeyValuePair<T>> iterator() {
      if (instantiated) {
        throw new IllegalStateException("iterable may only be used once!");
      }

      theIter = new KeyValueIterator<KeyValuePair<T>>() {
        private volatile KeyValueIterator<Map<String, Object>> inner = createKeyValueIterator(
            handle, query, sequences, prefix, type, sortDirection, offset, limit, doBind);

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
              key = KeyImpl.createInternal(type, ((Number) record.get(idColumn)).longValue());
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
            kvStore.delete(currentKv.getKey());
            currentKv = null;
          } catch (KazukiException e) {
            throw Throwables.propagate(e);
          }
        }

        @Override
        public void close() {
          IoHelper.closeQuietly(inner, log);
          inner = null;
        }
      };

      instantiated = true;

      return theIter;
    }

    @Override
    public void close() {
      IoHelper.closeQuietly(theIter, log);
      theIter = null;
    }
  }

  private static KeyValueIterator<Map<String, Object>> createKeyValueIterator(final Handle handle,
      final Query<Map<String, Object>> select, final SequenceService sequences,
      final String prefix, final String type, final SortDirection sortDirection, final Long offset,
      final Long limit, boolean doBind) {
    final Integer typeId;
    try {
      typeId = sequences.getTypeId(type, false);
    } catch (KazukiException e) {
      throw Throwables.propagate(e);
    }

    if (typeId == null) {
      return null;
    }

    if (doBind) {
      String order =
          (sortDirection == null || SortDirection.ASCENDING.equals(sortDirection)) ? "ASC" : "DESC";

      select.define("order", order);

      select.bind("key_type", typeId);
      select.bind("offset", offset);
      select.bind("limit", limit);
    }

    final Iterator<Map<String, Object>> iter = select.iterator();

    return new KeyValueIterator<Map<String, Object>>() {
      private Handle theHandle = handle;

      @Override
      public boolean hasNext() {
        if (theHandle == null) {
          return false;
        }

        boolean hasNext = iter.hasNext();

        if (!hasNext) {
          IoHelper.closeQuietly(theHandle, log);
          theHandle = null;
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
        IoHelper.closeQuietly(theHandle, log);
        theHandle = null;
      }
    };
  }
}
