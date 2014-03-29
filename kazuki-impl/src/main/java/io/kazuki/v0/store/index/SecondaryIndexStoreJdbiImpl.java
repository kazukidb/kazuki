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
package io.kazuki.v0.store.index;

import io.kazuki.v0.internal.availability.AvailabilityManager;
import io.kazuki.v0.internal.helper.IoHelper;
import io.kazuki.v0.internal.helper.JDBIHelper;
import io.kazuki.v0.internal.helper.LogTranslation;
import io.kazuki.v0.internal.helper.SqlParamBindings;
import io.kazuki.v0.internal.v2schema.compact.FieldTransform;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.index.query.QueryHelper;
import io.kazuki.v0.store.index.query.QueryTerm;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValueIterator;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteratorJdbiImpl.KeyValueIterableJdbiImpl;
import io.kazuki.v0.store.keyvalue.KeyValueStoreRegistration;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.SchemaStoreRegistration;
import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.IndexAttribute;
import io.kazuki.v0.store.schema.model.IndexDefinition;
import io.kazuki.v0.store.schema.model.Schema;
import io.kazuki.v0.store.sequence.ResolvedKey;
import io.kazuki.v0.store.sequence.SequenceService;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.slf4j.Logger;

import com.google.common.base.Throwables;
import com.google.inject.Inject;

public class SecondaryIndexStoreJdbiImpl implements SecondaryIndexSupport {
  private final Logger log = LogTranslation.getLogger(getClass());

  private final AvailabilityManager availability;
  private final IDBI database;
  private final SequenceService sequence;
  private final SchemaStore schema;
  private final KeyValueStore kvStore;
  private final SecondaryIndexTableHelper tableHelper;
  private final String groupName;
  private final String storeName;
  private final String partitionName;

  @Inject
  public SecondaryIndexStoreJdbiImpl(AvailabilityManager availability, IDBI database,
      SequenceService sequence, SchemaStore schema, KeyValueStore kvStore,
      SecondaryIndexTableHelper tableHelper, String groupName, String storeName,
      String partitionName) {
    this.availability = availability;
    this.database = database;
    this.sequence = sequence;
    this.schema = schema;
    this.kvStore = kvStore;
    this.tableHelper = tableHelper;
    this.groupName = groupName;
    this.storeName = storeName;
    this.partitionName = partitionName;
  }

  @Inject
  public void registerKeyValueStore(KeyValueStoreRegistration kvStore) {
    kvStore.addListener(this);
  }

  @Inject
  public void registerSchemaStore(SchemaStoreRegistration schemaStore) {
    schemaStore.addListener(this);
  }

  @Override
  public <T> KeyValueIterable<Key> queryWithoutPagination(String type, Class<T> clazz,
      String indexName, List<QueryTerm> query, SortDirection sortDirection, @Nullable Long offset,
      @Nullable Long limit) {
    try {
      return this.doIndexQuery(database, type, indexName, query, sortDirection, null, null, false,
          schema.retrieveSchema(type));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T> KeyValueIterable<Key> queryWithoutPagination(String type, Class<T> clazz,
      String indexName, String queryString, SortDirection sortDirection, @Nullable Long offset,
      @Nullable Long limit) {
    List<QueryTerm> query = QueryHelper.parseQuery(queryString);

    return queryWithoutPagination(type, clazz, indexName, query, sortDirection, offset, limit);
  }

  @Override
  public <T> QueryResultsPage<T> queryWithPagination(String type, Class<T> clazz, String indexName,
      List<QueryTerm> query, SortDirection sortDirection, Boolean loadResults, PageToken token,
      Long limit) {
    throw new UnsupportedOperationException("not supported - yet");
  }

  @Override
  public <T> QueryResultsPage<T> queryWithPagination(String type, Class<T> clazz, String indexName,
      String queryString, SortDirection sortDirection, Boolean loadResults, PageToken token,
      Long limit) {
    throw new UnsupportedOperationException("not supported - yet");
  }

  @Override
  public Map<UniqueEntityDescription<?>, ?> multiRetrieveUniqueEntities(
      Collection<UniqueEntityDescription<?>> entityDefinitions) {
    throw new UnsupportedOperationException("not supported - yet");
  }

  @Override
  public Map<UniqueEntityDescription<?>, Key> multiRetrieveUniqueKeys(
      Collection<UniqueEntityDescription<?>> entityDefinitions) {
    throw new UnsupportedOperationException("not supported - yet");
  }

  public void createTable(IDBI database, final String type, final String indexName,
      final Schema schema) {
    JDBIHelper.createTable(database, tableHelper.getTableDrop(type, indexName, groupName,
        storeName, partitionName), tableHelper.getTableDefinition(type, indexName, schema,
        groupName, storeName, partitionName));
  }

  @Override
  public <T> void onCreate(Handle handle, String type, Class<T> clazz, Schema schema,
      ResolvedKey resolvedKey, Map<String, Object> instance) {
    try {
      for (IndexDefinition indexDef : schema.getIndexes()) {
        this.insertEntity(handle, resolvedKey.getIdentifierLo(), instance, type,
            indexDef.getName(), schema);
      }
    } catch (KazukiException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T> void onUpdate(Handle handle, String type, Class<T> clazz, Schema schema,
      ResolvedKey resolvedKey, Map<String, Object> newInstance, Map<String, Object> oldInstance) {
    try {
      for (IndexDefinition indexDef : schema.getIndexes()) {
        this.updateEntity(handle, resolvedKey.getIdentifierLo(), newInstance, oldInstance, type,
            indexDef.getName(), schema);
      }
    } catch (KazukiException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T> void onDelete(Handle handle, String type, Class<T> clazz, Schema schema,
      ResolvedKey resolvedKey, Map<String, Object> oldInstance) {
    try {
      for (IndexDefinition indexDef : schema.getIndexes()) {
        this.deleteEntity(handle, resolvedKey.getIdentifierLo(), type, oldInstance,
            indexDef.getName(), schema);
      }
    } catch (KazukiException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void clear(Handle handle, Map<String, Schema> typeToSchemaMap, boolean preserveSchema) {
    for (Map.Entry<String, Schema> entry : typeToSchemaMap.entrySet()) {
      String type = entry.getKey();
      Schema schema = entry.getValue();

      for (IndexDefinition indexDef : schema.getIndexes()) {
        if (preserveSchema) {
          this.truncateTable(handle, type, indexDef.getName(), groupName, storeName, partitionName);
        } else {
          this.dropTableAndIndex(handle, type, indexDef.getName());
        }
      }
    }
  }

  @Override
  public void onSchemaCreate(String type, Schema schema) {
    for (IndexDefinition indexDef : schema.getIndexes()) {
      createTable(database, type, indexDef.getName(), schema);
      createIndex(database, type, indexDef.getName(), schema);
    }
  }


  @Override
  public void onSchemaUpdate(final String type, final Schema newSchema, final Schema oldSchema,
      final KeyValueIterable<KeyValuePair<LinkedHashMap>> entityCollection) {
    try {
      for (final IndexDefinition indexDef : oldSchema.getIndexes()) {
        database.inTransaction(new TransactionCallback<Void>() {
          public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
            dropTableAndIndex(handle, type, indexDef.getName());

            return null;
          };
        });
      }

      for (final IndexDefinition indexDef : oldSchema.getIndexes()) {
        createTable(database, type, indexDef.getName(), newSchema);
        createIndex(database, type, indexDef.getName(), newSchema);
      }

      final FieldTransform fieldTransform = new FieldTransform(oldSchema);

      database.inTransaction(new TransactionCallback<Void>() {
        @Override
        public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
          for (KeyValuePair<LinkedHashMap> entity : entityCollection) {
            Map<String, Object> fieldTransformed = fieldTransform.pack(entity.getValue());

            SecondaryIndexStoreJdbiImpl.this.onCreate(handle, type, LinkedHashMap.class, newSchema,
                sequence.resolveKey(entity.getKey()), entity.getValue());
          }

          return null;
        }
      });
    } finally {
      entityCollection.close();
    }
  }

  @Override
  public void onSchemaDelete(final String type, final Schema oldSchema) {
    for (final IndexDefinition indexDef : oldSchema.getIndexes()) {
      database.inTransaction(new TransactionCallback<Void>() {
        public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
          dropTableAndIndex(handle, type, indexDef.getName());

          return null;
        };
      });
    }
  }

  public void createIndex(IDBI database, final String type, final String indexName,
      final Schema schemaDefinition) {
    database.inTransaction(new TransactionCallback<Void>() {
      @Override
      public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
        try {
          handle
              .createStatement(tableHelper.getPrefix() + "drop_index")
              .define("table_name",
                  tableHelper.getTableName(type, indexName, groupName, storeName, partitionName))
              .define("index_name",
                  tableHelper.getIndexName(type, indexName, groupName, storeName, partitionName))
              .execute();
        } catch (UnableToExecuteStatementException ok) {
          // expected case in mysql - this is just best-effort anyway
        }

        handle.createStatement(
            tableHelper.getIndexDefinition(type, indexName, schemaDefinition, groupName, storeName,
                partitionName)).execute();

        return null;
      }
    });
  }

  public void dropTableAndIndex(Handle handle, final String type, final String indexName) {
    handle.createStatement(
        tableHelper.getTableDrop(type, indexName, groupName, storeName, partitionName)).execute();

    try {
      handle
          .createStatement(tableHelper.getPrefix() + "drop_index")
          .define("table_name",
              tableHelper.getTableName(type, indexName, groupName, storeName, partitionName))
          .define("index_name",
              tableHelper.getIndexName(type, indexName, groupName, storeName, partitionName))
          .execute();
    } catch (UnableToExecuteStatementException ok) {
      // expected case in mysql - this is just best-effort anyway
    }
  }

  public void dropTable(IDBI database, final String type, final String indexName) {
    database.inTransaction(new TransactionCallback<Void>() {
      @Override
      public Void inTransaction(Handle handle, TransactionStatus arg1) throws Exception {
        dropTableAndIndex(handle, type, indexName);

        return null;
      }
    });
  }

  public void insertEntity(Handle handle, final Long id, final Map<String, Object> value,
      final String type, final String indexName, final Schema schema) throws KazukiException {
    SqlParamBindings bindings = new SqlParamBindings(true);

    Update insert =
        handle.createStatement(tableHelper.getInsertStatement(type, indexName, schema, bindings,
            groupName, storeName, partitionName));

    IndexDefinition indexDefinition = schema.getIndex(indexName);
    bindings.bind("id", id, Attribute.Type.U64);

    for (IndexAttribute attr : indexDefinition.getIndexAttributes()) {
      String attrName = attr.getName();
      if ("id".equals(attrName)) {
        continue;
      } else {
        Object v = value.get(attrName) != null ? value.get(attrName).toString() : null;

        bindings.bind(attrName, tableHelper.transformAttributeValue(v, attr),
            schema.getAttribute(attrName).getType());
      }
    }

    bindings.bindToStatement(insert);

    try {
      insert.execute();
    } catch (UnableToExecuteStatementException e) {
      if (tableHelper.isConstraintViolation(e)) {
        throw new KazukiException("unique index constraint violation");
      } else {
        throw e;
      }
    }
  }

  public void updateEntity(Handle handle, final Long id, final Map<String, Object> value,
      final Map<String, Object> prev, final String type, final String indexName, final Schema schema)
      throws KazukiException {
    IndexDefinition indexDefinition = schema.getIndex(indexName);

    String origKey = tableHelper.computeIndexKey(type, indexName, indexDefinition, prev);
    String newKey = tableHelper.computeIndexKey(type, indexName, indexDefinition, value);

    if (origKey.equals(newKey)) {
      return;
    }

    SqlParamBindings bindings = new SqlParamBindings(true);

    Update update =
        handle.createStatement(tableHelper.getUpdateStatement(type, indexName, schema, bindings,
            groupName, storeName, partitionName));

    for (IndexAttribute attr : indexDefinition.getIndexAttributes()) {
      String attrName = attr.getName();
      if ("id".equals(attrName)) {
        continue;
      } else {
        Object v = value.get(attrName) != null ? value.get(attrName).toString() : null;

        bindings.bind(attrName, tableHelper.transformAttributeValue(v, attr),
            schema.getAttribute(attrName).getType());
      }
    }

    bindings.bind("id", id, Attribute.Type.U64);

    bindings.bindToStatement(update);

    try {
      update.execute();
    } catch (UnableToExecuteStatementException e) {
      if (tableHelper.isConstraintViolation(e)) {
        throw new KazukiException("unique index constraint violation");
      } else {
        throw e;
      }
    }
  }

  public void deleteEntity(Handle handle, final Long id, final String type,
      final Map<String, Object> value, final String indexName, final Schema schema)
      throws KazukiException {
    IndexDefinition indexDefinition = schema.getIndex(indexName);
    if (indexDefinition == null) {
      throw new KazukiException("schema or index not found " + type + "." + indexName);
    }

    SqlParamBindings bindings = new SqlParamBindings(true);

    Update delete =
        handle.createStatement(tableHelper.getDeleteStatement(type, indexName, bindings, groupName,
            storeName, partitionName));

    bindings.bind("id", id, Attribute.Type.U64);
    bindings.bindToStatement(delete);

    delete.execute();
  }

  public void setEntityQuarantine(Handle handle, final Long id, final String type,
      final String indexName, boolean isQuarantined, final Map<String, Object> original,
      final Schema schema) {
    SqlParamBindings bindings = new SqlParamBindings(true);

    Update quarantine =
        handle.createStatement(tableHelper.getQuarantineStatement(type, indexName, bindings,
            isQuarantined, groupName, storeName, partitionName));

    bindings.bind("id", id, Attribute.Type.U64);
    bindings.bindToStatement(quarantine);

    quarantine.execute();
  }

  public KeyValueIterable<Key> doIndexQuery(IDBI database, final String type, String indexName,
      List<QueryTerm> queryTerms, final SortDirection sortDirection, String token, Long pageSize,
      boolean includeQuarantine, final Schema schema) throws Exception {
    SecondaryIndexQueryValidation.validateQuery(indexName, queryTerms, schema);

    IndexDefinition indexDefinition = schema.getIndex(indexName);
    final FieldTransform transform = new FieldTransform(schema);

    Map<String, List<QueryTerm>> termMap = tableHelper.sortTerms(indexDefinition, queryTerms);

    if (!includeQuarantine && indexDefinition.isUnique()) {
      throw new UnsupportedOperationException("unique indexes not supported - yet");

      // return tableHelper.doUniqueIndexQuery(database, type, indexName, termMap, token,
      // indexDefinition, schema, transform);
    }

    final SqlParamBindings bindings = new SqlParamBindings(true);

    final String querySql =
        tableHelper.getIndexQuery(type, indexName, termMap, sortDirection, token, pageSize,
            includeQuarantine, indexDefinition, schema, transform, bindings, groupName, storeName,
            partitionName);

    log.debug("non-unique index query : {} : bindings : {}", querySql, bindings.asMap());

    final Handle handle = database.open();

    final Query<Map<String, Object>> select = handle.createQuery(querySql);
    bindings.bindToStatement(select);

    return new KeyValueIterable<Key>() {
      private volatile KeyValueIterableJdbiImpl<LinkedHashMap> inner =
          new KeyValueIterableJdbiImpl(availability, sequence, kvStore, schema, handle,
              tableHelper.getPrefix(), "_id", select, type, LinkedHashMap.class, sortDirection,
              null, null, false, false);

      @Override
      public KeyValueIterator<Key> iterator() {
        return new KeyValueIterator<Key>() {
          volatile KeyValueIterator<KeyValuePair<LinkedHashMap>> innerIter = inner.iterator();

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
            throw new UnsupportedOperationException("not supported - yet");
            // innerIter.remove();
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
  }

  public boolean tableExists(IDBI database, final String type, final String indexName) {
    String tablename =
        tableHelper.getTableName(type, indexName, groupName, storeName, partitionName);

    return JDBIHelper.tableExists(database, tableHelper.getPrefix(), tablename);
  }

  public void truncateTable(Handle handle, final String type, final String indexName,
      final String groupName, String storeName, String partitionName) {
    String indexTableName =
        tableHelper.getTableName(type, indexName, groupName, storeName, partitionName);
    handle.createStatement(tableHelper.getPrefix() + "truncate_table")
        .define("table_name", indexTableName).execute();
  }
}
