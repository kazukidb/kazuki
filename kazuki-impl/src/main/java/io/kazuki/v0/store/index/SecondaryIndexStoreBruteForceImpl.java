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

import io.kazuki.v0.internal.helper.OpaquePaginationHelper;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.index.query.QueryEvaluator;
import io.kazuki.v0.store.index.query.QueryHelper;
import io.kazuki.v0.store.index.query.QueryTerm;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.model.Schema;
import io.kazuki.v0.store.sequence.ResolvedKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.skife.jdbi.v2.Handle;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

public class SecondaryIndexStoreBruteForceImpl implements SecondaryIndexSupport {
  private final KeyValueStore kvStore;
  private final SchemaStore schemaStore;

  @Inject
  public SecondaryIndexStoreBruteForceImpl(KeyValueStore kvStore, SchemaStore schemaStore) {
    this.kvStore = kvStore;
    this.schemaStore = schemaStore;
  }

  @Override
  public void onSchemaCreate(String type, Schema schema) {}

  @Override
  public void onSchemaUpdate(String type, Schema newSchema, Schema oldSchema,
      KeyValueIterable<KeyValuePair<LinkedHashMap>> entityCollection) {}

  @Override
  public void onSchemaDelete(String type, Schema oldSchema) {}

  @Override
  public <T> void onCreate(Handle handle, String type, Class<T> clazz, Schema schema,
      ResolvedKey resolvedKey, Map<String, Object> instance) {}

  @Override
  public <T> void onUpdate(Handle handle, String type, Class<T> clazz, Schema schema,
      ResolvedKey resolvedKey, Map<String, Object> newInstance, Map<String, Object> oldInstance) {}

  @Override
  public <T> void onDelete(Handle handle, String type, Class<T> clazz, Schema schema,
      ResolvedKey resolvedKey, Map<String, Object> oldInstance) {}

  @Override
  public void clear(Handle handle, Map<String, Schema> typeToSchemaMap, boolean preserveSchema) {}

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public Map<UniqueEntityDescription, Object> multiRetrieveUniqueEntities(
      Collection<UniqueEntityDescription> entityDefinitions) {
    Map<UniqueEntityDescription, Key> keys = multiRetrieveUniqueKeys(entityDefinitions);
    Map<UniqueEntityDescription, Object> resultMap = new LinkedHashMap<>();

    for (Map.Entry<UniqueEntityDescription, Key> entry : keys.entrySet()) {
      UniqueEntityDescription desc = entry.getKey();
      Key key = entry.getValue();

      try {
        resultMap.put(desc, kvStore.retrieve(key, desc.getClazz()));
      } catch (KazukiException e) {
        throw Throwables.propagate(e);
      }
    }

    return Collections.unmodifiableMap(resultMap);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public Map<UniqueEntityDescription, Key> multiRetrieveUniqueKeys(
      Collection<UniqueEntityDescription> entityDefinitions) {
    Map<String, Schema> schemaMap = new LinkedHashMap<String, Schema>();
    Map<String, Map<String, Set<String>>> todo = new HashMap<String, Map<String, Set<String>>>();
    Map<String, UniqueEntityDescription> backMap = new HashMap<String, UniqueEntityDescription>();

    try {
      for (UniqueEntityDescription<?> desc : entityDefinitions) {
        String type = desc.getType();
        String indexName = desc.getIndexName();

        Schema schema = schemaMap.get(type);
        if (schema == null) {
          schema = schemaStore.retrieveSchema(type);
          schemaMap.put(type, schema);
        }

        if (!todo.containsKey(type)) {
          todo.put(type, new HashMap<String, Set<String>>());
        }

        Map<String, Set<String>> indexDesc = todo.get(type);
        if (!indexDesc.containsKey(indexName)) {
          indexDesc.put(indexName, new HashSet<String>());
        }

        Set<String> toFind = indexDesc.get(indexName);
        Map<String, Object> valMap = new LinkedHashMap<String, Object>();

        for (Map.Entry<String, QueryTerm> entry : desc.getColumnDefinitions().entrySet()) {
          valMap.put(entry.getKey(), entry.getValue().getValue());
        }

        String uKey = SecondaryIndexTableHelper.getUniqueIndexKey(type, schema, indexName, valMap);

        toFind.add(uKey);
        backMap.put(uKey, desc);
      }
    } catch (KazukiException e) {
      throw Throwables.propagate(e);
    }

    Map<UniqueEntityDescription, Key> resultMap = new LinkedHashMap<UniqueEntityDescription, Key>();

    TYPE: for (String type : todo.keySet()) {
      Schema schema = schemaMap.get(type);

      try (KeyValueIterable<KeyValuePair<LinkedHashMap>> iter =
          kvStore.iterators().entries(type, LinkedHashMap.class, SortDirection.ASCENDING)) {

        for (KeyValuePair<LinkedHashMap> kvPair : iter) {
          Key key = kvPair.getKey();
          LinkedHashMap entity = kvPair.getValue();

          Map<String, Set<String>> indexDesc = todo.get(type);

          for (Map.Entry<String, Set<String>> entry : indexDesc.entrySet()) {
            String indexName = entry.getKey();
            Set<String> toFind = entry.getValue();

            String candidate =
                SecondaryIndexTableHelper.getUniqueIndexKey(type, schema, indexName, entity);

            if (toFind.contains(candidate)) {
              resultMap.put(backMap.get(candidate), key);
              toFind.remove(candidate);

              if (toFind.isEmpty()) {
                indexDesc.remove(type);

                if (indexDesc.isEmpty()) {
                  continue TYPE;
                }
              }
            }
          }
        }
      }
    }

    LinkedHashMap<UniqueEntityDescription, Key> inOrderResultMap = new LinkedHashMap<>();

    for (UniqueEntityDescription desc : entityDefinitions) {
      inOrderResultMap.put(desc, resultMap.get(desc));
    }

    return Collections.unmodifiableMap(inOrderResultMap);
  }

  @Override
  public <T> KeyValueIterable<Key> queryWithoutPagination(final String type, final Class<T> clazz,
      final String indexName, final List<QueryTerm> query, final SortDirection sortDirection,
      final Long offset, final Long limit) {
    Schema schema = null;
    try {
      schema = schemaStore.retrieveSchema(type);
    } catch (KazukiException e) {
      throw Throwables.propagate(e);
    }

    Preconditions.checkNotNull(schema, "schema");
    SecondaryIndexQueryValidation.validateQuery(indexName, query, schema);

    final QueryEvaluator eval = new QueryEvaluator();

    return new FilteredKeyValueIterable<Key>(kvStore.iterators().entries(type, LinkedHashMap.class,
        sortDirection), new Predicate<Object>() {
      @SuppressWarnings("unchecked")
      @Override
      public boolean apply(Object instance) {
        return eval.matches((LinkedHashMap<String, Object>) instance, query);
      }
    }, new Function<KeyValuePair<?>, Key>() {
      @Override
      public Key apply(KeyValuePair<?> instance) {
        return instance.getKey();
      }
    }, offset, limit);
  }

  @Override
  public <T> KeyValueIterable<Key> queryWithoutPagination(final String type, final Class<T> clazz,
      final String indexName, final String queryString, final SortDirection sortDirection,
      final Long offset, final Long limit) {
    Preconditions.checkNotNull(queryString, "query");

    return queryWithoutPagination(type, clazz, indexName, QueryHelper.parseQuery(queryString),
        sortDirection, offset, limit);
  }

  @Override
  public <T> QueryResultsPage<T> queryWithPagination(final String type, final Class<T> clazz,
      final String indexName, final List<QueryTerm> query, final SortDirection sortDirection,
      final Boolean loadResults, final PageToken token, final Long limit) {
    try {
      KeyValueIterable<Key> kvIter =
          queryWithoutPagination(type, clazz, indexName, query, sortDirection,
              OpaquePaginationHelper.decodeOpaqueCursor(token.getToken()), limit);
      List<KeyValuePair<T>> kvPairs = new ArrayList<KeyValuePair<T>>();

      if (loadResults) {
        List<Key> toRetrieve = new ArrayList<Key>();
        Iterables.addAll(toRetrieve, kvIter);

        Map<Key, T> resultMap = kvStore.multiRetrieve(toRetrieve, clazz);

        for (Map.Entry<Key, T> entry : resultMap.entrySet()) {
          kvPairs.add(new KeyValuePair<T>(entry.getKey(), entry.getValue()));
        }
      } else {
        for (Key key : kvIter) {
          kvPairs.add(new KeyValuePair<T>(key, null));
        }
      }

      return new QueryResultsPageImpl<T>(kvPairs, loadResults);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T> QueryResultsPage<T> queryWithPagination(String type, Class<T> clazz, String indexName,
      String queryString, SortDirection sortDirection, Boolean loadResults, PageToken token,
      Long limit) {
    return queryWithPagination(type, clazz, indexName, QueryHelper.parseQuery(queryString),
        sortDirection, loadResults, token, limit);
  }
}
