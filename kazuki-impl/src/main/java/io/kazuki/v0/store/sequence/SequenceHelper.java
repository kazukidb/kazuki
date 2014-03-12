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
package io.kazuki.v0.store.sequence;

import io.kazuki.v0.internal.helper.JDBIHelper;
import io.kazuki.v0.store.KazukiException;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.Update;

import com.google.common.base.Preconditions;


public class SequenceHelper {
  private final boolean strictTypeCreation;
  private final String dbPrefix;
  private final String sequenceTableName;
  private final String keyTypesTableName;

  @Inject
  public SequenceHelper(SequenceServiceConfiguration config) {
    this(config.getDbPrefix(), config.getGroupName(), config.getStoreName(), config
        .isStrictTypeCreation());
  }

  public SequenceHelper(String dbPrefix, String groupName, String storeName,
      boolean strictTypeCreation) {
    Preconditions.checkNotNull(dbPrefix, "dbPrefix");
    Preconditions.checkNotNull(groupName, "groupName");
    Preconditions.checkNotNull(storeName, "storeName");

    this.dbPrefix = dbPrefix;
    this.sequenceTableName = "_" + groupName + "_" + storeName + "__seq";
    this.keyTypesTableName = "_" + groupName + "_" + storeName + "__types";

    this.strictTypeCreation = strictTypeCreation;
  }

  public String getDbPrefix() {
    return dbPrefix;
  }

  public String getSequenceTableName() {
    return sequenceTableName;
  }

  public String getKeyTypesTableName() {
    return keyTypesTableName;
  }

  public Integer validateType(Handle handle, Map<String, Integer> typeCodes,
      Map<Integer, String> typeNames, final String type, boolean doCreate) throws KazukiException {
    doCreate |= !strictTypeCreation;

    if (type == null || type.length() == 0 || type.indexOf(":") != -1 || type.length() > 72) {
      throw new IllegalArgumentException("Invalid entity 'type'");
    }

    if (typeCodes.containsKey(type)) {
      Integer typeId = typeCodes.get(type);

      return typeId;
    }

    Query<Map<String, Object>> query =
        JDBIHelper.getBoundQuery(handle, dbPrefix, "key_types_table_name", keyTypesTableName,
            "seq_types_get_id");
    query.bind("type_name", type);
    List<Map<String, Object>> result = query.list();

    Integer typeId = null;

    if (result == null || result.isEmpty()) {
      if (!doCreate) {
        return null;
      }

      typeId = getNextId(handle, 0, 1L).intValue();

      Update newType =
          JDBIHelper.getBoundStatement(handle, dbPrefix, "sequence_table_name", sequenceTableName,
              "seq_seq_insert");
      newType.bind("key_type", typeId);
      newType.bind("next_id", 0L);
      newType.execute();

      Update newTypeName =
          JDBIHelper.getBoundStatement(handle, dbPrefix, "key_types_table_name", keyTypesTableName,
              "seq_types_create");
      newTypeName.bind("key_type", typeId);
      newTypeName.bind("type_name", type);
      newTypeName.execute();
    } else {
      typeId = ((Number) result.iterator().next().get("_key_type")).intValue();
    }

    typeCodes.put(type, typeId);
    typeNames.put(typeId, type);

    return typeId;
  }

  public synchronized Long getNextId(Handle handle, Integer typeId, Long increment) {
    Query<Map<String, Object>> query =
        JDBIHelper.getBoundQuery(handle, dbPrefix, "sequence_table_name", sequenceTableName,
            "seq_seq_next");
    query.bind("key_type", typeId);

    Long nextId = ((Number) query.first().get("_next_id")).longValue();

    Update incrSeq =
        JDBIHelper.getBoundStatement(handle, dbPrefix, "sequence_table_name", sequenceTableName,
            "seq_seq_inc");
    incrSeq.bind("key_type", typeId);
    incrSeq.bind("increment", increment);
    incrSeq.execute();

    return nextId;
  }

  public synchronized void setNextId(Handle handle, Integer typeId, Long nextId) {
    Update setSeq =
        JDBIHelper.getBoundStatement(handle, dbPrefix, "sequence_table_name", sequenceTableName,
            "seq_seq_set");
    setSeq.bind("key_type", typeId);
    setSeq.bind("next_id", nextId);
    setSeq.execute();
  }

  public String getTypeName(Handle handle, Map<Integer, String> typeNames, Integer id)
      throws KazukiException {
    if (id == null || id < 0) {
      throw new IllegalArgumentException("Invalid entity 'type'");
    }

    if (typeNames.containsKey(id)) {
      return typeNames.get(id);
    }

    Query<Map<String, Object>> query =
        JDBIHelper.getBoundQuery(handle, dbPrefix, "key_types_table_name", keyTypesTableName,
            "seq_types_get_name");
    query.bind("key_type", id);
    List<Map<String, Object>> result = query.list();

    if (result == null || result.isEmpty()) {
      throw new IllegalArgumentException("Invalid entity 'type'");
    }

    String typeName = (String) result.iterator().next().get("_type_name");
    typeNames.put(id, typeName);

    return typeName;
  }
}
