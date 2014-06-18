/**
 * Copyright 2014 Sunny Gleason and original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kazuki.v0.internal.v2schema;

import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.IndexAttribute;
import io.kazuki.v0.store.schema.model.IndexDefinition;
import io.kazuki.v0.store.schema.model.Schema;
import io.kazuki.v0.store.schema.model.diff.SchemaDiff;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Objects;

/**
 * Utility class for performing "diff" operations on Schema instances.
 */
public class SchemaDiffUtil {
  /**
   * Returns a list of differences between two Schema instances.
   * 
   * @param oldSchema Schema instance for the "old" version
   * @param newSchema Schema instance for the "new" version
   * @return List<SchemaDiff> list of differences
   */
  @SuppressWarnings("rawtypes")
  public static List<SchemaDiff> diff(Schema oldSchema, Schema newSchema) {
    List<SchemaDiff> diffs = new ArrayList<SchemaDiff>();

    diffAttributes(oldSchema, newSchema, diffs);
    diffIndexes(oldSchema, newSchema, diffs);

    return diffs;
  }

  @SuppressWarnings("rawtypes")
  private static void diffAttributes(Schema oldSchema, Schema newSchema, List<SchemaDiff> diffs) {
    Map<String, Attribute> oldAttrs = oldSchema.getAttributeMap();
    Map<String, Attribute> newAttrs = newSchema.getAttributeMap();

    Set<String> oldAttrsDone = new HashSet<String>();

    for (Map.Entry<String, Attribute> attrEntry : newAttrs.entrySet()) {
      String attrName = attrEntry.getKey();
      Attribute attr = attrEntry.getValue();

      if (attr.getRenameOf() != null && !oldAttrs.containsKey(attr.getRenameOf())) {
        throw new IllegalArgumentException("new schema contains renameOf non-existent attribute: "
            + attrName);
      }

      if (attr.getRenameOf() != null) {
        diffs.add(new SchemaDiff<Attribute>(SchemaDiff.DiffType.ATTRIBUTE_RENAME, Attribute.class,
            oldAttrs.get(attr.getRenameOf()), attr));
        oldAttrsDone.add(attr.getRenameOf());
        continue;
      }

      if (!oldAttrs.containsKey(attrName)) {
        diffs.add(new SchemaDiff<Attribute>(SchemaDiff.DiffType.ATTRIBUTE_ADD, Attribute.class,
            null, attr));
        oldAttrsDone.add(attrName);
        continue;
      }

      Attribute oldAttr = oldAttrs.get(attrName);

      if (!oldAttr.getType().equals(attr.getType())) {
        diffs.add(new SchemaDiff<Attribute>(SchemaDiff.DiffType.ATTRIBUTE_MODIFY, Attribute.class,
            oldAttr, attr));
        oldAttrsDone.add(attrName);
        continue;
      }

      if (oldAttr.isNullable() != attr.isNullable()) {
        diffs.add(new SchemaDiff<Attribute>(SchemaDiff.DiffType.ATTRIBUTE_MODIFY, Attribute.class,
            oldAttr, attr));
        oldAttrsDone.add(attrName);
        continue;
      }

      List<String> oldVals = oldAttr.getValues();
      List<String> newVals = attr.getValues();

      if ((oldVals != null && (newVals == null || !oldVals.equals(newVals)))
          || (newVals != null && (oldVals == null || !newVals.equals(oldVals)))) {
        diffs.add(new SchemaDiff<Attribute>(SchemaDiff.DiffType.ATTRIBUTE_MODIFY, Attribute.class,
            oldAttr, attr));
        continue;
      }
    }

    for (Map.Entry<String, Attribute> attrEntry : oldAttrs.entrySet()) {
      String attrName = attrEntry.getKey();
      Attribute oldAttr = attrEntry.getValue();

      if (oldAttrsDone.contains(attrName)) {
        continue;
      }

      if (!newAttrs.containsKey(attrName)) {
        diffs.add(new SchemaDiff<Attribute>(SchemaDiff.DiffType.ATTRIBUTE_REMOVE, Attribute.class,
            oldAttr, null));
        oldAttrsDone.add(attrName);
        continue;
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private static void diffIndexes(Schema oldSchema, Schema newSchema, List<SchemaDiff> diffs) {
    Map<String, IndexDefinition> oldIndexes = oldSchema.getIndexMap();
    Map<String, IndexDefinition> newIndexes = newSchema.getIndexMap();

    Set<String> oldIndexDone = new HashSet<String>();

    for (Map.Entry<String, IndexDefinition> indexEntry : newIndexes.entrySet()) {
      String indexName = indexEntry.getKey();
      IndexDefinition index = indexEntry.getValue();

      if (index.getRenameOf() != null && !oldIndexes.containsKey(index.getRenameOf())) {
        throw new IllegalArgumentException("new schema contains renameOf non-existent index: "
            + indexName);
      }

      if (index.getRenameOf() != null) {
        diffs.add(new SchemaDiff<IndexDefinition>(SchemaDiff.DiffType.INDEX_RENAME,
            IndexDefinition.class, oldIndexes.get(index.getRenameOf()), index));
        oldIndexDone.add(index.getRenameOf());
        continue;
      }

      if (!oldIndexes.containsKey(indexName)) {
        diffs.add(new SchemaDiff<IndexDefinition>(SchemaDiff.DiffType.INDEX_ADD,
            IndexDefinition.class, null, index));
        oldIndexDone.add(indexName);
        continue;
      }

      IndexDefinition oldIndex = oldIndexes.get(indexName);

      if (oldIndex.isUnique() != index.isUnique()) {
        diffs.add(new SchemaDiff<IndexDefinition>(SchemaDiff.DiffType.INDEX_MODIFY,
            IndexDefinition.class, oldIndex, index));
        oldIndexDone.add(indexName);
        continue;
      }

      int oldAttrLen = oldIndex.getIndexAttributes().size();
      int newAttrLen = index.getIndexAttributes().size();

      if (oldAttrLen != newAttrLen) {
        diffs.add(new SchemaDiff<IndexDefinition>(SchemaDiff.DiffType.INDEX_MODIFY,
            IndexDefinition.class, oldIndex, index));
        oldIndexDone.add(indexName);
        continue;
      }

      for (int i = 0; i < oldAttrLen; i++) {
        IndexAttribute oldAttr = oldIndex.getIndexAttributes().get(i);
        IndexAttribute newAttr = index.getIndexAttributes().get(i);

        if (!oldAttr.getName().equals(newAttr.getName())
            || !Objects.equal(oldAttr.getSortDirection(), newAttr.getSortDirection())
            || !Objects.equal(oldAttr.getTransform(), newAttr.getTransform())) {
          diffs.add(new SchemaDiff<IndexDefinition>(SchemaDiff.DiffType.INDEX_MODIFY,
              IndexDefinition.class, oldIndex, index));
          oldIndexDone.add(indexName);
          continue;
        }
      }
    }

    for (Map.Entry<String, IndexDefinition> indexEntry : oldIndexes.entrySet()) {
      String indexName = indexEntry.getKey();
      IndexDefinition oldIndex = indexEntry.getValue();

      if (oldIndexDone.contains(indexName)) {
        continue;
      }

      if (!newIndexes.containsKey(indexName)) {
        diffs.add(new SchemaDiff<IndexDefinition>(SchemaDiff.DiffType.INDEX_REMOVE,
            IndexDefinition.class, oldIndex, null));
        oldIndexDone.add(indexName);
        continue;
      }
    }
  }
}
