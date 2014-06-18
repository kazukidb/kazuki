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
package io.kazuki.v0.store.schema.model.diff;

import static io.kazuki.v0.store.schema.model.diff.SchemaDiff.DiffType.ATTRIBUTE_ADD;
import static io.kazuki.v0.store.schema.model.diff.SchemaDiff.DiffType.ATTRIBUTE_MODIFY;
import static io.kazuki.v0.store.schema.model.diff.SchemaDiff.DiffType.ATTRIBUTE_REMOVE;
import static io.kazuki.v0.store.schema.model.diff.SchemaDiff.DiffType.ATTRIBUTE_RENAME;
import static io.kazuki.v0.store.schema.model.diff.SchemaDiff.DiffType.INDEX_ADD;
import static io.kazuki.v0.store.schema.model.diff.SchemaDiff.DiffType.INDEX_MODIFY;
import static io.kazuki.v0.store.schema.model.diff.SchemaDiff.DiffType.INDEX_REMOVE;
import static io.kazuki.v0.store.schema.model.diff.SchemaDiff.DiffType.INDEX_RENAME;
import io.kazuki.v0.internal.v2schema.SchemaDiffUtil;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.Attribute.Type;
import io.kazuki.v0.store.schema.model.AttributeTransform;
import io.kazuki.v0.store.schema.model.IndexAttribute;
import io.kazuki.v0.store.schema.model.IndexDefinition;
import io.kazuki.v0.store.schema.model.Schema;

import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

@Test
public class SchemaDiffUtilTest {
  private final Schema a1 = new Schema(Collections.<Attribute>emptyList(),
      Collections.<IndexDefinition>emptyList());
  private final Schema a2 = new Schema(ImmutableList.<Attribute>of(new Attribute("foo", Type.ANY,
      null, true, null)), ImmutableList.<IndexDefinition>of());
  private final Schema a3 = new Schema(ImmutableList.<Attribute>of(new Attribute("foo", Type.I8,
      null, true, null)), ImmutableList.<IndexDefinition>of());
  private final Schema a4 = new Schema(ImmutableList.<Attribute>of(new Attribute("foo2", Type.I8,
      null, true, "foo")), ImmutableList.<IndexDefinition>of());

  private final Schema i1 = new Schema(Collections.<Attribute>emptyList(),
      Collections.<IndexDefinition>emptyList());
  private final Schema i2 = new Schema(ImmutableList.<Attribute>of(new Attribute("foo", Type.ANY,
      null, true, null)),
      ImmutableList.<IndexDefinition>of(new IndexDefinition("fooIndex", ImmutableList
          .of(new IndexAttribute("foo", SortDirection.ASCENDING, AttributeTransform.NONE)), false,
          null)));
  private final Schema i3 = new Schema(ImmutableList.<Attribute>of(new Attribute("foo", Type.I8,
      null, true, null)), ImmutableList.<IndexDefinition>of(new IndexDefinition("fooIndex",
      ImmutableList
          .of(new IndexAttribute("foo", SortDirection.DESCENDING, AttributeTransform.NONE)), false,
      null)));
  private final Schema i4 = new Schema(ImmutableList.<Attribute>of(new Attribute("foo", Type.I8,
      null, true, null)), ImmutableList.<IndexDefinition>of(new IndexDefinition("fooIndex2",
      ImmutableList
          .of(new IndexAttribute("foo", SortDirection.DESCENDING, AttributeTransform.NONE)), false,
      "fooIndex")));

  public void testAttributes() {
    doAttributeDiff(a1, a2, 1, ATTRIBUTE_ADD, null, a2.getAttribute("foo"));
    doAttributeDiff(a2, a1, 1, ATTRIBUTE_REMOVE, a2.getAttribute("foo"), null);
    doAttributeDiff(a2, a3, 1, ATTRIBUTE_MODIFY, a2.getAttribute("foo"), a3.getAttribute("foo"));
    doAttributeDiff(a3, a4, 1, ATTRIBUTE_RENAME, a3.getAttribute("foo"), a4.getAttribute("foo2"));
  }

  public void testIndexes() {
    doIndexDiff(i1, i2, 2, INDEX_ADD, null, i2.getIndex("fooIndex"));
    doIndexDiff(i2, i1, 2, INDEX_REMOVE, i2.getIndex("fooIndex"), null);
    doIndexDiff(i2, i3, 2, INDEX_MODIFY, i2.getIndex("fooIndex"), i3.getIndex("fooIndex"));
    doIndexDiff(i3, i4, 1, INDEX_RENAME, i3.getIndex("fooIndex"), i4.getIndex("fooIndex2"));
  }

  @SuppressWarnings("rawtypes")
  private void doAttributeDiff(Schema a1, Schema a2, int expectedDiffs, SchemaDiff.DiffType type,
      Attribute oldAttr, Attribute newAttr) {
    List<SchemaDiff> diffs = SchemaDiffUtil.diff(a1, a2);
    Assert.assertEquals(diffs.size(), 1);

    SchemaDiff theDiff = diffs.get(expectedDiffs - 1);
    Assert.assertEquals(theDiff.getType(), type);
    Assert.assertEquals(theDiff.getClazz(), Attribute.class);
    Assert.assertEquals(theDiff.getNewInstance(), newAttr);
    Assert.assertEquals(theDiff.getOldInstance(), oldAttr);
  }

  @SuppressWarnings("rawtypes")
  private void doIndexDiff(Schema a1, Schema a2, int expectedDiffs, SchemaDiff.DiffType type,
      IndexDefinition oldIndex, IndexDefinition newIndex) {
    List<SchemaDiff> diffs = SchemaDiffUtil.diff(a1, a2);
    Assert.assertEquals(diffs.size(), expectedDiffs);

    SchemaDiff theDiff = diffs.get(expectedDiffs - 1);
    Assert.assertEquals(theDiff.getType(), type);
    Assert.assertEquals(theDiff.getClazz(), IndexDefinition.class);
    Assert.assertEquals(theDiff.getNewInstance(), newIndex);
    Assert.assertEquals(theDiff.getOldInstance(), oldIndex);
  }
}
