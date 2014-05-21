/**
 * Copyright 2014 the original author or authors
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
package io.kazuki.v0.internal.v2schema.compact;

import io.kazuki.v0.internal.v2schema.util.BitSetUtil;
import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.Schema;
import io.kazuki.v0.store.schema.model.Transform;
import io.kazuki.v0.store.schema.model.TransformException;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * Structure-based compaction for object instances. For example, a map containing only 2 out of 10
 * schema attribute values is represented by a BitSet with 2 bits set and an array of the 2 values.
 * This eliminates the duplication of string keys in document instances.
 */
public class StructureTransform implements Transform<Map<String, Object>, List<Object>> {
  private final Schema schema;

  public StructureTransform(Schema schema) {
    this.schema = schema;
  }

  @Override
  public List<Object> pack(Map<String, Object> invalue) throws TransformException {
    if (invalue == null) {
      return null;
    }

    if (invalue.isEmpty()) {
      return Collections.emptyList();
    }

    LinkedHashMap<String, Object> value = new LinkedHashMap<String, Object>();
    value.putAll(invalue);

    BitSet present = new BitSet();
    List<Object> packed = new ArrayList<Object>();

    int i = 0;
    for (Attribute attr : schema.getAttributes()) {
      if (value.containsKey(attr.getName())) {
        packed.add(value.get(attr.getName()));
        present.set(i);
        value.remove(attr.getName());
      }

      i += 1;
    }

    List<Object> result = new ArrayList<Object>();
    result.add(BitSetUtil.pack(present));
    result.add(packed);

    if (!value.isEmpty()) {
      result.add(value);
    }

    return result;
  }

  @Override
  public Map<String, Object> unpack(List<Object> invalue) throws TransformException {
    if (invalue == null) {
      return null;
    }

    if (invalue.isEmpty()) {
      return Collections.emptyMap();
    }

    if (invalue.size() < 2) {
      throw new IllegalArgumentException("packed representation must contain at least 2 elements");
    }

    BitSet present = BitSetUtil.unpack(invalue.get(0));

    @SuppressWarnings("unchecked")
    List<Object> packed = (List<Object>) invalue.get(1);

    Map<String, Object> result = new LinkedHashMap<String, Object>();

    int i = 0;
    int j = 0;
    for (Attribute attr : schema.getAttributes()) {
      if (present.get(i)) {
        result.put(attr.getName(), packed.get(j));
        j += 1;
      }
      i += 1;
    }

    if (invalue.size() > 2) {
      @SuppressWarnings("unchecked")
      Map<String, Object> extra = (Map<String, Object>) invalue.get(2);
      result.putAll(extra);
    }

    return result;
  }
}
