/**
 * Copyright 2014 Sunny Gleason
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
package io.kazuki.v0.store;

import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.AttributeTransform;
import io.kazuki.v0.store.schema.model.IndexAttribute;
import io.kazuki.v0.store.schema.model.IndexDefinition;
import io.kazuki.v0.store.schema.model.Schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

public class Foo {
  public static final Schema FOO_SCHEMA = new Schema(ImmutableList.of(new Attribute("fooKey",
      Attribute.Type.UTF8_SMALLSTRING, null, true), new Attribute("fooValue",
      Attribute.Type.UTF8_SMALLSTRING, null, true)), ImmutableList.<IndexDefinition>of(
      new IndexDefinition("uniqueFooKeyValue", ImmutableList.of(new IndexAttribute("fooKey",
          SortDirection.ASCENDING, AttributeTransform.NONE), new IndexAttribute("fooValue",
          SortDirection.ASCENDING, AttributeTransform.NONE)), true),
      new IndexDefinition("fooKey", ImmutableList.of(new IndexAttribute("fooKey",
          SortDirection.ASCENDING, AttributeTransform.NONE)), false)));

  private final String fooKey;
  private final String fooValue;

  @JsonCreator
  public Foo(@JsonProperty("fooKey") String fooKey, @JsonProperty("fooValue") String fooValue) {
    this.fooKey = fooKey;
    this.fooValue = fooValue;
  }

  public String getFooKey() {
    return fooKey;
  }

  public String getFooValue() {
    return fooValue;
  }

  @Override
  public int hashCode() {
    return fooKey.hashCode() ^ fooValue.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return (other instanceof Foo) && ((Foo) other).fooKey.equals(fooKey)
        && ((Foo) other).fooValue.equals(fooValue);
  }
}
