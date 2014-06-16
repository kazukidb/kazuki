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
package io.kazuki.v0.store.index.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;

/**
 * Represents a list of values, typically for an IN clause.
 */
public class ValueHolderList {
  private final List<ValueHolder> valueList;

  public ValueHolderList(List<ValueHolder> values) {
    Preconditions.checkNotNull(values, "no values");
    Preconditions.checkArgument(values.size() > 0, "no values");
    Preconditions.checkArgument(values.size() < 100, "too many values");

    List<ValueHolder> newValueList = new ArrayList<ValueHolder>();
    newValueList.addAll(values);
    this.valueList = Collections.unmodifiableList(newValueList);
  }

  public List<ValueHolder> getValueList() {
    return valueList;
  }

  @Override
  public String toString() {
    return valueList.toString();
  }
}
