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
package io.kazuki.v0.internal.helper;

import java.util.Collection;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;

public class StringHelper {
  public static String toCamelCase(String value) {
    Preconditions.checkNotNull(value, "value");

    String canonical = value.replace('.', '-').toLowerCase();

    return CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, canonical);
  }

  public static String join(String delim, String... values) {
    if (values == null || values.length == 0) {
      return "";
    }

    StringBuilder s = new StringBuilder();

    for (String v : values) {
      s.append(delim);
      s.append(v);
    }

    return s.toString().substring(delim.length());
  }

  public static String join(String delim, Collection<String> values) {
    if (values == null || values.size() == 0) {
      return "";
    }

    StringBuilder s = new StringBuilder();

    for (String v : values) {
      s.append(delim);
      s.append(v);
    }

    return s.toString().substring(delim.length());
  }
}
