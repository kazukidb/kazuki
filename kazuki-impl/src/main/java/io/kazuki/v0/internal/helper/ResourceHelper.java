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
package io.kazuki.v0.internal.helper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.common.base.Throwables;

public class ResourceHelper {
  public static Properties loadProperties(String fileName) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream stream = loader.getResourceAsStream(fileName);
    if (stream == null) {
      loader = ResourceHelper.class.getClassLoader();
      stream = loader.getResourceAsStream(fileName);
    }

    Properties properties = new Properties();

    if (stream != null) {
      try {
        properties.load(stream);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    return properties;
  }

  public static void forName(String className, Class<?> fallbackClassForLoading) {
    try {
      Class.forName(className, true, Thread.currentThread().getContextClassLoader());
    } catch (Exception unused) {
      try {
        Class.forName(className, true, fallbackClassForLoading.getClassLoader());
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
