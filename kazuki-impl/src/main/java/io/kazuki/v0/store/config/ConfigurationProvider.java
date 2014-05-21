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
package io.kazuki.v0.store.config;

import io.kazuki.v0.internal.helper.EncodingHelper;
import io.kazuki.v0.internal.helper.LogTranslation;
import io.kazuki.v0.internal.helper.ResourceHelper;
import io.kazuki.v0.internal.helper.StringHelper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.annotation.Nullable;

import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Provider;

public class ConfigurationProvider<T> implements Provider<T> {
  private final Logger log = LogTranslation.getLogger(getClass());

  private final String propertyPrefix;
  private final Class<T> configClass;
  private final String propertiesPath;
  private final boolean includeSystemProperties;

  public ConfigurationProvider(String propertyPrefix, Class<T> configClass) {
    this(propertyPrefix, configClass, null, true);
  }

  public ConfigurationProvider(String propertyPrefix, Class<T> configClass,
      @Nullable String propertiesPath, boolean includeSystemProperties) {
    Preconditions.checkNotNull(propertyPrefix, "propertyPrefix");
    Preconditions.checkNotNull(propertyPrefix, "configClass");

    if (!propertyPrefix.endsWith(".")) {
      propertyPrefix += ".";
    }

    this.propertyPrefix = propertyPrefix;
    this.configClass = configClass;
    this.propertiesPath = propertiesPath;
    this.includeSystemProperties = includeSystemProperties;
  }

  @Override
  public T get() {
    Map<String, Object> properties = new LinkedHashMap<String, Object>();

    if (propertiesPath != null) {
      log.debug("Loading classpath properties for {} from {}", configClass.getName(),
          propertiesPath);

      addProperties(ResourceHelper.loadProperties(propertiesPath), properties);
    }


    if (includeSystemProperties) {
      log.debug("Loading system properties for {}", configClass.getName());

      addProperties(System.getProperties(), properties);
    }

    if (properties.isEmpty()) {
      throw new IllegalArgumentException("unable to configure instance - no properties set");
    }

    try {
      log.debug("Instantiating new {} from properties", configClass.getName());

      return EncodingHelper.asValue(properties, configClass);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void addProperties(Properties inProperties, Map<String, Object> properties) {
    for (Entry<Object, Object> entry : inProperties.entrySet()) {
      String key = (String) entry.getKey();

      if (!key.startsWith(propertyPrefix)) {
        continue;
      }

      String prefixFreeKey = key.substring(propertyPrefix.length());
      String camelCaseKey = StringHelper.toCamelCase(prefixFreeKey);

      properties.put(camelCaseKey, (String) entry.getValue());
    }
  }
}
