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

import io.kazuki.v0.internal.v2schema.types.UTCDateSecsTransform;
import io.kazuki.v0.store.KazukiException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileParser;

public class EncodingHelper {
  private static final SmileFactory smileFactory = new SmileFactory();
  private static final ObjectMapper jsonMapper = new ObjectMapper();
  private static final ObjectMapper beanMapper = new ObjectMapper();

  static {
    beanMapper.registerModule(new UTCDateSecsTransform.DateTimeModule());
    jsonMapper.registerModule(new UTCDateSecsTransform.DateTimeModule());
  }

  @SuppressWarnings("unchecked")
  public static <T> Map<String, Object> asJsonMap(T value) throws Exception {
    return beanMapper.convertValue(value, LinkedHashMap.class);
  }

  public static <T> T asValue(Map<String, Object> objectMap, Class<T> clazz) throws Exception {
    return beanMapper.convertValue(objectMap, clazz);
  }

  public static <T> String convertToJson(T value) throws Exception {
    return jsonMapper.writeValueAsString(value);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> parseJsonString(String value) throws Exception {
    if (value == null || value.length() == 0 || value.equals("null")) {
      throw new KazukiException("Invalid entity 'value'");
    }

    Object parsed = jsonMapper.readValue(value, LinkedHashMap.class);

    if (!(parsed instanceof Map)) {
      throw new KazukiException("Invalid entity 'value'");
    }

    return (Map<String, Object>) parsed;
  }

  public static byte[] convertToSmile(Object value) throws KazukiException {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      SmileGenerator smile = smileFactory.createGenerator(out);
      jsonMapper.writeValue(smile, value);

      byte[] smileBytes = out.toByteArray();

      return smileBytes;
    } catch (Exception e) {
      throw new KazukiException(e);
    }
  }

  public static <T> T parseSmile(byte[] valueBytes, Class<T> clazz) throws KazukiException {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(valueBytes);
      SmileParser smile = smileFactory.createParser(in);

      return jsonMapper.readValue(smile, clazz);
    } catch (Exception e) {
      throw new KazukiException(e);
    }
  }

}
