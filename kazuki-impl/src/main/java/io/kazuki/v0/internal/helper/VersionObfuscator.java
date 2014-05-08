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

import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.Version;
import io.kazuki.v0.store.sequence.KeyImpl;
import io.kazuki.v0.store.sequence.VersionImpl;

import java.nio.ByteBuffer;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.base.Throwables;

public class VersionObfuscator {
  private static final String keyString = System.getProperty("key.encrypt.password", "changeme");
  private static final byte[] saltBytes = System.getProperty("key.encrypt.salt", "asalt")
      .getBytes();
  private static final byte[] ivBytes;
  private static final Cipher cipher;

  static {
    try {
      ivBytes =
          Hex.decodeHex(System.getProperty("key.encrypt.iv", "0123456789ABCDEF").toCharArray());
      cipher = Cipher.getInstance("DES/CBC/NoPadding");
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static final AlgorithmParameterSpec paramSpec = new IvParameterSpec(ivBytes);

  private static ConcurrentHashMap<String, SecretKey> keyCache =
      new ConcurrentHashMap<String, SecretKey>();

  public static synchronized String encrypt(Key key, Number id) {
    StringBuilder encryptedIdentifier = new StringBuilder();

    encryptedIdentifier.append(key.getIdentifier());

    try {
      byte[] plain = ByteBuffer.allocate(8).putLong(id.longValue()).array();
      cipher.init(Cipher.ENCRYPT_MODE, getKey(key.getTypePart() + "#version"), paramSpec);
      byte[] encrypted = cipher.doFinal(plain);

      encryptedIdentifier.append("#");
      encryptedIdentifier.append(Hex.encodeHex(encrypted));

      return encryptedIdentifier.toString();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static synchronized Version decrypt(String encryptedText) {
    if (encryptedText == null
        || encryptedText.length() == 0
        || (!encryptedText.startsWith("@") && !encryptedText.contains(":") && !encryptedText
            .contains("#"))) {
      throw new IllegalArgumentException("Invalid Version: " + encryptedText);
    }

    String[] parts = encryptedText.split("#");

    if (parts.length != 2 || parts[1].length() != 16) {
      throw new IllegalArgumentException("Invalid version: " + encryptedText);
    }

    Key realKey = KeyImpl.valueOf(parts[0]);

    try {
      byte[] encrypted = Hex.decodeHex(parts[1].toCharArray());
      cipher.init(Cipher.DECRYPT_MODE, getKey(realKey.getTypePart() + "#version"), paramSpec);
      byte[] decrypted = cipher.doFinal(encrypted);

      Long id = ByteBuffer.allocate(8).put(decrypted).getLong(0);

      return VersionImpl.createInternal(KeyImpl.valueOf(keyString), id);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static SecretKey getKey(String type) throws Exception {
    if (!keyCache.containsKey(type)) {
      SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
      String password = keyString + ":" + type;
      KeySpec spec = new PBEKeySpec(password.toCharArray(), saltBytes, 1024, 64);
      SecretKey tmp = factory.generateSecret(spec);
      SecretKey key = new SecretKeySpec(tmp.getEncoded(), "DES");

      keyCache.put(type, key);
    }

    return keyCache.get(type);
  }
}
