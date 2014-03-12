package io.kazuki.v0.internal.helper;

import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.sequence.KeyImpl;

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

public class KeyObfuscator {
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

  public static synchronized String encrypt(String type, Long id) {
    StringBuilder encryptedIdentifier = new StringBuilder();
    encryptedIdentifier.append("@");
    encryptedIdentifier.append(type);
    encryptedIdentifier.append(":");

    try {
      byte[] plain = ByteBuffer.allocate(8).putLong(id).array();
      cipher.init(Cipher.ENCRYPT_MODE, getKey(type), paramSpec);
      byte[] encrypted = cipher.doFinal(plain);

      encryptedIdentifier.append(Hex.encodeHex(encrypted));

      return encryptedIdentifier.toString();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static synchronized Key decrypt(String encryptedText) {
    if (encryptedText == null || encryptedText.length() == 0 || !encryptedText.contains(":")) {
      throw new IllegalArgumentException("Invalid key");
    }

    if (!encryptedText.startsWith("@")) {
      return KeyImpl.valueOf(encryptedText);
    }

    String[] parts = encryptedText.substring(1).split(":");

    if (parts.length != 2 || parts[1].length() != 16) {
      throw new IllegalArgumentException("Invalid key");
    }

    String type = parts[0];

    try {
      byte[] encrypted = Hex.decodeHex(parts[1].toCharArray());
      cipher.init(Cipher.DECRYPT_MODE, getKey(type), paramSpec);
      byte[] decrypted = cipher.doFinal(encrypted);

      Long id = ByteBuffer.allocate(8).put(decrypted).getLong(0);

      return KeyImpl.createInternal(type, id);
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
