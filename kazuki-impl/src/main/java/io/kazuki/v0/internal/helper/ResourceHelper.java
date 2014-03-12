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
