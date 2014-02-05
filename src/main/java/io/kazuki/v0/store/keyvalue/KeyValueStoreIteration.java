package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.store.Key;

import java.util.Iterator;

import javax.annotation.Nullable;

public interface KeyValueStoreIteration {
  <T> Iterator<T> iterator(String type, Class<T> clazz);

  <T> Iterator<T> iterator(String type, Class<T> clazz, @Nullable Long offset, @Nullable Long limit);

  <T> KeyValueIterable<Key> keys(String type, Class<T> clazz);

  <T> KeyValueIterable<Key> keys(String type, Class<T> clazz, @Nullable Long offset,
      @Nullable Long limit);

  <T> KeyValueIterable<T> values(String type, Class<T> clazz);

  <T> KeyValueIterable<T> values(String type, Class<T> clazz, @Nullable Long offset,
      @Nullable Long limit);

  <T> KeyValueIterable<KeyValuePair<T>> entries(String type, Class<T> clazz);

  <T> KeyValueIterable<KeyValuePair<T>> entries(String type, Class<T> clazz, @Nullable Long offset,
      @Nullable Long limit);
}
