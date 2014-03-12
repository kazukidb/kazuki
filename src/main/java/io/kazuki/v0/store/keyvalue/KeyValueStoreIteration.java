package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.store.Key;

import javax.annotation.Nullable;

public interface KeyValueStoreIteration {
  public enum SortDirection {
    ASCENDING, DESCENDING
  };

  <T> KeyValueIterator<T> iterator(String type, Class<T> clazz, SortDirection sortDirection);

  <T> KeyValueIterator<T> iterator(String type, Class<T> clazz, SortDirection sortDirection,
      @Nullable Long offset, @Nullable Long limit);

  <T> KeyValueIterable<Key> keys(String type, Class<T> clazz, SortDirection sortDirection);

  <T> KeyValueIterable<Key> keys(String type, Class<T> clazz, SortDirection sortDirection,
      @Nullable Long offset, @Nullable Long limit);

  <T> KeyValueIterable<T> values(String type, Class<T> clazz, SortDirection sortDirection);

  <T> KeyValueIterable<T> values(String type, Class<T> clazz, SortDirection sortDirection,
      @Nullable Long offset, @Nullable Long limit);

  <T> KeyValueIterable<KeyValuePair<T>> entries(String type, Class<T> clazz,
      SortDirection sortDirection);

  <T> KeyValueIterable<KeyValuePair<T>> entries(String type, Class<T> clazz,
      SortDirection sortDirection, @Nullable Long offset, @Nullable Long limit);
}
