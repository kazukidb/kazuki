package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.schema.TypeValidation;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nullable;

public interface KeyValueStore {
  void initialize();

  <T> Key create(String type, Class<T> clazz, T inValue, TypeValidation typeSafety)
      throws KazukiException;

  <T> Key create(String type, Class<T> clazz, T inValue, @Nullable Long idOverride,
      TypeValidation typeSafety) throws KazukiException;

  <T> T retrieve(Key key, Class<T> clazz) throws KazukiException;

  <T> Map<Key, T> multiRetrieve(Collection<Key> keys, Class<T> clazz) throws KazukiException;

  <T> boolean update(Key key, Class<T> clazz, T inValue) throws KazukiException;

  boolean delete(Key key) throws KazukiException;

  boolean deleteHard(Key key) throws KazukiException;

  void clear(boolean preserveTypes, boolean preserveCounters) throws KazukiException;

  void clear(String type) throws KazukiException;

  Long approximateSize(String type) throws KazukiException;

  KeyValueStoreIteration iterators();

  void destroy() throws KazukiException;
}
