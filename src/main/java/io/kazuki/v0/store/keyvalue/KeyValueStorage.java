package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public interface KeyValueStorage {
  public abstract void initialize();

  public abstract <T> Key create(String type, Class<T> clazz, T inValue, boolean strictType)
      throws KazukiException;

  public abstract <T> Key create(String type, Class<T> clazz, T inValue, Long idOverride,
      boolean strictType) throws KazukiException;

  public abstract <T> T retrieve(Key key, Class<T> clazz) throws KazukiException;

  public abstract <T> Map<Key, T> multiRetrieve(Collection<Key> keys, Class<T> clazz)
      throws KazukiException;

  public abstract <T> boolean update(Key key, Class<T> clazz, T inValue) throws KazukiException;

  public abstract boolean delete(Key key) throws KazukiException;

  public abstract void clear(boolean preserveSchema);

  public abstract <T> Iterator<T> iterator(String type, Class<T> clazz) throws Exception;

  public abstract <T> Iterator<T> iterator(String type, Class<T> clazz, Long offset, Long limit)
      throws Exception;
}
