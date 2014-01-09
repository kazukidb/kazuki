package io.kazuki.v0.store.journal;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.keyvalue.KeyValueStorage;

import java.util.Collection;
import java.util.Iterator;

import javax.inject.Inject;

public class SimpleH2JournalStorage implements JournalStorage {
  @Inject
  protected KeyValueStorage store;

  @Override
  public void initialize() {
    store.initialize();
  }

  @Override
  public <T> void append(String type, Class<T> clazz, T inValue, boolean strictType)
      throws KazukiException {
    store.create(type, clazz, inValue, strictType);
  }

  @Override
  public <T> Iterator<T> getIteratorAbsolute(String type, Class<T> clazz, Long offset, Long limit)
      throws Exception {
    return store.iterator(type, clazz, offset, limit);
  }

  @Override
  public <T> Iterator<T> getIteratorRelative(String type, Class<T> clazz, Long offset, Long limit)
      throws Exception {
    //
    // TODO: implement partition offset lookup
    //
    return getIteratorAbsolute(type, clazz, offset, limit);
  }

  @Override
  public boolean close(String partitionId) {
    throw new UnsupportedOperationException("close() not supported");
  }

  @Override
  public boolean drop(String partitionId) {
    throw new UnsupportedOperationException("drop() not yet supported");
  }

  @Override
  public PartitionInfo getActivePartition() {
    throw new UnsupportedOperationException("getActivePartition() not yet supported");
  }

  @Override
  public Collection<PartitionInfo> getAllPartitions() {
    throw new UnsupportedOperationException("getAllPartitions() not yet supported");
  }
}
