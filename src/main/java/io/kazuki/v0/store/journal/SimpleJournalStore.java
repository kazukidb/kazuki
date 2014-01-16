package io.kazuki.v0.store.journal;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.schema.TypeValidation;

import java.util.Collection;
import java.util.Iterator;

import com.google.inject.Inject;

public class SimpleJournalStore implements JournalStore {
  private final KeyValueStore store;

  @Inject
  public SimpleJournalStore(KeyValueStore store) {
    this.store = store;
  }

  @Override
  public void initialize() {
    store.initialize();
  }

  @Override
  public <T> void append(String type, Class<T> clazz, T inValue, TypeValidation typeSafety)
      throws KazukiException {
    store.create(type, clazz, inValue, typeSafety);
  }

  @Override
  public <T> Iterator<T> getIteratorAbsolute(String type, Class<T> clazz, Long offset, Long limit)
      throws KazukiException {
    return store.iterator(type, clazz, offset, limit);
  }

  @Override
  public <T> Iterator<T> getIteratorRelative(String type, Class<T> clazz, Long offset, Long limit)
      throws KazukiException {
    //
    // TODO: implement partition offset lookup
    //
    return getIteratorAbsolute(type, clazz, offset, limit);
  }

  @Override
  public Long approximateSize(String type) throws KazukiException {
    return store.approximateSize(type);
  }

  @Override
  public void clear(boolean preserveTypes, boolean preserveCounters) throws KazukiException {
    store.clear(preserveTypes, preserveCounters);
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
