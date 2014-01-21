package io.kazuki.v0.store.journal;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.schema.TypeValidation;

import java.util.Iterator;

import javax.inject.Inject;


public class SimpleJournalStore implements JournalStore {
  private final KeyValueStore store;

  @Inject
  public SimpleJournalStore(KeyValueStore store) {
    this.store = store;
  }

  @Override
  public void initialize() throws KazukiException {}

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
  public boolean closeActivePartition() {
    throw new UnsupportedOperationException("closeActivePartition() not supported");
  }

  @Override
  public boolean dropPartition(String partitionId) {
    throw new UnsupportedOperationException("dropPartition() not yet supported");
  }

  @Override
  public PartitionInfo getActivePartition() {
    throw new UnsupportedOperationException("getActivePartition() not yet supported");
  }

  @Override
  public Iterator<PartitionInfoSnapshot> getAllPartitions() {
    throw new UnsupportedOperationException("getAllPartitions() not yet supported");
  }
}
