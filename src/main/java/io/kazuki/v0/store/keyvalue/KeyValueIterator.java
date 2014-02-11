package io.kazuki.v0.store.keyvalue;

import java.util.Iterator;

public interface KeyValueIterator<T> extends Iterator<T>, AutoCloseable {
  @Override
  boolean hasNext();

  @Override
  T next();

  @Override
  void remove();

  @Override
  void close();
}
