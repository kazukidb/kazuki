package io.kazuki.v0.store.keyvalue;

import java.io.Closeable;
import java.util.Iterator;

public interface KeyValueIterator<T> extends Iterator<T>, Closeable {
  @Override
  boolean hasNext();

  @Override
  T next();

  @Override
  void remove();

  @Override
  void close();
}
