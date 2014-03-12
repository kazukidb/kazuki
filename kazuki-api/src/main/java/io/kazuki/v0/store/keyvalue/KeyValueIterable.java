package io.kazuki.v0.store.keyvalue;


public interface KeyValueIterable<T> extends Iterable<T>, AutoCloseable {
  KeyValueIterator<T> iterator();
  
  void close();
}
