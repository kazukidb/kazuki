package io.kazuki.v0.store.keyvalue;


public interface KeyValueIterable<T> extends Iterable<T> {
  KeyValueIterator<T> iterator();
}
