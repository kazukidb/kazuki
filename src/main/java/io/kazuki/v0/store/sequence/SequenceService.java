package io.kazuki.v0.store.sequence;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;

public interface SequenceService {
  String getTypeName(final Integer id) throws KazukiException;

  Integer getTypeId(final String type, final boolean create) throws KazukiException;

  Key nextKey(String type) throws KazukiException;

  ResolvedKey resolveKey(Key key) throws KazukiException;

  Key unresolveKey(ResolvedKey key) throws KazukiException;

  void clear(boolean clearTypes, boolean clearCounters);

  void resetCounter(String type) throws KazukiException;
}
