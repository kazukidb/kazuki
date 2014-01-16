package io.kazuki.v0.store.sequence;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;

public interface SequenceService {
  String getTypeName(final Integer id) throws KazukiException;

  Integer getTypeId(final String type, final boolean create) throws KazukiException;

  Integer getTypeId(final String type, final boolean create, final boolean strict)
      throws KazukiException;

  Key nextKey(String type) throws KazukiException;

  void reload();

  void clear(boolean clearTypes, boolean clearCounters);
}
