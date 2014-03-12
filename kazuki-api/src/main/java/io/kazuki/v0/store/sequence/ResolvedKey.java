package io.kazuki.v0.store.sequence;



public interface ResolvedKey {

  int getTypeTag();

  long getIdentifierHi();

  long getIdentifierLo();

}
