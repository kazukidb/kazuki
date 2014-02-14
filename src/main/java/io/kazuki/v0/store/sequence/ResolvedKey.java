package io.kazuki.v0.store.sequence;

import com.google.common.hash.HashCode;

/**
 * Resolves the internal representation of a Key. This class should only be used within Kazuki
 * itself.
 */
public class ResolvedKey {
  private final int typeTag;
  private final long identifierLo;
  private final long identifierHi;
  private final int hashCode;

  public ResolvedKey(int typeTag, long identifierHi, long identifierLo) {
    this.typeTag = typeTag;
    this.identifierHi = identifierHi;
    this.identifierLo = identifierLo;
    this.hashCode =
        HashCode.fromInt(typeTag).asInt() ^ HashCode.fromLong(identifierHi).asInt()
            ^ HashCode.fromLong(identifierLo).asInt();
  }

  public int getTypeTag() {
    return typeTag;
  }

  public long getIdentifierHi() {
    return identifierHi;
  }

  public long getIdentifierLo() {
    return identifierLo;
  }

  @Override
  public String toString() {
    return "ResolvedKey:typeTag=" + typeTag + ",hi=0x" + Long.toHexString(identifierHi) + ",lo="
        + Long.toHexString(identifierLo);
  }

  @Override
  public int hashCode() {
    return this.hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ResolvedKey)) {
      return false;
    }

    ResolvedKey other = (ResolvedKey) obj;

    return other.typeTag == typeTag && other.identifierHi == identifierHi
        && other.identifierLo == identifierLo;
  }
}
