package io.kazuki.v0.store.sequence;


/**
 * Resolves the internal representation of a Key. This class should only be used within Kazuki
 * itself.
 */
public class ResolvedKeyImpl implements ResolvedKey {
  private final int typeTag;
  private final long identifierLo;
  private final long identifierHi;
  private final int hashCode;

  public ResolvedKeyImpl(int typeTag, long identifierHi, long identifierLo) {
    this.typeTag = typeTag;
    this.identifierHi = identifierHi;
    this.identifierLo = identifierLo;
    this.hashCode =
        Integer.valueOf(this.typeTag).hashCode() ^ Long.valueOf(identifierHi).hashCode()
            ^ Long.valueOf(identifierLo).hashCode();
  }

  @Override
  public int getTypeTag() {
    return typeTag;
  }

  @Override
  public long getIdentifierHi() {
    return identifierHi;
  }

  @Override
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
    if (!(obj instanceof ResolvedKeyImpl)) {
      return false;
    }

    ResolvedKeyImpl other = (ResolvedKeyImpl) obj;

    return other.typeTag == typeTag && other.identifierHi == identifierHi
        && other.identifierLo == identifierLo;
  }
}
