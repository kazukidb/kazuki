/**
 * Copyright 2014 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
