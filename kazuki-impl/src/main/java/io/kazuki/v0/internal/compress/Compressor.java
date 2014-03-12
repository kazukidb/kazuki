package io.kazuki.v0.internal.compress;

public interface Compressor<T> {
  byte[] encode(byte[] original) throws CompressionException;

  byte[] decode(byte[] compressed) throws CompressionException;
}
