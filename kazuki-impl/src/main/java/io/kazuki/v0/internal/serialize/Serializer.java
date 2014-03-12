package io.kazuki.v0.internal.serialize;

public interface Serializer<T> {
  byte[] encode(T instance) throws SerializationException;

  T decode(byte[] representation) throws SerializationException;
}
