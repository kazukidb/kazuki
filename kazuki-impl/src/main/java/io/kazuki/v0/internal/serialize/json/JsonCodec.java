package io.kazuki.v0.internal.serialize.json;

import io.kazuki.v0.internal.serialize.SerializationException;
import io.kazuki.v0.internal.serialize.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonCodec<T> implements Serializer<T> {
  private final ObjectMapper mapper = new ObjectMapper();
  private final Class<T> theClass;

  public JsonCodec(Class<T> theClass) {
    this.theClass = theClass;
  }

  @Override
  public T decode(byte[] bytes) throws SerializationException {
    try {
      return mapper.readValue(bytes, theClass);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public byte[] encode(Object instance) throws SerializationException {
    try {
      return mapper.writeValueAsBytes(instance);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }
}
