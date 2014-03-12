package io.kazuki.v0.internal.serialize.smile;

import io.kazuki.v0.internal.serialize.SerializationException;
import io.kazuki.v0.internal.serialize.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;

public class SmileCodec<T> implements Serializer<T> {
  private final SmileFactory smile = new SmileFactory();
  private final ObjectMapper mapper = new ObjectMapper(smile);
  private final Class<T> theClass;

  public SmileCodec(Class<T> theClass) {
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
