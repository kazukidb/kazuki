package io.kazuki.v0.internal.serialize.msgpack;

import io.kazuki.v0.internal.serialize.SerializationException;
import io.kazuki.v0.internal.serialize.Serializer;

import org.msgpack.MessagePack;

public class MsgPackCodec<T> implements Serializer<T> {
  private final Class<T> theClass;

  public MsgPackCodec(Class<T> theClass) {
    this.theClass = theClass;
  }

  @Override
  public T decode(byte[] bytes) throws SerializationException {
    MessagePack msgpack = new MessagePack();
    try {
      return msgpack.read(bytes, theClass);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public byte[] encode(Object instance) throws SerializationException {
    MessagePack msgpack = new MessagePack();
    try {
      return msgpack.write(instance);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }
}
