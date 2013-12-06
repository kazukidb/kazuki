package io.kazuki.v0.internal.serialize.kryo;

import io.kazuki.v0.internal.serialize.SerializationException;
import io.kazuki.v0.internal.serialize.Serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoCodec<T> implements Serializer<T> {
  private final Class<T> theClass;

  public KryoCodec(Class<T> theClass) {
    this.theClass = theClass;
  }

  @Override
  public T decode(byte[] bytes) throws SerializationException {
    Kryo kryo = new Kryo();
    try {
      return kryo.readObject(new Input(bytes), theClass);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public byte[] encode(Object instance) throws SerializationException {
    Kryo kryo = new Kryo();
    try {
      Output output = new Output();
      kryo.writeObject(output, instance);
      return output.toBytes();
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }
}
