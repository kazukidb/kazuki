/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
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
