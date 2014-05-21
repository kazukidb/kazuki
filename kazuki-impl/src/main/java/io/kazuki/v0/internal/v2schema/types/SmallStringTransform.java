/**
 * Copyright 2014 Sunny Gleason
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
package io.kazuki.v0.internal.v2schema.types;

import io.kazuki.v0.store.schema.model.Transform;
import io.kazuki.v0.store.schema.model.TransformException;



/**
 * Validates / transforms String values.
 */
public class SmallStringTransform implements Transform<Object, String> {
  public static final int SMALLSTRING_MAX_LENGTH = 255;

  public String pack(Object instance) throws TransformException {
    if (instance == null) {
      throw new TransformException("must not be null");
    }

    String instanceString = instance.toString();
    int length = instanceString.length();

    if (length > SMALLSTRING_MAX_LENGTH) {
      throw new TransformException("length must be less than or equal to " + SMALLSTRING_MAX_LENGTH);
    }

    return instanceString;
  }

  public Object unpack(String instance) throws TransformException {
    if (instance == null) {
      throw new TransformException("must not be null");
    }

    return instance;
  }
}
