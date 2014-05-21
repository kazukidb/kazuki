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

import java.util.Map;



/**
 * Validates "ARRAY" type
 */
public class MapTransform implements Transform<Object, Object> {
  @Override
  public Object pack(Object instance) throws TransformException {
    if (instance == null) {
      throw new TransformException("must not be null");
    }

    if (!(instance instanceof Map)) {
      throw new TransformException("must be a map");
    }

    return instance;
  }

  @Override
  public Object unpack(Object instance) throws TransformException {
    if (instance == null) {
      throw new TransformException("must not be null");
    }

    if (!(instance instanceof Map)) {
      throw new TransformException("must be a map");
    }

    return instance;
  }
}
