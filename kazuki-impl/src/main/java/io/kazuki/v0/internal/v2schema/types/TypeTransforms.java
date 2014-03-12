/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.Transform;

/**
 * Helper class for mapping attribute types to ValidatorTransformer instances.
 */
public class TypeTransforms {
    public static Transform<?, ?> validatorFor(Attribute attribute) {
        switch (attribute.getType()) {
        case ANY:
            return new AnyTransform();
        case MAP:
            return new MapTransform();
        case ARRAY:
            return new ArrayTransform();
        case BOOLEAN:
            return new BooleanTransform();
        case ENUM:
            return new EnumTransform(attribute.getValues());

        case U8:
            return new IntegerTransform("0", Integer.toString(0x000000FF));
        case U16:
            return new IntegerTransform("0", Integer.toString(0x0000FFFF));
        case U32:
            return new IntegerTransform("0", Long.toString(0x00000000FFFFFFFFL));
        case U64:
            return new IntegerTransform("0", Long.toString(Long.MAX_VALUE)
            /* was "18446744073709551615", but that's unsupported by h2 */);

        case I8:
            return new IntegerTransform(Integer.toString(Byte.MIN_VALUE),
                    Integer.toString(Byte.MAX_VALUE));
        case I16:
            return new IntegerTransform(Integer.toString(-32768),
                    Integer.toString(32767));
        case I32:
            return new IntegerTransform(Integer.toString(Integer.MIN_VALUE),
                    Integer.toString(Integer.MAX_VALUE));
        case I64:
            return new IntegerTransform(Long.toString(Long.MIN_VALUE),
                    Long.toString(Long.MAX_VALUE));

        case UTC_DATE_SECS:
            return new UTCDateTransform();
        case UTF8_SMALLSTRING:
            return new SmallStringTransform();
        case UTF8_TEXT:
            return new TextTransform();

        default:
            throw new IllegalArgumentException("Unknown type "
                    + attribute.getType());
        }
    }
}
