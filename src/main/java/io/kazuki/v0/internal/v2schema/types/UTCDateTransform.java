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

import io.kazuki.v0.internal.v2schema.Transform;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Validates / transforms UTC date values.
 */
public class UTCDateTransform implements Transform<Object, Number> {
    private final DateTimeFormatter format = ISODateTimeFormat
            .basicDateTimeNoMillis();

    public Long pack(Object instance) throws TransformException {
        if (instance == null) {
            throw new TransformException("must not be null");
        }

        try {
            String instanceString = (instance instanceof DateTime) ? format
                    .print((DateTime) instance) : instance.toString();

            return format.parseMillis(instanceString) / 1000L;
        } catch (IllegalArgumentException e) {
            throw new TransformException(
                    "is not ISO8601 datetime (yyyyMMdd'T'HHmmssZ)");
        }
    }

    @Override
    public Object unpack(Number instance) throws TransformException {
        if (instance == null) {
            throw new TransformException("must not be null");
        }

        return format.print(new DateTime(instance.longValue() * 1000L,
                DateTimeZone.UTC));
    }
}
