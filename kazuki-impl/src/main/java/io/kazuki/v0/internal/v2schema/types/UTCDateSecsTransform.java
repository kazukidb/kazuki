/**
 * Copyright 2014 Sunny Gleason and original author or authors
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

import java.io.IOException;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

/**
 * Validates / transforms UTC date values.
 */
public class UTCDateSecsTransform implements Transform<Object, Number> {
  private final DateTimeFormatter format = getFormat();

  public Long pack(Object instance) throws TransformException {
    if (instance == null) {
      throw new TransformException("must not be null");
    }

    if (instance instanceof Number) {
      return ((Number) instance).longValue() / 1000;
    }

    try {
      String instanceString =
          (instance instanceof DateTime) ? format.print((DateTime) instance) : instance.toString();

      return format.parseMillis(instanceString) / 1000L;
    } catch (IllegalArgumentException e) {
      throw new TransformException("is not ISO8601 datetime (yyyyMMdd'T'HHmmssZ)");
    }
  }

  @Override
  public Object unpack(Number instance) throws TransformException {
    if (instance == null) {
      throw new TransformException("must not be null");
    }

    return format.print(new DateTime(instance.longValue() * 1000L, DateTimeZone.UTC));
  }

  private static final DateTimeFormatter getFormat() {
    return ISODateTimeFormat.basicDateTimeNoMillis().withZoneUTC();
  }

  public static class DateTimeModule extends SimpleModule {
    private static final long serialVersionUID = 1L;
    private static final DateTimeFormatter format = getFormat();

    public DateTimeModule() {
      addDeserializer(DateTime.class, new StdDeserializer<DateTime>(DateTime.class) {
        private static final long serialVersionUID = 1L;

        @Override
        public DateTime deserialize(JsonParser jp, DeserializationContext ctx) throws IOException,
            JsonProcessingException {
          JsonToken tok = jp.getCurrentToken();

          if (tok.equals(JsonToken.VALUE_NULL)) {
            return null;
          }

          if (tok.equals(JsonToken.VALUE_STRING)) {
            return format.parseDateTime(jp.getValueAsString());
          }

          if (tok.equals(JsonToken.VALUE_NUMBER_INT)) {
            return new DateTime(jp.getValueAsLong(), DateTimeZone.UTC);
          }

          throw new IllegalStateException("unable to deserialize DateTime from: " + tok.name());
        }
      });

      addSerializer(DateTime.class, new StdSerializer<DateTime>(DateTime.class) {
        @Override
        public void serialize(DateTime instance, JsonGenerator jg, SerializerProvider sp)
            throws IOException, JsonGenerationException {
          if (instance == null) {
            jg.writeNull();
            return;
          }

          jg.writeString(format.print(instance));
        }
      });
    }
  }
}
