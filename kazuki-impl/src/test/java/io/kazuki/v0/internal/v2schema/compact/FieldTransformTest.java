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
package io.kazuki.v0.internal.v2schema.compact;

import io.kazuki.v0.store.schema.model.Attribute.Type;
import io.kazuki.v0.store.schema.model.Schema;
import io.kazuki.v0.store.schema.model.Transform;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import javax.xml.crypto.dsig.TransformException;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

@Test
public class FieldTransformTest {
  Schema EMPTY_SCHEMA = (new Schema.Builder()).build();
  Schema BOOLEAN_SCHEMA = (new Schema.Builder()).addAttribute("baz", Type.BOOLEAN, true).build();
  Schema DATE_SCHEMA = (new Schema.Builder()).addAttribute("quux", Type.UTC_DATE_SECS, true)
      .build();
  Schema ENUM_SCHEMA = (new Schema.Builder()).addAttribute("foo", Type.ENUM,
      Arrays.asList((Object) "ONE", "TWO", "THREE"), true).build();

  public void testTransforms() throws TransformException {
    DateTime now = new DateTime().withMillis(0L).withZone(DateTimeZone.UTC);
    DateTime then = now.minusYears(5000);

    Map<String, Object> nothing = Collections.emptyMap();
    Map<String, Object> something =
        ImmutableMap.<String, Object>of("foo", "ONE", "bar", 1L, "baz", "true", "quux", now);
    Map<String, Object> somethingElse =
        ImmutableMap.<String, Object>of("foo", "THREE", "bar", -1L, "baz", "false", "quux", then);

    FieldTransform empty = new FieldTransform(EMPTY_SCHEMA);

    assertRoundTrip(empty, nothing, something, somethingElse);
    Assert.assertEquals(nothing, empty.pack(nothing));
    Assert.assertEquals(something, empty.pack(something));
    Assert.assertEquals(somethingElse, empty.pack(somethingElse));

    FieldTransform bools = new FieldTransform(BOOLEAN_SCHEMA);

    assertRoundTrip(bools, nothing, something, somethingElse);
    Assert.assertEquals(bools.pack(nothing), nothing);
    Assert.assertEquals(bools.pack(something),
        ImmutableMap.<String, Object>of("foo", "ONE", "bar", 1L, "baz", true, "quux", now));
    Assert.assertEquals(bools.pack(somethingElse),
        ImmutableMap.<String, Object>of("foo", "THREE", "bar", -1L, "baz", false, "quux", then));

    FieldTransform dates = new FieldTransform(DATE_SCHEMA);

    assertRoundTrip(dates, nothing, something, somethingElse);
    Assert.assertEquals(dates.pack(nothing), nothing);
    Assert.assertEquals(
        dates.pack(something),
        ImmutableMap.<String, Object>of("foo", "ONE", "bar", 1L, "baz", "true", "quux",
            now.getMillis() / 1000L));
    Assert.assertEquals(
        dates.pack(somethingElse),
        ImmutableMap.<String, Object>of("foo", "THREE", "bar", -1L, "baz", "false", "quux",
            then.getMillis() / 1000L));

    FieldTransform enums = new FieldTransform(ENUM_SCHEMA);

    assertRoundTrip(enums, nothing, something, somethingElse);
    Assert.assertEquals(enums.pack(nothing), nothing);
    Assert.assertEquals(enums.pack(something),
        ImmutableMap.<String, Object>of("foo", 0, "bar", 1L, "baz", "true", "quux", now));
    Assert.assertEquals(enums.pack(somethingElse),
        ImmutableMap.<String, Object>of("foo", 2, "bar", -1L, "baz", "false", "quux", then));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void assertRoundTrip(Transform transform, Object... objects) throws TransformException {
    for (Object object : objects) {
      Assert.assertEquals(transform.pack(object),
          transform.pack(transform.unpack(transform.pack(object))));
    }
  }
}
