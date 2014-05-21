/**
 * Copyright 2014 the original author or authors
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
package io.kazuki.v0.internal.v2schema.util;

import io.kazuki.v0.internal.v2schema.util.BitSetUtil;

import java.util.BitSet;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class BitSetUtilTest {
  public void testSmall() {
    String testString = "";
    long testLong = 0;

    for (int i = 0; i <= 64; i++) {
      BitSet todo = bitSetOf(testString);
      Long packed = (Long) BitSetUtil.pack(todo);

      Assert.assertEquals(packed, Long.valueOf(testLong));
      Assert.assertEquals(BitSetUtil.unpack(packed), BitSetUtil.unpack(testLong));

      testString += "1";
      testLong |= (1L << i);
    }
  }

  public void testBig() {
    String testString = "11111111111111111111111111111111111111111111111111111111111111111";
    long testLong = 1L;

    for (int i = 1; i <= 64; i++) {
      BitSet todo = bitSetOf(testString);
      @SuppressWarnings("unchecked")
      List<Long> packed = (List<Long>) BitSetUtil.pack(todo);

      Assert.assertEquals(packed.get(1), Long.valueOf(testLong));
      Assert.assertEquals(BitSetUtil.unpack(packed), bitSetOf(testString));

      testString += "1";
      testLong |= (1L << i);
    }
  }

  private static BitSet bitSetOf(String inString) {
    BitSet bitSet = new BitSet();

    int i = 0;
    for (byte b : inString.getBytes()) {
      if (b == '1') {
        bitSet.set(i);
      }

      i += 1;
    }

    return bitSet;
  }
}
