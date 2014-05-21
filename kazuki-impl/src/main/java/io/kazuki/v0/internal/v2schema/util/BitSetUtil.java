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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Utility methods for converting BitSet instances to/from Long array representation. For
 * compactness, BitSet instances with fewer than 64 bits are represented as a single long (instead
 * of an array of 1 or zero elements).
 */
public class BitSetUtil {
  public static Object pack(BitSet bitSet) {
    List<Long> values = new ArrayList<Long>();

    long value = 0L;
    int j = 0;

    for (int i = 0; i < bitSet.size();) {
      if (bitSet.get(i)) {
        value |= (1L << j);
      }

      i += 1;
      j += 1;

      if (j == 64) {
        j = 0;
        values.add(value);
        value = 0L;
      }
    }

    if (value != 0) {
      values.add(value);
    }

    return (values.size() > 1) ? values : values.get(0);
  }

  @SuppressWarnings("unchecked")
  public static BitSet unpack(Object representation) {
    List<Long> values = null;

    if (representation instanceof Number) {
      values = ImmutableList.<Long>of(((Number) representation).longValue());
    } else {
      values = (List<Long>) representation;
    }

    BitSet bitSet = new BitSet();

    int i = 0;
    for (Long value : values) {
      long v = value.longValue();
      if (v != 0) {
        for (int j = 0; j < 64; j++) {
          if ((v & (1L << j)) != 0) {
            bitSet.set((i * 64) + j);
          }
        }
      }

      i += 1;
    }

    return bitSet;
  }
}
