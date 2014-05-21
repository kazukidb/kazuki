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
package io.kazuki.v0.internal.helper;

import java.util.LinkedHashMap;
import java.util.Map;

public class OpaquePaginationHelper {
  public static String createOpaqueCursor(Long offset) throws Exception {
    Map<String, Object> enc = new LinkedHashMap<String, Object>();
    enc.put("o", offset);

    return new String(Hex.encodeHex(EncodingHelper.convertToSmile(enc)));
  }

  public static Long decodeOpaqueCursor(String token) throws Exception {
    if (token == null || token.length() == 0) {
      return 0L;
    }

    try {
      byte[] tokenValue = Hex.decodeHex(token.toCharArray());
      Map<String, Object> vals =
          (Map<String, Object>) EncodingHelper.parseSmile(tokenValue, LinkedHashMap.class);

      return ((Number) vals.get("o")).longValue();
    } catch (Exception e) {
      throw new IllegalArgumentException("invalid page token: " + token);
    }
  }
}
