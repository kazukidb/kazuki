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
package io.kazuki.v0.internal.compress;

public class CompressionException extends Exception {
  private static final long serialVersionUID = -3686501941704454384L;

  public CompressionException() {}

  public CompressionException(String message, Throwable cause) {
    super(message, cause);
  }

  public CompressionException(String message) {
    super(message);
  }

  public CompressionException(Throwable cause) {
    super(cause);
  }
}
