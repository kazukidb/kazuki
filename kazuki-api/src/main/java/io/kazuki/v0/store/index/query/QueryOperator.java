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
package io.kazuki.v0.store.index.query;

/**
 * All the supported Query operators.
 */
public enum QueryOperator {
  /**
   * Equality comparison operator
   */
  EQ,
  
  /**
   * Inequality comparison operator
   */
  NE,
  
  /**
   * Greater-than comparison operator
   */
  GT,
  
  /**
   * Greater-than-or-equal-to comparison operator
   */
  GE,
  
  /**
   * Less-than comparison operator
   */
  LT,
  
  /**
   * Less-than-or-equal-to comparison operator
   */
  LE,
  
  /**
   * "IN" comparison operator (value contained within set)
   */
  IN;
}
