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
package io.kazuki.v0.internal.hash;

/**
 * The LongHash interface provides pluggable implementations of long hash codes
 * for Bloom Filter and other nifty use cases.
 */
public interface LongHash {
    /** returns an 8-byte magic number representing the hash */
    public byte[] getMagic();

    /** returns the String name identifying this hash code implementation */
    public String getName();

    /** returns the order-zero long hash code for the given object */
    public long getLongHashCode(String object);

    /** returns the order-zero long hash code for the given object */
    public long getLongHashCode(byte[] data);

    /** returns an array of the first k long hash codes for the given object */
    public long[] getLongHashCodes(String object, int k);

    /** returns the order-zero integer hash code for the given object */
    public int getIntHashCode(String object);

    /** returns the order-zero integer hash code for the given object */
    public int getIntHashCode(byte[] data);

    /** returns an array of the first k int hash codes for the given object */
    public int[] getIntHashCodes(String object, int k);
}
