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
package io.kazuki.v0.store.index.query;

/**
 * Helper class to encapsulate building a string literal with escapes in ANTLR.
 */
public class StringLiteral {
    private final StringBuilder buf = new StringBuilder();

    public void append(int achar) {
        buf.appendCodePoint(achar);
    }

    public void append(String chars) {
        buf.append(chars);
    }

    public void appendEscaped(String chars) {
        if (chars.startsWith("\\") && chars.length() == 2) {
            switch (chars.charAt(1)) {
            case 'n':
                buf.append("\n");
                break;
            case 't':
                buf.append("\t");
                break;
            case '\\':
                buf.append("\\");
                break;
            case '"':
                buf.append("\"");
                break;
            }
        } else {
            throw new IllegalArgumentException("Unexpected content: " + chars);
        }
    }

    @Override
    public String toString() {
        return buf.toString();
    }
}
