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

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

import com.google.common.base.Throwables;

public class QueryHelper {
  public static List<QueryTerm> parseQuery(String queryString) {
    QueryLexer lex = new QueryLexer(new ANTLRStringStream(queryString));
    CommonTokenStream tokens = new CommonTokenStream(lex);
    QueryParser parser = new QueryParser(tokens);

    List<QueryTerm> query = new ArrayList<QueryTerm>();
    try {
      parser.term_list(query);
    } catch (RecognitionException e) {
      throw Throwables.propagate(e);
    }

    return query;
  }
}
