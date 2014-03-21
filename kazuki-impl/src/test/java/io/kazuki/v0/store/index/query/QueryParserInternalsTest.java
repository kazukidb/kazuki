/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.kazuki.v0.store.index.query;

import io.kazuki.v0.store.index.query.QueryParser.term_list_return;

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class QueryParserInternalsTest {
  public void testSimpleQuery() throws Exception {
    String query = "foo eq -1 and _x ne \"wha\" and _bar_val gt 9.91";

    parseQuery(query);
  }

  public void testDoubleQuotes() throws Exception {
    String query = "_x eq \"\\\"\"";

    QueryLexer lex = new QueryLexer(new ANTLRStringStream(query));
    CommonTokenStream tokens = new CommonTokenStream(lex);

    QueryParser parser = new QueryParser(tokens);
    parser.setTreeAdaptor(adaptor);

    List<QueryTerm> foo = new ArrayList<QueryTerm>();
    parser.term_list(foo);

    Assert.assertEquals(foo.toString(), "[_x EQ \"\"\"]");
    Assert.assertTrue(foo.size() == 1);
  }

  static final TreeAdaptor adaptor = new CommonTreeAdaptor() {
    public Object create(Token payload) {
      return new CommonTree(payload);
    }
  };

  private Object parseQuery(String query) throws RecognitionException {
    QueryLexer lex = new QueryLexer(new ANTLRStringStream(query));
    CommonTokenStream tokens = new CommonTokenStream(lex);

    QueryParser parser = new QueryParser(tokens);
    parser.setTreeAdaptor(adaptor);

    List<QueryTerm> foo = new ArrayList<QueryTerm>();
    term_list_return x = parser.term_list(foo);

    Assert.assertEquals(foo.toString(), "[foo EQ -1, _x NE \"wha\", _bar_val GT 9.91]");

    Assert.assertTrue(foo.size() == 3);

    return x;
  }
}
