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

import java.math.BigInteger;
import java.util.List;

import org.junit.Assert;
import org.testng.annotations.Test;

@Test
public class QueryHelperTest {
  public void simpleTest() {
    List<QueryTerm> query = QueryHelper.parseQuery("a eq \"foo\"");
    Assert.assertEquals(query.size(), 1);
    QueryTerm term = query.get(0);

    Assert.assertEquals(term.getField(), "a");
    Assert.assertEquals(term.getOperator(), QueryOperator.EQ);
    Assert.assertEquals(term.getValue().getValue(), "foo");
  }

  public void compoundTest() {
    List<QueryTerm> query = QueryHelper.parseQuery("a eq \"foo\" and b ne 4");
    Assert.assertEquals(query.size(), 2);

    QueryTerm term = query.get(0);
    Assert.assertEquals(term.getField(), "a");
    Assert.assertEquals(term.getOperator(), QueryOperator.EQ);
    Assert.assertEquals(term.getValue().getValue(), "foo");


    QueryTerm term2 = query.get(1);
    Assert.assertEquals(term2.getField(), "b");
    Assert.assertEquals(term2.getOperator(), QueryOperator.NE);
    Assert.assertEquals(term2.getValue().getValue(), new BigInteger("4"));
  }
}
