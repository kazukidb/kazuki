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
package io.kazuki.v0.store.cmd.impl;

import java.util.Iterator;
import java.util.Map;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;

public class SqlCommandHelper {
  public static void outputResults(final IDBI database, final String query,
      final RowHandler rowHandler) {
    database.inTransaction(new TransactionCallback<Void>() {
      @Override
      public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
        Iterator<Map<String, Object>> iter = handle.createQuery(query).iterator();

        while (iter.hasNext()) {
          rowHandler.handleRow(iter.next());
        }

        return null;
      }
    });
  }

  public static void executeStatement(final IDBI database, final String query,
      final ResultHandler resultHandler) {
    database.inTransaction(new TransactionCallback<Void>() {
      @Override
      public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
        int result = handle.createStatement(query).execute();
        resultHandler.handleResult(result);

        return null;
      }
    });
  }

  public static interface RowHandler {
    void handleRow(Map<String, Object> row);
  }

  public static interface ResultHandler {
    void handleResult(int result);
  }
}
