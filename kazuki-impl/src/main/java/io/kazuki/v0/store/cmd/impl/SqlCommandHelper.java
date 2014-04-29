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
