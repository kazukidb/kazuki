package io.kazuki.v0.internal.helper;

import io.kazuki.v0.internal.v1schema.AttributeType;

public interface SqlTypeHelper {
  public String getPrefix();

  public String getSqlType(AttributeType type);

  public String getInsertIgnore();

  public String getPKConflictResolve();

  public String quote(String name);

  public String getTableOptions();
}
