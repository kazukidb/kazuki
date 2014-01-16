package io.kazuki.v0.internal.helper;

import io.kazuki.v0.internal.v2schema.Attribute.Type;

public interface SqlTypeHelper {
  public String getPrefix();

  public String getSqlType(Type type);

  public String getInsertIgnore();

  public String getPKConflictResolve();

  public String quote(String name);

  public String getTableOptions();
}
