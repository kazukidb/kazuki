package io.kazuki.v0.internal.helper;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;

public class StringHelper {
  public static String toCamelCase(String value) {
    Preconditions.checkNotNull(value, "value");

    String canonical = value.replace('.', '-').toLowerCase();

    return CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, canonical);
  }
}
