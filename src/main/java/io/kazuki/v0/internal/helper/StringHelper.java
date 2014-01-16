package io.kazuki.v0.internal.helper;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;

public class StringHelper {
  public static String toCamelCase(String inValue) {
    Preconditions.checkNotNull(inValue, "inValue must not be null");

    String canonical = inValue.replace('.', '-').toLowerCase();

    return CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, canonical);
  }
}
