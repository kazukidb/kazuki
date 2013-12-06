package io.kazuki.v0.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Foo {
  private final String fooKey;
  private final String fooValue;

  @JsonCreator
  public Foo(@JsonProperty("fooKey") String fooKey, @JsonProperty("fooValue") String fooValue) {
    this.fooKey = fooKey;
    this.fooValue = fooValue;
  }

  public String getFooKey() {
    return fooKey;
  }

  public String getFooValue() {
    return fooValue;
  }
}
