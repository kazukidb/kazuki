package io.kazuki.v0.internal.compress;

public class CompressionException extends Exception {
  private static final long serialVersionUID = -3686501941704454384L;

  public CompressionException() {}

  public CompressionException(String message, Throwable cause) {
    super(message, cause);
  }

  public CompressionException(String message) {
    super(message);
  }

  public CompressionException(Throwable cause) {
    super(cause);
  }
}
