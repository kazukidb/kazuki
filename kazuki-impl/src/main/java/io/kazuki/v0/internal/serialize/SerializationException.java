package io.kazuki.v0.internal.serialize;

public class SerializationException extends Exception {
  private static final long serialVersionUID = -3686501941704454384L;

  public SerializationException() {}

  public SerializationException(String message, Throwable cause) {
    super(message, cause);
  }

  public SerializationException(String message) {
    super(message);
  }

  public SerializationException(Throwable cause) {
    super(cause);
  }
}
