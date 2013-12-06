package io.kazuki.v0.store;

public class KazukiException extends Exception {
  private static final long serialVersionUID = -8942530541508421814L;

  public KazukiException() {
    super();
  }

  public KazukiException(String message, Throwable cause) {
    super(message, cause);
  }

  public KazukiException(String message) {
    super(message);
  }

  public KazukiException(Throwable cause) {
    super(cause);
  }
}
