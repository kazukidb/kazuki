package io.kazuki.v0.internal.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class TestSupport
{
  private static final String TEST_CLASS_NAME = "testClassName";

  protected Logger log = LoggerFactory.getLogger("TEST");

  @BeforeClass
  public void setUpLogging() {
    MDC.put(TEST_CLASS_NAME, getClass().getName());
  }

  @AfterClass
  public void tearDownLogging() {
    MDC.remove(TEST_CLASS_NAME);
  }

  @BeforeClass
  public void reportProgress() {
    System.out.println("Running test " + getClass().getName());
  }
}
