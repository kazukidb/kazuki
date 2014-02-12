package io.kazuki.v0.internal.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.testng.ITestResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
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
  public void announceClass() {
    System.out.println("Running test " + getClass().getName());
  }

  @AfterClass
  public void denounceClass() {
    System.out.println();
  }

  @AfterMethod
  public void reportMethod(final ITestResult result) {
    final StringBuilder sb = new StringBuilder("[");
    sb.append(result.getMethod().getMethodName());
    sb.append(" ");
    if (result.isSuccess()) {
      sb.append(" Ok]");
    }
    else {
      sb.append(" Failed]");
    }
    sb.append(" ");
    System.out.print(sb);
  }
}
