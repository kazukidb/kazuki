package io.kazuki.v0.internal.helper;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.testng.ITestResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class TestSupport {
  private static final String TEST_CLASS_NAME = "testClassName";

  protected Logger log = LoggerFactory.getLogger("TEST_OUTPUT");

  @BeforeClass
  public void beforeClass() {
    MDC.put(TEST_CLASS_NAME, getClass().getName());
    System.out.println("Running test " + getClass().getName());
  }

  @AfterClass
  public void afterClass() {
    MDC.remove(TEST_CLASS_NAME);
    System.out.println();
  }

  @BeforeMethod
  public void prepareMethod(final Method method) {
    log.info(" === Starting {}", method.getName());
  }

  @AfterMethod
  public void afterMethod(final Method method) {
    log.info(" === Finished {}", method.getName());
  }

  @AfterMethod
  public void reportMethod(final ITestResult result) {
    final StringBuilder sb = new StringBuilder("[");
    sb.append(result.getMethod().getMethodName());
    sb.append(" ");
    if (result.isSuccess()) {
      sb.append(" OK]");
    } else {
      sb.append(" FAIL]");
    }
    sb.append(" ");
    System.out.print(sb);
  }
}
