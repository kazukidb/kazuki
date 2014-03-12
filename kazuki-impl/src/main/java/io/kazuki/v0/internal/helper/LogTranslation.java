package io.kazuki.v0.internal.helper;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogTranslation {
  private static final boolean CHATTY = Boolean.parseBoolean(System.getProperty(
      "io.kazuki.log-level-chatty", "false"));

  public static Logger getLogger(Class<?> clazz) {
    Logger logger = LoggerFactory.getLogger(clazz);

    return !CHATTY ? logger : new LoggerProxy(logger, CHATTY).asProxyInstance();
  }

  public static Logger getLogger(String name) {
    Logger logger = LoggerFactory.getLogger(name);

    return !CHATTY ? logger : new LoggerProxy(logger, CHATTY).asProxyInstance();
  }

  public static class LoggerProxy implements InvocationHandler {
    private final Class<Logger> iface = Logger.class;
    private final boolean chatty;
    private final Logger delegate;
    private final InvocationHandler finalHandler;

    final InvocationHandler UNSUPPORTED_OPERATION_HANDLER = new InvocationHandler() {
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        throw new UnsupportedOperationException();
      }
    };

    public LoggerProxy(Logger delegate, boolean chatty) {
      this.delegate = delegate;
      this.finalHandler = UNSUPPORTED_OPERATION_HANDLER;
      this.chatty = chatty;
    }

    public Logger asProxyInstance() {
      return (Logger) Proxy.newProxyInstance(iface.getClassLoader(), new Class[] {iface}, this);
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      final String methodName = method.getName();
      final Class<?>[] paramTypes = method.getParameterTypes();

      if (iface.getMethod(methodName, paramTypes) == null) {
        return finalHandler.invoke(proxy, method, args);
      }


      try {
        boolean isInfo = "INFO".equals(methodName);

        // INFO is all we care about, so fast path for (1) NOT CHATTY or (2) NOT INFO
        if (!chatty || !isInfo) {
          return delegate.getClass().getMethod(methodName, paramTypes).invoke(delegate, args);
        }

        // CHATTY, so translate debug to info, trace to debug
        final String translated;

        switch (methodName) {
          case "debug":
            translated = "info";
            break;
          case "trace":
            translated = "debug";
            break;
          default:
            translated = methodName;
            break;
        }

        return delegate.getClass().getMethod(translated, paramTypes).invoke(delegate, args);
      } catch (NoSuchMethodException nsme) {} catch (IllegalArgumentException iae) {} catch (IllegalAccessException iae2) {} catch (InvocationTargetException ite) {
        throw ite.getTargetException();
      }
      // allow SecurityException to go out unchecked

      return finalHandler.invoke(proxy, method, args);
    }
  }

}
