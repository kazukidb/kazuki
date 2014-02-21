package io.kazuki.v0.internal.helper;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class MaskProxy<T, U extends T> implements InvocationHandler {
  private final Class<T> iface;
  private volatile U delegate;
  private final InvocationHandler finalHandler;

  public static final InvocationHandler UNSUPPORTED_OPERATION_HANDLER = new InvocationHandler() {
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      throw new UnsupportedOperationException();
    }
  };

  public Class<T> getInterface() {
    return iface;
  }

  public MaskProxy(Class<T> iface, U delegate) {
    this.iface = iface;
    this.delegate = delegate;
    this.finalHandler = UNSUPPORTED_OPERATION_HANDLER;
  }

  public U getAndSet(U newInstance) {
    U old = this.delegate;
    this.delegate = newInstance;

    return old;
  }

  @SuppressWarnings("unchecked")
  public T asProxyInstance() {
    return (T) Proxy.newProxyInstance(iface.getClassLoader(), new Class[] {iface}, this);
  }

  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if (iface.getMethod(method.getName(), method.getParameterTypes()) == null) {
      return finalHandler.invoke(proxy, method, args);
    }

    try {
      return delegate.getClass().getMethod(method.getName(), method.getParameterTypes())
          .invoke(delegate, args);
    } catch (NoSuchMethodException nsme) {} catch (IllegalArgumentException iae) {} catch (IllegalAccessException iae2) {} catch (InvocationTargetException ite) {
      throw ite.getTargetException();
    }
    // allow SecurityException to go out unchecked

    return finalHandler.invoke(proxy, method, args);
  }
}
