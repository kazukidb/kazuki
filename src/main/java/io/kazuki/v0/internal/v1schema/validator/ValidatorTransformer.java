package io.kazuki.v0.internal.v1schema.validator;

/**
 * Interface for transforming and validating values for KV storage.
 * 
 * @param <T>
 * @param <V>
 */
public interface ValidatorTransformer<T, V> {
    public V validateTransform(T instance) throws ValidationException;

    public T untransform(V instance) throws ValidationException;
}
