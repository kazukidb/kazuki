package io.kazuki.v0.store.schema;

import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.schema.model.Schema;

public interface SchemaStore {
  Key createSchema(String type, Schema value) throws KazukiException;

  Schema retrieveSchema(String type) throws KazukiException;

  boolean updateSchema(final String type, final Schema value) throws KazukiException;

  boolean deleteSchema(final String type) throws KazukiException;

  void clear() throws KazukiException;
}
