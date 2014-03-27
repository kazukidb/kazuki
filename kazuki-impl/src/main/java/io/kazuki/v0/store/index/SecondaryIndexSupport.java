package io.kazuki.v0.store.index;

import io.kazuki.v0.store.keyvalue.KeyValueStoreListener;
import io.kazuki.v0.store.schema.SchemaStoreListener;

public interface SecondaryIndexSupport
    extends
      SecondaryIndexStore,
      SchemaStoreListener,
      KeyValueStoreListener {}
