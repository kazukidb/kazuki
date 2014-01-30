package io.kazuki.v0.store.keyvalue;

import io.kazuki.v0.internal.helper.H2TypeHelper;
import io.kazuki.v0.internal.helper.SqlTypeHelper;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.sequence.SequenceService;

import javax.inject.Inject;

import org.skife.jdbi.v2.IDBI;

/**
 * H2 SQL implementation of key-value storage using JDBI.
 */
public class KeyValueStoreJdbiH2Impl extends KeyValueStoreJdbiBaseImpl {
  protected String getPrefix() {
    return H2TypeHelper.DATABASE_PREFIX;
  }

  @Inject
  public KeyValueStoreJdbiH2Impl(IDBI database, SqlTypeHelper typeHelper,
      SchemaStore schemaManager, SequenceService sequences, KeyValueStoreConfiguration config) {
    this(database, typeHelper, schemaManager, sequences, config.getGroupName(), config
        .getStoreName(), config.getPartitionName());
  }

  public KeyValueStoreJdbiH2Impl(IDBI database, SqlTypeHelper typeHelper,
      SchemaStore schemaManager, SequenceService sequences, String groupName, String storeName,
      String partitionName) {
    super(database, typeHelper, schemaManager, sequences, groupName, storeName, partitionName);
  }
}
