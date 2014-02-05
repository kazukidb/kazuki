package io.kazuki.v0.store.journal;

import io.kazuki.v0.internal.helper.SqlTypeHelper;
import io.kazuki.v0.internal.v2schema.Attribute;
import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.keyvalue.KeyValueStoreConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueStoreJdbiH2Impl;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleRegistration;
import io.kazuki.v0.store.lifecycle.LifecycleSupportBase;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.sequence.SequenceService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;
import javax.inject.Inject;

import org.skife.jdbi.v2.IDBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.inject.Provider;

public class PartitionedJournalStore implements JournalStore, LifecycleRegistration {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final IDBI database;
  private final SqlTypeHelper typeHelper;
  private final SequenceService sequence;
  private final SchemaStore schema;
  private KeyValueStore metaStore;
  private final Lock nukeLock = new ReentrantLock();
  private final String dbType;
  private final String groupName;
  private final String storeName;
  private final boolean strictTypeCreation;
  private final Long partitionSize;
  private final String dataType;
  private final String typeName;
  private final AtomicReference<KeyValueStore> activePartitionStore;
  private final AtomicReference<PartitionInfoImpl> activePartitionInfo;

  public PartitionedJournalStore(IDBI database, SqlTypeHelper typeHelper, SchemaStore schema,
      SequenceService sequence, String dbType, String groupName, String storeName,
      Long partitionSize, String dataType, boolean strictTypeCreation) {
    Preconditions.checkNotNull(dataType, "dataType");

    this.database = database;
    this.typeHelper = typeHelper;
    this.schema = schema;
    this.sequence = sequence;
    this.dbType = dbType;
    this.dataType = dataType;
    this.groupName = groupName;
    this.storeName = storeName;
    this.strictTypeCreation = strictTypeCreation;
    this.partitionSize = partitionSize;
    this.typeName = "PartitionInfo-" + groupName + "-" + storeName;
    this.activePartitionInfo = new AtomicReference<PartitionInfoImpl>();
    this.activePartitionStore = new AtomicReference<KeyValueStore>();
  }

  @Inject
  public PartitionedJournalStore(IDBI database, SqlTypeHelper typeHelper, SchemaStore schema,
      SequenceService sequence, KeyValueStoreConfiguration config) {
    this(database, typeHelper, schema, sequence, config.getDbType(), config.getGroupName(), config
        .getStoreName(), config.getPartitionSize(), config.getDataType(), config
        .isStrictTypeCreation());
  }

  @Inject
  public void register(Lifecycle lifecycle) {
    lifecycle.register(new LifecycleSupportBase() {
      @Override
      public void init() {
        PartitionedJournalStore.this.initialize();
      }
    });
  }

  @Override
  public synchronized void initialize() {
    log.info("Intitializing PartitionedJournalStore {}", this);
    this.metaStore = getKeyValueStore("META", true);

    try {
      if (this.schema.retrieveSchema(this.typeName) == null) {
        this.schema.createSchema(this.typeName, new Schema(Collections.<Attribute>emptyList()));
      }

      Iterator<PartitionInfoSnapshot> iter = this.getAllPartitions();

      while (iter.hasNext()) {
        PartitionInfoSnapshot partition = iter.next();

        if (!partition.isClosed()) {
          log.debug("Found active partition: {}", partition.getPartitionId());

          this.activePartitionInfo.set(new PartitionInfoImpl(partition.getPartitionId(), partition
              .getMinId(), partition.getMaxId(), partition.isClosed()));
          this.activePartitionStore.set(getKeyValueStore(
              getPartitionName(Key.valueOf(partition.getPartitionId())), false));

          break;
        }
      }
    } catch (KazukiException e) {
      throw Throwables.propagate(e);
    }

    log.info("Intitialized PartitionedJournalStore {}", this);
  }

  @Override
  public synchronized <T> void append(String type, Class<T> clazz, T inValue,
      TypeValidation typeSafety) throws KazukiException {
    if (!this.dataType.equals(type)) {
      throw new IllegalArgumentException("invalid type: expected " + this.dataType + ", was "
          + type);
    }

    Key theKey = sequence.nextKey(type);

    if (theKey == null) {
      throw new IllegalStateException("unable to allocate new key of type: " + type);
    }

    KeyValueStore targetStore = activePartitionStore.get();
    PartitionInfoImpl theActivePartitionInfo = activePartitionInfo.get();

    if (theActivePartitionInfo == null) {
      Iterator<PartitionInfoImpl> iter =
          metaStore.iterators().iterator(this.typeName, PartitionInfoImpl.class);

      while (iter.hasNext()) {
        PartitionInfoImpl part = iter.next();

        if (!part.isClosed()) {
          theActivePartitionInfo = part;
        }
      }
    }

    if (theActivePartitionInfo != null
        && theActivePartitionInfo.getSize() + 1 >= this.partitionSize) {
      this.closeActivePartition();
    }

    if (theActivePartitionInfo == null || theActivePartitionInfo.isClosed()) {
      Key partitionKey = sequence.nextKey(this.typeName);

      if (partitionKey == null) {
        throw new IllegalStateException("unable to allocate new partition key of type: "
            + this.typeName);
      }

      theActivePartitionInfo =
          new PartitionInfoImpl(partitionKey.getIdentifier(), theKey.getId(), theKey.getId(), false);

      this.metaStore.create(this.typeName, PartitionInfo.class, theActivePartitionInfo,
          partitionKey.getId(), typeSafety);

      String partitionName = getPartitionName(partitionKey);
      targetStore = getKeyValueStore(partitionName, true);

      this.activePartitionInfo.set(theActivePartitionInfo);
      this.activePartitionStore.set(targetStore);
    }


    targetStore.create(type, clazz, inValue, theKey.getId(), typeSafety);

    theActivePartitionInfo.setMaxId(theKey.getId());
    this.metaStore.update(Key.valueOf(theActivePartitionInfo.getPartitionId()),
        PartitionInfo.class, theActivePartitionInfo);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Iterator<T> getIteratorAbsolute(String type, Class<T> clazz, Long offset, Long limit)
      throws KazukiException {
    if (!this.dataType.equals(type)) {
      throw new IllegalArgumentException("invalid type: expected " + this.dataType + ", was "
          + type);
    }

    long idOffset = 0L;
    if (offset != null) {
      idOffset = offset.longValue();
    }

    idOffset += 1;

    Iterator<PartitionInfoSnapshot> iter = this.getAllPartitions();
    List<Iterator<T>> iters = new ArrayList<Iterator<T>>();

    while (iter.hasNext() && (limit == null || limit > 0L)) {
      PartitionInfo partition = iter.next();

      if (idOffset >= partition.getMinId() && idOffset <= partition.getMaxId()) {
        Long specificLimit = limit == null ? null : limit;

        if (specificLimit != null) {
          long contained = 1 + partition.getMaxId() - idOffset;
          specificLimit = Math.min(contained, specificLimit);
          limit -= specificLimit;
        }

        iters.add(new LazyIterator<T>(getIteratorProvider(type, clazz,
            getPartitionName(Key.valueOf(partition.getPartitionId())),
            idOffset - partition.getMinId(), specificLimit)));

        idOffset = partition.getMaxId() + 1;
      }
    }

    if (iters.isEmpty()) {
      return Iterators.emptyIterator();
    }

    return Iterators.concat(iters.toArray(new Iterator[iters.size()]));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Iterator<T> getIteratorRelative(String type, Class<T> clazz, Long offset, Long limit)
      throws KazukiException {
    if (!this.dataType.equals(type)) {
      throw new IllegalArgumentException("invalid type: expected " + this.dataType + ", was "
          + type);
    }

    long sizeOffset = 0L;

    if (offset != null) {
      sizeOffset = offset.longValue();
    }

    Iterator<PartitionInfoSnapshot> iter = this.getAllPartitions();
    List<Iterator<T>> iters = new ArrayList<Iterator<T>>();

    while (iter.hasNext() && (limit == null || limit > 0L)) {
      PartitionInfo partition = iter.next();
      long size = 1 + partition.getMaxId() - partition.getMinId();
      long toIgnore = Math.min(sizeOffset, size);

      if (toIgnore == size) {
        sizeOffset -= size;
        continue;
      }

      Long specificLimit = limit == null ? null : limit;

      if (specificLimit != null) {
        long toTake = size - toIgnore;
        specificLimit = Math.min(toTake, specificLimit);
        limit -= specificLimit;
      }

      iters.add(new LazyIterator<T>(getIteratorProvider(type, clazz,
          getPartitionName(Key.valueOf(partition.getPartitionId())), sizeOffset, specificLimit)));

      sizeOffset = 0L;
    }

    if (iters.isEmpty()) {
      return Iterators.emptyIterator();
    }

    return Iterators.concat(iters.toArray(new Iterator[iters.size()]));
  }

  @Override
  public Long approximateSize() throws KazukiException {
    Iterator<PartitionInfoSnapshot> iter = this.getAllPartitions();
    long size = 0L;

    while (iter.hasNext()) {
      PartitionInfo partition = iter.next();
      size += 1 + partition.getMaxId() - partition.getMinId();
    }

    return size;
  }

  @Override
  public synchronized void clear() throws KazukiException {
    log.info("Clearing PartitionedJournalStore {}", this);
    nukeLock.lock();

    try {
      this.closeActivePartition();

      Iterator<PartitionInfoSnapshot> iter = this.getAllPartitions();

      while (iter.hasNext()) {
        PartitionInfo partition = iter.next();

        if (!this.dropPartition(partition.getPartitionId())) {
          throw new KazukiException("unable to delete partition");
        }
      }

      sequence.resetCounter(this.dataType);
      sequence.resetCounter(this.typeName);
      metaStore.destroy();

      this.activePartitionInfo.set(null);
      this.activePartitionStore.set(null);

      this.initialize();
    } finally {
      nukeLock.unlock();
    }

    log.info("Cleared PartitionedJournalStore {}", this);
  }

  @Override
  public synchronized boolean closeActivePartition() throws KazukiException {
    log.info("Closing Active Partition for PartitionedJournalStore {}", this);

    PartitionInfoImpl partition = activePartitionInfo.get();

    if (partition == null || partition.isClosed()) {
      return false;
    }

    this.activePartitionInfo.set(null);
    this.activePartitionStore.set(null);

    partition.setClosed(true);

    boolean result =
        metaStore.update(Key.valueOf(partition.getPartitionId()), PartitionInfo.class, partition);

    if (result) {
      log.info("Closed Active Partition for PartitionedJournalStore {}", this);
    }

    return result;
  }

  @Override
  public synchronized boolean dropPartition(String partitionId) throws KazukiException {
    log.info("Dropping Partition {} of PartitionedJournalStore {}", partitionId, this);

    Key partitionKey = Key.valueOf(partitionId);
    PartitionInfo partition = metaStore.retrieve(partitionKey, PartitionInfoSnapshot.class);

    if (partition == null) {
      return false;
    }

    if (!partition.isClosed()) {
      throw new IllegalStateException("drop() applies to closed partitions only");
    }

    KeyValueStore keyValue = getKeyValueStore(getPartitionName(partitionKey), false);
    keyValue.destroy();

    boolean result = metaStore.delete(partitionKey);

    if (result) {
      log.info("Dropped Partition {} of PartitionedJournalStore {}", partitionId, this);
    }

    return result;
  }

  @Override
  @Nullable
  public PartitionInfo getActivePartition() throws KazukiException {
    PartitionInfoImpl info = activePartitionInfo.get();

    return info == null ? null : info.snapshot();
  }

  @Override
  public Iterator<PartitionInfoSnapshot> getAllPartitions() throws KazukiException {
    return metaStore.iterators().iterator(this.typeName, PartitionInfoSnapshot.class);
  }

  private KeyValueStore getKeyValueStore(String partitionName, boolean initialize) {
    KeyValueStoreConfiguration.Builder config = new KeyValueStoreConfiguration.Builder();

    config.withDbType(this.dbType);
    config.withGroupName(this.groupName);
    config.withStoreName(this.storeName);
    config.withPartitionName(partitionName);
    config.withPartitionSize(this.partitionSize);
    config.withStrictTypeCreation(this.strictTypeCreation);

    KeyValueStore keyValueStore =
        new KeyValueStoreJdbiH2Impl(database, typeHelper, schema, sequence, config.build());

    if (initialize) {
      keyValueStore.initialize();
    }

    return keyValueStore;
  }

  private static String getPartitionName(Key partitionKey) {
    return String.format("%016x", partitionKey.getId());
  }

  private <T> Provider<Iterator<T>> getIteratorProvider(final String type, final Class<T> clazz,
      final String partitionName, final Long offset, final Long limit) {
    return new Provider<Iterator<T>>() {
      @Override
      public Iterator<T> get() {
        try {
          return getKeyValueStore(partitionName, false).iterators().iterator(type, clazz, offset,
              limit);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public String toString() {
        return "Provider<Iterator>(t=" + type + ",c=" + clazz.getName() + ",p=" + partitionName
            + ",o=" + offset + ",l=" + limit + ")";
      }
    };
  }

  public static class LazyIterator<T> implements Iterator<T> {
    private final Provider<Iterator<T>> provider;
    private Iterator<T> iter = null;

    public LazyIterator(Provider<Iterator<T>> provider) {
      this.provider = provider;
    }

    @Override
    public boolean hasNext() {
      if (iter == null) {
        iter = provider.get();
      }

      return iter.hasNext();
    }

    @Override
    public T next() {
      if (iter == null) {
        iter = provider.get();
      }

      return iter.next();
    }

    @Override
    public void remove() {
      if (iter == null) {
        iter = provider.get();
      }

      iter.remove();
    }

    @Override
    public String toString() {
      return "LazyIterator(" + provider.toString() + ")";
    }
  }
}
