package io.kazuki.v0.store.journal;

import io.kazuki.v0.internal.availability.AvailabilityManager;
import io.kazuki.v0.internal.helper.SqlTypeHelper;
import io.kazuki.v0.internal.v2schema.Attribute;
import io.kazuki.v0.internal.v2schema.Schema;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValueIterator;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.keyvalue.KeyValueStoreConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueStoreJdbiH2Impl;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleRegistration;
import io.kazuki.v0.store.lifecycle.LifecycleSupportBase;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.sequence.KeyImpl;
import io.kazuki.v0.store.sequence.ResolvedKey;
import io.kazuki.v0.store.sequence.SequenceService;

import java.util.ArrayList;
import java.util.Collection;
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
import com.google.common.collect.ImmutableList;
import com.google.inject.Provider;

public class PartitionedJournalStore implements JournalStore, LifecycleRegistration {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final AvailabilityManager availability;
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

  public PartitionedJournalStore(AvailabilityManager availability, IDBI database,
      SqlTypeHelper typeHelper, SchemaStore schema, SequenceService sequence, String dbType,
      String groupName, String storeName, Long partitionSize, String dataType,
      boolean strictTypeCreation) {
    Preconditions.checkNotNull(dataType, "dataType");

    this.availability = availability;
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
  public PartitionedJournalStore(AvailabilityManager availability, IDBI database,
      SqlTypeHelper typeHelper, SchemaStore schema, SequenceService sequence,
      KeyValueStoreConfiguration config) {
    this(availability, database, typeHelper, schema, sequence, config.getDbType(), config
        .getGroupName(), config.getStoreName(), config.getPartitionSize(), config.getDataType(),
        config.isStrictTypeCreation());
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

      try (KeyValueIterable<PartitionInfoSnapshot> parts = this.getAllPartitions()) {
        for (PartitionInfoSnapshot partition : parts) {
          if (!partition.isClosed()) {
            log.debug("Found active partition: {}", partition.getPartitionId());

            this.activePartitionInfo.set(new PartitionInfoImpl(partition.getPartitionId(),
                partition.getMinId(), partition.getMaxId(), partition.isClosed()));
            this.activePartitionStore.set(getKeyValueStore(
                getPartitionName(sequence.resolveKey(KeyImpl.valueOf(partition.getPartitionId()))),
                false));

            break;
          }
        }
      }
    } catch (KazukiException e) {
      throw Throwables.propagate(e);
    }

    availability.setAvailable(true);
    log.info("Intitialized PartitionedJournalStore {}", this);
  }

  @Override
  public synchronized <T> Key append(String type, Class<T> clazz, T inValue,
      TypeValidation typeSafety) throws KazukiException {
    availability.assertAvailable();

    if (!this.dataType.equals(type)) {
      throw new IllegalArgumentException("invalid type: expected " + this.dataType + ", was "
          + type);
    }

    Key theKey = sequence.nextKey(type);
    ResolvedKey resolvedKey = sequence.resolveKey(theKey);

    if (theKey == null) {
      throw new IllegalStateException("unable to allocate new key of type: " + type);
    }

    KeyValueStore targetStore = activePartitionStore.get();
    PartitionInfoImpl theActivePartitionInfo = activePartitionInfo.get();

    if (theActivePartitionInfo == null) {
      try (KeyValueIterator<PartitionInfoImpl> iter =
          metaStore.iterators().values(this.typeName, PartitionInfoImpl.class).iterator()) {
        while (iter.hasNext()) {
          PartitionInfoImpl part = iter.next();

          if (!part.isClosed()) {
            theActivePartitionInfo = part;
          }
        }
      }
    }

    if (theActivePartitionInfo != null && theActivePartitionInfo.getSize() >= this.partitionSize) {
      this.closeActivePartition();
    }

    if (theActivePartitionInfo == null || theActivePartitionInfo.isClosed()) {
      KeyImpl partitionKey = (KeyImpl) sequence.nextKey(this.typeName);

      if (partitionKey == null) {
        throw new IllegalStateException("unable to allocate new partition key of type: "
            + this.typeName);
      }

      ResolvedKey resolvedPartitionKey = sequence.resolveKey(partitionKey);

      theActivePartitionInfo =
          new PartitionInfoImpl(partitionKey.getInternalIdentifier(),
              resolvedKey.getIdentifierLo(), resolvedKey.getIdentifierLo(), false);

      this.metaStore.create(this.typeName, PartitionInfo.class, theActivePartitionInfo,
          resolvedPartitionKey, typeSafety);

      String partitionName = getPartitionName(resolvedPartitionKey);
      targetStore = getKeyValueStore(partitionName, true);

      this.activePartitionInfo.set(theActivePartitionInfo);
      this.activePartitionStore.set(targetStore);
    }

    targetStore.create(type, clazz, inValue, resolvedKey, typeSafety);

    theActivePartitionInfo.setMaxId(resolvedKey.getIdentifierLo());
    this.metaStore.update(KeyImpl.valueOf(theActivePartitionInfo.getPartitionId()),
        PartitionInfo.class, theActivePartitionInfo);

    return theKey;
  }

  @Override
  public <T> KeyValueIterable<KeyValuePair<T>> entriesAbsolute(String type, Class<T> clazz,
      Long offset, Long limit) throws KazukiException {
    availability.assertAvailable();

    if (!this.dataType.equals(type)) {
      throw new IllegalArgumentException("invalid type: expected " + this.dataType + ", was "
          + type);
    }

    long idOffset = 0L;
    if (offset != null) {
      idOffset = offset.longValue();
    }

    idOffset += 1;

    List<KeyValueIterable<KeyValuePair<T>>> iters =
        new ArrayList<KeyValueIterable<KeyValuePair<T>>>();

    try (KeyValueIterator<PartitionInfoSnapshot> iter = this.getAllPartitions().iterator()) {
      while (iter.hasNext() && (limit == null || limit > 0L)) {
        PartitionInfo partition = iter.next();

        if (idOffset >= partition.getMinId() && idOffset <= partition.getMaxId()) {
          Long specificLimit = limit == null ? null : limit;

          if (specificLimit != null) {
            long contained = 1 + partition.getMaxId() - idOffset;
            specificLimit = Math.min(contained, specificLimit);
            limit -= specificLimit;
          }

          iters.add(new LazyIterable<KeyValuePair<T>>(getIterableProvider(type, clazz,
              getPartitionName(sequence.resolveKey(KeyImpl.valueOf(partition.getPartitionId()))),
              idOffset - partition.getMinId(), specificLimit)));

          idOffset = partition.getMaxId() + 1;
        }
      }
    }

    if (iters.isEmpty()) {
      return emptyKeyValueIterable();
    }

    return concatKeyValueIterables(iters);
  }

  @Override
  public <T> KeyValueIterable<KeyValuePair<T>> entriesRelative(String type, Class<T> clazz,
      Long offset, Long limit) throws KazukiException {
    availability.assertAvailable();

    if (!this.dataType.equals(type)) {
      throw new IllegalArgumentException("invalid type: expected " + this.dataType + ", was "
          + type);
    }

    long sizeOffset = 0L;

    if (offset != null) {
      sizeOffset = offset.longValue();
    }

    List<KeyValueIterable<KeyValuePair<T>>> iters =
        new ArrayList<KeyValueIterable<KeyValuePair<T>>>();

    try (KeyValueIterator<PartitionInfoSnapshot> iter = this.getAllPartitions().iterator()) {
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

        iters.add(new LazyIterable<KeyValuePair<T>>(getIterableProvider(type, clazz,
            getPartitionName(sequence.resolveKey(KeyImpl.valueOf(partition.getPartitionId()))),
            sizeOffset, specificLimit)));

        sizeOffset = 0L;
      }
    }

    if (iters.isEmpty()) {
      return emptyKeyValueIterable();
    }

    return concatKeyValueIterables(iters);
  }

  @Override
  public Long approximateSize() throws KazukiException {
    availability.assertAvailable();

    long size = 0L;

    for (PartitionInfo partition : this.getAllPartitions()) {
      size += 1 + partition.getMaxId() - partition.getMinId();
    }

    return size;
  }

  @Override
  public synchronized void clear() throws KazukiException {
    log.info("Clearing PartitionedJournalStore {}", this);

    availability.assertAvailable();

    nukeLock.lock();

    try {
      this.closeActivePartition();

      try (KeyValueIterable<PartitionInfoSnapshot> parts = this.getAllPartitions()) {
        for (PartitionInfo partition : parts) {
          if (!this.dropPartition(partition.getPartitionId())) {
            throw new KazukiException("unable to delete partition");
          }
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
    log.debug("Closing Active Partition for PartitionedJournalStore {}", this);

    availability.assertAvailable();

    PartitionInfoImpl partition = activePartitionInfo.get();

    if (partition == null || partition.isClosed()) {
      return false;
    }

    this.activePartitionInfo.set(null);
    this.activePartitionStore.set(null);

    partition.setClosed(true);

    boolean result =
        metaStore.update(KeyImpl.valueOf(partition.getPartitionId()), PartitionInfo.class,
            partition);

    if (result) {
      log.debug("Closed Active Partition for PartitionedJournalStore {}", this);
    }

    return result;
  }

  @Override
  public synchronized boolean dropPartition(String partitionId) throws KazukiException {
    log.debug("Dropping Partition {} of PartitionedJournalStore {}", partitionId, this);

    availability.assertAvailable();

    Key partitionKey = KeyImpl.valueOf(partitionId);
    PartitionInfo partition = metaStore.retrieve(partitionKey, PartitionInfoSnapshot.class);

    if (partition == null) {
      return false;
    }

    if (!partition.isClosed()) {
      throw new IllegalStateException("drop() applies to closed partitions only");
    }

    ResolvedKey resolvedKey = sequence.resolveKey(partitionKey);
    KeyValueStore keyValue = getKeyValueStore(getPartitionName(resolvedKey), false);

    keyValue.destroy();

    boolean result = metaStore.delete(partitionKey);

    if (result) {
      log.debug("Dropped Partition {} of PartitionedJournalStore {}", partitionId, this);
    }

    return result;
  }

  @Override
  @Nullable
  public PartitionInfo getActivePartition() throws KazukiException {
    availability.assertAvailable();

    PartitionInfoImpl info = activePartitionInfo.get();

    return info == null ? null : info.snapshot();
  }

  @Override
  public KeyValueIterable<PartitionInfoSnapshot> getAllPartitions() throws KazukiException {
    availability.assertAvailable();

    return metaStore.iterators().values(this.typeName, PartitionInfoSnapshot.class);
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
        new KeyValueStoreJdbiH2Impl(availability, database, typeHelper, schema, sequence,
            config.build());

    if (initialize) {
      keyValueStore.initialize();
    }

    return keyValueStore;
  }

  private static String getPartitionName(ResolvedKey resolvedKey) {
    return String.format("%016x", resolvedKey.getIdentifierLo());
  }

  private static <T> KeyValueIterable<T> emptyKeyValueIterable() {
    return new KeyValueIterable<T>() {
      @Override
      public KeyValueIterator<T> iterator() {
        return new KeyValueIterator<T>() {
          @Override
          public boolean hasNext() {
            return false;
          }

          @Override
          public T next() {
            throw new IllegalStateException();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

          @Override
          public void close() {}
        };
      }

      @Override
      public void close() {}
    };
  }

  private static <T> KeyValueIterable<T> concatKeyValueIterables(
      final Collection<KeyValueIterable<T>> iterables) {
    return new KeyValueIterable<T>() {
      private final List<KeyValueIterable<T>> innerIterables = ImmutableList.copyOf(iterables);
      private boolean instantiated = false;

      @SuppressWarnings("unchecked")
      @Override
      public KeyValueIterator<T> iterator() {
        if (instantiated) {
          throw new IllegalStateException("iterable may only be used once!");
        }

        if (iterables.isEmpty()) {
          return (KeyValueIterator<T>) emptyKeyValueIterable().iterator();
        }

        return new KeyValueIterator<T>() {
          private final Iterator<KeyValueIterable<T>> outerIter = innerIterables.iterator();
          private KeyValueIterator<T> innerIter = null;
          private boolean initialized = false;

          private void advance() {
            while (outerIter.hasNext() && (innerIter == null || !innerIter.hasNext())) {
              innerIter = outerIter.next().iterator();
              if (innerIter.hasNext()) {
                break;
              }
            }
          }

          @Override
          public boolean hasNext() {
            if (!initialized) {
              advance();
              initialized = true;
            }

            return innerIter.hasNext();
          }

          @Override
          public T next() {
            if (!hasNext()) {
              throw new IllegalStateException("iterator has no next()");
            }

            T nextVal = innerIter.next();

            if (!innerIter.hasNext()) {
              advance();
            }

            return nextVal;
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

          @Override
          public void close() {
            for (KeyValueIterable<T> iter : innerIterables) {
              iter.close();
            }
          }
        };
      }

      @Override
      public void close() {
        for (KeyValueIterable<T> iter : innerIterables) {
          iter.close();
        }
      }
    };
  }

  private <T> Provider<KeyValueIterable<KeyValuePair<T>>> getIterableProvider(final String type,
      final Class<T> clazz, final String partitionName, final Long offset, final Long limit) {
    return new Provider<KeyValueIterable<KeyValuePair<T>>>() {
      @Override
      public KeyValueIterable<KeyValuePair<T>> get() {
        try {
          return getKeyValueStore(partitionName, false).iterators().entries(type, clazz, offset,
              limit);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public String toString() {
        return "Provider<Iterable>(t=" + type + ",c=" + clazz.getName() + ",p=" + partitionName
            + ",o=" + offset + ",l=" + limit + ")";
      }
    };
  }

  public static class LazyIterable<T> implements KeyValueIterable<T> {
    private final Provider<KeyValueIterable<T>> provider;
    private KeyValueIterator<T> instance;
    private boolean instantiated = false;

    public LazyIterable(Provider<KeyValueIterable<T>> provider) {
      this.provider = provider;
    }

    @Override
    public KeyValueIterator<T> iterator() {
      if (instantiated) {
        throw new IllegalStateException("iterable may only be used once!");
      }

      if (this.instance == null) {
        this.instance = provider.get().iterator();
        this.instantiated = true;
      }

      return this.instance;
    }

    @Override
    public void close() {
      if (this.instantiated && this.instance != null) {
        this.instance.close();
        this.instance = null;
      }
    }

    @Override
    public String toString() {
      return "LazyIterator(" + provider.toString() + ")";
    }
  }
}
