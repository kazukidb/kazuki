/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.kazuki.v0.store.journal;

import io.kazuki.v0.internal.availability.AvailabilityManager;
import io.kazuki.v0.internal.helper.LockManager;
import io.kazuki.v0.internal.helper.LogTranslation;
import io.kazuki.v0.internal.helper.SqlTypeHelper;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.keyvalue.KeyValueIterable;
import io.kazuki.v0.store.keyvalue.KeyValueIterator;
import io.kazuki.v0.store.keyvalue.KeyValuePair;
import io.kazuki.v0.store.keyvalue.KeyValueStore;
import io.kazuki.v0.store.keyvalue.KeyValueStoreConfiguration;
import io.kazuki.v0.store.keyvalue.KeyValueStoreIteration.SortDirection;
import io.kazuki.v0.store.keyvalue.KeyValueStoreJdbiH2Impl;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleRegistration;
import io.kazuki.v0.store.lifecycle.LifecycleSupportBase;
import io.kazuki.v0.store.schema.SchemaStore;
import io.kazuki.v0.store.schema.TypeValidation;
import io.kazuki.v0.store.schema.model.Attribute;
import io.kazuki.v0.store.schema.model.IndexDefinition;
import io.kazuki.v0.store.schema.model.Schema;
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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Provider;

public class PartitionedJournalStore implements JournalStore, LifecycleRegistration {
  private final Logger log = LogTranslation.getLogger(getClass());
  private final AvailabilityManager availability;
  private final LockManager lockManager;
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

  public PartitionedJournalStore(AvailabilityManager availability, LockManager lockManager,
      IDBI database, SqlTypeHelper typeHelper, SchemaStore schema, SequenceService sequence,
      String dbType, String groupName, String storeName, Long partitionSize, String dataType,
      boolean strictTypeCreation) {
    Preconditions.checkNotNull(dataType, "dataType");

    this.availability = availability;
    this.lockManager = lockManager;
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
  public PartitionedJournalStore(AvailabilityManager availability, LockManager lockManager,
      IDBI database, SqlTypeHelper typeHelper, SchemaStore schema, SequenceService sequence,
      KeyValueStoreConfiguration config) {
    this(availability, lockManager, database, typeHelper, schema, sequence, config.getDbType(),
        config.getGroupName(), config.getStoreName(), config.getPartitionSize(), config
            .getDataType(), config.isStrictTypeCreation());
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
  public void initialize() {
    log.debug("Intitializing PartitionedJournalStore {}", this);

    try (LockManager toRelease = lockManager.acquire()) {
      this.metaStore = getKeyValueStore("META", true);

      try {
        if (this.schema.retrieveSchema(this.typeName) == null) {
          this.schema.createSchema(this.typeName, new Schema(Collections.<Attribute>emptyList(),
              Collections.<IndexDefinition>emptyList()));
        }

        try (KeyValueIterable<PartitionInfoSnapshot> parts = this.getAllPartitions()) {
          for (PartitionInfoSnapshot partition : parts) {
            if (!partition.isClosed()) {
              log.debug("Found active partition: {}", partition.getPartitionId());

              this.activePartitionInfo.set(new PartitionInfoImpl(partition.getPartitionId(),
                  partition.getMinId(), partition.getMaxId(), partition.getSize(), partition
                      .isClosed()));
              this.activePartitionStore
                  .set(getKeyValueStore(getPartitionName(sequence.resolveKey(KeyImpl
                      .valueOf(partition.getPartitionId()))), false));

              break;
            }
          }
        }
      } catch (KazukiException e) {
        throw Throwables.propagate(e);
      }
    }

    availability.setAvailable(true);
    log.debug("Intitialized PartitionedJournalStore {}", this);
  }

  @Override
  public <T> Key append(String type, Class<T> clazz, T inValue, TypeValidation typeSafety)
      throws KazukiException {
    availability.assertAvailable();

    if (!this.dataType.equals(type)) {
      throw new IllegalArgumentException("invalid type: expected " + this.dataType + ", was "
          + type);
    }

    try (LockManager toRelease = lockManager.acquire()) {
      Key theKey = sequence.nextKey(type);
      ResolvedKey resolvedKey = sequence.resolveKey(theKey);

      if (theKey == null) {
        throw new IllegalStateException("unable to allocate new key of type: " + type);
      }

      PartitionInfoImpl theActivePartitionInfo = activePartitionInfo.get();
      KeyValueStore targetStore = activePartitionStore.get();

      if (theActivePartitionInfo == null) {
        KeyImpl partitionKey = (KeyImpl) sequence.nextKey(this.typeName);

        if (partitionKey == null) {
          throw new IllegalStateException("unable to allocate new partition key of type: "
              + this.typeName);
        }

        ResolvedKey resolvedPartitionKey = sequence.resolveKey(partitionKey);
        String partitionName = getPartitionName(resolvedPartitionKey);

        theActivePartitionInfo =
            new PartitionInfoImpl(partitionKey.getInternalIdentifier(),
                resolvedKey.getIdentifierLo(), resolvedKey.getIdentifierLo(), 0L, false);

        this.activePartitionInfo.set(theActivePartitionInfo);

        this.metaStore.create(this.typeName, PartitionInfo.class,
            theActivePartitionInfo.snapshot(), resolvedPartitionKey, TypeValidation.STRICT);

        targetStore = getKeyValueStore(partitionName, true);
        this.activePartitionStore.set(targetStore);
      }

      targetStore.create(type, clazz, inValue, resolvedKey, typeSafety);

      theActivePartitionInfo.setMaxId(resolvedKey.getIdentifierLo());
      theActivePartitionInfo.setSize(theActivePartitionInfo.getSize() + 1L);

      boolean success =
          this.metaStore.update(KeyImpl.valueOf(theActivePartitionInfo.getPartitionId()),
              PartitionInfo.class, theActivePartitionInfo.snapshot());

      if (!success) {
        throw new KazukiException("unable to update partition info");
      }

      if (theActivePartitionInfo.getSize() >= this.partitionSize) {
        this.closeActivePartition();
      }

      return theKey;
    }
  }

  @Override
  public <T> KeyValueIterable<KeyValuePair<T>> entriesAbsolute(String type, Class<T> clazz,
      SortDirection sortDirection, Long offset, Long limit) throws KazukiException {
    availability.assertAvailable();

    if (sortDirection != null && SortDirection.DESCENDING.equals(sortDirection)) {
      throw new IllegalArgumentException("absolute iterator only supports order ASCENDING");
    }

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

    try (KeyValueIterator<PartitionInfoSnapshot> iter =
        this.getAllPartitions(sortDirection).iterator()) {
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
              sortDirection, idOffset - partition.getMinId(), specificLimit)));

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
      SortDirection sortDirection, Long offset, Long limit) throws KazukiException {
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

    try (KeyValueIterator<PartitionInfoSnapshot> iter =
        this.getAllPartitions(sortDirection).iterator()) {
      while (iter.hasNext() && (limit == null || limit > 0L)) {
        PartitionInfo partition = iter.next();
        long size = partition.getSize();
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
            sortDirection, sizeOffset, specificLimit)));

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

    try (KeyValueIterable<PartitionInfoSnapshot> parts = this.getAllPartitions()) {
      for (PartitionInfo partition : parts) {
        size += partition.getSize();
      }
    }

    return size;
  }

  @Override
  public void clear() throws KazukiException {
    log.debug("Clearing PartitionedJournalStore {}", this);

    availability.assertAvailable();

    try (LockManager toRelease = lockManager.acquire()) {
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
    }

    log.debug("Cleared PartitionedJournalStore {}", this);
  }

  @Override
  public boolean closeActivePartition() throws KazukiException {
    log.debug("Closing Active Partition for PartitionedJournalStore {}", this);

    availability.assertAvailable();

    try (LockManager toRelease = lockManager.acquire()) {
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
  }

  @Override
  public boolean dropPartition(String partitionId) throws KazukiException {
    log.debug("Dropping Partition {} of PartitionedJournalStore {}", partitionId, this);

    availability.assertAvailable();

    try (LockManager toRelease = lockManager.acquire()) {
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
    return getAllPartitions(SortDirection.ASCENDING);
  }

  @Override
  public KeyValueIterable<PartitionInfoSnapshot> getAllPartitions(SortDirection sortDirection)
      throws KazukiException {
    availability.assertAvailable();

    return metaStore.iterators().values(this.typeName, PartitionInfoSnapshot.class, sortDirection);
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
        new KeyValueStoreJdbiH2Impl(availability, lockManager, database, typeHelper, schema,
            sequence, config.build());

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
      final Class<T> clazz, final String partitionName, final SortDirection sortDirection,
      final Long offset, final Long limit) {
    return new Provider<KeyValueIterable<KeyValuePair<T>>>() {
      @Override
      public KeyValueIterable<KeyValuePair<T>> get() {
        try {
          return getKeyValueStore(partitionName, false).iterators().entries(type, clazz,
              sortDirection, offset, limit);
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
