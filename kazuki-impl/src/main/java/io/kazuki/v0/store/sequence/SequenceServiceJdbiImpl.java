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
package io.kazuki.v0.store.sequence;

import io.kazuki.v0.internal.availability.AvailabilityManager;
import io.kazuki.v0.internal.availability.AvailabilityManager.ProtectedCommand;
import io.kazuki.v0.internal.availability.Releasable;
import io.kazuki.v0.internal.helper.JDBIHelper;
import io.kazuki.v0.internal.helper.LockManager;
import io.kazuki.v0.internal.helper.LogTranslation;
import io.kazuki.v0.internal.helper.SqlTypeHelper;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.Version;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleRegistration;
import io.kazuki.v0.store.lifecycle.LifecycleSupportBase;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.inject.Inject;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.slf4j.Logger;

import com.google.common.base.Throwables;


public class SequenceServiceJdbiImpl implements SequenceService, LifecycleRegistration {
  public static final long DEFAULT_INCREMENT_BLOCK_SIZE = 100000L;

  private final Logger log = LogTranslation.getLogger(getClass());

  protected final Map<String, Counter> counters = new ConcurrentHashMap<String, Counter>();
  protected final Map<String, Integer> typeCodes = new ConcurrentHashMap<String, Integer>();
  protected final Map<Integer, String> typeNames = new ConcurrentHashMap<Integer, String>();
  protected final SequenceHelper sequenceHelper;
  protected final SqlTypeHelper typeHelper;
  protected final AvailabilityManager availabilityManager;
  protected final LockManager lockManager;
  protected final IDBI dataSource;
  protected final long incrementBlockSize;

  @Inject
  public SequenceServiceJdbiImpl(SequenceServiceConfiguration sequenceConfiguration,
      AvailabilityManager availabilityManager, LockManager lockManager,
      SequenceHelper sequenceHelper, IDBI dataSource, SqlTypeHelper typeHelper) {
    this(sequenceHelper, availabilityManager, lockManager, dataSource, typeHelper,
        sequenceConfiguration.getGroupName(), sequenceConfiguration.getStoreName(),
        sequenceConfiguration.getIncrementBlockSize());
  }

  public SequenceServiceJdbiImpl(SequenceHelper sequenceHelper,
      AvailabilityManager availabilityManager, LockManager lockManager, IDBI dataSource,
      SqlTypeHelper typeHelper, String groupName, String storeName, Long incrementBlockSize) {
    this.sequenceHelper = sequenceHelper;
    this.availabilityManager = availabilityManager;
    this.lockManager = lockManager;
    this.dataSource = dataSource;
    this.typeHelper = typeHelper;
    this.incrementBlockSize =
        incrementBlockSize != null ? incrementBlockSize : DEFAULT_INCREMENT_BLOCK_SIZE;
  }

  @Inject
  public void register(Lifecycle lifecycle) {
    lifecycle.register(new LifecycleSupportBase() {
      @Override
      public void init() {
        SequenceServiceJdbiImpl.this.initialize();
      }

      @Override
      public void shutdown() {
        SequenceServiceJdbiImpl.this.shutdown();
      }
    });
  }

  public void initialize() {
    log.debug("Initializing Sequence Service {}", this);

    availabilityManager.setAvailable(false);

    try (LockManager toRelease = lockManager.acquire()) {
      dataSource.inTransaction(new TransactionCallback<Void>() {
        @Override
        public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
          JDBIHelper.getBoundStatement(handle, sequenceHelper.getDbPrefix(),
              "key_types_table_name", sequenceHelper.getKeyTypesTableName(),
              "seq_types_create_table").execute();

          JDBIHelper.getBoundStatement(handle, sequenceHelper.getDbPrefix(), "sequence_table_name",
              sequenceHelper.getSequenceTableName(), "seq_seq_create_table").execute();

          try {
            JDBIHelper.getBoundStatement(handle, sequenceHelper.getDbPrefix(),
                "key_types_table_name", sequenceHelper.getKeyTypesTableName(), "seq_types_init")
                .execute();
          } catch (Throwable t) {
            if (!typeHelper.isDuplicateKeyException(t)) {
              throw Throwables.propagate(t);
            }
          }

          try {
            JDBIHelper.getBoundStatement(handle, sequenceHelper.getDbPrefix(),
                "sequence_table_name", sequenceHelper.getSequenceTableName(), "seq_seq_init")
                .execute();
          } catch (Throwable t) {
            if (!typeHelper.isDuplicateKeyException(t)) {
              throw Throwables.propagate(t);
            }
          }

          return null;
        }
      });
    }

    availabilityManager.setAvailable(true);
    log.debug("Initialized Sequence Service {}", this);
  }

  public void shutdown() {
    log.debug("Shutting down Sequence Service {}", this);

    availabilityManager.assertAvailable();
    availabilityManager.setAvailable(false);

    try (LockManager toRelease = lockManager.acquire()) {
      dataSource.inTransaction(new TransactionCallback<Void>() {
        @Override
        public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
          for (Counter counter : SequenceServiceJdbiImpl.this.counters.values()) {
            sequenceHelper.setNextId(handle, counter.typeId,
                Long.valueOf(counter.base + counter.offset.get()));
          }

          return null;
        }
      });
    }

    log.debug("Shut down Sequence Service {}", this);
  }

  public void bumpKey(final String type, long id) throws Exception {
    try (LockManager toRelease = lockManager.acquire()) {
      Counter counter = this.counters.get(type);
      if (counter == null) {
        this.nextKey(type);
      }

      this.counters.get(type).bumpKey(id);
    }
  }

  public Key nextKey(final String type) throws KazukiException {
    if (type == null) {
      throw new IllegalArgumentException("Invalid entity 'type'");
    }

    try (LockManager toRelease = lockManager.acquire()) {
      Counter counter = counters.get(type);

      if (counter == null) {
        counter = createCounter(type);
        counters.put(type, counter);
      }

      Key nextKey = counter.getNext();

      if (nextKey == null) {
        counter = createCounter(type);
        counters.put(type, counter);

        nextKey = counter.getNext();
      }

      return nextKey;
    }
  }

  @Override
  public ResolvedKey resolveKey(Key key) throws KazukiException {
    try (LockManager toRelease = lockManager.acquire()) {
      Integer typeId = this.getTypeId(key.getTypePart(), false);

      if (typeId == null) {
        throw new IllegalArgumentException("Invalid entity 'type'");
      }

      KeyImpl keyImpl = (KeyImpl) key;

      return new ResolvedKeyImpl(typeId, 0L, keyImpl.getInternalId());
    }
  }

  @Override
  public Key unresolveKey(ResolvedKey key) throws KazukiException {
    try (LockManager toRelease = lockManager.acquire()) {
      return KeyImpl.createInternal(this.getTypeName(key.getTypeTag()), key.getIdentifierLo());
    }
  }

  @Nullable
  public Key peekKey(final String type) throws KazukiException {
    try (LockManager toRelease = lockManager.acquire()) {
      Counter counter = counters.get(type);

      if (counter == null) {
        return null;
      }

      return counter.peekNext();
    }
  }

  public Integer getTypeId(final String type, final boolean create) throws KazukiException {
    if (type == null) {
      throw new IllegalArgumentException("Invalid entity 'type'");
    }

    try (LockManager toRelease = lockManager.acquire()) {
      if (typeCodes.containsKey(type)) {
        return typeCodes.get(type);
      }

      availabilityManager.assertAvailable();

      Integer result = dataSource.inTransaction(new TransactionCallback<Integer>() {
        @Override
        public Integer inTransaction(Handle handle, TransactionStatus status) throws Exception {
          return sequenceHelper.validateType(handle, typeCodes, typeNames, type, create);
        }
      });

      if (result == null) {
        throw new KazukiException("unknown type: " + type);
      }

      return result;
    }
  }

  public String getTypeName(final Integer id) throws KazukiException {
    if (typeNames.containsKey(id)) {
      return typeNames.get(id);
    }

    availabilityManager.assertAvailable();

    try (LockManager toRelease = lockManager.acquire()) {
      return dataSource.inTransaction(new TransactionCallback<String>() {
        @Override
        public String inTransaction(Handle handle, TransactionStatus status) throws Exception {
          try {
            return sequenceHelper.getTypeName(handle, typeNames, id);
          } catch (KazukiException e) {
            return null;
          }
        }
      });
    }
  }

  @Override
  public Key parseKey(String keyString) throws KazukiException {
    return KeyImpl.valueOf(keyString);
  }

  @Override
  public Version parseVersion(String versionString) throws KazukiException {
    return VersionImpl.valueOf(versionString);
  }

  public void clear(final boolean preserveTypes, final boolean preserveCounters) {
    log.debug("Clearing SequenceService {}", this);

    availabilityManager.doProtected(new ProtectedCommand<Void>() {
      @Override
      public Void execute(Releasable resource) throws Exception {
        try {
          try (LockManager toRelease = lockManager.acquire()) {
            if (!preserveTypes) {
              SequenceServiceJdbiImpl.this.typeCodes.clear();
              SequenceServiceJdbiImpl.this.typeNames.clear();
            }

            if (!preserveCounters) {
              SequenceServiceJdbiImpl.this.counters.clear();
            }

            dataSource.inTransaction(new TransactionCallback<Void>() {
              @Override
              public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
                if (!preserveCounters) {
                  log.debug("Truncating SequenceService {} table {}", this,
                      sequenceHelper.getSequenceTableName());

                  JDBIHelper.getBoundStatement(handle, sequenceHelper.getDbPrefix(),
                      "sequence_table_name", sequenceHelper.getSequenceTableName(),
                      "seq_seq_truncate").execute();
                }

                if (!preserveTypes) {
                  log.debug("Truncating SequenceService {} table {}", this,
                      sequenceHelper.getKeyTypesTableName());

                  JDBIHelper.getBoundStatement(handle, sequenceHelper.getDbPrefix(),
                      "key_types_table_name", sequenceHelper.getKeyTypesTableName(),
                      "seq_types_truncate").execute();
                }

                return null;
              }
            });

            SequenceServiceJdbiImpl.this.initialize();

            return null;
          }
        } finally {
          resource.release();
        }
      }
    });

    log.debug("Cleared SequenceService {}", this);
  }

  @Override
  public void resetCounter(final String type) throws KazukiException {
    try (LockManager toRelease = lockManager.acquire()) {
      final Integer typeId = SequenceServiceJdbiImpl.this.getTypeId(type, false);

      dataSource.inTransaction(new TransactionCallback<Void>() {
        @Override
        public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
          SequenceServiceJdbiImpl.this.sequenceHelper.setNextId(handle, typeId, 0L);
          SequenceServiceJdbiImpl.this.counters.remove(type);

          return null;
        }
      });
    }
  }

  public Map<String, Counter> getCurrentCounters() {
    return Collections.unmodifiableMap(counters);
  }

  private Counter createCounter(final String type) {
    try (LockManager toRelease = lockManager.acquire()) {
      final int typeId = this.dataSource.inTransaction(new TransactionCallback<Integer>() {
        @Override
        public Integer inTransaction(Handle handle, TransactionStatus status) throws Exception {
          return sequenceHelper.validateType(handle, typeCodes, typeNames, type, true);
        }
      });

      long nextBase = this.dataSource.inTransaction(new TransactionCallback<Long>() {
        @Override
        public Long inTransaction(Handle handle, TransactionStatus status) throws Exception {
          return sequenceHelper.getNextId(handle, typeId, incrementBlockSize);
        }
      });

      return new Counter(typeId, type, nextBase, nextBase + incrementBlockSize);
    }
  }

  public class Counter {
    private final int typeId;
    private final String type;
    private final long base;
    private final long max;
    private final AtomicLong offset = new AtomicLong();

    public Counter(int typeId, String type, long base, long max) {
      this.typeId = typeId;
      this.type = type;
      this.base = base;
      this.max = max;
    }

    public void bumpKey(long id) throws KazukiException {
      long wouldBe = base + offset.get();
      long diff = id - wouldBe;

      if (diff <= 0) {
        return;
      }

      if (id >= max) {
        throw new IllegalStateException("cannot move counter from " + wouldBe
            + " to desired position " + id + " past " + max);
      }

      this.offset.addAndGet(diff);
    }

    @Nullable
    public Key getNext() throws KazukiException {
      long next = base + offset.incrementAndGet();

      if (next <= max) {
        return KeyImpl.createInternal(type, next);
      }

      return null;
    }

    @Nullable
    public Key peekNext() throws KazukiException {
      long next = base + offset.get() + 1L;

      if (next <= max) {
        return KeyImpl.createInternal(type, next);
      }

      return null;
    }

    @Override
    public String toString() {
      return "Counter[type=" + type + ",base=" + base + ",offset=" + offset.get() + ",max=" + max
          + "]";
    }
  }
}
