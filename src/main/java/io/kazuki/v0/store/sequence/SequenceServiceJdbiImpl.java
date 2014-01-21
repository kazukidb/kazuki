package io.kazuki.v0.store.sequence;

import io.kazuki.v0.internal.availability.AvailabilityManager;
import io.kazuki.v0.internal.availability.AvailabilityManager.ProtectedCommand;
import io.kazuki.v0.internal.availability.Releasable;
import io.kazuki.v0.internal.helper.JDBIHelper;
import io.kazuki.v0.store.KazukiException;
import io.kazuki.v0.store.Key;
import io.kazuki.v0.store.lifecycle.Lifecycle;
import io.kazuki.v0.store.lifecycle.LifecycleRegistration;
import io.kazuki.v0.store.lifecycle.LifecycleSupportBase;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.inject.Inject;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SequenceServiceJdbiImpl implements SequenceService, LifecycleRegistration {
  public static final long DEFAULT_INCREMENT_BLOCK_SIZE = 100000L;

  private final Logger log = LoggerFactory.getLogger(getClass());

  protected final Map<String, Counter> counters = new ConcurrentHashMap<String, Counter>();
  protected final Map<String, Integer> typeCodes = new ConcurrentHashMap<String, Integer>();
  protected final Map<Integer, String> typeNames = new ConcurrentHashMap<Integer, String>();
  protected final SequenceHelper sequenceHelper;
  protected final AvailabilityManager availabilityManager;
  protected final IDBI dataSource;
  protected final long incrementBlockSize;

  @Inject
  public SequenceServiceJdbiImpl(SequenceServiceConfiguration sequenceConfiguration,
      AvailabilityManager availabilityManager, SequenceHelper sequenceHelper, IDBI dataSource) {
    this(sequenceHelper, availabilityManager, dataSource, sequenceConfiguration.getGroupName(),
        sequenceConfiguration.getStoreName(), sequenceConfiguration.getIncrementBlockSize());
  }

  public SequenceServiceJdbiImpl(SequenceHelper sequenceHelper,
      AvailabilityManager availabilityManager, IDBI dataSource, String groupName, String storeName,
      Long incrementBlockSize) {
    this.sequenceHelper = sequenceHelper;
    this.availabilityManager = availabilityManager;
    this.dataSource = dataSource;
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

  public synchronized void initialize() {
    log.info("Initializing Sequence Service {}", this);

    availabilityManager.setAvailable(false);

    dataSource.withHandle(new HandleCallback<Void>() {
      @Override
      public Void withHandle(Handle handle) throws Exception {
        JDBIHelper.getBoundStatement(handle, sequenceHelper.getDbPrefix(), "key_types_table_name",
            sequenceHelper.getKeyTypesTableName(), "seq_types_create_table").execute();
        JDBIHelper.getBoundStatement(handle, sequenceHelper.getDbPrefix(), "key_types_table_name",
            sequenceHelper.getKeyTypesTableName(), "seq_types_init").execute();

        JDBIHelper.getBoundStatement(handle, sequenceHelper.getDbPrefix(), "sequence_table_name",
            sequenceHelper.getSequenceTableName(), "seq_seq_create_table").execute();
        JDBIHelper.getBoundStatement(handle, sequenceHelper.getDbPrefix(), "sequence_table_name",
            sequenceHelper.getSequenceTableName(), "seq_seq_init").execute();

        return null;
      }
    });

    availabilityManager.setAvailable(true);
    log.debug("Initialized Sequence Service {}", this);
  }

  public synchronized void shutdown() {
    log.info("Shutting down Sequence Service {}", this);

    availabilityManager.assertAvailable();
    availabilityManager.setAvailable(false);

    dataSource.withHandle(new HandleCallback<Void>() {
      @Override
      public Void withHandle(Handle handle) throws Exception {
        for (Counter counter : SequenceServiceJdbiImpl.this.counters.values()) {
          sequenceHelper.setNextId(handle, counter.typeId,
              Long.valueOf(counter.base + counter.offset.get()));
        }

        return null;
      }
    });

    log.debug("Initialized Sequence Service {}", this);
  }

  public synchronized void bumpKey(final String type, long id) throws Exception {
    Counter counter = this.counters.get(type);
    if (counter == null) {
      this.nextKey(type);
    }

    this.counters.get(type).bumpKey(id);
  }

  public synchronized Key nextKey(final String type) throws KazukiException {
    if (type == null) {
      throw new IllegalArgumentException("Invalid entity 'type'");
    }

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

  @Nullable
  public Key peekKey(final String type) throws KazukiException {
    Counter counter = counters.get(type);

    if (counter == null) {
      return null;
    }

    return counter.peekNext();
  }

  public Integer getTypeId(final String type, final boolean create) throws KazukiException {
    return getTypeId(type, create, true);
  }

  public Integer getTypeId(final String type, final boolean create, final boolean strict)
      throws KazukiException {
    if (type == null) {
      throw new IllegalArgumentException("Invalid entity 'type'");
    }

    if (typeCodes.containsKey(type)) {
      return typeCodes.get(type);
    }

    availabilityManager.assertAvailable();

    Integer result = dataSource.inTransaction(new TransactionCallback<Integer>() {
      @Override
      public Integer inTransaction(Handle handle, TransactionStatus status) throws Exception {
        try {
          return sequenceHelper.validateType(handle, typeCodes, typeNames, type, create);
        } catch (KazukiException e) {
          return null;
        }
      }
    });

    if (result == null && strict) {
      throw new IllegalArgumentException("Invalid entity 'type'");
    }

    return result;
  }

  public String getTypeName(final Integer id) throws KazukiException {
    if (typeNames.containsKey(id)) {
      return typeNames.get(id);
    }

    availabilityManager.assertAvailable();

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

  public synchronized void clear(final boolean preserveTypes, final boolean preserveCounters) {
    log.info("Clearing SequenceService {}", this);

    availabilityManager.doProtected(new ProtectedCommand<Void>() {
      @Override
      public Void execute(Releasable resource) throws Exception {
        try {
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
        } finally {
          resource.release();
        }
      }
    });

    log.info("Cleared SequenceService {}", this);
  }

  public void reload() {
    dataSource.inTransaction(new TransactionCallback<Void>() {
      @Override
      public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
        try {
          List<Map<String, Object>> currentLimits = getCountersFromDatabase(handle);

          List<Map<String, Object>> currentOffsets = Collections.emptyList();
          // JDBIHelper.getBoundQuery(handle, sequenceHelper.getDbPrefix(),
          // sequenceHelper.getSequenceTableName(), "select_max_key_ids").list();

          Map<String, Counter> theCounters = computeCounters(currentLimits, currentOffsets);

          SequenceServiceJdbiImpl.this.counters.putAll(theCounters);

          getCurrentCounters();

          return null;
        } catch (KazukiException e) {
          e.printStackTrace();
          return null;
        }
      }
    });
  }

  public List<Map<String, Object>> getCountersFromDatabase(Handle handle) {
    return JDBIHelper.getBoundQuery(handle, sequenceHelper.getDbPrefix(), "sequence_table_name",
        sequenceHelper.getSequenceTableName(), "seq_seq_list").list();
  }

  public Map<String, Counter> getCurrentCounters() {
    return Collections.unmodifiableMap(counters);
  }

  private Counter createCounter(final String type) {
    final int typeId = this.dataSource.withHandle(new HandleCallback<Integer>() {
      @Override
      public Integer withHandle(Handle handle) throws Exception {
        return sequenceHelper.validateType(handle, typeCodes, typeNames, type, true);
      }
    });

    long nextBase = this.dataSource.withHandle(new HandleCallback<Long>() {
      @Override
      public Long withHandle(Handle handle) throws Exception {
        return sequenceHelper.getNextId(handle, typeId, incrementBlockSize);
      }
    });

    return new Counter(typeId, type, nextBase, nextBase + incrementBlockSize);
  }

  private Map<String, Counter> computeCounters(List<Map<String, Object>> currentLimits,
      List<Map<String, Object>> currentOffsets) throws Exception {
    Map<Long, Long> lims = convert(currentLimits);
    Map<Long, Long> offs = convert(currentOffsets);

    Map<String, Counter> toReturn = new LinkedHashMap<String, Counter>();

    for (Map.Entry<Long, Long> entry : offs.entrySet()) {
      int typeId = entry.getKey().intValue();
      String typeName = this.getTypeName(typeId);
      Long limit = lims.get(entry.getKey());
      Long base = limit - DEFAULT_INCREMENT_BLOCK_SIZE;
      Long offset = offs.get(entry.getKey());

      if ("$schema".equals(typeName)) {
        base = Long.valueOf(0);
        offset += 1;
      }

      Counter theCount = new Counter(typeId, typeName, base, limit);
      theCount.bumpKey(offset);

      toReturn.put(typeName, theCount);
    }

    return toReturn;
  }

  private static Map<Long, Long> convert(List<Map<String, Object>> inputList) {
    Map<Long, Long> toReturn = new LinkedHashMap<Long, Long>();

    for (Map<String, Object> input : inputList) {
      Long type = null;
      Long value = null;

      for (Map.Entry<String, Object> entry : input.entrySet()) {
        if (entry.getKey().equals("_key_type")) {
          type = Long.parseLong(entry.getValue().toString());
        } else {
          value = Long.parseLong(entry.getValue().toString());
        }
      }

      toReturn.put(type, value);
    }

    return toReturn;
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

      if (next < max) {
        return new Key(type, next);
      }

      return null;
    }

    @Nullable
    public Key peekNext() throws KazukiException {
      long next = base + offset.get() + 1L;

      if (next < max) {
        return new Key(type, next);
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
