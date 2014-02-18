package io.kazuki.v0.store.sequence;

import io.kazuki.v0.internal.availability.AvailabilityManager;
import io.kazuki.v0.internal.helper.H2TypeHelper;
import io.kazuki.v0.internal.helper.SqlTypeHelper;
import io.kazuki.v0.store.config.ConfigurationProvider;
import io.kazuki.v0.store.jdbi.IdbiProvider;
import io.kazuki.v0.store.lifecycle.Lifecycle;

import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.skife.jdbi.v2.IDBI;

import com.google.common.base.Preconditions;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.jolbox.bonecp.BoneCPDataSource;

public class H2SequenceServiceModule extends PrivateModule {
  private final String name;
  private final String propertiesPath;
  private final AtomicReference<SequenceServiceConfiguration> config;

  public H2SequenceServiceModule(String name, @Nullable String propertiesPath) {
    Preconditions.checkNotNull(name, "name");

    this.name = name;
    this.propertiesPath = propertiesPath;
    this.config = new AtomicReference<SequenceServiceConfiguration>();
  }

  public H2SequenceServiceModule withConfiguration(SequenceServiceConfiguration config) {
    this.config.set(config);

    return this;
  }

  @Override
  protected void configure() {
    bind(Lifecycle.class).to(Key.get(Lifecycle.class, Names.named(name))).in(Scopes.SINGLETON);

    Provider<BoneCPDataSource> provider =
        binder().getProvider(Key.get(BoneCPDataSource.class, Names.named(name)));

    bind(IDBI.class).toProvider(new IdbiProvider(SequenceService.class, provider)).in(
        Scopes.SINGLETON);
    bind(SqlTypeHelper.class).to(H2TypeHelper.class).in(Scopes.SINGLETON);

    SequenceServiceConfiguration theConfig = config.get();

    if (theConfig != null) {
      bind(SequenceServiceConfiguration.class).toInstance(theConfig);
    } else if (propertiesPath != null) {
      bind(SequenceServiceConfiguration.class).toProvider(
          new ConfigurationProvider<SequenceServiceConfiguration>(name,
              SequenceServiceConfiguration.class, propertiesPath, true));
    } else {
      bind(SequenceServiceConfiguration.class).to(
          Key.get(SequenceServiceConfiguration.class, Names.named(name)));
    }

    bind(SequenceHelper.class).in(Scopes.SINGLETON);
    bind(AvailabilityManager.class).in(Scopes.SINGLETON);

    // AvailabilityManager needs to be exposed in this private module
    expose(AvailabilityManager.class);

    bind(SequenceService.class).annotatedWith(Names.named(name)).to(SequenceServiceJdbiImpl.class)
        .in(Scopes.SINGLETON);

    expose(SequenceService.class).annotatedWith(Names.named(name));
  }
}
