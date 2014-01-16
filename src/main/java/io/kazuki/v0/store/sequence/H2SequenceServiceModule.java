package io.kazuki.v0.store.sequence;

import io.kazuki.v0.internal.availability.AvailabilityManager;
import io.kazuki.v0.store.config.ConfigurationProvider;
import io.kazuki.v0.store.jdbi.IdbiProvider;

import javax.annotation.Nullable;

import org.skife.jdbi.v2.IDBI;

import com.google.common.base.Preconditions;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.name.Names;
import com.jolbox.bonecp.BoneCPDataSource;

public class H2SequenceServiceModule extends PrivateModule {
  private final String name;
  private final String propertiesPath;

  public H2SequenceServiceModule(String name, @Nullable String propertiesPath) {
    Preconditions.checkNotNull(name, "name must not be null");

    this.name = name;
    this.propertiesPath = propertiesPath;
  }

  @Override
  protected void configure() {
    Provider<BoneCPDataSource> provider =
        binder().getProvider(Key.get(BoneCPDataSource.class, Names.named(name)));

    bind(IDBI.class).toProvider(new IdbiProvider(SequenceService.class, provider))
        .asEagerSingleton();

    bind(SequenceServiceConfiguration.class).toProvider(
        new ConfigurationProvider<SequenceServiceConfiguration>(name,
            SequenceServiceConfiguration.class, propertiesPath, true)).asEagerSingleton();

    bind(SequenceHelper.class).asEagerSingleton();
    bind(AvailabilityManager.class).asEagerSingleton();

    bind(SequenceService.class).annotatedWith(Names.named(name)).to(SequenceServiceJdbiImpl.class)
        .asEagerSingleton();

    expose(SequenceService.class).annotatedWith(Names.named(name));
  }
}
