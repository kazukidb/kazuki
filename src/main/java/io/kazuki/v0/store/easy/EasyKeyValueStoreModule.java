package io.kazuki.v0.store.easy;

import io.kazuki.v0.store.jdbi.JdbiDataSourceModule;
import io.kazuki.v0.store.keyvalue.KeyValueStoreJdbiH2Module;
import io.kazuki.v0.store.sequence.H2SequenceServiceModule;

import com.google.inject.AbstractModule;

public class EasyKeyValueStoreModule extends AbstractModule {
  private final String name;
  private final String propertiesBase;

  public EasyKeyValueStoreModule(String name, String propertiesBase) {
    this.name = name;
    this.propertiesBase = propertiesBase;
  }

  @Override
  protected void configure() {
    install(new JdbiDataSourceModule(name, propertiesBase + "/jdbi.properties"));
    install(new H2SequenceServiceModule(name, propertiesBase + "/sequence.properties"));
    install(new KeyValueStoreJdbiH2Module(name, propertiesBase + "/keyvalue.properties"));
  }
}
