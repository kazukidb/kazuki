package io.kazuki.v0.store.easy;

import io.kazuki.v0.store.jdbi.JdbiDataSourceModule;
import io.kazuki.v0.store.journal.SimpleJournalStoreModule;
import io.kazuki.v0.store.sequence.H2SequenceServiceModule;

import com.google.inject.AbstractModule;

public class EasyJournalStoreModule extends AbstractModule {
  private final String name;
  private final String propertiesBase;

  public EasyJournalStoreModule(String name, String propertiesBase) {
    this.name = name;
    this.propertiesBase = propertiesBase;
  }

  @Override
  protected void configure() {
    install(new JdbiDataSourceModule(name, propertiesBase + "/jdbi.properties"));
    install(new H2SequenceServiceModule(name, propertiesBase + "/sequence.properties"));
    install(new SimpleJournalStoreModule(name, propertiesBase + "/keyvalue.properties"));
  }
}
