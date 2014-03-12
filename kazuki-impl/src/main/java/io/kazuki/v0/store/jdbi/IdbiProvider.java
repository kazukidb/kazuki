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
package io.kazuki.v0.store.jdbi;

import io.kazuki.v0.internal.helper.JDBIHelper;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.skife.jdbi.v2.IDBI;

import com.google.inject.Provider;

public class IdbiProvider implements Provider<IDBI> {
  private final Class<?> clazz;
  private final Provider<DataSource> dataSourceProvider;

  @Inject
  public IdbiProvider(Class<?> clazz, Provider<DataSource> dataSourceProvider) {
    this.clazz = clazz;
    this.dataSourceProvider = dataSourceProvider;
  }

  @Override
  public IDBI get() {
    return JDBIHelper.getDBI(clazz, dataSourceProvider.get());
  }
}
