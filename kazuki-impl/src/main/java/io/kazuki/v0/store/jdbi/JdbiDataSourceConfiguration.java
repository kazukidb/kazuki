/**
 * Copyright 2014 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kazuki.v0.store.jdbi;

import io.kazuki.v0.store.config.ConfigurationBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class JdbiDataSourceConfiguration {
  private final String jdbcDriver;
  private final String jdbcUrl;
  private final String jdbcUser;
  private final String jdbcPassword;
  private final int poolMinConnections;
  private final int poolMaxConnections;

  public JdbiDataSourceConfiguration(@JsonProperty("jdbcDriver") String jdbcDriver,
      @JsonProperty("jdbcUrl") String jdbcUrl, @JsonProperty("jdbcUser") String jdbcUser,
      @JsonProperty("jdbcPassword") String jdbcPassword,
      @JsonProperty("poolMinConnections") int poolMinConnections,
      @JsonProperty("poolMaxConnections") int poolMaxConnections) {
    Preconditions.checkNotNull(jdbcDriver, "jdbcDriver");
    Preconditions.checkNotNull(jdbcUrl, "jdbcUrl");
    Preconditions.checkNotNull(jdbcUser, "jdbcUser");
    Preconditions.checkNotNull(jdbcPassword, "jdbcPassword");

    this.jdbcDriver = jdbcDriver;
    this.jdbcUrl = jdbcUrl;
    this.jdbcUser = jdbcUser;
    this.jdbcPassword = jdbcPassword;
    this.poolMinConnections = poolMinConnections;
    this.poolMaxConnections = poolMaxConnections;
  }

  public String getJdbcDriver() {
    return jdbcDriver;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public String getJdbcUser() {
    return jdbcUser;
  }

  public String getJdbcPassword() {
    return jdbcPassword;
  }

  public int getPoolMinConnections() {
    return poolMinConnections;
  }

  public int getPoolMaxConnections() {
    return poolMaxConnections;
  }

  public static class Builder implements ConfigurationBuilder<JdbiDataSourceConfiguration> {
    private String jdbcDriver;
    private String jdbcUrl;
    private String jdbcUser;
    private String jdbcPassword;
    private int poolMinConnections;
    private int poolMaxConnections;

    public Builder withJdbcDriver(String jdbcDriver) {
      this.jdbcDriver = jdbcDriver;

      return this;
    }

    public Builder withJdbcUrl(String jdbcUrl) {
      this.jdbcUrl = jdbcUrl;

      return this;
    }

    public Builder withJdbcUser(String jdbcUser) {
      this.jdbcUser = jdbcUser;

      return this;
    }

    public Builder withJdbcPassword(String jdbcPassword) {
      this.jdbcPassword = jdbcPassword;

      return this;
    }

    public Builder withPoolMinConnections(int poolMinConnections) {
      this.poolMinConnections = poolMinConnections;

      return this;
    }

    public Builder withPoolMaxConnections(int poolMaxConnections) {
      this.poolMaxConnections = poolMaxConnections;

      return this;
    }

    public JdbiDataSourceConfiguration build() {
      return new JdbiDataSourceConfiguration(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword,
          poolMinConnections, poolMaxConnections);
    }
  }
}
