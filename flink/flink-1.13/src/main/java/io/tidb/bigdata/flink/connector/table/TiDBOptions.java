/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.flink.connector.table;

import java.time.Duration;
import java.util.Set;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableSet;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class TiDBOptions {

  private static ConfigOption<String> required(String key) {
    return ConfigOptions.key(key).stringType().noDefaultValue();
  }

  private static ConfigOption<String> optional(String key, String value) {
    return ConfigOptions.key(key).stringType().defaultValue(value);
  }

  private static ConfigOption<Integer> optional(String key, int value) {
    return ConfigOptions.key(key).intType().defaultValue(value);
  }

  private static ConfigOption<String> optional(String key) {
    return optional(key, null);
  }

  public static final String OLD_MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";
  public static final String NEW_MYSQL_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";

  public static final ConfigOption<String> DATABASE_URL = required("tidb.database.url");

  public static final ConfigOption<String> USERNAME = required("tidb.username");

  public static final ConfigOption<String> PASSWORD = required("tidb.password");

  public static final ConfigOption<String> DATABASE_NAME = required("tidb.database.name");

  public static final ConfigOption<String> TABLE_NAME = required("tidb.table.name");

  public static final ConfigOption<Boolean> LOOKUP_ASYNC_MODE =
      ConfigOptions.key("tidb.lookup.async").booleanType().defaultValue(false);

  public static final ConfigOption<Integer> LOOKUP_MAX_POOL_SIZE =
      optional("tidb.lookup.max_pool_size", 4);

  // jdbc options
  public static final ConfigOption<Boolean> JDBC_SOURCE_FLAG =
      ConfigOptions.key("jdbc.source.flag").booleanType().defaultValue(false);

  public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
      optional("sink.buffer-flush.max-rows", 100);
  public static final ConfigOption<Integer> SINK_PARALLELISM =
      ConfigOptions.key("sink.parallelism").intType().noDefaultValue();
  public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
      ConfigOptions.key("sink.buffer-flush.interval")
          .durationType()
          .defaultValue(Duration.ofSeconds(1));
  public static final ConfigOption<Integer> SINK_MAX_RETRIES =
      ConfigOptions.key("sink.max-retries")
          .intType()
          .defaultValue(3)
          .withDescription("the max retry times if writing records to database failed.");
  public static final ConfigOption<String> UPDATE_COLUMNS =
      ConfigOptions.key("tidb.sink.update-columns")
          .stringType()
          .noDefaultValue()
          .withDescription("The columns to be updated");
  public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
      ConfigOptions.key("lookup.cache.max-rows")
          .longType()
          .defaultValue(-1L)
          .withDescription(
              "the max number of rows of lookup cache, over this value, "
                  + "the oldest rows will be eliminated. \"cache.max-rows\" and \"cache.ttl\" options "
                  + "must all be specified if any of them is specified. "
                  + "Cache is not enabled as default.");
  public static final ConfigOption<Duration> LOOKUP_CACHE_TTL =
      ConfigOptions.key("lookup.cache.ttl")
          .durationType()
          .defaultValue(Duration.ofSeconds(10))
          .withDescription("the cache time to live.");
  public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES =
      ConfigOptions.key("lookup.max-retries")
          .intType()
          .defaultValue(3)
          .withDescription("the max retry times if lookup database failed.");

  public static final ConfigOption<String> STREAMING_SOURCE = optional("tidb.streaming.source");

  public static final String STREAMING_SOURCE_KAFKA = "kafka";

  public static final Set<String> VALID_STREAMING_SOURCES = ImmutableSet.of(STREAMING_SOURCE_KAFKA);

  public static final ConfigOption<String> STREAMING_CODEC = optional("tidb.streaming.codec");

  public static final String STREAMING_CODEC_JSON = "json";
  public static final String STREAMING_CODEC_CRAFT = "craft";
  public static final Set<String> VALID_STREAMING_CODECS =
      ImmutableSet.of(STREAMING_CODEC_CRAFT, STREAMING_CODEC_JSON);

  public static Set<ConfigOption<?>> requiredOptions() {
    return withMoreRequiredOptions();
  }

  public static Set<ConfigOption<?>> withMoreRequiredOptions(ConfigOption<?>... options) {
    return ImmutableSet.<ConfigOption<?>>builder()
        .add(DATABASE_URL, DATABASE_NAME, TABLE_NAME, USERNAME)
        .add(options)
        .build();
  }

  public static Set<ConfigOption<?>> optionalOptions() {
    return withMoreOptionalOptions();
  }

  public static String determineDriverName() {
    try {
      Class.forName(NEW_MYSQL_DRIVER_NAME);
      return NEW_MYSQL_DRIVER_NAME;
    } catch (ClassNotFoundException e) {
      return OLD_MYSQL_DRIVER_NAME;
    }
  }

  public static Set<ConfigOption<?>> withMoreOptionalOptions(ConfigOption<?>... options) {
    return ImmutableSet.<ConfigOption<?>>builder()
        .add(PASSWORD, STREAMING_SOURCE)
        .add(options)
        .build();
  }
}
