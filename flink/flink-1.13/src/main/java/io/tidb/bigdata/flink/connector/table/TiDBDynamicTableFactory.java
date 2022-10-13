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

import static io.tidb.bigdata.flink.connector.table.TiDBOptions.DATABASE_NAME;
import static io.tidb.bigdata.flink.connector.table.TiDBOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static io.tidb.bigdata.flink.connector.table.TiDBOptions.SINK_MAX_RETRIES;
import static io.tidb.bigdata.flink.connector.table.TiDBOptions.STREAMING_SOURCE;
import static io.tidb.bigdata.flink.connector.table.TiDBOptions.UPDATE_COLUMNS;

import io.tidb.bigdata.flink.connector.table.AsyncLookupOptions.Builder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableColumn.MetadataColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

public class TiDBDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

  public static final String IDENTIFIER = "tidb";

  private static AsyncLookupOptions getAsyncJdbcOptions(ReadableConfig readableConfig) {
    Builder builder = AsyncLookupOptions.builder();
    readableConfig.getOptional(TiDBOptions.LOOKUP_ASYNC_MODE).ifPresent(builder::setAsync);
    readableConfig.getOptional(TiDBOptions.LOOKUP_MAX_POOL_SIZE).ifPresent(builder::setMaxPoolSize);
    return builder.build();
  }

  private static JdbcLookupOptions getJdbcLookupOptions(ReadableConfig readableConfig) {
    JdbcLookupOptions.Builder builder = JdbcLookupOptions.builder();
    builder.setCacheMaxSize(readableConfig.get(TiDBOptions.LOOKUP_CACHE_MAX_ROWS));
    builder.setMaxRetryTimes(readableConfig.get(TiDBOptions.LOOKUP_MAX_RETRIES));
    builder.setCacheExpireMs(readableConfig.get(TiDBOptions.LOOKUP_CACHE_TTL).toMillis());
    return builder.build();
  }

  private static JdbcExecutionOptions getExecJdbcOptions(ReadableConfig readableConfig) {
    JdbcExecutionOptions.Builder builder =
        JdbcExecutionOptions.builder()
            .withBatchIntervalMs(
                readableConfig.get(TiDBOptions.SINK_BUFFER_FLUSH_INTERVAL).toMillis())
            .withBatchSize(readableConfig.get(SINK_MAX_RETRIES))
            .withBatchSize(readableConfig.get(SINK_BUFFER_FLUSH_MAX_ROWS));
    return builder.build();
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return TiDBOptions.requiredOptions();
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    ReadableConfig config = helper.getOptions();
    helper.validate();
    return new TiDBDynamicTableSource(
        context.getCatalogTable(),
        config.getOptional(STREAMING_SOURCE).isPresent()
            ? ChangelogMode.all()
            : ChangelogMode.insertOnly(),
        config.get(TiDBOptions.JDBC_SOURCE_FLAG),
        getJdbcLookupOptions(config),
        getAsyncJdbcOptions(config));
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return TiDBOptions.withMoreOptionalOptions(
        TiDBOptions.LOOKUP_MAX_POOL_SIZE,
        UPDATE_COLUMNS,
        SINK_BUFFER_FLUSH_MAX_ROWS,
        TiDBOptions.LOOKUP_ASYNC_MODE,
        TiDBOptions.JDBC_SOURCE_FLAG,
        TiDBOptions.LOOKUP_CACHE_TTL,
        TiDBOptions.LOOKUP_CACHE_MAX_ROWS,
        TiDBOptions.LOOKUP_MAX_RETRIES);
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    // Metadata columns is not real columns, should not be created for sink.
    if (context.getCatalogTable().getSchema().getTableColumns().stream()
        .anyMatch(column -> column instanceof MetadataColumn)) {
      throw new IllegalStateException("Metadata columns is not supported for sink");
    }

    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    ReadableConfig config = helper.getOptions();
    TableSchema schema = context.getCatalogTable().getSchema();
    String databaseName = config.get(DATABASE_NAME);
    // jdbc options
    JdbcOptions jdbcOptions = JdbcUtils.getJdbcOptions(context.getCatalogTable().toProperties());
    // dml options
    TableSchema physicalSchema =
        TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

    String[] keyFields =
        schema.getPrimaryKey().map(pk -> pk.getColumns().toArray(new String[0])).orElse(null);
    JdbcDmlOptions jdbcDmlOptions =
        JdbcDmlOptions.builder()
            .withTableName(jdbcOptions.getTableName())
            .withDialect(jdbcOptions.getDialect())
            .withFieldNames(schema.getFieldNames())
            .withKeyFields(keyFields)
            .build();
    JdbcExecutionOptions jdbcExecutionOptions = getExecJdbcOptions(config);
    String updateColumnsOption = config.get(UPDATE_COLUMNS);
    if (updateColumnsOption == null) {
      return new JdbcDynamicTableSink(
          jdbcOptions, getExecJdbcOptions(config), jdbcDmlOptions, physicalSchema);
    } else {
      String[] updateColumnNames = updateColumnsOption.split("\\s*,\\s*");
      List<TableColumn> updateColumns = new ArrayList<>();
      int[] updateColumnIndexes =
          getUpdateColumnAndIndexes(
              schema, databaseName, jdbcOptions, updateColumnNames, updateColumns);
      return new InsertOnDuplicateUpdateSink(
          jdbcOptions,
          jdbcExecutionOptions,
          jdbcDmlOptions,
          schema,
          updateColumns,
          updateColumnIndexes);
    }
  }

  private int[] getUpdateColumnAndIndexes(
      TableSchema schema,
      String databaseName,
      JdbcOptions jdbcOptions,
      String[] updateColumnNames,
      List<TableColumn> updateColumns) {
    int[] index = new int[updateColumnNames.length];
    for (int i = 0; i < updateColumnNames.length; i++) {
      String updateColumnName = updateColumnNames[i];
      Optional<TableColumn> tableColumn = schema.getTableColumn(updateColumnName);
      if (!tableColumn.isPresent()) {
        throw new IllegalStateException(
            String.format(
                "Unknown updateColumn %s in table %s.%s",
                updateColumnName, databaseName, jdbcOptions.getTableName()));
      } else {
        updateColumns.add(tableColumn.get());
        index[i] = schema.getTableColumns().indexOf(tableColumn.get());
      }
    }
    return index;
  }
}
