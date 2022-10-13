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

import java.util.Collections;
import java.util.List;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBDynamicTableSource
    implements ScanTableSource, LookupTableSource, SupportsFilterPushDown, SupportsLimitPushDown {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBDynamicTableSource.class);

  private final ResolvedCatalogTable table;
  private final ChangelogMode changelogMode;
  private final LookupTableSourceHelper lookupTableSourceHelper;
  private int[] projectedFields;
  private Integer limit;
  private final Boolean jdbcSourceFlag;
  private ScanRuntimeProvider scanSource;

  public TiDBDynamicTableSource(
      ResolvedCatalogTable table,
      ChangelogMode changelogMode,
      Boolean jdbcSourceFlag,
      JdbcLookupOptions lookupOptions,
      AsyncLookupOptions asyncLookupOptions) {
    this(
        table,
        changelogMode,
        jdbcSourceFlag,
        new LookupTableSourceHelper(lookupOptions, asyncLookupOptions));
  }

  private TiDBDynamicTableSource(
      ResolvedCatalogTable table,
      ChangelogMode changelogMode,
      Boolean jdbcSourceFlag,
      LookupTableSourceHelper lookupTableSourceHelper) {
    this.table = table;
    this.changelogMode = changelogMode;
    this.jdbcSourceFlag = jdbcSourceFlag;
    this.lookupTableSourceHelper = lookupTableSourceHelper;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return changelogMode;
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    return InputFormatProvider.of(
        new JdbcSourceBuilder(table, scanContext, projectedFields, limit).build());
  }

  @Override
  public DynamicTableSource copy() {
    TiDBDynamicTableSource otherSource =
        new TiDBDynamicTableSource(table, changelogMode, jdbcSourceFlag, lookupTableSourceHelper);
    otherSource.projectedFields = this.projectedFields;
    return otherSource;
  }

  @Override
  public String asSummaryString() {
    return "";
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    return lookupTableSourceHelper.getLookupRuntimeProvider(table, context);
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> filters) {
    return Result.of(Collections.emptyList(), filters);
  }

  private String getRequiredProperties(String key) {
    return Preconditions.checkNotNull(table.getOptions().get(key), key + " can not be null");
  }

  @Override
  public void applyLimit(long limit) {
    this.limit = limit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) limit;
  }
}
