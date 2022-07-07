package io.tidb.bigdata.flink.connector.table;

import static org.apache.flink.table.api.DataTypes.ROW;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.tikv.common.expression.Expression;

public class JdbcSourceBuilder {
  private final ResolvedCatalogTable table;
  private final ScanContext scanContext;
  private final int[] projectedFields;
  private final Expression expression;
  private final Integer limit;

  public JdbcSourceBuilder(
      ResolvedCatalogTable table,
      ScanContext scanContext,
      int[] projectedFields,
      Expression expression,
      Integer limit) {
    this.table = table;
    this.scanContext = scanContext;
    this.projectedFields = projectedFields;
    this.expression = expression;
    this.limit = limit;
  }

  public JdbcRowDataInputFormat build() {
    JdbcOptions jdbcOptions = JdbcUtils.getJdbcOptions(table.getOptions());
    final JdbcRowDataInputFormat.Builder builder =
        JdbcRowDataInputFormat.builder()
            .setDrivername(jdbcOptions.getDriverName())
            .setDBUrl(jdbcOptions.getDbURL())
            .setUsername(jdbcOptions.getUsername().orElse(null))
            .setPassword(jdbcOptions.getPassword().orElse(null))
            .setAutoCommit(true);
    // 流式读取
    builder.setFetchSize(Integer.MIN_VALUE);
    final JdbcDialect dialect = jdbcOptions.getDialect();

    ResolvedSchema schema = table.getResolvedSchema();
    List<DataType> dataTypes =
        schema.getColumns().stream()
            .filter(Column::isPhysical)
            .map(Column::getDataType)
            .collect(Collectors.toList());
    ;
    String query =
        dialect.getSelectFromStatement(
            jdbcOptions.getTableName(),
            schema.getColumns().stream()
                .filter(Column::isPhysical)
                .map(Column::getName)
                .toArray(String[]::new),
            new String[0]);

    builder.setQuery(query);

    final RowType rowType =
        (RowType) ROW(dataTypes.toArray(new DataType[0])).notNull().getLogicalType();
    builder.setRowConverter(dialect.getRowConverter(rowType));
    builder.setRowDataTypeInfo(
        scanContext.createTypeInformation(table.getResolvedSchema().toPhysicalRowDataType()));
    return builder.build();
  }
}
