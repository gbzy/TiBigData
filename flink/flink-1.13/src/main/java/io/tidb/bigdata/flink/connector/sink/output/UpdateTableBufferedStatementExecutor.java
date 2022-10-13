package io.tidb.bigdata.flink.connector.sink.output;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class UpdateTableBufferedStatementExecutor implements JdbcBatchStatementExecutor<RowData> {

  private final JdbcBatchStatementExecutor<RowData> statementExecutor;
  private final Function<RowData, RowData> valueTransform;
  private final List<RowData> buffer = new ArrayList<>();

  public UpdateTableBufferedStatementExecutor(
      JdbcBatchStatementExecutor<RowData> statementExecutor,
      Function<RowData, RowData> valueTransform) {
    this.statementExecutor = statementExecutor;
    this.valueTransform = valueTransform;
  }

  @Override
  public void prepareStatements(Connection connection) throws SQLException {
    statementExecutor.prepareStatements(connection);
  }

  @Override
  public void addToBatch(RowData record) throws SQLException {
    if (!(record.getRowKind() == RowKind.DELETE || record.getRowKind() == RowKind.UPDATE_BEFORE)) {
      // copy or not
      RowData value = valueTransform.apply(record);
      buffer.add(value);
    }
  }

  @Override
  public void executeBatch() throws SQLException {
    for (RowData value : buffer) {
      statementExecutor.addToBatch(value);
    }
    statementExecutor.executeBatch();
    buffer.clear();
  }

  @Override
  public void closeStatements() throws SQLException {
    statementExecutor.closeStatements();
  }
}
