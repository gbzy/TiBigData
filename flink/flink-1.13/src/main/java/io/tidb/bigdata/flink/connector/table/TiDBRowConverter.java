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

import static org.apache.flink.util.Preconditions.checkNotNull;

import io.vertx.jdbcclient.SqlOutParam;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

/**
 * @author ly
 */
public class TiDBRowConverter implements Serializable {

  protected final RowType rowType;
  protected final LogicalType[] fieldTypes;


  public TiDBRowConverter(RowType rowType) {
    this.rowType = checkNotNull(rowType);
    this.fieldTypes =
        rowType.getFields().stream()
            .map(RowType.RowField::getType)
            .toArray(LogicalType[]::new);
  }

  private Object internalConvert(LogicalType type, Row val, int pos) {
    if (val.get(Object.class, pos) == null) {
      return null;
    }
    switch (type.getTypeRoot()) {
      case NULL:
        return null;
      case BOOLEAN:
        return val.getBoolean(pos);
      case FLOAT:
        return val.getFloat(pos);
      case DOUBLE:
        return val.getDouble(pos);
      case INTERVAL_DAY_TIME:
      case BIGINT:
        return val.getLong(pos);
      case INTERVAL_YEAR_MONTH:
      case INTEGER:
        return val.getInteger(pos);
      case SMALLINT:
        return val.getShort(pos);
      case TINYINT:
        return val.getInteger(pos).byteValue();
      case DECIMAL:
        final int precision = ((DecimalType) type).getPrecision();
        final int scale = ((DecimalType) type).getScale();
        return DecimalData.fromBigDecimal(val.getBigDecimal(pos), precision, scale);
      case DATE:
        return (int) val.getLocalDate(pos).toEpochDay();
      case TIME_WITHOUT_TIME_ZONE:
        return (int) (val.getLocalDateTime(pos).toLocalTime().toNanoOfDay() / 1_000_000L);
      case TIMESTAMP_WITH_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return TimestampData.fromLocalDateTime(val.getLocalDateTime(pos));
      case CHAR:
      case VARCHAR:
        return StringData.fromString(val.getString(pos));
      case BINARY:
      case VARBINARY:
        return val.getString(pos).getBytes();
      case ARRAY:
      case ROW:
      case MAP:
      case MULTISET:
      case RAW:
      default:
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
  }

  public RowData toInternal(Row resultSet)  {
    GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
    for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
      genericRowData.setField(pos, internalConvert(rowType.getTypeAt(pos), resultSet, pos));
    }
    return genericRowData;
  }


  public Tuple toExternal(RowData rowData) {
    ArrayList<Object> res = new ArrayList<>();
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      res.add(externalConvert(rowType.getTypeAt(i), rowData, i));
    }
    return Tuple.from(res);
  }

  protected Object externalConvert(LogicalType type, RowData rowData, int pos) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
        return rowData.getBoolean(pos);
      case TINYINT:
        return rowData.getByte(pos);
      case SMALLINT:
        return rowData.getShort(pos);
      case INTEGER:
      case INTERVAL_YEAR_MONTH:
        return rowData.getInt(pos);
      case BIGINT:
      case INTERVAL_DAY_TIME:
        return rowData.getLong(pos);
      case FLOAT:
        return rowData.getFloat(pos);
      case DOUBLE:
        return rowData.getDouble(pos);
      case CHAR:
      case VARCHAR:
        // value is BinaryString
        return rowData.getString(pos).toString();
      case BINARY:
      case VARBINARY:
        return rowData.getBinary(pos);
      case DATE:
        return Date.valueOf(LocalDate.ofEpochDay(rowData.getInt(pos)));
      case TIME_WITHOUT_TIME_ZONE:
        return Time.valueOf(
            LocalTime.ofNanoOfDay(rowData.getInt(pos) * 1_000_000L));
      case TIMESTAMP_WITH_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        final int timestampPrecision = ((TimestampType) type).getPrecision();
        return rowData.getTimestamp(pos, timestampPrecision).toTimestamp();
      case DECIMAL:
        final int decimalPrecision = ((DecimalType) type).getPrecision();
        final int decimalScale = ((DecimalType) type).getScale();
        return rowData.getDecimal(pos, decimalPrecision, decimalScale)
            .toBigDecimal();
      case ARRAY:
      case MAP:
      case MULTISET:
      case ROW:
      case RAW:
      default:
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
  }
}
